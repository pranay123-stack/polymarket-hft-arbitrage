//! Opinion (prediction market) API client for cross-platform arbitrage.
//!
//! Opinion-style exchanges typically use a Betfair-like model with back/lay orders.
//! This module provides a unified interface for Opinion and similar exchanges.
//!
//! Key characteristics:
//! - Back (bet for) and Lay (bet against) order types
//! - Different liquidity model than CLOB exchanges
//! - Commission on net winnings

use crate::core::{
    error::{ArbitrageError, Result},
    types::{Order, OrderStatus, OrderType, Outcome, Position, Price, Quantity, Side, Trade},
};
use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Opinion API configuration
#[derive(Debug, Clone)]
pub struct OpinionConfig {
    pub base_url: String,
    pub api_key: String,
    pub api_secret: String,
    pub timeout_ms: u64,
    pub commission_rate: Decimal,  // e.g., 0.02 for 2%
}

impl Default for OpinionConfig {
    fn default() -> Self {
        Self {
            base_url: "https://api.opinion.exchange/v1".to_string(),
            api_key: String::new(),
            api_secret: String::new(),
            timeout_ms: 5000,
            commission_rate: Decimal::new(2, 2),  // 2%
        }
    }
}

/// Opinion order action (Back = bet for, Lay = bet against)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpinionAction {
    Back,  // Betting that outcome WILL happen (buy YES)
    Lay,   // Betting that outcome will NOT happen (sell YES / buy NO)
}

impl OpinionAction {
    /// Convert from Side and Outcome to Opinion action
    pub fn from_side_outcome(side: Side, outcome: Outcome) -> Self {
        match (side, outcome) {
            (Side::Buy, Outcome::Yes) => OpinionAction::Back,
            (Side::Sell, Outcome::Yes) => OpinionAction::Lay,
            (Side::Buy, Outcome::No) => OpinionAction::Lay,
            (Side::Sell, Outcome::No) => OpinionAction::Back,
        }
    }

    /// Convert to Side and Outcome
    pub fn to_side_outcome(&self) -> (Side, Outcome) {
        match self {
            OpinionAction::Back => (Side::Buy, Outcome::Yes),
            OpinionAction::Lay => (Side::Sell, Outcome::Yes),
        }
    }
}

/// Opinion market representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionMarket {
    pub id: String,
    pub event_id: String,
    pub name: String,
    pub description: Option<String>,
    pub status: String,
    pub back_price: Option<Decimal>,   // Best available back price
    pub lay_price: Option<Decimal>,    // Best available lay price
    pub back_size: Option<Decimal>,    // Size available at back price
    pub lay_size: Option<Decimal>,     // Size available at lay price
    pub matched_amount: Decimal,
    pub total_matched: Decimal,
    pub start_time: Option<DateTime<Utc>>,
    pub close_time: Option<DateTime<Utc>>,
    pub in_play: bool,
    pub tags: Vec<String>,
}

impl OpinionMarket {
    /// Check if market is active
    pub fn is_active(&self) -> bool {
        self.status == "OPEN" || self.status == "active"
    }

    /// Get the best back price (price to buy YES at)
    pub fn best_back_price(&self) -> Option<Price> {
        self.back_price.and_then(|p| Price::new(p))
    }

    /// Get the best lay price (price to sell YES at / buy NO at)
    pub fn best_lay_price(&self) -> Option<Price> {
        self.lay_price.and_then(|p| Price::new(p))
    }

    /// Calculate implied probability from back price
    pub fn back_implied_prob(&self) -> Option<Decimal> {
        self.back_price.map(|p| Decimal::ONE / p)
    }

    /// Calculate implied probability from lay price
    pub fn lay_implied_prob(&self) -> Option<Decimal> {
        self.lay_price.map(|p| Decimal::ONE / p)
    }
}

/// Opinion order representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionOrder {
    pub order_id: String,
    pub market_id: String,
    pub selection_id: String,
    pub action: OpinionAction,
    pub price: Decimal,
    pub size: Decimal,
    pub matched_size: Decimal,
    pub remaining_size: Decimal,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub client_ref: Option<String>,
}

impl OpinionOrder {
    /// Convert to internal OrderStatus
    pub fn to_order_status(&self) -> OrderStatus {
        match self.status.as_str() {
            "EXECUTABLE" | "PENDING" => OrderStatus::Live,
            "EXECUTION_COMPLETE" | "MATCHED" => OrderStatus::Matched,
            "CANCELLED" | "LAPSED" => OrderStatus::Cancelled,
            "PARTIALLY_MATCHED" => OrderStatus::PartiallyFilled,
            "EXPIRED" => OrderStatus::Expired,
            _ => OrderStatus::Rejected,
        }
    }

    /// Check if fully filled
    pub fn is_filled(&self) -> bool {
        self.remaining_size == Decimal::ZERO &&
        (self.status == "EXECUTION_COMPLETE" || self.status == "MATCHED")
    }

    /// Check if partially filled
    pub fn is_partial(&self) -> bool {
        self.matched_size > Decimal::ZERO && self.remaining_size > Decimal::ZERO
    }

    /// Get fill percentage
    pub fn fill_pct(&self) -> Decimal {
        if self.size > Decimal::ZERO {
            (self.matched_size / self.size) * Decimal::from(100)
        } else {
            Decimal::ZERO
        }
    }
}

/// Opinion orderbook level
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionOrderbookLevel {
    pub price: Decimal,
    pub size: Decimal,
}

/// Opinion orderbook
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionOrderbook {
    pub market_id: String,
    pub selection_id: String,
    pub available_to_back: Vec<OpinionOrderbookLevel>,  // Sorted by price desc
    pub available_to_lay: Vec<OpinionOrderbookLevel>,   // Sorted by price asc
    pub timestamp: DateTime<Utc>,
}

impl OpinionOrderbook {
    /// Best back price (highest price someone is willing to take your back bet)
    pub fn best_back(&self) -> Option<&OpinionOrderbookLevel> {
        self.available_to_back.first()
    }

    /// Best lay price (lowest price someone is willing to take your lay bet)
    pub fn best_lay(&self) -> Option<&OpinionOrderbookLevel> {
        self.available_to_lay.first()
    }

    /// Spread in decimal (e.g., lay - back)
    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_back(), self.best_lay()) {
            (Some(back), Some(lay)) => Some(lay.price - back.price),
            _ => None,
        }
    }

    /// Check if there's a crossed market (arbitrage on same exchange)
    pub fn is_crossed(&self) -> bool {
        match (self.best_back(), self.best_lay()) {
            (Some(back), Some(lay)) => back.price >= lay.price,
            _ => false,
        }
    }
}

/// Opinion balance/wallet
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionBalance {
    pub available: Decimal,
    pub exposure: Decimal,
    pub total: Decimal,
    pub currency: String,
}

/// Request to place an Opinion order
#[derive(Debug, Serialize)]
struct PlaceOrderRequest {
    market_id: String,
    selection_id: String,
    action: String,
    price: String,
    size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    persistence_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_ref: Option<String>,
}

/// Opinion API client
pub struct OpinionClient {
    config: OpinionConfig,
    client: Client,
    /// Cache of market data
    market_cache: Arc<RwLock<HashMap<String, OpinionMarket>>>,
    /// Cache of orderbooks
    orderbook_cache: Arc<RwLock<HashMap<String, OpinionOrderbook>>>,
}

impl OpinionClient {
    /// Create a new Opinion client
    pub fn new(config: OpinionConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .pool_max_idle_per_host(10)
            .build()
            .map_err(|e| ArbitrageError::Api(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            config,
            client,
            market_cache: Arc::new(RwLock::new(HashMap::new())),
            orderbook_cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Generate authentication headers
    fn auth_headers(&self) -> Vec<(String, String)> {
        let timestamp = Utc::now().timestamp_millis().to_string();

        // HMAC signature (simplified - real implementation would include proper signing)
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        type HmacSha256 = Hmac<Sha256>;

        let message = format!("{}{}", timestamp, self.config.api_key);
        let mut mac = HmacSha256::new_from_slice(self.config.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        vec![
            ("X-API-Key".to_string(), self.config.api_key.clone()),
            ("X-Timestamp".to_string(), timestamp),
            ("X-Signature".to_string(), signature),
        ]
    }

    /// Get market data
    pub async fn get_market(&self, market_id: &str) -> Result<OpinionMarket> {
        // Check cache first
        {
            let cache = self.market_cache.read().await;
            if let Some(market) = cache.get(market_id) {
                return Ok(market.clone());
            }
        }

        let url = format!("{}/markets/{}", self.config.base_url, market_id);

        let mut req = self.client.get(&url);
        for (key, value) in self.auth_headers() {
            req = req.header(&key, &value);
        }

        let response = req.send().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to get market: {}", e)))?;

        if !response.status().is_success() {
            return Err(ArbitrageError::Api(format!(
                "Failed to get market {}: {}",
                market_id,
                response.status()
            )));
        }

        let market: OpinionMarket = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse market: {}", e)))?;

        // Update cache
        {
            let mut cache = self.market_cache.write().await;
            cache.insert(market_id.to_string(), market.clone());
        }

        Ok(market)
    }

    /// Search markets by query
    pub async fn search_markets(&self, query: &str, tags: Option<&[&str]>) -> Result<Vec<OpinionMarket>> {
        let url = format!("{}/markets/search", self.config.base_url);

        let mut req = self.client.get(&url)
            .query(&[("q", query)]);

        if let Some(t) = tags {
            for tag in t {
                req = req.query(&[("tag", *tag)]);
            }
        }

        for (key, value) in self.auth_headers() {
            req = req.header(&key, &value);
        }

        let response = req.send().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to search markets: {}", e)))?;

        if !response.status().is_success() {
            return Err(ArbitrageError::Api(format!(
                "Failed to search markets: {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct SearchResponse {
            markets: Vec<OpinionMarket>,
        }

        let resp: SearchResponse = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse search results: {}", e)))?;

        Ok(resp.markets)
    }

    /// Get orderbook
    pub async fn get_orderbook(&self, market_id: &str, selection_id: &str) -> Result<OpinionOrderbook> {
        let cache_key = format!("{}:{}", market_id, selection_id);

        let url = format!(
            "{}/markets/{}/orderbook/{}",
            self.config.base_url, market_id, selection_id
        );

        let mut req = self.client.get(&url);
        for (key, value) in self.auth_headers() {
            req = req.header(&key, &value);
        }

        let response = req.send().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to get orderbook: {}", e)))?;

        if !response.status().is_success() {
            return Err(ArbitrageError::Api(format!(
                "Failed to get orderbook: {}",
                response.status()
            )));
        }

        let orderbook: OpinionOrderbook = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse orderbook: {}", e)))?;

        // Update cache
        {
            let mut cache = self.orderbook_cache.write().await;
            cache.insert(cache_key, orderbook.clone());
        }

        Ok(orderbook)
    }

    /// Place an order
    pub async fn place_order(
        &self,
        market_id: &str,
        selection_id: &str,
        action: OpinionAction,
        price: Price,
        size: Quantity,
    ) -> Result<OpinionOrder> {
        let url = format!("{}/orders", self.config.base_url);

        let request = PlaceOrderRequest {
            market_id: market_id.to_string(),
            selection_id: selection_id.to_string(),
            action: match action {
                OpinionAction::Back => "BACK".to_string(),
                OpinionAction::Lay => "LAY".to_string(),
            },
            price: price.as_decimal().to_string(),
            size: size.as_decimal().to_string(),
            persistence_type: Some("LAPSE".to_string()),  // Cancel at market close
            client_ref: Some(Uuid::new_v4().to_string()),
        };

        debug!("Placing Opinion order: {:?}", request);

        let mut req = self.client.post(&url).json(&request);
        for (key, value) in self.auth_headers() {
            req = req.header(&key, &value);
        }

        let response = req.send().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to place order: {}", e)))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            error!("Opinion order failed: {} - {}", status, body);
            return Err(ArbitrageError::OrderRejected(format!(
                "Opinion order rejected: {} - {}",
                status, body
            )));
        }

        let order: OpinionOrder = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse order response: {}", e)))?;

        info!(
            "Opinion order placed: {} {:?} @ {} size {}",
            order.order_id, order.action, order.price, order.size
        );

        Ok(order)
    }

    /// Get order status
    pub async fn get_order(&self, order_id: &str) -> Result<OpinionOrder> {
        let url = format!("{}/orders/{}", self.config.base_url, order_id);

        let mut req = self.client.get(&url);
        for (key, value) in self.auth_headers() {
            req = req.header(&key, &value);
        }

        let response = req.send().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to get order: {}", e)))?;

        if !response.status().is_success() {
            return Err(ArbitrageError::Api(format!(
                "Failed to get order {}: {}",
                order_id,
                response.status()
            )));
        }

        let order: OpinionOrder = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse order: {}", e)))?;

        Ok(order)
    }

    /// Cancel an order
    pub async fn cancel_order(&self, order_id: &str) -> Result<OpinionOrder> {
        let url = format!("{}/orders/{}/cancel", self.config.base_url, order_id);

        let mut req = self.client.post(&url);
        for (key, value) in self.auth_headers() {
            req = req.header(&key, &value);
        }

        let response = req.send().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to cancel order: {}", e)))?;

        if !response.status().is_success() {
            return Err(ArbitrageError::Api(format!(
                "Failed to cancel order {}: {}",
                order_id,
                response.status()
            )));
        }

        let order: OpinionOrder = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse cancel response: {}", e)))?;

        Ok(order)
    }

    /// Get balance
    pub async fn get_balance(&self) -> Result<OpinionBalance> {
        let url = format!("{}/account/balance", self.config.base_url);

        let mut req = self.client.get(&url);
        for (key, value) in self.auth_headers() {
            req = req.header(&key, &value);
        }

        let response = req.send().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to get balance: {}", e)))?;

        if !response.status().is_success() {
            return Err(ArbitrageError::Api(format!(
                "Failed to get balance: {}",
                response.status()
            )));
        }

        let balance: OpinionBalance = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse balance: {}", e)))?;

        Ok(balance)
    }

    /// Get open orders
    pub async fn get_open_orders(&self, market_id: Option<&str>) -> Result<Vec<OpinionOrder>> {
        let url = format!("{}/orders", self.config.base_url);

        let mut req = self.client.get(&url)
            .query(&[("status", "EXECUTABLE")]);

        if let Some(mid) = market_id {
            req = req.query(&[("market_id", mid)]);
        }

        for (key, value) in self.auth_headers() {
            req = req.header(&key, &value);
        }

        let response = req.send().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to get orders: {}", e)))?;

        if !response.status().is_success() {
            return Err(ArbitrageError::Api(format!(
                "Failed to get orders: {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct OrdersResponse {
            orders: Vec<OpinionOrder>,
        }

        let resp: OrdersResponse = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse orders: {}", e)))?;

        Ok(resp.orders)
    }

    /// Calculate commission for a winning trade
    pub fn calculate_commission(&self, winnings: Decimal) -> Decimal {
        if winnings > Decimal::ZERO {
            winnings * self.config.commission_rate
        } else {
            Decimal::ZERO
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_opinion_action_conversion() {
        assert_eq!(
            OpinionAction::from_side_outcome(Side::Buy, Outcome::Yes),
            OpinionAction::Back
        );
        assert_eq!(
            OpinionAction::from_side_outcome(Side::Sell, Outcome::Yes),
            OpinionAction::Lay
        );
        assert_eq!(
            OpinionAction::from_side_outcome(Side::Buy, Outcome::No),
            OpinionAction::Lay
        );
        assert_eq!(
            OpinionAction::from_side_outcome(Side::Sell, Outcome::No),
            OpinionAction::Back
        );
    }

    #[test]
    fn test_order_fill_pct() {
        let order = OpinionOrder {
            order_id: "test".to_string(),
            market_id: "market".to_string(),
            selection_id: "sel".to_string(),
            action: OpinionAction::Back,
            price: dec!(1.5),
            size: dec!(100),
            matched_size: dec!(75),
            remaining_size: dec!(25),
            status: "PARTIALLY_MATCHED".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            client_ref: None,
        };

        assert_eq!(order.fill_pct(), dec!(75));
        assert!(order.is_partial());
        assert!(!order.is_filled());
    }

    #[test]
    fn test_orderbook_spread() {
        let orderbook = OpinionOrderbook {
            market_id: "test".to_string(),
            selection_id: "sel".to_string(),
            available_to_back: vec![
                OpinionOrderbookLevel { price: dec!(1.90), size: dec!(100) },
                OpinionOrderbookLevel { price: dec!(1.85), size: dec!(200) },
            ],
            available_to_lay: vec![
                OpinionOrderbookLevel { price: dec!(2.00), size: dec!(150) },
                OpinionOrderbookLevel { price: dec!(2.10), size: dec!(100) },
            ],
            timestamp: Utc::now(),
        };

        assert_eq!(orderbook.spread(), Some(dec!(0.10)));  // 2.00 - 1.90
        assert!(!orderbook.is_crossed());

        // Test crossed market
        let crossed_orderbook = OpinionOrderbook {
            market_id: "test".to_string(),
            selection_id: "sel".to_string(),
            available_to_back: vec![
                OpinionOrderbookLevel { price: dec!(2.10), size: dec!(100) },
            ],
            available_to_lay: vec![
                OpinionOrderbookLevel { price: dec!(2.00), size: dec!(150) },
            ],
            timestamp: Utc::now(),
        };

        assert!(crossed_orderbook.is_crossed());
    }
}
