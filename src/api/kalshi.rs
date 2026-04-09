//! Kalshi API client for cross-platform arbitrage.
//!
//! Kalshi is a CFTC-regulated prediction market exchange.
//! API Documentation: https://trading-api.readme.io/reference/
//!
//! Key differences from Polymarket:
//! - Centralized exchange with traditional order matching
//! - USD-denominated (not USDC)
//! - Different fee structure
//! - REST + WebSocket API

use crate::core::{
    error::{ArbitrageError, Result},
    types::{Order, OrderStatus, OrderType, Outcome, Position, Price, Quantity, Side, Trade},
};
use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Kalshi API configuration
#[derive(Debug, Clone)]
pub struct KalshiConfig {
    pub base_url: String,
    pub email: String,
    pub password: String,
    pub api_key: Option<String>,
    pub timeout_ms: u64,
}

impl Default for KalshiConfig {
    fn default() -> Self {
        Self {
            base_url: "https://trading-api.kalshi.com/trade-api/v2".to_string(),
            email: String::new(),
            password: String::new(),
            api_key: None,
            timeout_ms: 5000,
        }
    }
}

/// Kalshi authentication token
#[derive(Debug, Clone)]
struct AuthToken {
    token: String,
    user_id: String,
    expires_at: DateTime<Utc>,
}

/// Kalshi market representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiMarket {
    pub ticker: String,
    pub event_ticker: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub status: String,
    pub yes_bid: Option<i64>,
    pub yes_ask: Option<i64>,
    pub no_bid: Option<i64>,
    pub no_ask: Option<i64>,
    pub last_price: Option<i64>,
    pub volume: i64,
    pub volume_24h: i64,
    pub open_interest: i64,
    pub close_time: Option<DateTime<Utc>>,
    pub expiration_time: Option<DateTime<Utc>>,
    pub result: Option<String>,
    pub cap_strike: Option<i64>,
    pub floor_strike: Option<i64>,
}

impl KalshiMarket {
    /// Convert Kalshi price (cents 0-100) to normalized price (0-1)
    pub fn normalize_price(cents: i64) -> Price {
        Price::new_unchecked(Decimal::from(cents) / Decimal::from(100))
    }

    /// Get best YES bid price
    pub fn yes_bid_price(&self) -> Option<Price> {
        self.yes_bid.map(Self::normalize_price)
    }

    /// Get best YES ask price
    pub fn yes_ask_price(&self) -> Option<Price> {
        self.yes_ask.map(Self::normalize_price)
    }

    /// Get best NO bid price
    pub fn no_bid_price(&self) -> Option<Price> {
        self.no_bid.map(Self::normalize_price)
    }

    /// Get best NO ask price
    pub fn no_ask_price(&self) -> Option<Price> {
        self.no_ask.map(Self::normalize_price)
    }

    /// Check if market is active
    pub fn is_active(&self) -> bool {
        self.status == "active" || self.status == "open"
    }
}

/// Kalshi order representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiOrder {
    pub order_id: String,
    pub user_id: String,
    pub ticker: String,
    pub status: String,
    pub side: String,
    pub action: String,
    pub yes_price: Option<i64>,
    pub no_price: Option<i64>,
    pub count: i64,
    pub remaining_count: i64,
    pub created_time: DateTime<Utc>,
    pub expiration_time: Option<DateTime<Utc>>,
    pub client_order_id: Option<String>,
}

impl KalshiOrder {
    /// Convert to internal OrderStatus
    pub fn to_order_status(&self) -> OrderStatus {
        match self.status.as_str() {
            "resting" => OrderStatus::Live,
            "pending" => OrderStatus::Live,
            "executed" => OrderStatus::Matched,
            "canceled" | "cancelled" => OrderStatus::Cancelled,
            "partial" => OrderStatus::PartiallyFilled,
            _ => OrderStatus::Rejected,
        }
    }

    /// Get filled quantity
    pub fn filled_quantity(&self) -> Quantity {
        Quantity::new(Decimal::from(self.count - self.remaining_count))
            .unwrap_or(Quantity::ZERO)
    }

    /// Check if fully filled
    pub fn is_filled(&self) -> bool {
        self.remaining_count == 0 && self.status == "executed"
    }

    /// Check if partially filled
    pub fn is_partial(&self) -> bool {
        self.remaining_count > 0 && self.remaining_count < self.count
    }
}

/// Kalshi fill/trade representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiFill {
    pub trade_id: String,
    pub order_id: String,
    pub ticker: String,
    pub side: String,
    pub action: String,
    pub count: i64,
    pub yes_price: i64,
    pub no_price: i64,
    pub created_time: DateTime<Utc>,
    pub is_taker: bool,
}

/// Kalshi position representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiPosition {
    pub ticker: String,
    pub position: i64,  // Positive = YES, Negative = NO
    pub market_exposure: i64,
    pub total_traded: i64,
    pub resting_orders_count: i64,
}

impl KalshiPosition {
    /// Convert to internal Position type
    pub fn to_position(&self, market_id: &str) -> Position {
        let (outcome, qty) = if self.position >= 0 {
            (Outcome::Yes, self.position)
        } else {
            (Outcome::No, -self.position)
        };

        Position {
            market_id: market_id.to_string(),
            token_id: self.ticker.clone(),
            outcome,
            quantity: Quantity::new(Decimal::from(qty)).unwrap_or(Quantity::ZERO),
            average_entry_price: Price::ZERO,  // Kalshi doesn't provide this directly
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            last_updated: Utc::now(),
        }
    }
}

/// Kalshi balance information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiBalance {
    pub balance: i64,  // In cents
    pub portfolio_value: i64,
}

/// Request to create a Kalshi order
#[derive(Debug, Serialize)]
struct CreateOrderRequest {
    ticker: String,
    action: String,  // "buy" or "sell"
    side: String,    // "yes" or "no"
    count: i64,
    #[serde(rename = "type")]
    order_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    yes_price: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    no_price: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expiration_ts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_order_id: Option<String>,
}

/// Kalshi API client
pub struct KalshiClient {
    config: KalshiConfig,
    client: Client,
    auth: Arc<RwLock<Option<AuthToken>>>,
    /// Cache of market data
    market_cache: Arc<RwLock<HashMap<String, KalshiMarket>>>,
}

impl KalshiClient {
    /// Create a new Kalshi client
    pub fn new(config: KalshiConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .pool_max_idle_per_host(10)
            .build()
            .map_err(|e| ArbitrageError::Api(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            config,
            client,
            auth: Arc::new(RwLock::new(None)),
            market_cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Authenticate with Kalshi
    pub async fn login(&self) -> Result<()> {
        let url = format!("{}/login", self.config.base_url);

        #[derive(Serialize)]
        struct LoginRequest<'a> {
            email: &'a str,
            password: &'a str,
        }

        #[derive(Deserialize)]
        struct LoginResponse {
            token: String,
            member_id: String,
        }

        let response = self.client
            .post(&url)
            .json(&LoginRequest {
                email: &self.config.email,
                password: &self.config.password,
            })
            .send()
            .await
            .map_err(|e| ArbitrageError::Api(format!("Kalshi login failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ArbitrageError::Api(format!(
                "Kalshi login failed: {} - {}",
                status, body
            )));
        }

        let login_resp: LoginResponse = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse login response: {}", e)))?;

        let auth_token = AuthToken {
            token: login_resp.token,
            user_id: login_resp.member_id,
            expires_at: Utc::now() + chrono::Duration::hours(24),
        };

        *self.auth.write().await = Some(auth_token);
        info!("Successfully authenticated with Kalshi");
        Ok(())
    }

    /// Get the auth token, refreshing if needed
    async fn get_auth_header(&self) -> Result<String> {
        let auth = self.auth.read().await;
        match auth.as_ref() {
            Some(token) if token.expires_at > Utc::now() => {
                Ok(format!("Bearer {}", token.token))
            }
            _ => {
                drop(auth);
                self.login().await?;
                let auth = self.auth.read().await;
                auth.as_ref()
                    .map(|t| format!("Bearer {}", t.token))
                    .ok_or_else(|| ArbitrageError::Api("Auth token not available".to_string()))
            }
        }
    }

    /// Get market data
    pub async fn get_market(&self, ticker: &str) -> Result<KalshiMarket> {
        // Check cache first
        {
            let cache = self.market_cache.read().await;
            if let Some(market) = cache.get(ticker) {
                return Ok(market.clone());
            }
        }

        let url = format!("{}/markets/{}", self.config.base_url, ticker);
        let auth = self.get_auth_header().await?;

        let response = self.client
            .get(&url)
            .header("Authorization", auth)
            .send()
            .await
            .map_err(|e| ArbitrageError::Api(format!("Failed to get market: {}", e)))?;

        if !response.status().is_success() {
            return Err(ArbitrageError::Api(format!(
                "Failed to get market {}: {}",
                ticker,
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct MarketResponse {
            market: KalshiMarket,
        }

        let resp: MarketResponse = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse market: {}", e)))?;

        // Update cache
        {
            let mut cache = self.market_cache.write().await;
            cache.insert(ticker.to_string(), resp.market.clone());
        }

        Ok(resp.market)
    }

    /// Get multiple markets by event ticker
    pub async fn get_markets_by_event(&self, event_ticker: &str) -> Result<Vec<KalshiMarket>> {
        let url = format!("{}/markets", self.config.base_url);
        let auth = self.get_auth_header().await?;

        let response = self.client
            .get(&url)
            .header("Authorization", auth)
            .query(&[("event_ticker", event_ticker)])
            .send()
            .await
            .map_err(|e| ArbitrageError::Api(format!("Failed to get markets: {}", e)))?;

        if !response.status().is_success() {
            return Err(ArbitrageError::Api(format!(
                "Failed to get markets for event {}: {}",
                event_ticker,
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct MarketsResponse {
            markets: Vec<KalshiMarket>,
        }

        let resp: MarketsResponse = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse markets: {}", e)))?;

        Ok(resp.markets)
    }

    /// Place an order on Kalshi
    pub async fn place_order(
        &self,
        ticker: &str,
        side: Side,
        outcome: Outcome,
        price: Price,
        quantity: Quantity,
        order_type: OrderType,
    ) -> Result<KalshiOrder> {
        let url = format!("{}/portfolio/orders", self.config.base_url);
        let auth = self.get_auth_header().await?;

        // Convert price to cents (0-100)
        let price_cents = (price.as_decimal() * Decimal::from(100))
            .round()
            .to_string()
            .parse::<i64>()
            .unwrap_or(0);

        let (yes_price, no_price) = match outcome {
            Outcome::Yes => (Some(price_cents), None),
            Outcome::No => (None, Some(price_cents)),
        };

        let request = CreateOrderRequest {
            ticker: ticker.to_string(),
            action: match side {
                Side::Buy => "buy".to_string(),
                Side::Sell => "sell".to_string(),
            },
            side: match outcome {
                Outcome::Yes => "yes".to_string(),
                Outcome::No => "no".to_string(),
            },
            count: quantity.as_decimal().to_string().parse::<i64>().unwrap_or(0),
            order_type: match order_type {
                OrderType::Fok => "fok".to_string(),
                OrderType::Gtc => "limit".to_string(),
                OrderType::Gtd => "limit".to_string(),
            },
            yes_price,
            no_price,
            expiration_ts: None,
            client_order_id: Some(Uuid::new_v4().to_string()),
        };

        debug!("Placing Kalshi order: {:?}", request);

        let response = self.client
            .post(&url)
            .header("Authorization", auth)
            .json(&request)
            .send()
            .await
            .map_err(|e| ArbitrageError::Api(format!("Failed to place order: {}", e)))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            error!("Kalshi order failed: {} - {}", status, body);
            return Err(ArbitrageError::OrderRejected(format!(
                "Kalshi order rejected: {} - {}",
                status, body
            )));
        }

        #[derive(Deserialize)]
        struct OrderResponse {
            order: KalshiOrder,
        }

        let resp: OrderResponse = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse order response: {}", e)))?;

        info!(
            "Kalshi order placed: {} {} {} @ {} cents",
            resp.order.order_id, resp.order.action, resp.order.side,
            resp.order.yes_price.unwrap_or(resp.order.no_price.unwrap_or(0))
        );

        Ok(resp.order)
    }

    /// Get order status
    pub async fn get_order(&self, order_id: &str) -> Result<KalshiOrder> {
        let url = format!("{}/portfolio/orders/{}", self.config.base_url, order_id);
        let auth = self.get_auth_header().await?;

        let response = self.client
            .get(&url)
            .header("Authorization", auth)
            .send()
            .await
            .map_err(|e| ArbitrageError::Api(format!("Failed to get order: {}", e)))?;

        if !response.status().is_success() {
            return Err(ArbitrageError::Api(format!(
                "Failed to get order {}: {}",
                order_id,
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct OrderResponse {
            order: KalshiOrder,
        }

        let resp: OrderResponse = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse order: {}", e)))?;

        Ok(resp.order)
    }

    /// Cancel an order
    pub async fn cancel_order(&self, order_id: &str) -> Result<KalshiOrder> {
        let url = format!("{}/portfolio/orders/{}", self.config.base_url, order_id);
        let auth = self.get_auth_header().await?;

        let response = self.client
            .delete(&url)
            .header("Authorization", auth)
            .send()
            .await
            .map_err(|e| ArbitrageError::Api(format!("Failed to cancel order: {}", e)))?;

        if !response.status().is_success() {
            return Err(ArbitrageError::Api(format!(
                "Failed to cancel order {}: {}",
                order_id,
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct OrderResponse {
            order: KalshiOrder,
        }

        let resp: OrderResponse = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse cancel response: {}", e)))?;

        Ok(resp.order)
    }

    /// Get fills (trades) for an order
    pub async fn get_fills(&self, order_id: Option<&str>, ticker: Option<&str>) -> Result<Vec<KalshiFill>> {
        let url = format!("{}/portfolio/fills", self.config.base_url);
        let auth = self.get_auth_header().await?;

        let mut req = self.client.get(&url).header("Authorization", auth);

        if let Some(oid) = order_id {
            req = req.query(&[("order_id", oid)]);
        }
        if let Some(t) = ticker {
            req = req.query(&[("ticker", t)]);
        }

        let response = req.send().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to get fills: {}", e)))?;

        if !response.status().is_success() {
            return Err(ArbitrageError::Api(format!(
                "Failed to get fills: {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct FillsResponse {
            fills: Vec<KalshiFill>,
        }

        let resp: FillsResponse = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse fills: {}", e)))?;

        Ok(resp.fills)
    }

    /// Get positions
    pub async fn get_positions(&self) -> Result<Vec<KalshiPosition>> {
        let url = format!("{}/portfolio/positions", self.config.base_url);
        let auth = self.get_auth_header().await?;

        let response = self.client
            .get(&url)
            .header("Authorization", auth)
            .send()
            .await
            .map_err(|e| ArbitrageError::Api(format!("Failed to get positions: {}", e)))?;

        if !response.status().is_success() {
            return Err(ArbitrageError::Api(format!(
                "Failed to get positions: {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct PositionsResponse {
            market_positions: Vec<KalshiPosition>,
        }

        let resp: PositionsResponse = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse positions: {}", e)))?;

        Ok(resp.market_positions)
    }

    /// Get balance
    pub async fn get_balance(&self) -> Result<KalshiBalance> {
        let url = format!("{}/portfolio/balance", self.config.base_url);
        let auth = self.get_auth_header().await?;

        let response = self.client
            .get(&url)
            .header("Authorization", auth)
            .send()
            .await
            .map_err(|e| ArbitrageError::Api(format!("Failed to get balance: {}", e)))?;

        if !response.status().is_success() {
            return Err(ArbitrageError::Api(format!(
                "Failed to get balance: {}",
                response.status()
            )));
        }

        let resp: KalshiBalance = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse balance: {}", e)))?;

        Ok(resp)
    }

    /// Get orderbook for a market
    pub async fn get_orderbook(&self, ticker: &str, depth: Option<i32>) -> Result<KalshiOrderbook> {
        let url = format!("{}/markets/{}/orderbook", self.config.base_url, ticker);
        let auth = self.get_auth_header().await?;

        let mut req = self.client.get(&url).header("Authorization", auth);

        if let Some(d) = depth {
            req = req.query(&[("depth", d.to_string())]);
        }

        let response = req.send().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to get orderbook: {}", e)))?;

        if !response.status().is_success() {
            return Err(ArbitrageError::Api(format!(
                "Failed to get orderbook for {}: {}",
                ticker,
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct OrderbookResponse {
            orderbook: KalshiOrderbook,
        }

        let resp: OrderbookResponse = response.json().await
            .map_err(|e| ArbitrageError::Api(format!("Failed to parse orderbook: {}", e)))?;

        Ok(resp.orderbook)
    }
}

/// Kalshi orderbook representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiOrderbook {
    pub ticker: String,
    pub yes: Vec<KalshiOrderbookLevel>,
    pub no: Vec<KalshiOrderbookLevel>,
}

/// Kalshi orderbook price level
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiOrderbookLevel {
    pub price: i64,  // In cents (0-100)
    pub count: i64,
}

impl KalshiOrderbook {
    /// Get best YES bid (highest buy price)
    pub fn best_yes_bid(&self) -> Option<(Price, Quantity)> {
        self.yes.iter()
            .max_by_key(|l| l.price)
            .map(|l| (
                KalshiMarket::normalize_price(l.price),
                Quantity::new(Decimal::from(l.count)).unwrap_or(Quantity::ZERO),
            ))
    }

    /// Get best YES ask (lowest sell price)
    pub fn best_yes_ask(&self) -> Option<(Price, Quantity)> {
        // In Kalshi, to buy YES you pay the YES price
        // The ask is the lowest price someone is willing to sell at
        self.yes.iter()
            .min_by_key(|l| l.price)
            .map(|l| (
                KalshiMarket::normalize_price(l.price),
                Quantity::new(Decimal::from(l.count)).unwrap_or(Quantity::ZERO),
            ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_price_normalization() {
        let price = KalshiMarket::normalize_price(65);
        assert_eq!(price.as_decimal(), dec!(0.65));

        let price = KalshiMarket::normalize_price(100);
        assert_eq!(price.as_decimal(), dec!(1.00));

        let price = KalshiMarket::normalize_price(1);
        assert_eq!(price.as_decimal(), dec!(0.01));
    }

    #[test]
    fn test_kalshi_order_status() {
        let order = KalshiOrder {
            order_id: "test".to_string(),
            user_id: "user".to_string(),
            ticker: "BTC-UP".to_string(),
            status: "partial".to_string(),
            side: "yes".to_string(),
            action: "buy".to_string(),
            yes_price: Some(55),
            no_price: None,
            count: 100,
            remaining_count: 30,
            created_time: Utc::now(),
            expiration_time: None,
            client_order_id: None,
        };

        assert_eq!(order.to_order_status(), OrderStatus::PartiallyFilled);
        assert_eq!(order.filled_quantity().as_decimal(), dec!(70));
        assert!(order.is_partial());
        assert!(!order.is_filled());
    }
}
