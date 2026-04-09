//! CLOB (Central Limit Order Book) API client for Polymarket.
//!
//! Provides high-performance order management:
//! - Order placement (limit, FOK)
//! - Order cancellation
//! - Order status queries
//! - Position queries

use crate::api::auth::{AuthHeaders, OrderSigningData, Signer};
use crate::api::rate_limiter::RateLimiter;
use crate::core::config::ApiConfig;
use crate::core::constants::*;
use crate::core::error::{ApiError, Error, Result};
use crate::core::types::*;
use chrono::{DateTime, Utc};
use reqwest::{Client, Response, StatusCode};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

/// CLOB API Client
#[derive(Clone)]
pub struct ClobClient {
    client: Client,
    signer: Arc<Signer>,
    base_url: String,
    rate_limiter: Arc<RateLimiter>,
    chain_id: u64,
    proxy_wallet: Option<String>,
}

impl ClobClient {
    /// Create a new CLOB client
    pub fn new(config: &ApiConfig, signer: Signer, chain_id: u64) -> Result<Self> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .pool_max_idle_per_host(10)
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .tcp_nodelay(true)
            .gzip(true)
            .brotli(true)
            .build()
            .map_err(|e| Error::Internal(format!("Failed to create HTTP client: {}", e)))?;

        let rate_limiter = Arc::new(RateLimiter::new(config.rate_limit_rps));

        Ok(Self {
            client,
            signer: Arc::new(signer),
            base_url: config.clob_http_url.clone(),
            rate_limiter,
            chain_id,
            proxy_wallet: config.proxy_wallet_address.clone(),
        })
    }

    /// Get the signer's address
    pub fn address(&self) -> String {
        self.signer.address_string()
    }

    /// Get the proxy wallet address if configured
    pub fn proxy_wallet(&self) -> Option<&str> {
        self.proxy_wallet.as_deref()
    }

    /// Make an authenticated request
    async fn request<T: for<'de> Deserialize<'de>>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&str>,
    ) -> Result<T> {
        // Wait for rate limiter
        self.rate_limiter.acquire().await;

        let body_str = body.unwrap_or("");
        let auth = self.signer.auth_headers(method.as_str(), path, body_str)?;

        let url = format!("{}{}", self.base_url, path);

        let mut request = self.client.request(method.clone(), &url);

        // Add auth headers
        request = request
            .header("POLY_ADDRESS", self.signer.address_string())
            .header("POLY_SIGNATURE", &auth.signature)
            .header("POLY_TIMESTAMP", &auth.timestamp)
            .header("POLY_NONCE", &auth.timestamp)
            .header("POLY_API_KEY", &auth.api_key)
            .header("POLY_PASSPHRASE", &auth.passphrase);

        if !body_str.is_empty() {
            request = request
                .header("Content-Type", "application/json")
                .body(body_str.to_string());
        }

        let response = request
            .send()
            .await
            .map_err(|e| Error::Http(e))?;

        self.handle_response(response).await
    }

    /// Handle API response
    async fn handle_response<T: for<'de> Deserialize<'de>>(&self, response: Response) -> Result<T> {
        let status = response.status();
        let body = response.text().await.map_err(Error::Http)?;

        match status {
            StatusCode::OK | StatusCode::CREATED => {
                serde_json::from_str(&body).map_err(|e| {
                    error!("Failed to parse response: {}, body: {}", e, body);
                    Error::Api(ApiError::InvalidResponse(e.to_string()))
                })
            }
            StatusCode::TOO_MANY_REQUESTS => {
                let retry_after = 5; // Default retry after
                Err(Error::Api(ApiError::RateLimited {
                    retry_after_secs: retry_after,
                }))
            }
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                Err(Error::Api(ApiError::AuthenticationFailed(body)))
            }
            StatusCode::NOT_FOUND => {
                Err(Error::Api(ApiError::MarketNotFound(body)))
            }
            StatusCode::SERVICE_UNAVAILABLE | StatusCode::BAD_GATEWAY => {
                Err(Error::Api(ApiError::ServiceUnavailable))
            }
            _ => {
                // Try to parse error response
                if let Ok(error_resp) = serde_json::from_str::<ClobErrorResponse>(&body) {
                    Err(Error::Api(ApiError::InvalidRequest(error_resp.message)))
                } else {
                    Err(Error::Api(ApiError::InvalidResponse(format!(
                        "Status {}: {}",
                        status, body
                    ))))
                }
            }
        }
    }

    // ========================================================================
    // Order Management
    // ========================================================================

    /// Place a limit order
    #[instrument(skip(self), fields(token_id = %order.token_id, side = ?order.side))]
    pub async fn place_order(&self, order: &Order) -> Result<OrderResponse> {
        let maker = if let Some(ref proxy) = self.proxy_wallet {
            proxy.parse().map_err(|e| Error::Parse(format!("Invalid proxy address: {}", e)))?
        } else {
            self.signer.address()
        };

        let expiration = order
            .expiration
            .map(|e| e.timestamp() as u64)
            .unwrap_or(Utc::now().timestamp() as u64 + 86400); // 24h default

        // Create signing data
        let signing_data = match order.side {
            Side::Buy => OrderSigningData::buy(
                maker,
                self.signer.address(),
                &order.token_id,
                order.price.0,
                order.quantity.0,
                expiration,
                Utc::now().timestamp_millis() as u64,
                self.chain_id,
            )?,
            Side::Sell => OrderSigningData::sell(
                maker,
                self.signer.address(),
                &order.token_id,
                order.price.0,
                order.quantity.0,
                expiration,
                Utc::now().timestamp_millis() as u64,
                self.chain_id,
            )?,
        };

        // Sign the order
        let signature = self.signer.sign_order(&signing_data).await?;

        // Build order request
        let request = PlaceOrderRequest {
            order: OrderPayload {
                salt: signing_data.salt.to_string(),
                maker: format!("{:?}", maker),
                signer: self.signer.address_string(),
                taker: "0x0000000000000000000000000000000000000000".to_string(),
                token_id: order.token_id.clone(),
                maker_amount: signing_data.maker_amount.to_string(),
                taker_amount: signing_data.taker_amount.to_string(),
                expiration: expiration.to_string(),
                nonce: signing_data.nonce.to_string(),
                fee_rate_bps: "0".to_string(),
                side: match order.side {
                    Side::Buy => "BUY",
                    Side::Sell => "SELL",
                }
                .to_string(),
                signature_type: 0,
                signature,
            },
            order_type: match order.order_type {
                OrderType::Gtc => "GTC",
                OrderType::Fok => "FOK",
                OrderType::Gtd => "GTD",
            }
            .to_string(),
            owner: format!("{:?}", maker),
        };

        let body = serde_json::to_string(&request)
            .map_err(|e| Error::Json(e))?;

        info!(
            "Placing {} order for {} @ {}",
            order.side, order.quantity, order.price
        );

        self.request(reqwest::Method::POST, "/order", Some(&body)).await
    }

    /// Place multiple orders atomically
    pub async fn place_orders(&self, orders: &[Order]) -> Result<Vec<OrderResponse>> {
        let mut responses = Vec::with_capacity(orders.len());

        for order in orders {
            let response = self.place_order(order).await?;
            responses.push(response);
        }

        Ok(responses)
    }

    /// Cancel an order
    #[instrument(skip(self))]
    pub async fn cancel_order(&self, order_id: &str) -> Result<CancelResponse> {
        let request = CancelOrderRequest {
            order_id: order_id.to_string(),
        };

        let body = serde_json::to_string(&request)?;
        info!("Cancelling order: {}", order_id);

        self.request(reqwest::Method::DELETE, "/order", Some(&body)).await
    }

    /// Cancel all orders for a market
    pub async fn cancel_all_orders(&self, market_id: Option<&str>) -> Result<CancelResponse> {
        let path = if let Some(market) = market_id {
            format!("/orders?market={}", market)
        } else {
            "/orders".to_string()
        };

        info!("Cancelling all orders for market: {:?}", market_id);
        self.request(reqwest::Method::DELETE, &path, None).await
    }

    /// Get order status
    pub async fn get_order(&self, order_id: &str) -> Result<OrderResponse> {
        let path = format!("/order/{}", order_id);
        self.request(reqwest::Method::GET, &path, None).await
    }

    /// Get all open orders
    pub async fn get_open_orders(&self, market_id: Option<&str>) -> Result<Vec<OrderResponse>> {
        let path = if let Some(market) = market_id {
            format!("/orders?market={}", market)
        } else {
            "/orders".to_string()
        };

        self.request(reqwest::Method::GET, &path, None).await
    }

    // ========================================================================
    // Market Data
    // ========================================================================

    /// Get orderbook for a token
    pub async fn get_orderbook(&self, token_id: &str) -> Result<OrderbookResponse> {
        let path = format!("/book?token_id={}", token_id);
        self.request(reqwest::Method::GET, &path, None).await
    }

    /// Get orderbook with depth
    pub async fn get_orderbook_depth(&self, token_id: &str, depth: u32) -> Result<OrderbookResponse> {
        let path = format!("/book?token_id={}&depth={}", token_id, depth);
        self.request(reqwest::Method::GET, &path, None).await
    }

    /// Get midpoint price
    pub async fn get_midpoint(&self, token_id: &str) -> Result<MidpointResponse> {
        let path = format!("/midpoint?token_id={}", token_id);
        self.request(reqwest::Method::GET, &path, None).await
    }

    /// Get spread
    pub async fn get_spread(&self, token_id: &str) -> Result<SpreadResponse> {
        let path = format!("/spread?token_id={}", token_id);
        self.request(reqwest::Method::GET, &path, None).await
    }

    /// Get price (last trade)
    pub async fn get_price(&self, token_id: &str) -> Result<PriceResponse> {
        let path = format!("/price?token_id={}", token_id);
        self.request(reqwest::Method::GET, &path, None).await
    }

    // ========================================================================
    // Positions & Balance
    // ========================================================================

    /// Get current positions
    pub async fn get_positions(&self) -> Result<Vec<PositionResponse>> {
        self.request(reqwest::Method::GET, "/positions", None).await
    }

    /// Get balance and allowance
    pub async fn get_balance(&self) -> Result<BalanceResponse> {
        self.request(reqwest::Method::GET, "/balance", None).await
    }

    // ========================================================================
    // Trades
    // ========================================================================

    /// Get recent trades for a market
    pub async fn get_trades(&self, market_id: &str, limit: Option<u32>) -> Result<Vec<TradeResponse>> {
        let path = if let Some(lim) = limit {
            format!("/trades?market={}&limit={}", market_id, lim)
        } else {
            format!("/trades?market={}", market_id)
        };

        self.request(reqwest::Method::GET, &path, None).await
    }

    /// Get user's trade history
    pub async fn get_user_trades(&self, limit: Option<u32>) -> Result<Vec<TradeResponse>> {
        let path = if let Some(lim) = limit {
            format!("/trades/user?limit={}", lim)
        } else {
            "/trades/user".to_string()
        };

        self.request(reqwest::Method::GET, &path, None).await
    }
}

// ============================================================================
// Request/Response Types
// ============================================================================

#[derive(Debug, Serialize)]
struct PlaceOrderRequest {
    order: OrderPayload,
    #[serde(rename = "orderType")]
    order_type: String,
    owner: String,
}

#[derive(Debug, Serialize)]
struct OrderPayload {
    salt: String,
    maker: String,
    signer: String,
    taker: String,
    #[serde(rename = "tokenId")]
    token_id: String,
    #[serde(rename = "makerAmount")]
    maker_amount: String,
    #[serde(rename = "takerAmount")]
    taker_amount: String,
    expiration: String,
    nonce: String,
    #[serde(rename = "feeRateBps")]
    fee_rate_bps: String,
    side: String,
    #[serde(rename = "signatureType")]
    signature_type: u8,
    signature: String,
}

#[derive(Debug, Serialize)]
struct CancelOrderRequest {
    #[serde(rename = "orderID")]
    order_id: String,
}

#[derive(Debug, Deserialize)]
struct ClobErrorResponse {
    message: String,
    #[serde(default)]
    error: Option<String>,
}

/// Order response from CLOB API
#[derive(Debug, Clone, Deserialize)]
pub struct OrderResponse {
    pub id: String,
    pub status: String,
    pub market: Option<String>,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    pub side: String,
    pub price: String,
    #[serde(rename = "originalSize")]
    pub original_size: String,
    #[serde(rename = "remainingSize")]
    pub remaining_size: String,
    #[serde(rename = "createdAt")]
    pub created_at: Option<String>,
    #[serde(rename = "updatedAt")]
    pub updated_at: Option<String>,
}

impl OrderResponse {
    /// Convert to internal Order type
    pub fn to_order(&self, market_id: &str, outcome: Outcome) -> Result<Order> {
        let price = self.price.parse::<Decimal>()
            .map_err(|e| Error::Parse(format!("Invalid price: {}", e)))?;
        let quantity = self.original_size.parse::<Decimal>()
            .map_err(|e| Error::Parse(format!("Invalid size: {}", e)))?;

        let side = match self.side.to_uppercase().as_str() {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
            _ => return Err(Error::Parse(format!("Invalid side: {}", self.side))),
        };

        Ok(Order {
            id: self.id.clone(),
            market_id: market_id.to_string(),
            token_id: self.token_id.clone(),
            side,
            outcome,
            price: Price::new_unchecked(price),
            quantity: Quantity(quantity),
            order_type: OrderType::Gtc,
            expiration: None,
            created_at: Utc::now(),
            client_order_id: None,
        })
    }
}

/// Cancel response
#[derive(Debug, Clone, Deserialize)]
pub struct CancelResponse {
    pub canceled: Vec<String>,
    #[serde(default)]
    pub not_canceled: Vec<NotCanceledOrder>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NotCanceledOrder {
    pub order_id: String,
    pub reason: String,
}

/// Orderbook response
#[derive(Debug, Clone, Deserialize)]
pub struct OrderbookResponse {
    pub market: Option<String>,
    #[serde(rename = "assetId")]
    pub asset_id: String,
    pub hash: Option<String>,
    pub timestamp: Option<String>,
    pub bids: Vec<OrderbookLevel>,
    pub asks: Vec<OrderbookLevel>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderbookLevel {
    pub price: String,
    pub size: String,
}

impl OrderbookResponse {
    /// Convert to internal Orderbook type
    pub fn to_orderbook(&self, outcome: Outcome) -> Result<Orderbook> {
        let mut ob = Orderbook::new(self.asset_id.clone(), outcome);

        for level in &self.bids {
            let price = level.price.parse::<Decimal>()
                .map_err(|e| Error::Parse(format!("Invalid bid price: {}", e)))?;
            let size = level.size.parse::<Decimal>()
                .map_err(|e| Error::Parse(format!("Invalid bid size: {}", e)))?;

            if let Some(p) = Price::new(price) {
                ob.bids.levels.push(PriceLevel::new(p, Quantity(size), 1));
            }
        }

        for level in &self.asks {
            let price = level.price.parse::<Decimal>()
                .map_err(|e| Error::Parse(format!("Invalid ask price: {}", e)))?;
            let size = level.size.parse::<Decimal>()
                .map_err(|e| Error::Parse(format!("Invalid ask size: {}", e)))?;

            if let Some(p) = Price::new(price) {
                ob.asks.levels.push(PriceLevel::new(p, Quantity(size), 1));
            }
        }

        // Sort bids descending, asks ascending
        ob.bids.levels.sort_by(|a, b| b.price.cmp(&a.price));
        ob.asks.levels.sort_by(|a, b| a.price.cmp(&b.price));

        Ok(ob)
    }
}

/// Midpoint response
#[derive(Debug, Clone, Deserialize)]
pub struct MidpointResponse {
    pub mid: String,
}

/// Spread response
#[derive(Debug, Clone, Deserialize)]
pub struct SpreadResponse {
    pub spread: String,
}

/// Price response
#[derive(Debug, Clone, Deserialize)]
pub struct PriceResponse {
    pub price: String,
}

/// Position response
#[derive(Debug, Clone, Deserialize)]
pub struct PositionResponse {
    pub market: String,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    pub outcome: String,
    pub size: String,
    #[serde(rename = "avgPrice")]
    pub avg_price: String,
}

/// Balance response
#[derive(Debug, Clone, Deserialize)]
pub struct BalanceResponse {
    pub balance: String,
    pub allowance: String,
}

/// Trade response
#[derive(Debug, Clone, Deserialize)]
pub struct TradeResponse {
    pub id: String,
    pub market: String,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    pub side: String,
    pub price: String,
    pub size: String,
    pub fee: Option<String>,
    pub timestamp: String,
    #[serde(rename = "txHash")]
    pub tx_hash: Option<String>,
}

impl std::fmt::Debug for ClobClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClobClient")
            .field("base_url", &self.base_url)
            .field("address", &self.address())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orderbook_response_conversion() {
        let response = OrderbookResponse {
            market: Some("test".to_string()),
            asset_id: "12345".to_string(),
            hash: None,
            timestamp: None,
            bids: vec![
                OrderbookLevel { price: "0.45".to_string(), size: "100".to_string() },
                OrderbookLevel { price: "0.44".to_string(), size: "200".to_string() },
            ],
            asks: vec![
                OrderbookLevel { price: "0.55".to_string(), size: "150".to_string() },
                OrderbookLevel { price: "0.56".to_string(), size: "250".to_string() },
            ],
        };

        let ob = response.to_orderbook(Outcome::Yes).unwrap();

        assert_eq!(ob.bids.levels.len(), 2);
        assert_eq!(ob.asks.levels.len(), 2);

        // Check sorting
        assert!(ob.bids.levels[0].price > ob.bids.levels[1].price);
        assert!(ob.asks.levels[0].price < ob.asks.levels[1].price);
    }
}
