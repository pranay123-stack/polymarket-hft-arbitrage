//! Gamma API client for Polymarket market data.
//!
//! The Gamma API provides:
//! - Market discovery and listing
//! - Market details and metadata
//! - Price history
//! - Volume and liquidity data

use crate::api::rate_limiter::RateLimiter;
use crate::core::config::ApiConfig;
use crate::core::error::{ApiError, Error, Result};
use crate::core::types::*;
use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, instrument};

/// Gamma API client for market data
#[derive(Clone)]
pub struct GammaClient {
    client: Client,
    base_url: String,
    rate_limiter: Arc<RateLimiter>,
}

impl GammaClient {
    /// Create a new Gamma client
    pub fn new(config: &ApiConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .pool_max_idle_per_host(5)
            .tcp_nodelay(true)
            .gzip(true)
            .build()
            .map_err(|e| Error::Internal(format!("Failed to create HTTP client: {}", e)))?;

        let rate_limiter = Arc::new(RateLimiter::new(5)); // Gamma API has lower limits

        Ok(Self {
            client,
            base_url: config.gamma_api_url.clone(),
            rate_limiter,
        })
    }

    /// Make a GET request
    async fn get<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T> {
        self.rate_limiter.acquire().await;

        let url = format!("{}{}", self.base_url, path);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(Error::Http)?;

        let status = response.status();
        let body = response.text().await.map_err(Error::Http)?;

        if status.is_success() {
            serde_json::from_str(&body).map_err(|e| {
                Error::Api(ApiError::InvalidResponse(format!(
                    "Failed to parse response: {}, body: {}",
                    e,
                    &body[..body.len().min(500)]
                )))
            })
        } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            Err(Error::Api(ApiError::RateLimited { retry_after_secs: 5 }))
        } else {
            Err(Error::Api(ApiError::InvalidResponse(format!(
                "Status {}: {}",
                status,
                &body[..body.len().min(500)]
            ))))
        }
    }

    // ========================================================================
    // Market Discovery
    // ========================================================================

    /// Get all active markets
    #[instrument(skip(self))]
    pub async fn get_markets(&self) -> Result<Vec<GammaMarket>> {
        let response: GammaMarketsResponse = self.get("/markets").await?;
        Ok(response.data)
    }

    /// Get markets with filtering
    pub async fn get_markets_filtered(
        &self,
        active: Option<bool>,
        closed: Option<bool>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<Vec<GammaMarket>> {
        let mut params = Vec::new();

        if let Some(a) = active {
            params.push(format!("active={}", a));
        }
        if let Some(c) = closed {
            params.push(format!("closed={}", c));
        }
        if let Some(l) = limit {
            params.push(format!("limit={}", l));
        }
        if let Some(o) = offset {
            params.push(format!("offset={}", o));
        }

        let path = if params.is_empty() {
            "/markets".to_string()
        } else {
            format!("/markets?{}", params.join("&"))
        };

        let response: GammaMarketsResponse = self.get(&path).await?;
        Ok(response.data)
    }

    /// Get market by condition ID
    #[instrument(skip(self))]
    pub async fn get_market(&self, condition_id: &str) -> Result<GammaMarket> {
        let path = format!("/markets/{}", condition_id);
        self.get(&path).await
    }

    /// Get market by slug
    #[instrument(skip(self))]
    pub async fn get_market_by_slug(&self, slug: &str) -> Result<GammaMarket> {
        let path = format!("/markets?slug={}", slug);
        let response: GammaMarketsResponse = self.get(&path).await?;

        response
            .data
            .into_iter()
            .next()
            .ok_or_else(|| Error::Api(ApiError::MarketNotFound(slug.to_string())))
    }

    /// Search markets
    pub async fn search_markets(&self, query: &str) -> Result<Vec<GammaMarket>> {
        let path = format!("/markets?_q={}", urlencoding::encode(query));
        let response: GammaMarketsResponse = self.get(&path).await?;
        Ok(response.data)
    }

    /// Get markets by tag
    pub async fn get_markets_by_tag(&self, tag: &str) -> Result<Vec<GammaMarket>> {
        let path = format!("/markets?tag={}", urlencoding::encode(tag));
        let response: GammaMarketsResponse = self.get(&path).await?;
        Ok(response.data)
    }

    // ========================================================================
    // Market Events (for event-based markets)
    // ========================================================================

    /// Get event details
    pub async fn get_event(&self, event_id: &str) -> Result<GammaEvent> {
        let path = format!("/events/{}", event_id);
        self.get(&path).await
    }

    /// Get all events
    pub async fn get_events(&self, active: Option<bool>) -> Result<Vec<GammaEvent>> {
        let path = if let Some(a) = active {
            format!("/events?active={}", a)
        } else {
            "/events".to_string()
        };

        let response: GammaEventsResponse = self.get(&path).await?;
        Ok(response.data)
    }

    // ========================================================================
    // Price Data
    // ========================================================================

    /// Get price history for a token
    pub async fn get_price_history(
        &self,
        token_id: &str,
        interval: &str,
        start_ts: Option<i64>,
        end_ts: Option<i64>,
    ) -> Result<Vec<PricePoint>> {
        let mut params = vec![format!("interval={}", interval)];

        if let Some(start) = start_ts {
            params.push(format!("startTs={}", start));
        }
        if let Some(end) = end_ts {
            params.push(format!("endTs={}", end));
        }

        let path = format!("/prices/{}?{}", token_id, params.join("&"));
        self.get(&path).await
    }

    /// Get current prices for multiple tokens
    pub async fn get_prices(&self, token_ids: &[&str]) -> Result<HashMap<String, GammaPrice>> {
        let ids = token_ids.join(",");
        let path = format!("/prices?ids={}", ids);
        self.get(&path).await
    }

    // ========================================================================
    // 5-Minute Markets (BTC/ETH Up/Down)
    // ========================================================================

    /// Get active 5-minute BTC markets
    pub async fn get_btc_5min_markets(&self) -> Result<Vec<GammaMarket>> {
        self.search_markets("btc-updown-5m").await
    }

    /// Get active 5-minute ETH markets
    pub async fn get_eth_5min_markets(&self) -> Result<Vec<GammaMarket>> {
        self.search_markets("eth-updown-5m").await
    }

    /// Get the next upcoming 5-minute market
    pub async fn get_next_5min_market(&self, asset: &str) -> Result<Option<GammaMarket>> {
        let markets = match asset.to_uppercase().as_str() {
            "BTC" => self.get_btc_5min_markets().await?,
            "ETH" => self.get_eth_5min_markets().await?,
            _ => return Err(Error::Api(ApiError::InvalidRequest(
                format!("Unsupported asset: {}", asset)
            ))),
        };

        let now = Utc::now();

        // Find the market that's about to start or just started
        let next_market = markets
            .into_iter()
            .filter(|m| m.active && !m.closed)
            .filter(|m| {
                if let Some(end_date) = m.end_date_iso.as_ref() {
                    if let Ok(end) = DateTime::parse_from_rfc3339(end_date) {
                        return end.with_timezone(&Utc) > now;
                    }
                }
                false
            })
            .min_by_key(|m| {
                m.end_date_iso
                    .as_ref()
                    .and_then(|d| DateTime::parse_from_rfc3339(d).ok())
                    .map(|d| d.timestamp())
                    .unwrap_or(i64::MAX)
            });

        Ok(next_market)
    }

    // ========================================================================
    // Related Markets
    // ========================================================================

    /// Get markets related to a given market
    pub async fn get_related_markets(&self, market_id: &str) -> Result<Vec<GammaMarket>> {
        let market = self.get_market(market_id).await?;

        // Get markets with similar tags
        let mut related = Vec::new();
        for tag in &market.tags {
            let tagged_markets = self.get_markets_by_tag(tag).await?;
            for m in tagged_markets {
                if m.condition_id != market.condition_id && !related.iter().any(|r: &GammaMarket| r.condition_id == m.condition_id) {
                    related.push(m);
                }
            }
        }

        Ok(related)
    }
}

impl std::fmt::Debug for GammaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GammaClient")
            .field("base_url", &self.base_url)
            .finish()
    }
}

// ============================================================================
// Response Types
// ============================================================================

#[derive(Debug, Deserialize)]
struct GammaMarketsResponse {
    #[serde(default)]
    data: Vec<GammaMarket>,
}

#[derive(Debug, Deserialize)]
struct GammaEventsResponse {
    #[serde(default)]
    data: Vec<GammaEvent>,
}

/// Market data from Gamma API
#[derive(Debug, Clone, Deserialize)]
pub struct GammaMarket {
    #[serde(rename = "id")]
    pub id: String,

    #[serde(rename = "conditionId")]
    pub condition_id: String,

    pub slug: String,
    pub question: String,

    #[serde(default)]
    pub description: Option<String>,

    #[serde(default)]
    pub outcomes: Vec<String>,

    #[serde(rename = "outcomePrices", default)]
    pub outcome_prices: Vec<String>,

    #[serde(rename = "tokens", default)]
    pub tokens: Vec<GammaToken>,

    pub active: bool,
    pub closed: bool,

    #[serde(rename = "endDateIso")]
    pub end_date_iso: Option<String>,

    #[serde(rename = "createdAt")]
    pub created_at: Option<String>,

    #[serde(rename = "volume", default)]
    pub volume: String,

    #[serde(rename = "volumeNum", default)]
    pub volume_num: f64,

    #[serde(rename = "liquidity", default)]
    pub liquidity: String,

    #[serde(rename = "liquidityNum", default)]
    pub liquidity_num: f64,

    #[serde(default)]
    pub tags: Vec<String>,

    #[serde(rename = "marketType", default)]
    pub market_type: Option<String>,

    #[serde(rename = "enableOrderBook", default)]
    pub enable_order_book: bool,

    #[serde(rename = "negRisk", default)]
    pub neg_risk: bool,
}

impl GammaMarket {
    /// Convert to internal Market type
    pub fn to_market(&self) -> Result<Market> {
        let market_type = if self.outcomes.len() == 2 {
            MarketType::Binary
        } else {
            MarketType::MultiOutcome {
                num_outcomes: self.outcomes.len() as u8,
            }
        };

        let status = if self.closed {
            MarketStatus::Closed
        } else if self.active {
            MarketStatus::Active
        } else {
            MarketStatus::Paused
        };

        let mut outcomes = Vec::new();
        for (i, outcome_name) in self.outcomes.iter().enumerate() {
            let token_id = self
                .tokens
                .get(i)
                .map(|t| t.token_id.clone())
                .unwrap_or_default();

            let price = self
                .outcome_prices
                .get(i)
                .and_then(|p| p.parse::<Decimal>().ok())
                .and_then(Price::new)
                .unwrap_or(Price::ZERO);

            let outcome = if i == 0 {
                Outcome::Yes
            } else {
                Outcome::No
            };

            outcomes.push(OutcomeInfo {
                outcome,
                token_id,
                price,
                name: outcome_name.clone(),
            });
        }

        let end_date = self
            .end_date_iso
            .as_ref()
            .and_then(|d| DateTime::parse_from_rfc3339(d).ok())
            .map(|d| d.with_timezone(&Utc));

        let created_at = self
            .created_at
            .as_ref()
            .and_then(|d| DateTime::parse_from_rfc3339(d).ok())
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);

        let volume = self
            .volume
            .parse::<Decimal>()
            .unwrap_or(Decimal::from_f64_retain(self.volume_num).unwrap_or(Decimal::ZERO));

        let liquidity = self
            .liquidity
            .parse::<Decimal>()
            .unwrap_or(Decimal::from_f64_retain(self.liquidity_num).unwrap_or(Decimal::ZERO));

        Ok(Market {
            id: self.id.clone(),
            condition_id: self.condition_id.clone(),
            slug: self.slug.clone(),
            question: self.question.clone(),
            description: self.description.clone(),
            market_type,
            status,
            outcomes,
            end_date,
            created_at,
            volume,
            liquidity,
            fee_rate: rust_decimal_macros::dec!(0.02),
            tags: self.tags.clone(),
            related_markets: Vec::new(),
        })
    }

    /// Get YES token ID
    pub fn yes_token_id(&self) -> Option<&str> {
        self.tokens.first().map(|t| t.token_id.as_str())
    }

    /// Get NO token ID
    pub fn no_token_id(&self) -> Option<&str> {
        self.tokens.get(1).map(|t| t.token_id.as_str())
    }

    /// Check if this is a 5-minute market
    pub fn is_5min_market(&self) -> bool {
        self.slug.contains("-5m-") || self.slug.contains("5min")
    }
}

/// Token data from Gamma API
#[derive(Debug, Clone, Deserialize)]
pub struct GammaToken {
    #[serde(rename = "token_id")]
    pub token_id: String,

    pub outcome: String,

    #[serde(default)]
    pub price: Option<String>,

    #[serde(default)]
    pub winner: bool,
}

/// Event data from Gamma API
#[derive(Debug, Clone, Deserialize)]
pub struct GammaEvent {
    pub id: String,
    pub slug: String,
    pub title: String,

    #[serde(default)]
    pub description: Option<String>,

    #[serde(rename = "startDate")]
    pub start_date: Option<String>,

    #[serde(rename = "endDate")]
    pub end_date: Option<String>,

    #[serde(default)]
    pub markets: Vec<GammaMarket>,

    #[serde(default)]
    pub tags: Vec<String>,
}

/// Price from Gamma API
#[derive(Debug, Clone, Deserialize)]
pub struct GammaPrice {
    #[serde(rename = "tokenId")]
    pub token_id: String,

    pub price: String,

    #[serde(rename = "timestamp")]
    pub timestamp: Option<i64>,
}

/// Price point for historical data
#[derive(Debug, Clone, Deserialize)]
pub struct PricePoint {
    #[serde(rename = "t")]
    pub timestamp: i64,

    #[serde(rename = "p")]
    pub price: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gamma_market_conversion() {
        let gamma_market = GammaMarket {
            id: "123".to_string(),
            condition_id: "0xabc".to_string(),
            slug: "test-market".to_string(),
            question: "Will this test pass?".to_string(),
            description: Some("A test market".to_string()),
            outcomes: vec!["Yes".to_string(), "No".to_string()],
            outcome_prices: vec!["0.6".to_string(), "0.4".to_string()],
            tokens: vec![
                GammaToken {
                    token_id: "token1".to_string(),
                    outcome: "Yes".to_string(),
                    price: Some("0.6".to_string()),
                    winner: false,
                },
                GammaToken {
                    token_id: "token2".to_string(),
                    outcome: "No".to_string(),
                    price: Some("0.4".to_string()),
                    winner: false,
                },
            ],
            active: true,
            closed: false,
            end_date_iso: None,
            created_at: None,
            volume: "1000".to_string(),
            volume_num: 1000.0,
            liquidity: "500".to_string(),
            liquidity_num: 500.0,
            tags: vec!["test".to_string()],
            market_type: Some("binary".to_string()),
            enable_order_book: true,
            neg_risk: false,
        };

        let market = gamma_market.to_market().unwrap();

        assert_eq!(market.slug, "test-market");
        assert!(market.is_binary());
        assert!(market.is_active());
        assert_eq!(market.outcomes.len(), 2);
    }

    #[test]
    fn test_5min_market_detection() {
        let market = GammaMarket {
            slug: "btc-updown-5m-1234567890".to_string(),
            ..Default::default()
        };

        assert!(market.is_5min_market());
    }
}

impl Default for GammaMarket {
    fn default() -> Self {
        Self {
            id: String::new(),
            condition_id: String::new(),
            slug: String::new(),
            question: String::new(),
            description: None,
            outcomes: Vec::new(),
            outcome_prices: Vec::new(),
            tokens: Vec::new(),
            active: false,
            closed: false,
            end_date_iso: None,
            created_at: None,
            volume: String::new(),
            volume_num: 0.0,
            liquidity: String::new(),
            liquidity_num: 0.0,
            tags: Vec::new(),
            market_type: None,
            enable_order_book: false,
            neg_risk: false,
        }
    }
}
