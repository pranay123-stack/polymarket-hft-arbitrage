//! Venue scanner for discovering markets from prediction market APIs.
//!
//! Scans Polymarket, Kalshi, and Opinion APIs to discover active markets
//! and normalize them for matching.

use std::collections::HashMap;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use std::sync::Arc;

/// Category of a prediction market.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketCategory {
    Politics,
    Sports,
    Crypto,
    Finance,
    Entertainment,
    Science,
    Weather,
    Technology,
    Other(String),
}

impl MarketCategory {
    pub fn from_string(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "politics" | "political" | "elections" | "government" => MarketCategory::Politics,
            "sports" | "sporting" | "athletics" => MarketCategory::Sports,
            "crypto" | "cryptocurrency" | "bitcoin" | "blockchain" => MarketCategory::Crypto,
            "finance" | "financial" | "markets" | "stocks" | "economics" => MarketCategory::Finance,
            "entertainment" | "movies" | "tv" | "celebrities" => MarketCategory::Entertainment,
            "science" | "scientific" | "research" => MarketCategory::Science,
            "weather" | "climate" => MarketCategory::Weather,
            "tech" | "technology" => MarketCategory::Technology,
            other => MarketCategory::Other(other.to_string()),
        }
    }
}

/// A discovered market from a venue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredMarket {
    /// Unique identifier on the venue.
    pub venue_id: String,
    /// Venue name.
    pub venue: String,
    /// Market title.
    pub title: String,
    /// Market description.
    pub description: String,
    /// Category.
    pub category: MarketCategory,
    /// Possible outcomes.
    pub outcomes: Vec<OutcomeInfo>,
    /// Resolution date (if known).
    pub resolution_date: Option<String>,
    /// Creation date.
    pub created_at: Option<String>,
    /// Current status.
    pub status: MarketStatus,
    /// Total volume traded.
    pub volume: Option<f64>,
    /// Current liquidity.
    pub liquidity: Option<f64>,
    /// Additional metadata.
    pub metadata: HashMap<String, String>,
}

/// Information about a market outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutcomeInfo {
    pub id: String,
    pub name: String,
    pub current_price: Option<f64>,
}

/// Market status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketStatus {
    Active,
    Paused,
    Closed,
    Resolved,
    Unknown,
}

/// Trait for venue-specific market scanners.
#[async_trait]
pub trait VenueScannerTrait: Send + Sync {
    /// Get venue name.
    fn venue_name(&self) -> &str;

    /// Scan for all active markets.
    async fn scan_markets(&self) -> Result<Vec<DiscoveredMarket>, ScanError>;

    /// Scan markets in a specific category.
    async fn scan_category(&self, category: MarketCategory) -> Result<Vec<DiscoveredMarket>, ScanError>;

    /// Get market details by ID.
    async fn get_market(&self, market_id: &str) -> Result<Option<DiscoveredMarket>, ScanError>;

    /// Search markets by query.
    async fn search_markets(&self, query: &str) -> Result<Vec<DiscoveredMarket>, ScanError>;
}

/// Scan error types.
#[derive(Debug, Clone)]
pub enum ScanError {
    NetworkError(String),
    RateLimited,
    ParseError(String),
    AuthenticationError,
    VenueUnavailable,
}

impl std::fmt::Display for ScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScanError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            ScanError::RateLimited => write!(f, "Rate limited"),
            ScanError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            ScanError::AuthenticationError => write!(f, "Authentication error"),
            ScanError::VenueUnavailable => write!(f, "Venue unavailable"),
        }
    }
}

impl std::error::Error for ScanError {}

/// Unified venue scanner that aggregates multiple venue scanners.
pub struct VenueScanner {
    /// Individual venue scanners.
    scanners: Vec<Arc<dyn VenueScannerTrait>>,
    /// Cache of discovered markets.
    market_cache: Arc<RwLock<HashMap<String, DiscoveredMarket>>>,
    /// Last scan timestamp per venue.
    last_scan: Arc<RwLock<HashMap<String, u64>>>,
    /// Minimum interval between scans (milliseconds).
    scan_interval_ms: u64,
}

impl VenueScanner {
    /// Create a new venue scanner.
    pub fn new(scan_interval_ms: u64) -> Self {
        Self {
            scanners: Vec::new(),
            market_cache: Arc::new(RwLock::new(HashMap::new())),
            last_scan: Arc::new(RwLock::new(HashMap::new())),
            scan_interval_ms,
        }
    }

    /// Add a venue scanner.
    pub fn add_scanner(&mut self, scanner: Arc<dyn VenueScannerTrait>) {
        self.scanners.push(scanner);
    }

    /// Scan all venues for markets.
    pub async fn scan_all(&self) -> Result<Vec<DiscoveredMarket>, Vec<(String, ScanError)>> {
        let mut all_markets = Vec::new();
        let mut errors = Vec::new();

        for scanner in &self.scanners {
            let venue = scanner.venue_name().to_string();

            // Check if we need to scan (respect rate limit)
            let should_scan = {
                let last_scan = self.last_scan.read().await;
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;

                match last_scan.get(&venue) {
                    Some(last) => now - last >= self.scan_interval_ms,
                    None => true,
                }
            };

            if !should_scan {
                // Return cached markets
                let cache = self.market_cache.read().await;
                let cached: Vec<_> = cache
                    .values()
                    .filter(|m| m.venue == venue)
                    .cloned()
                    .collect();
                all_markets.extend(cached);
                continue;
            }

            match scanner.scan_markets().await {
                Ok(markets) => {
                    // Update cache
                    {
                        let mut cache = self.market_cache.write().await;
                        for market in &markets {
                            let key = format!("{}:{}", market.venue, market.venue_id);
                            cache.insert(key, market.clone());
                        }
                    }

                    // Update last scan time
                    {
                        let mut last_scan = self.last_scan.write().await;
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64;
                        last_scan.insert(venue, now);
                    }

                    all_markets.extend(markets);
                }
                Err(e) => {
                    errors.push((scanner.venue_name().to_string(), e));
                }
            }
        }

        if errors.is_empty() {
            Ok(all_markets)
        } else if all_markets.is_empty() {
            Err(errors)
        } else {
            // Partial success - return what we got
            Ok(all_markets)
        }
    }

    /// Scan for markets in a specific category.
    pub async fn scan_category(&self, category: MarketCategory) -> Result<Vec<DiscoveredMarket>, Vec<(String, ScanError)>> {
        let mut all_markets = Vec::new();
        let mut errors = Vec::new();

        for scanner in &self.scanners {
            match scanner.scan_category(category.clone()).await {
                Ok(markets) => all_markets.extend(markets),
                Err(e) => errors.push((scanner.venue_name().to_string(), e)),
            }
        }

        if errors.is_empty() || !all_markets.is_empty() {
            Ok(all_markets)
        } else {
            Err(errors)
        }
    }

    /// Search markets across all venues.
    pub async fn search(&self, query: &str) -> Result<Vec<DiscoveredMarket>, Vec<(String, ScanError)>> {
        let mut all_markets = Vec::new();
        let mut errors = Vec::new();

        for scanner in &self.scanners {
            match scanner.search_markets(query).await {
                Ok(markets) => all_markets.extend(markets),
                Err(e) => errors.push((scanner.venue_name().to_string(), e)),
            }
        }

        if errors.is_empty() || !all_markets.is_empty() {
            Ok(all_markets)
        } else {
            Err(errors)
        }
    }

    /// Get a specific market by venue and ID.
    pub async fn get_market(&self, venue: &str, market_id: &str) -> Option<DiscoveredMarket> {
        // Check cache first
        let cache_key = format!("{}:{}", venue, market_id);
        {
            let cache = self.market_cache.read().await;
            if let Some(market) = cache.get(&cache_key) {
                return Some(market.clone());
            }
        }

        // Fetch from venue
        for scanner in &self.scanners {
            if scanner.venue_name() == venue {
                if let Ok(Some(market)) = scanner.get_market(market_id).await {
                    // Update cache
                    {
                        let mut cache = self.market_cache.write().await;
                        cache.insert(cache_key, market.clone());
                    }
                    return Some(market);
                }
            }
        }

        None
    }

    /// Get all cached markets.
    pub async fn cached_markets(&self) -> Vec<DiscoveredMarket> {
        self.market_cache.read().await.values().cloned().collect()
    }

    /// Clear the cache.
    pub async fn clear_cache(&self) {
        self.market_cache.write().await.clear();
        self.last_scan.write().await.clear();
    }
}

/// Polymarket scanner implementation.
pub struct PolymarketScanner {
    api_base: String,
    http_client: reqwest::Client,
}

impl PolymarketScanner {
    pub fn new(api_base: &str) -> Self {
        Self {
            api_base: api_base.to_string(),
            http_client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl VenueScannerTrait for PolymarketScanner {
    fn venue_name(&self) -> &str {
        "polymarket"
    }

    async fn scan_markets(&self) -> Result<Vec<DiscoveredMarket>, ScanError> {
        // In production, this would call the Polymarket Gamma API
        // For now, return empty to demonstrate the interface
        let _url = format!("{}/markets", self.api_base);

        // Example API call (commented out for compilation):
        // let response = self.http_client.get(&url).send().await
        //     .map_err(|e| ScanError::NetworkError(e.to_string()))?;

        Ok(Vec::new())
    }

    async fn scan_category(&self, _category: MarketCategory) -> Result<Vec<DiscoveredMarket>, ScanError> {
        Ok(Vec::new())
    }

    async fn get_market(&self, _market_id: &str) -> Result<Option<DiscoveredMarket>, ScanError> {
        Ok(None)
    }

    async fn search_markets(&self, _query: &str) -> Result<Vec<DiscoveredMarket>, ScanError> {
        Ok(Vec::new())
    }
}

/// Kalshi scanner implementation.
pub struct KalshiScanner {
    api_base: String,
    http_client: reqwest::Client,
    api_key: Option<String>,
}

impl KalshiScanner {
    pub fn new(api_base: &str, api_key: Option<String>) -> Self {
        Self {
            api_base: api_base.to_string(),
            http_client: reqwest::Client::new(),
            api_key,
        }
    }
}

#[async_trait]
impl VenueScannerTrait for KalshiScanner {
    fn venue_name(&self) -> &str {
        "kalshi"
    }

    async fn scan_markets(&self) -> Result<Vec<DiscoveredMarket>, ScanError> {
        // In production, this would call the Kalshi API
        let _url = format!("{}/markets", self.api_base);
        Ok(Vec::new())
    }

    async fn scan_category(&self, _category: MarketCategory) -> Result<Vec<DiscoveredMarket>, ScanError> {
        Ok(Vec::new())
    }

    async fn get_market(&self, _market_id: &str) -> Result<Option<DiscoveredMarket>, ScanError> {
        Ok(None)
    }

    async fn search_markets(&self, _query: &str) -> Result<Vec<DiscoveredMarket>, ScanError> {
        Ok(Vec::new())
    }
}

/// Opinion scanner implementation.
pub struct OpinionScanner {
    api_base: String,
    http_client: reqwest::Client,
}

impl OpinionScanner {
    pub fn new(api_base: &str) -> Self {
        Self {
            api_base: api_base.to_string(),
            http_client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl VenueScannerTrait for OpinionScanner {
    fn venue_name(&self) -> &str {
        "opinion"
    }

    async fn scan_markets(&self) -> Result<Vec<DiscoveredMarket>, ScanError> {
        let _url = format!("{}/markets", self.api_base);
        Ok(Vec::new())
    }

    async fn scan_category(&self, _category: MarketCategory) -> Result<Vec<DiscoveredMarket>, ScanError> {
        Ok(Vec::new())
    }

    async fn get_market(&self, _market_id: &str) -> Result<Option<DiscoveredMarket>, ScanError> {
        Ok(None)
    }

    async fn search_markets(&self, _query: &str) -> Result<Vec<DiscoveredMarket>, ScanError> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_category_parsing() {
        assert_eq!(MarketCategory::from_string("politics"), MarketCategory::Politics);
        assert_eq!(MarketCategory::from_string("CRYPTO"), MarketCategory::Crypto);
        assert_eq!(MarketCategory::from_string("sports"), MarketCategory::Sports);
    }

    #[tokio::test]
    async fn test_venue_scanner_creation() {
        let scanner = VenueScanner::new(60000);
        assert!(scanner.cached_markets().await.is_empty());
    }
}
