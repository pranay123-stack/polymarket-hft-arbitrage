//! Market mapper service for cross-venue market discovery.
//!
//! Orchestrates:
//! - Venue scanning
//! - Semantic matching
//! - Mapping storage and verification
//! - Real-time mapping updates

use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

use super::semantic_matcher::{SemanticMatcher, MatchCandidate, MatcherConfig, MarketForMatching};
use super::venue_scanner::{VenueScanner, DiscoveredMarket, MarketCategory};
use super::mapping_store::{MappingStore, StoredMapping, MappingStatus};

/// A verified market mapping for trading.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketMapping {
    /// Source venue.
    pub source_venue: String,
    /// Source market ID.
    pub source_market_id: String,
    /// Source market title.
    pub source_title: String,
    /// Target venue.
    pub target_venue: String,
    /// Target market ID.
    pub target_market_id: String,
    /// Target market title.
    pub target_title: String,
    /// Match confidence score.
    pub confidence: f64,
    /// Whether this mapping is verified for trading.
    pub verified: bool,
    /// Category of the market.
    pub category: Option<String>,
}

pub use super::mapping_store::MappingStatus;

/// Configuration for the market mapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketMapperConfig {
    /// Scan interval in milliseconds.
    pub scan_interval_ms: u64,
    /// Minimum score for auto-verification.
    pub auto_verify_threshold: f64,
    /// Whether to auto-verify high-confidence matches.
    pub auto_verify_enabled: bool,
    /// Categories to scan.
    pub categories: Vec<MarketCategory>,
    /// Matcher configuration.
    pub matcher_config: MatcherConfig,
    /// Database connection string.
    pub database_url: String,
}

impl Default for MarketMapperConfig {
    fn default() -> Self {
        Self {
            scan_interval_ms: 300000, // 5 minutes
            auto_verify_threshold: 0.90,
            auto_verify_enabled: true,
            categories: vec![
                MarketCategory::Politics,
                MarketCategory::Crypto,
                MarketCategory::Finance,
                MarketCategory::Sports,
            ],
            matcher_config: MatcherConfig::default(),
            database_url: "postgres://localhost/arbitrage".to_string(),
        }
    }
}

/// The market mapper service.
pub struct MarketMapper {
    /// Configuration.
    config: MarketMapperConfig,
    /// Venue scanner.
    scanner: Arc<RwLock<VenueScanner>>,
    /// Semantic matcher.
    matcher: SemanticMatcher,
    /// Mapping store.
    store: Arc<RwLock<MappingStore>>,
    /// Discovered markets cache.
    discovered_markets: Arc<RwLock<Vec<DiscoveredMarket>>>,
    /// Active mappings cache.
    active_mappings: Arc<RwLock<Vec<MarketMapping>>>,
    /// Running flag.
    running: Arc<RwLock<bool>>,
}

impl MarketMapper {
    /// Create a new market mapper.
    pub fn new(config: MarketMapperConfig) -> Self {
        let scanner = VenueScanner::new(config.scan_interval_ms);
        let matcher = SemanticMatcher::new(config.matcher_config.clone());
        let store = MappingStore::new(&config.database_url);

        Self {
            config,
            scanner: Arc::new(RwLock::new(scanner)),
            matcher,
            store: Arc::new(RwLock::new(store)),
            discovered_markets: Arc::new(RwLock::new(Vec::new())),
            active_mappings: Arc::new(RwLock::new(Vec::new())),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Initialize the mapper (connect to database, etc.).
    pub async fn initialize(&self) -> Result<(), String> {
        self.store.write().await.connect().await
            .map_err(|e| format!("Database connection failed: {}", e))?;

        // Load existing mappings
        self.refresh_mappings().await?;

        Ok(())
    }

    /// Start the background scanning service.
    pub async fn start(&self) {
        *self.running.write().await = true;

        let running = self.running.clone();
        let scanner = self.scanner.clone();
        let discovered = self.discovered_markets.clone();
        let store = self.store.clone();
        let active = self.active_mappings.clone();
        let config = self.config.clone();
        let matcher = SemanticMatcher::new(self.config.matcher_config.clone());

        tokio::spawn(async move {
            while *running.read().await {
                // Scan venues
                let markets = {
                    let scanner = scanner.read().await;
                    scanner.scan_all().await
                };

                if let Ok(markets) = markets {
                    // Update discovered markets
                    *discovered.write().await = markets.clone();

                    // Find new matches
                    let new_matches = Self::find_matches_static(&matcher, &markets);

                    // Store new mappings
                    {
                        let mut store = store.write().await;
                        for candidate in new_matches {
                            if candidate.score >= config.auto_verify_threshold {
                                let _ = store.store_mapping(&candidate).await;
                            }
                        }
                    }

                    // Refresh active mappings
                    let verified = store.read().await.get_verified_mappings()
                        .iter()
                        .map(|m| MarketMapping {
                            source_venue: m.source_venue.clone(),
                            source_market_id: m.source_market_id.clone(),
                            source_title: String::new(), // Would need to lookup
                            target_venue: m.target_venue.clone(),
                            target_market_id: m.target_market_id.clone(),
                            target_title: String::new(),
                            confidence: m.score,
                            verified: true,
                            category: None,
                        })
                        .collect();

                    *active.write().await = verified;
                }

                // Wait for next scan
                tokio::time::sleep(tokio::time::Duration::from_millis(config.scan_interval_ms)).await;
            }
        });
    }

    /// Stop the background service.
    pub async fn stop(&self) {
        *self.running.write().await = false;
    }

    /// Find matches between discovered markets.
    fn find_matches_static(matcher: &SemanticMatcher, markets: &[DiscoveredMarket]) -> Vec<MatchCandidate> {
        let mut all_candidates = Vec::new();

        // Convert to matching format
        let matching_markets: Vec<MarketForMatching> = markets
            .iter()
            .map(|m| MarketForMatching {
                id: m.venue_id.clone(),
                venue: m.venue.clone(),
                title: m.title.clone(),
                description: m.description.clone(),
                category: Some(format!("{:?}", m.category)),
                outcomes: m.outcomes.iter().map(|o| o.name.clone()).collect(),
                resolution_date: m.resolution_date.clone(),
                created_at: m.created_at.clone(),
            })
            .collect();

        // Find matches for each market
        for source in &matching_markets {
            let candidates = matcher.find_matches(source, &matching_markets);
            all_candidates.extend(candidates);
        }

        // Deduplicate (keep highest score for each pair)
        let mut best_matches: std::collections::HashMap<(String, String), MatchCandidate> = std::collections::HashMap::new();

        for candidate in all_candidates {
            let key = if candidate.source_id < candidate.target_id {
                (candidate.source_id.clone(), candidate.target_id.clone())
            } else {
                (candidate.target_id.clone(), candidate.source_id.clone())
            };

            best_matches
                .entry(key)
                .and_modify(|existing| {
                    if candidate.score > existing.score {
                        *existing = candidate.clone();
                    }
                })
                .or_insert(candidate);
        }

        best_matches.into_values().collect()
    }

    /// Refresh active mappings from storage.
    async fn refresh_mappings(&self) -> Result<(), String> {
        let store = self.store.read().await;
        let verified = store.get_verified_mappings();

        let mappings: Vec<MarketMapping> = verified
            .iter()
            .map(|m| MarketMapping {
                source_venue: m.source_venue.clone(),
                source_market_id: m.source_market_id.clone(),
                source_title: String::new(),
                target_venue: m.target_venue.clone(),
                target_market_id: m.target_market_id.clone(),
                target_title: String::new(),
                confidence: m.score,
                verified: true,
                category: None,
            })
            .collect();

        *self.active_mappings.write().await = mappings;
        Ok(())
    }

    /// Get all active (verified) mappings.
    pub async fn get_active_mappings(&self) -> Vec<MarketMapping> {
        self.active_mappings.read().await.clone()
    }

    /// Get mapping for a specific source market.
    pub async fn get_mapping(&self, source_venue: &str, source_market_id: &str) -> Option<MarketMapping> {
        self.active_mappings
            .read()
            .await
            .iter()
            .find(|m| m.source_venue == source_venue && m.source_market_id == source_market_id)
            .cloned()
    }

    /// Get all mappings for a venue.
    pub async fn get_mappings_for_venue(&self, venue: &str) -> Vec<MarketMapping> {
        self.active_mappings
            .read()
            .await
            .iter()
            .filter(|m| m.source_venue == venue || m.target_venue == venue)
            .cloned()
            .collect()
    }

    /// Manually add a mapping.
    pub async fn add_mapping(
        &self,
        source_venue: &str,
        source_market_id: &str,
        target_venue: &str,
        target_market_id: &str,
        verified_by: &str,
    ) -> Result<MarketMapping, String> {
        use super::semantic_matcher::MatchConfidence;

        let candidate = MatchCandidate {
            source_id: source_market_id.to_string(),
            target_id: target_market_id.to_string(),
            source_venue: source_venue.to_string(),
            target_venue: target_venue.to_string(),
            score: 1.0, // Manual mappings are 100% confident
            score_breakdown: Default::default(),
            confidence: MatchConfidence::High,
            match_reasons: vec!["Manual mapping".to_string()],
        };

        let stored = self.store.write().await.store_mapping(&candidate).await
            .map_err(|e| format!("Failed to store mapping: {}", e))?;

        // Verify immediately
        self.store.write().await.verify_mapping(stored.id, verified_by, true).await
            .map_err(|e| format!("Failed to verify mapping: {}", e))?;

        let mapping = MarketMapping {
            source_venue: source_venue.to_string(),
            source_market_id: source_market_id.to_string(),
            source_title: String::new(),
            target_venue: target_venue.to_string(),
            target_market_id: target_market_id.to_string(),
            target_title: String::new(),
            confidence: 1.0,
            verified: true,
            category: None,
        };

        self.active_mappings.write().await.push(mapping.clone());

        Ok(mapping)
    }

    /// Remove a mapping.
    pub async fn remove_mapping(&self, source_venue: &str, source_market_id: &str) -> Result<(), String> {
        self.store.write().await.expire_mapping(source_venue, source_market_id).await
            .map_err(|e| format!("Failed to expire mapping: {}", e))?;

        self.active_mappings.write().await.retain(|m| {
            !(m.source_venue == source_venue && m.source_market_id == source_market_id)
        });

        Ok(())
    }

    /// Get discovered markets.
    pub async fn get_discovered_markets(&self) -> Vec<DiscoveredMarket> {
        self.discovered_markets.read().await.clone()
    }

    /// Get pending mappings for review.
    pub async fn get_pending_mappings(&self) -> Result<Vec<StoredMapping>, String> {
        self.store.read().await.get_pending_mappings().await
            .map_err(|e| format!("Failed to get pending mappings: {}", e))
    }

    /// Verify a pending mapping.
    pub async fn verify_mapping(
        &self,
        mapping_id: uuid::Uuid,
        verified_by: &str,
        approve: bool,
    ) -> Result<(), String> {
        self.store.write().await.verify_mapping(mapping_id, verified_by, approve).await
            .map_err(|e| format!("Failed to verify mapping: {}", e))?;

        // Refresh active mappings if approved
        if approve {
            self.refresh_mappings().await?;
        }

        Ok(())
    }

    /// Get mapping statistics.
    pub async fn get_stats(&self) -> MappingStatistics {
        let store_stats = self.store.read().await.get_stats();
        let discovered_count = self.discovered_markets.read().await.len();
        let active_count = self.active_mappings.read().await.len();

        MappingStatistics {
            total_discovered_markets: discovered_count,
            active_mappings: active_count,
            auto_verified: store_stats.auto_verified,
            manually_verified: store_stats.manually_verified,
            pending_review: store_stats.pending,
            rejected: store_stats.rejected,
            avg_confidence: store_stats.avg_score,
        }
    }
}

/// Statistics about market mappings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingStatistics {
    pub total_discovered_markets: usize,
    pub active_mappings: usize,
    pub auto_verified: usize,
    pub manually_verified: usize,
    pub pending_review: usize,
    pub rejected: usize,
    pub avg_confidence: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mapper_creation() {
        let config = MarketMapperConfig::default();
        let mapper = MarketMapper::new(config);
        assert!(mapper.config.auto_verify_enabled);
    }

    #[tokio::test]
    async fn test_empty_mappings() {
        let config = MarketMapperConfig::default();
        let mapper = MarketMapper::new(config);

        let mappings = mapper.get_active_mappings().await;
        assert!(mappings.is_empty());
    }
}
