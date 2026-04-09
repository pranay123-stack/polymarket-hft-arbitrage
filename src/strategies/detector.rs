//! Main arbitrage detection coordinator.
//!
//! Coordinates multiple arbitrage strategies and manages opportunity lifecycle.

use crate::core::config::StrategyConfig;
use crate::core::constants::*;
use crate::core::error::Result;
use crate::core::events::{Event, EventBus};
use crate::core::types::*;
use crate::orderbook::OrderbookEngine;
use crate::strategies::{
    BinaryArbitrageStrategy, CrossMarketStrategy, MultiOutcomeStrategy, TemporalStrategy,
};
use chrono::{Duration, Utc};
use dashmap::DashMap;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

/// Coordinates arbitrage detection across multiple strategies
pub struct ArbitrageDetector {
    config: StrategyConfig,
    orderbook_engine: Arc<OrderbookEngine>,
    event_bus: EventBus,

    // Strategies
    binary_strategy: Option<BinaryArbitrageStrategy>,
    multi_outcome_strategy: Option<MultiOutcomeStrategy>,
    cross_market_strategy: Option<CrossMarketStrategy>,
    temporal_strategy: Option<TemporalStrategy>,

    // Active opportunities
    opportunities: DashMap<Uuid, ArbitrageOpportunity>,

    // Market registry
    markets: Arc<RwLock<Vec<Market>>>,

    // Statistics
    stats: Arc<RwLock<DetectorStats>>,
}

impl ArbitrageDetector {
    /// Create a new arbitrage detector
    pub fn new(
        config: StrategyConfig,
        orderbook_engine: Arc<OrderbookEngine>,
        event_bus: EventBus,
    ) -> Self {
        let binary_strategy = if config.binary_mispricing {
            Some(BinaryArbitrageStrategy::new(
                Arc::clone(&orderbook_engine),
                config.binary_threshold,
            ))
        } else {
            None
        };

        let multi_outcome_strategy = if config.multi_outcome {
            Some(MultiOutcomeStrategy::new(Arc::clone(&orderbook_engine)))
        } else {
            None
        };

        let cross_market_strategy = if config.cross_market {
            Some(CrossMarketStrategy::new(
                Arc::clone(&orderbook_engine),
                config.cross_market_threshold,
            ))
        } else {
            None
        };

        let temporal_strategy = if config.temporal {
            Some(TemporalStrategy::new(Arc::clone(&orderbook_engine)))
        } else {
            None
        };

        Self {
            config,
            orderbook_engine,
            event_bus,
            binary_strategy,
            multi_outcome_strategy,
            cross_market_strategy,
            temporal_strategy,
            opportunities: DashMap::new(),
            markets: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(DetectorStats::default())),
        }
    }

    /// Update the market registry
    pub async fn update_markets(&self, markets: Vec<Market>) {
        let mut registry = self.markets.write().await;
        *registry = markets;
    }

    /// Scan all markets for arbitrage opportunities
    #[instrument(skip(self))]
    pub async fn scan(&self) -> Vec<ArbitrageOpportunity> {
        let markets = self.markets.read().await;
        let mut opportunities = Vec::new();

        // Filter to active markets with sufficient volume
        let active_markets: Vec<_> = markets
            .iter()
            .filter(|m| {
                m.is_active()
                    && m.volume >= self.config.min_market_volume
                    && !self.is_excluded(&m.slug)
                    && self.is_focused(&m.slug)
            })
            .collect();

        debug!("Scanning {} active markets", active_markets.len());

        // Run binary arbitrage detection
        if let Some(ref strategy) = self.binary_strategy {
            for market in &active_markets {
                if market.is_binary() {
                    if let Some(opp) = strategy.detect(market).await {
                        opportunities.push(opp);
                    }
                }
            }
        }

        // Run multi-outcome arbitrage detection
        if let Some(ref strategy) = self.multi_outcome_strategy {
            for market in &active_markets {
                if !market.is_binary() {
                    if let Some(opp) = strategy.detect(market).await {
                        opportunities.push(opp);
                    }
                }
            }
        }

        // Run cross-market arbitrage detection
        if let Some(ref strategy) = self.cross_market_strategy {
            // Group markets by tags for correlation analysis
            let related_groups = self.group_related_markets(&active_markets);
            for group in related_groups {
                if let Some(opps) = strategy.detect_in_group(&group).await {
                    opportunities.extend(opps);
                }
            }
        }

        // Run temporal arbitrage detection (5-min markets)
        if let Some(ref strategy) = self.temporal_strategy {
            let temporal_markets: Vec<_> = active_markets
                .iter()
                .filter(|m| {
                    m.slug.contains("-5m-")
                        || m.slug.contains("5min")
                        || m.slug.contains("updown")
                })
                .collect();

            for market in temporal_markets {
                if let Some(opp) = strategy.detect(market).await {
                    opportunities.push(opp);
                }
            }
        }

        // Update statistics
        {
            let mut stats = self.stats.write().await;
            stats.scans_performed += 1;
            stats.opportunities_found += opportunities.len() as u64;
            stats.last_scan = Utc::now();
        }

        // Store and emit opportunities
        for opp in &opportunities {
            self.opportunities.insert(opp.id, opp.clone());
            self.event_bus.publish(Event::arbitrage_opportunity(opp.clone()));
        }

        // Clean up expired opportunities
        self.cleanup_expired();

        opportunities
    }

    /// Check if a market is excluded
    fn is_excluded(&self, slug: &str) -> bool {
        self.config
            .exclude_markets
            .iter()
            .any(|e| slug.contains(e))
    }

    /// Check if a market is in the focus list (or if list is empty)
    fn is_focused(&self, slug: &str) -> bool {
        self.config.focus_markets.is_empty()
            || self.config.focus_markets.iter().any(|f| slug.contains(f))
    }

    /// Group markets by related tags
    fn group_related_markets<'a>(&self, markets: &[&'a Market]) -> Vec<Vec<&'a Market>> {
        let mut groups: Vec<Vec<&'a Market>> = Vec::new();

        for market in markets {
            let mut found_group = false;

            for group in &mut groups {
                // Check if this market shares tags with the group
                if let Some(first) = group.first() {
                    let shared_tags = market
                        .tags
                        .iter()
                        .any(|t| first.tags.contains(t));

                    if shared_tags {
                        group.push(market);
                        found_group = true;
                        break;
                    }
                }
            }

            if !found_group && !market.tags.is_empty() {
                groups.push(vec![market]);
            }
        }

        // Filter to groups with at least 2 markets
        groups.into_iter().filter(|g| g.len() >= 2).collect()
    }

    /// Clean up expired opportunities
    fn cleanup_expired(&self) {
        let now = Utc::now();
        self.opportunities.retain(|_, opp| opp.expires_at > now);
    }

    /// Get an opportunity by ID
    pub fn get_opportunity(&self, id: &Uuid) -> Option<ArbitrageOpportunity> {
        self.opportunities.get(id).map(|opp| opp.clone())
    }

    /// Get all active opportunities
    pub fn get_opportunities(&self) -> Vec<ArbitrageOpportunity> {
        self.opportunities
            .iter()
            .filter(|opp| opp.is_valid())
            .map(|opp| opp.clone())
            .collect()
    }

    /// Mark an opportunity as executed
    pub fn mark_executed(&self, id: &Uuid) {
        self.opportunities.remove(id);
    }

    /// Get detector statistics
    pub async fn get_stats(&self) -> DetectorStats {
        self.stats.read().await.clone()
    }

    /// Reset statistics
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = DetectorStats::default();
    }
}

/// Statistics for the arbitrage detector
#[derive(Debug, Clone, Default)]
pub struct DetectorStats {
    pub scans_performed: u64,
    pub opportunities_found: u64,
    pub opportunities_executed: u64,
    pub total_profit: Decimal,
    pub last_scan: chrono::DateTime<Utc>,
    pub avg_opportunity_lifetime_ms: f64,
}

impl std::fmt::Debug for ArbitrageDetector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArbitrageDetector")
            .field("binary_enabled", &self.binary_strategy.is_some())
            .field("multi_outcome_enabled", &self.multi_outcome_strategy.is_some())
            .field("cross_market_enabled", &self.cross_market_strategy.is_some())
            .field("temporal_enabled", &self.temporal_strategy.is_some())
            .field("active_opportunities", &self.opportunities.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_detector_creation() {
        let event_bus = EventBus::new(100);
        let engine = Arc::new(OrderbookEngine::new(event_bus.clone(), 5000));
        let config = StrategyConfig::default();

        let detector = ArbitrageDetector::new(config, engine, event_bus);

        assert!(detector.binary_strategy.is_some());
        assert!(detector.multi_outcome_strategy.is_some());
    }

    #[tokio::test]
    async fn test_market_filtering() {
        let event_bus = EventBus::new(100);
        let engine = Arc::new(OrderbookEngine::new(event_bus.clone(), 5000));
        let mut config = StrategyConfig::default();
        config.exclude_markets = vec!["test-exclude".to_string()];
        config.focus_markets = vec!["btc".to_string()];

        let detector = ArbitrageDetector::new(config, engine, event_bus);

        assert!(detector.is_excluded("test-exclude-market"));
        assert!(!detector.is_excluded("btc-market"));
        assert!(detector.is_focused("btc-5m"));
        assert!(!detector.is_focused("eth-5m"));
    }
}
