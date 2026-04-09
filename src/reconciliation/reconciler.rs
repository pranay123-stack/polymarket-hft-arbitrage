//! Position reconciliation service.
//!
//! Compares local position state with exchange-reported positions
//! and handles discrepancies.

use std::collections::HashMap;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use std::sync::Arc;
use chrono::{DateTime, Utc};

use super::position_tracker::{Position, PositionTracker, PositionSide};

/// A discrepancy between local and exchange positions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Discrepancy {
    /// Unique discrepancy ID.
    pub id: String,
    /// Venue where discrepancy was found.
    pub venue: String,
    /// Market ID.
    pub market_id: String,
    /// Outcome ID (if applicable).
    pub outcome_id: Option<String>,
    /// Type of discrepancy.
    pub discrepancy_type: DiscrepancyType,
    /// Local position quantity.
    pub local_quantity: Decimal,
    /// Exchange-reported quantity.
    pub exchange_quantity: Decimal,
    /// Quantity difference.
    pub difference: Decimal,
    /// Local average price.
    pub local_avg_price: Option<Decimal>,
    /// Exchange average price.
    pub exchange_avg_price: Option<Decimal>,
    /// When the discrepancy was detected.
    pub detected_at: DateTime<Utc>,
    /// Whether the discrepancy has been resolved.
    pub resolved: bool,
    /// Resolution details.
    pub resolution: Option<DiscrepancyResolution>,
}

/// Type of position discrepancy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiscrepancyType {
    /// Position exists locally but not on exchange.
    OrphanedLocal,
    /// Position exists on exchange but not locally.
    OrphanedExchange,
    /// Quantities don't match.
    QuantityMismatch,
    /// Prices don't match (informational).
    PriceMismatch,
    /// Position side mismatch.
    SideMismatch,
}

/// Resolution of a discrepancy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscrepancyResolution {
    /// How the discrepancy was resolved.
    pub resolution_type: ResolutionType,
    /// When resolved.
    pub resolved_at: DateTime<Utc>,
    /// Who/what resolved it.
    pub resolved_by: String,
    /// Notes.
    pub notes: Option<String>,
}

/// How a discrepancy was resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResolutionType {
    /// Synced local to match exchange.
    SyncedToExchange,
    /// Manual adjustment.
    ManualAdjustment,
    /// Position was closed.
    PositionClosed,
    /// Determined to be false positive.
    FalsePositive,
    /// Auto-resolved (transient discrepancy).
    AutoResolved,
}

/// Exchange position data for reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangePosition {
    pub venue: String,
    pub market_id: String,
    pub outcome_id: Option<String>,
    pub quantity: Decimal,
    pub avg_entry_price: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
    pub side: Option<String>,
}

/// Result of a reconciliation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationResult {
    /// When the reconciliation ran.
    pub timestamp: DateTime<Utc>,
    /// Duration of reconciliation in milliseconds.
    pub duration_ms: u64,
    /// Venues that were reconciled.
    pub venues_checked: Vec<String>,
    /// Total positions checked.
    pub positions_checked: usize,
    /// Positions that matched.
    pub positions_matched: usize,
    /// New discrepancies found.
    pub discrepancies_found: Vec<Discrepancy>,
    /// Discrepancies auto-resolved.
    pub auto_resolved: usize,
    /// Overall status.
    pub status: ReconciliationStatus,
}

/// Status of reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReconciliationStatus {
    /// All positions match.
    Clean,
    /// Minor discrepancies found.
    MinorDiscrepancies,
    /// Significant discrepancies found.
    SignificantDiscrepancies,
    /// Critical issues found.
    Critical,
    /// Failed to complete.
    Failed,
}

/// Configuration for reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcilerConfig {
    /// Tolerance for quantity differences (in base units).
    pub quantity_tolerance: Decimal,
    /// Tolerance for price differences (percentage).
    pub price_tolerance_pct: Decimal,
    /// Auto-resolve transient discrepancies after this many consistent checks.
    pub auto_resolve_after_checks: u32,
    /// Reconciliation interval in milliseconds.
    pub reconciliation_interval_ms: u64,
    /// Whether to automatically sync to exchange.
    pub auto_sync_enabled: bool,
    /// Maximum discrepancy value for auto-sync.
    pub max_auto_sync_value: Decimal,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            quantity_tolerance: Decimal::new(1, 2), // 0.01
            price_tolerance_pct: Decimal::new(1, 2), // 1%
            auto_resolve_after_checks: 3,
            reconciliation_interval_ms: 60000, // 1 minute
            auto_sync_enabled: true,
            max_auto_sync_value: Decimal::new(100, 0), // $100
        }
    }
}

/// Position reconciler service.
pub struct PositionReconciler {
    /// Configuration.
    config: ReconcilerConfig,
    /// Position tracker.
    tracker: Arc<PositionTracker>,
    /// Active discrepancies.
    discrepancies: Arc<RwLock<Vec<Discrepancy>>>,
    /// Discrepancy history.
    history: Arc<RwLock<Vec<ReconciliationResult>>>,
    /// Transient discrepancy counter (for auto-resolve).
    transient_counts: Arc<RwLock<HashMap<String, u32>>>,
    /// Exchange position fetchers.
    exchange_fetchers: Arc<RwLock<HashMap<String, Box<dyn ExchangePositionFetcher>>>>,
    /// Running flag.
    running: Arc<RwLock<bool>>,
}

/// Trait for fetching positions from an exchange.
#[async_trait::async_trait]
pub trait ExchangePositionFetcher: Send + Sync {
    /// Get venue name.
    fn venue(&self) -> &str;

    /// Fetch all positions from the exchange.
    async fn fetch_positions(&self) -> Result<Vec<ExchangePosition>, String>;

    /// Fetch position for a specific market.
    async fn fetch_position(&self, market_id: &str, outcome_id: Option<&str>) -> Result<Option<ExchangePosition>, String>;
}

impl PositionReconciler {
    /// Create a new position reconciler.
    pub fn new(config: ReconcilerConfig, tracker: Arc<PositionTracker>) -> Self {
        Self {
            config,
            tracker,
            discrepancies: Arc::new(RwLock::new(Vec::new())),
            history: Arc::new(RwLock::new(Vec::new())),
            transient_counts: Arc::new(RwLock::new(HashMap::new())),
            exchange_fetchers: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Register an exchange position fetcher.
    pub async fn register_fetcher(&self, fetcher: Box<dyn ExchangePositionFetcher>) {
        let venue = fetcher.venue().to_string();
        self.exchange_fetchers.write().await.insert(venue, fetcher);
    }

    /// Start the reconciliation daemon.
    pub async fn start(&self) {
        *self.running.write().await = true;

        let running = self.running.clone();
        let config = self.config.clone();
        let tracker = self.tracker.clone();
        let discrepancies = self.discrepancies.clone();
        let history = self.history.clone();
        let transient_counts = self.transient_counts.clone();
        let fetchers = self.exchange_fetchers.clone();

        tokio::spawn(async move {
            while *running.read().await {
                let result = Self::reconcile_positions_internal(
                    &config,
                    &tracker,
                    &fetchers,
                    &discrepancies,
                    &transient_counts,
                ).await;

                // Record result
                let mut history = history.write().await;
                history.push(result);
                if history.len() > 1000 {
                    history.remove(0);
                }

                // Wait for next reconciliation
                tokio::time::sleep(tokio::time::Duration::from_millis(config.reconciliation_interval_ms)).await;
            }
        });
    }

    /// Stop the reconciliation daemon.
    pub async fn stop(&self) {
        *self.running.write().await = false;
    }

    /// Run reconciliation manually.
    pub async fn reconcile(&self) -> ReconciliationResult {
        Self::reconcile_positions_internal(
            &self.config,
            &self.tracker,
            &self.exchange_fetchers,
            &self.discrepancies,
            &self.transient_counts,
        ).await
    }

    /// Internal reconciliation logic.
    async fn reconcile_positions_internal(
        config: &ReconcilerConfig,
        tracker: &Arc<PositionTracker>,
        fetchers: &Arc<RwLock<HashMap<String, Box<dyn ExchangePositionFetcher>>>>,
        discrepancies: &Arc<RwLock<Vec<Discrepancy>>>,
        transient_counts: &Arc<RwLock<HashMap<String, u32>>>,
    ) -> ReconciliationResult {
        let start = std::time::Instant::now();
        let now = Utc::now();

        let mut result = ReconciliationResult {
            timestamp: now,
            duration_ms: 0,
            venues_checked: Vec::new(),
            positions_checked: 0,
            positions_matched: 0,
            discrepancies_found: Vec::new(),
            auto_resolved: 0,
            status: ReconciliationStatus::Clean,
        };

        // Get local positions
        let local_positions = tracker.get_all_positions().await;
        let mut local_map: HashMap<String, Position> = local_positions
            .into_iter()
            .map(|p| (p.key(), p))
            .collect();

        // Fetch and compare positions from each exchange
        let fetchers = fetchers.read().await;
        for (venue, fetcher) in fetchers.iter() {
            result.venues_checked.push(venue.clone());

            match fetcher.fetch_positions().await {
                Ok(exchange_positions) => {
                    for ex_pos in exchange_positions {
                        result.positions_checked += 1;

                        let key = match &ex_pos.outcome_id {
                            Some(o) => format!("{}:{}:{}", ex_pos.venue, ex_pos.market_id, o),
                            None => format!("{}:{}", ex_pos.venue, ex_pos.market_id),
                        };

                        if let Some(local_pos) = local_map.remove(&key) {
                            // Compare positions
                            let qty_diff = (local_pos.quantity - ex_pos.quantity).abs();

                            if qty_diff > config.quantity_tolerance {
                                let discrepancy = Discrepancy {
                                    id: uuid::Uuid::new_v4().to_string(),
                                    venue: ex_pos.venue.clone(),
                                    market_id: ex_pos.market_id.clone(),
                                    outcome_id: ex_pos.outcome_id.clone(),
                                    discrepancy_type: DiscrepancyType::QuantityMismatch,
                                    local_quantity: local_pos.quantity,
                                    exchange_quantity: ex_pos.quantity,
                                    difference: qty_diff,
                                    local_avg_price: Some(local_pos.avg_entry_price),
                                    exchange_avg_price: ex_pos.avg_entry_price,
                                    detected_at: now,
                                    resolved: false,
                                    resolution: None,
                                };

                                // Check for auto-resolve
                                let mut counts = transient_counts.write().await;
                                let count = counts.entry(key.clone()).or_insert(0);
                                *count += 1;

                                if *count >= config.auto_resolve_after_checks {
                                    // Auto-sync if enabled and within limits
                                    if config.auto_sync_enabled {
                                        let value = qty_diff * ex_pos.avg_entry_price.unwrap_or(Decimal::ONE);
                                        if value <= config.max_auto_sync_value {
                                            // Sync local to exchange
                                            let mut synced_pos = local_pos.clone();
                                            synced_pos.quantity = ex_pos.quantity;
                                            tracker.set_position(synced_pos).await;
                                            result.auto_resolved += 1;
                                            counts.remove(&key);
                                            continue;
                                        }
                                    }
                                }

                                result.discrepancies_found.push(discrepancy);
                            } else {
                                result.positions_matched += 1;
                                transient_counts.write().await.remove(&key);
                            }
                        } else {
                            // Position exists on exchange but not locally
                            let discrepancy = Discrepancy {
                                id: uuid::Uuid::new_v4().to_string(),
                                venue: ex_pos.venue.clone(),
                                market_id: ex_pos.market_id.clone(),
                                outcome_id: ex_pos.outcome_id.clone(),
                                discrepancy_type: DiscrepancyType::OrphanedExchange,
                                local_quantity: Decimal::ZERO,
                                exchange_quantity: ex_pos.quantity,
                                difference: ex_pos.quantity,
                                local_avg_price: None,
                                exchange_avg_price: ex_pos.avg_entry_price,
                                detected_at: now,
                                resolved: false,
                                resolution: None,
                            };
                            result.discrepancies_found.push(discrepancy);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to fetch positions from {}: {}", venue, e);
                    result.status = ReconciliationStatus::Failed;
                }
            }
        }

        // Check for orphaned local positions
        for (key, local_pos) in local_map {
            let discrepancy = Discrepancy {
                id: uuid::Uuid::new_v4().to_string(),
                venue: local_pos.venue.clone(),
                market_id: local_pos.market_id.clone(),
                outcome_id: local_pos.outcome_id.clone(),
                discrepancy_type: DiscrepancyType::OrphanedLocal,
                local_quantity: local_pos.quantity,
                exchange_quantity: Decimal::ZERO,
                difference: local_pos.quantity,
                local_avg_price: Some(local_pos.avg_entry_price),
                exchange_avg_price: None,
                detected_at: now,
                resolved: false,
                resolution: None,
            };
            result.discrepancies_found.push(discrepancy);
        }

        // Update discrepancy list
        let mut disc = discrepancies.write().await;
        disc.extend(result.discrepancies_found.clone());

        // Determine status
        if result.status != ReconciliationStatus::Failed {
            result.status = if result.discrepancies_found.is_empty() {
                ReconciliationStatus::Clean
            } else if result.discrepancies_found.len() <= 2 {
                ReconciliationStatus::MinorDiscrepancies
            } else if result.discrepancies_found.iter().any(|d| d.difference > Decimal::new(1000, 0)) {
                ReconciliationStatus::Critical
            } else {
                ReconciliationStatus::SignificantDiscrepancies
            };
        }

        result.duration_ms = start.elapsed().as_millis() as u64;
        result
    }

    /// Get active discrepancies.
    pub async fn get_discrepancies(&self) -> Vec<Discrepancy> {
        self.discrepancies
            .read()
            .await
            .iter()
            .filter(|d| !d.resolved)
            .cloned()
            .collect()
    }

    /// Resolve a discrepancy manually.
    pub async fn resolve_discrepancy(
        &self,
        discrepancy_id: &str,
        resolution_type: ResolutionType,
        resolved_by: &str,
        notes: Option<&str>,
    ) -> Result<(), String> {
        let mut discrepancies = self.discrepancies.write().await;

        let disc = discrepancies
            .iter_mut()
            .find(|d| d.id == discrepancy_id)
            .ok_or("Discrepancy not found")?;

        disc.resolved = true;
        disc.resolution = Some(DiscrepancyResolution {
            resolution_type,
            resolved_at: Utc::now(),
            resolved_by: resolved_by.to_string(),
            notes: notes.map(String::from),
        });

        // If syncing to exchange, update local position
        if resolution_type == ResolutionType::SyncedToExchange {
            if disc.exchange_quantity != Decimal::ZERO {
                let position = Position::new(
                    &disc.venue,
                    &disc.market_id,
                    disc.outcome_id.as_deref(),
                    disc.exchange_quantity,
                    disc.exchange_avg_price.unwrap_or(Decimal::ZERO),
                    Decimal::ZERO,
                );
                self.tracker.set_position(position).await;
            } else {
                // Exchange has no position - clear local
                // This would need a remove_position method
            }
        }

        Ok(())
    }

    /// Get reconciliation history.
    pub async fn get_history(&self) -> Vec<ReconciliationResult> {
        self.history.read().await.clone()
    }

    /// Get the last reconciliation result.
    pub async fn get_last_result(&self) -> Option<ReconciliationResult> {
        self.history.read().await.last().cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discrepancy_types() {
        let disc = Discrepancy {
            id: "test".to_string(),
            venue: "kalshi".to_string(),
            market_id: "MKT1".to_string(),
            outcome_id: None,
            discrepancy_type: DiscrepancyType::QuantityMismatch,
            local_quantity: Decimal::new(100, 0),
            exchange_quantity: Decimal::new(95, 0),
            difference: Decimal::new(5, 0),
            local_avg_price: Some(Decimal::new(50, 2)),
            exchange_avg_price: Some(Decimal::new(50, 2)),
            detected_at: Utc::now(),
            resolved: false,
            resolution: None,
        };

        assert_eq!(disc.discrepancy_type, DiscrepancyType::QuantityMismatch);
        assert!(!disc.resolved);
    }

    #[tokio::test]
    async fn test_reconciler_creation() {
        let tracker = Arc::new(PositionTracker::new(100));
        let config = ReconcilerConfig::default();
        let reconciler = PositionReconciler::new(config, tracker);

        let discrepancies = reconciler.get_discrepancies().await;
        assert!(discrepancies.is_empty());
    }
}
