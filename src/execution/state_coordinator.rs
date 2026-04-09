//! Multi-Venue State Coordinator
//!
//! Manages state across multiple trading venues with:
//! - Persistent state storage for crash recovery
//! - Real-time position reconciliation
//! - Cross-venue exposure tracking
//! - Automatic recovery of incomplete executions
//!
//! This is critical for cross-platform arbitrage where failures can leave
//! positions open on one venue but not the other.

use crate::api::{
    ClobClient,
    kalshi::{KalshiClient, KalshiPosition, KalshiBalance},
    opinion::{OpinionClient, OpinionBalance},
};
use crate::core::{
    error::{ArbitrageError, Result},
    types::{Position, Price, Quantity, Side, Outcome},
};
use crate::execution::cross_platform::{
    Venue, ExecutionRecord, ExecutionState, FilledLegInfo, CrossPlatformExecutor,
};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// State coordinator configuration
#[derive(Debug, Clone)]
pub struct StateCoordinatorConfig {
    /// Database connection string for persistent state
    pub database_url: String,
    /// How often to reconcile positions (seconds)
    pub reconciliation_interval_secs: u64,
    /// Maximum position age before alert (hours)
    pub max_position_age_hours: u64,
    /// Enable automatic recovery of incomplete executions
    pub auto_recovery_enabled: bool,
    /// Maximum exposure per venue (USD)
    pub max_venue_exposure: Decimal,
    /// Maximum total cross-venue exposure (USD)
    pub max_total_exposure: Decimal,
}

impl Default for StateCoordinatorConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://localhost/polymarket_hft".to_string(),
            reconciliation_interval_secs: 30,
            max_position_age_hours: 24,
            auto_recovery_enabled: true,
            max_venue_exposure: dec!(10000),
            max_total_exposure: dec!(25000),
        }
    }
}

/// Aggregated position across venues
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedPosition {
    /// Unique identifier for the underlying market/event
    pub underlying_id: String,
    /// Human-readable description
    pub description: String,
    /// Positions by venue
    pub venue_positions: HashMap<Venue, VenuePosition>,
    /// Net exposure (positive = long, negative = short)
    pub net_exposure: Decimal,
    /// Is this a hedged position (has offsetting positions)?
    pub is_hedged: bool,
    /// Hedge ratio (1.0 = fully hedged)
    pub hedge_ratio: Decimal,
    /// Total P&L across venues
    pub total_pnl: Decimal,
    /// Last updated timestamp
    pub last_updated: DateTime<Utc>,
}

/// Position on a specific venue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenuePosition {
    pub venue: Venue,
    pub market_id: String,
    pub token_id: String,
    pub side: Side,
    pub outcome: Outcome,
    pub quantity: Decimal,
    pub avg_entry_price: Decimal,
    pub current_price: Option<Decimal>,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
    pub opened_at: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
}

/// Execution that needs recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingRecovery {
    pub id: Uuid,
    pub execution_id: Uuid,
    pub state: RecoveryState,
    pub first_leg: Option<FilledLegInfo>,
    pub hedge_leg: Option<FilledLegInfo>,
    pub recovery_attempts: u32,
    pub last_attempt: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub error_message: Option<String>,
}

/// State of recovery process
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecoveryState {
    /// Waiting to be processed
    Pending,
    /// Currently being recovered
    InProgress,
    /// Recovery completed successfully
    Completed,
    /// Recovery failed, needs manual intervention
    Failed,
    /// Manually resolved
    ManuallyResolved,
}

/// Venue health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueHealth {
    pub venue: Venue,
    pub is_connected: bool,
    pub last_heartbeat: DateTime<Utc>,
    pub latency_ms: u64,
    pub error_count_1h: u32,
    pub order_success_rate: f64,
}

/// Cross-venue exposure summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExposureSummary {
    pub venue_exposures: HashMap<Venue, Decimal>,
    pub total_exposure: Decimal,
    pub hedged_exposure: Decimal,
    pub unhedged_exposure: Decimal,
    pub largest_single_position: Decimal,
    pub position_count: u32,
    pub calculated_at: DateTime<Utc>,
}

/// Multi-Venue State Coordinator
pub struct StateCoordinator {
    config: StateCoordinatorConfig,
    db_pool: Option<PgPool>,
    polymarket: Arc<ClobClient>,
    kalshi: Arc<KalshiClient>,
    opinion: Arc<OpinionClient>,
    /// Cached aggregated positions
    positions: Arc<RwLock<HashMap<String, AggregatedPosition>>>,
    /// Pending recoveries
    pending_recoveries: Arc<RwLock<HashMap<Uuid, PendingRecovery>>>,
    /// Venue health status
    venue_health: Arc<RwLock<HashMap<Venue, VenueHealth>>>,
    /// Execution records awaiting reconciliation
    execution_records: Arc<RwLock<HashMap<Uuid, ExecutionRecord>>>,
}

impl StateCoordinator {
    /// Create a new state coordinator
    pub async fn new(
        config: StateCoordinatorConfig,
        polymarket: Arc<ClobClient>,
        kalshi: Arc<KalshiClient>,
        opinion: Arc<OpinionClient>,
    ) -> Result<Self> {
        // Initialize database connection
        let db_pool = if !config.database_url.is_empty() {
            Some(
                PgPoolOptions::new()
                    .max_connections(5)
                    .connect(&config.database_url)
                    .await
                    .map_err(|e| ArbitrageError::Database(format!("Failed to connect: {}", e)))?
            )
        } else {
            None
        };

        let coordinator = Self {
            config,
            db_pool,
            polymarket,
            kalshi,
            opinion,
            positions: Arc::new(RwLock::new(HashMap::new())),
            pending_recoveries: Arc::new(RwLock::new(HashMap::new())),
            venue_health: Arc::new(RwLock::new(HashMap::new())),
            execution_records: Arc::new(RwLock::new(HashMap::new())),
        };

        // Initialize venue health
        coordinator.initialize_venue_health().await;

        // Load pending recoveries from database
        if coordinator.db_pool.is_some() {
            coordinator.load_pending_recoveries().await?;
        }

        Ok(coordinator)
    }

    /// Initialize database schema
    pub async fn initialize_schema(&self) -> Result<()> {
        if let Some(pool) = &self.db_pool {
            sqlx::query(r#"
                CREATE TABLE IF NOT EXISTS execution_state (
                    id UUID PRIMARY KEY,
                    opportunity_id UUID NOT NULL,
                    state JSONB NOT NULL,
                    leg_a JSONB NOT NULL,
                    leg_b JSONB NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL,
                    last_updated TIMESTAMPTZ NOT NULL,
                    hedge_retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS pending_recovery (
                    id UUID PRIMARY KEY,
                    execution_id UUID NOT NULL,
                    state VARCHAR(50) NOT NULL,
                    first_leg JSONB,
                    hedge_leg JSONB,
                    recovery_attempts INTEGER DEFAULT 0,
                    last_attempt TIMESTAMPTZ,
                    error_message TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS position_snapshot (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    underlying_id VARCHAR(255) NOT NULL,
                    venue VARCHAR(50) NOT NULL,
                    market_id VARCHAR(255) NOT NULL,
                    token_id VARCHAR(255) NOT NULL,
                    side VARCHAR(10) NOT NULL,
                    outcome VARCHAR(10) NOT NULL,
                    quantity DECIMAL NOT NULL,
                    avg_entry_price DECIMAL NOT NULL,
                    realized_pnl DECIMAL DEFAULT 0,
                    opened_at TIMESTAMPTZ NOT NULL,
                    closed_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_execution_state_opportunity ON execution_state(opportunity_id);
                CREATE INDEX IF NOT EXISTS idx_pending_recovery_state ON pending_recovery(state);
                CREATE INDEX IF NOT EXISTS idx_position_snapshot_underlying ON position_snapshot(underlying_id);
            "#)
            .execute(pool)
            .await
            .map_err(|e| ArbitrageError::Database(format!("Schema init failed: {}", e)))?;

            info!("Database schema initialized");
        }
        Ok(())
    }

    /// Initialize venue health tracking
    async fn initialize_venue_health(&self) {
        let mut health = self.venue_health.write().await;

        for venue in [Venue::Polymarket, Venue::Kalshi, Venue::Opinion] {
            health.insert(venue, VenueHealth {
                venue,
                is_connected: false,
                last_heartbeat: Utc::now(),
                latency_ms: 0,
                error_count_1h: 0,
                order_success_rate: 1.0,
            });
        }
    }

    /// Save execution state to database
    pub async fn save_execution_state(&self, record: &ExecutionRecord) -> Result<()> {
        // Save to memory
        {
            let mut records = self.execution_records.write().await;
            records.insert(record.id, record.clone());
        }

        // Save to database
        if let Some(pool) = &self.db_pool {
            let state_json = serde_json::to_value(&record.state)
                .map_err(|e| ArbitrageError::Internal(format!("Serialization error: {}", e)))?;
            let leg_a_json = serde_json::to_value(&record.leg_a)
                .map_err(|e| ArbitrageError::Internal(format!("Serialization error: {}", e)))?;
            let leg_b_json = serde_json::to_value(&record.leg_b)
                .map_err(|e| ArbitrageError::Internal(format!("Serialization error: {}", e)))?;

            sqlx::query(r#"
                INSERT INTO execution_state (id, opportunity_id, state, leg_a, leg_b, started_at, last_updated, hedge_retry_count)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (id) DO UPDATE SET
                    state = EXCLUDED.state,
                    last_updated = EXCLUDED.last_updated,
                    hedge_retry_count = EXCLUDED.hedge_retry_count
            "#)
            .bind(record.id)
            .bind(record.opportunity_id)
            .bind(state_json)
            .bind(leg_a_json)
            .bind(leg_b_json)
            .bind(record.started_at)
            .bind(Utc::now())
            .bind(record.hedge_retry_count as i32)
            .execute(pool)
            .await
            .map_err(|e| ArbitrageError::Database(format!("Failed to save execution: {}", e)))?;
        }

        // Check if this execution needs recovery tracking
        if record.needs_recovery() {
            self.create_pending_recovery(record).await?;
        }

        Ok(())
    }

    /// Create a pending recovery entry
    async fn create_pending_recovery(&self, record: &ExecutionRecord) -> Result<()> {
        let (first_leg, hedge_leg) = match &record.state {
            ExecutionState::FirstLegFilled { venue, order_id, filled_qty, avg_price, filled_at } => {
                (Some(FilledLegInfo {
                    venue: *venue,
                    order_id: order_id.clone(),
                    market_id: record.leg_a.market_id.clone(),
                    token_id: record.leg_a.token_id.clone(),
                    side: record.leg_a.side,
                    outcome: record.leg_a.outcome,
                    filled_qty: *filled_qty,
                    avg_price: *avg_price,
                    fees: Decimal::ZERO,
                    filled_at: *filled_at,
                }), None)
            }
            ExecutionState::HedgeLegPartialFill { first_leg, hedge_venue, hedge_order_id, hedge_filled_qty, hedge_avg_price, .. } => {
                (Some(first_leg.clone()), Some(FilledLegInfo {
                    venue: *hedge_venue,
                    order_id: hedge_order_id.clone(),
                    market_id: record.leg_b.market_id.clone(),
                    token_id: record.leg_b.token_id.clone(),
                    side: record.leg_b.side,
                    outcome: record.leg_b.outcome,
                    filled_qty: *hedge_filled_qty,
                    avg_price: *hedge_avg_price,
                    fees: Decimal::ZERO,
                    filled_at: Utc::now(),
                }))
            }
            ExecutionState::Unwinding { first_leg, partial_hedge, .. } => {
                (Some(first_leg.clone()), partial_hedge.clone())
            }
            _ => (None, None),
        };

        let recovery = PendingRecovery {
            id: Uuid::new_v4(),
            execution_id: record.id,
            state: RecoveryState::Pending,
            first_leg,
            hedge_leg,
            recovery_attempts: 0,
            last_attempt: None,
            created_at: Utc::now(),
            error_message: None,
        };

        // Save to memory
        {
            let mut recoveries = self.pending_recoveries.write().await;
            recoveries.insert(recovery.id, recovery.clone());
        }

        // Save to database
        if let Some(pool) = &self.db_pool {
            let first_leg_json = serde_json::to_value(&recovery.first_leg).ok();
            let hedge_leg_json = serde_json::to_value(&recovery.hedge_leg).ok();

            sqlx::query(r#"
                INSERT INTO pending_recovery (id, execution_id, state, first_leg, hedge_leg, created_at)
                VALUES ($1, $2, $3, $4, $5, $6)
            "#)
            .bind(recovery.id)
            .bind(recovery.execution_id)
            .bind("Pending")
            .bind(first_leg_json)
            .bind(hedge_leg_json)
            .bind(recovery.created_at)
            .execute(pool)
            .await
            .map_err(|e| ArbitrageError::Database(format!("Failed to create recovery: {}", e)))?;
        }

        warn!("Created pending recovery {} for execution {}", recovery.id, record.id);
        Ok(())
    }

    /// Load pending recoveries from database
    async fn load_pending_recoveries(&self) -> Result<()> {
        if let Some(pool) = &self.db_pool {
            let rows = sqlx::query_as::<_, (Uuid, Uuid, String, Option<serde_json::Value>, Option<serde_json::Value>, i32, Option<DateTime<Utc>>, Option<String>, DateTime<Utc>)>(
                r#"
                SELECT id, execution_id, state, first_leg, hedge_leg, recovery_attempts, last_attempt, error_message, created_at
                FROM pending_recovery
                WHERE state IN ('Pending', 'InProgress')
                "#
            )
            .fetch_all(pool)
            .await
            .map_err(|e| ArbitrageError::Database(format!("Failed to load recoveries: {}", e)))?;

            let mut recoveries = self.pending_recoveries.write().await;

            for (id, execution_id, state_str, first_leg_json, hedge_leg_json, attempts, last_attempt, error_msg, created_at) in rows {
                let first_leg = first_leg_json.and_then(|j| serde_json::from_value(j).ok());
                let hedge_leg = hedge_leg_json.and_then(|j| serde_json::from_value(j).ok());

                let state = match state_str.as_str() {
                    "Pending" => RecoveryState::Pending,
                    "InProgress" => RecoveryState::InProgress,
                    _ => RecoveryState::Pending,
                };

                recoveries.insert(id, PendingRecovery {
                    id,
                    execution_id,
                    state,
                    first_leg,
                    hedge_leg,
                    recovery_attempts: attempts as u32,
                    last_attempt,
                    created_at,
                    error_message: error_msg,
                });
            }

            info!("Loaded {} pending recoveries", recoveries.len());
        }
        Ok(())
    }

    /// Reconcile positions across all venues
    pub async fn reconcile_positions(&self) -> Result<ExposureSummary> {
        info!("Starting position reconciliation across venues");

        let mut aggregated: HashMap<String, AggregatedPosition> = HashMap::new();

        // Fetch positions from each venue
        let polymarket_positions = self.fetch_polymarket_positions().await.unwrap_or_default();
        let kalshi_positions = self.fetch_kalshi_positions().await.unwrap_or_default();
        let opinion_positions = self.fetch_opinion_positions().await.unwrap_or_default();

        // Aggregate positions by underlying
        for pos in polymarket_positions {
            self.add_to_aggregated(&mut aggregated, pos);
        }
        for pos in kalshi_positions {
            self.add_to_aggregated(&mut aggregated, pos);
        }
        for pos in opinion_positions {
            self.add_to_aggregated(&mut aggregated, pos);
        }

        // Calculate exposure summary
        let mut venue_exposures: HashMap<Venue, Decimal> = HashMap::new();
        let mut total_exposure = Decimal::ZERO;
        let mut hedged_exposure = Decimal::ZERO;
        let mut unhedged_exposure = Decimal::ZERO;
        let mut largest_single = Decimal::ZERO;

        for (_, agg) in &aggregated {
            for (venue, vp) in &agg.venue_positions {
                let exposure = vp.quantity * vp.avg_entry_price;
                *venue_exposures.entry(*venue).or_insert(Decimal::ZERO) += exposure;
                total_exposure += exposure;
                largest_single = largest_single.max(exposure);
            }

            if agg.is_hedged {
                hedged_exposure += agg.net_exposure.abs();
            } else {
                unhedged_exposure += agg.net_exposure.abs();
            }
        }

        // Update cached positions
        {
            let mut positions = self.positions.write().await;
            *positions = aggregated;
        }

        let summary = ExposureSummary {
            venue_exposures,
            total_exposure,
            hedged_exposure,
            unhedged_exposure,
            largest_single_position: largest_single,
            position_count: self.positions.read().await.len() as u32,
            calculated_at: Utc::now(),
        };

        // Check exposure limits
        if total_exposure > self.config.max_total_exposure {
            warn!(
                "Total exposure ${:.2} exceeds limit ${:.2}",
                total_exposure, self.config.max_total_exposure
            );
        }

        for (venue, exposure) in &summary.venue_exposures {
            if *exposure > self.config.max_venue_exposure {
                warn!(
                    "{} exposure ${:.2} exceeds limit ${:.2}",
                    venue, exposure, self.config.max_venue_exposure
                );
            }
        }

        Ok(summary)
    }

    /// Fetch positions from Polymarket
    async fn fetch_polymarket_positions(&self) -> Result<Vec<VenuePosition>> {
        let positions = self.polymarket.get_positions().await?;

        Ok(positions.into_iter().map(|p| VenuePosition {
            venue: Venue::Polymarket,
            market_id: p.market_id.clone(),
            token_id: p.token_id.clone(),
            side: if p.size > Decimal::ZERO { Side::Buy } else { Side::Sell },
            outcome: Outcome::Yes,  // TODO: Determine from token
            quantity: p.size.abs(),
            avg_entry_price: p.avg_price,
            current_price: p.cur_price,
            unrealized_pnl: p.unrealized_pnl.unwrap_or(Decimal::ZERO),
            realized_pnl: p.realized_pnl.unwrap_or(Decimal::ZERO),
            opened_at: p.created_at.unwrap_or(Utc::now()),
            last_updated: Utc::now(),
        }).collect())
    }

    /// Fetch positions from Kalshi
    async fn fetch_kalshi_positions(&self) -> Result<Vec<VenuePosition>> {
        let positions = self.kalshi.get_positions().await?;

        Ok(positions.into_iter().map(|p| {
            let (side, outcome) = if p.position >= 0 {
                (Side::Buy, Outcome::Yes)
            } else {
                (Side::Sell, Outcome::No)
            };

            VenuePosition {
                venue: Venue::Kalshi,
                market_id: p.ticker.clone(),
                token_id: p.ticker.clone(),
                side,
                outcome,
                quantity: Decimal::from(p.position.abs()),
                avg_entry_price: Decimal::ZERO,  // Kalshi doesn't provide this
                current_price: None,
                unrealized_pnl: Decimal::ZERO,
                realized_pnl: Decimal::ZERO,
                opened_at: Utc::now(),
                last_updated: Utc::now(),
            }
        }).collect())
    }

    /// Fetch positions from Opinion
    async fn fetch_opinion_positions(&self) -> Result<Vec<VenuePosition>> {
        // Opinion doesn't have a direct positions endpoint in our mock
        // In production, this would call the actual API
        Ok(vec![])
    }

    /// Add a venue position to aggregated positions
    fn add_to_aggregated(&self, aggregated: &mut HashMap<String, AggregatedPosition>, pos: VenuePosition) {
        // Use market_id as underlying identifier (in production, would map to common underlying)
        let underlying_id = pos.market_id.clone();

        let agg = aggregated.entry(underlying_id.clone()).or_insert_with(|| AggregatedPosition {
            underlying_id: underlying_id.clone(),
            description: pos.market_id.clone(),
            venue_positions: HashMap::new(),
            net_exposure: Decimal::ZERO,
            is_hedged: false,
            hedge_ratio: Decimal::ZERO,
            total_pnl: Decimal::ZERO,
            last_updated: Utc::now(),
        });

        // Update net exposure
        let exposure = match pos.side {
            Side::Buy => pos.quantity * pos.avg_entry_price,
            Side::Sell => -pos.quantity * pos.avg_entry_price,
        };
        agg.net_exposure += exposure;
        agg.total_pnl += pos.unrealized_pnl + pos.realized_pnl;

        // Add venue position
        agg.venue_positions.insert(pos.venue, pos);

        // Check if hedged (positions on multiple venues)
        if agg.venue_positions.len() > 1 {
            agg.is_hedged = true;

            // Calculate hedge ratio
            let long_exposure: Decimal = agg.venue_positions.values()
                .filter(|p| p.side == Side::Buy)
                .map(|p| p.quantity * p.avg_entry_price)
                .sum();
            let short_exposure: Decimal = agg.venue_positions.values()
                .filter(|p| p.side == Side::Sell)
                .map(|p| p.quantity * p.avg_entry_price)
                .sum();

            if long_exposure > Decimal::ZERO || short_exposure > Decimal::ZERO {
                agg.hedge_ratio = short_exposure.min(long_exposure) /
                    long_exposure.max(short_exposure).max(dec!(0.01));
            }
        }

        agg.last_updated = Utc::now();
    }

    /// Process pending recoveries
    pub async fn process_recoveries(&self, executor: &CrossPlatformExecutor) -> Result<Vec<PendingRecovery>> {
        if !self.config.auto_recovery_enabled {
            return Ok(vec![]);
        }

        let recoveries: Vec<PendingRecovery> = {
            let recoveries = self.pending_recoveries.read().await;
            recoveries.values()
                .filter(|r| matches!(r.state, RecoveryState::Pending))
                .cloned()
                .collect()
        };

        let mut processed = vec![];

        for mut recovery in recoveries {
            info!("Processing recovery {} for execution {}", recovery.id, recovery.execution_id);

            recovery.state = RecoveryState::InProgress;
            recovery.recovery_attempts += 1;
            recovery.last_attempt = Some(Utc::now());

            // Get the execution record
            let execution_opt = {
                let records = self.execution_records.read().await;
                records.get(&recovery.execution_id).cloned()
            };

            if let Some(record) = execution_opt {
                match executor.recover_execution(&record).await {
                    Ok(recovered) => {
                        recovery.state = RecoveryState::Completed;
                        info!("Recovery {} completed successfully", recovery.id);

                        // Update execution record
                        self.save_execution_state(&recovered).await?;
                    }
                    Err(e) => {
                        recovery.error_message = Some(e.to_string());

                        if recovery.recovery_attempts >= 3 {
                            recovery.state = RecoveryState::Failed;
                            error!(
                                "Recovery {} failed after {} attempts: {}. Manual intervention required.",
                                recovery.id, recovery.recovery_attempts, e
                            );
                        } else {
                            recovery.state = RecoveryState::Pending;
                            warn!(
                                "Recovery {} attempt {} failed: {}. Will retry.",
                                recovery.id, recovery.recovery_attempts, e
                            );
                        }
                    }
                }
            } else {
                recovery.state = RecoveryState::Failed;
                recovery.error_message = Some("Execution record not found".to_string());
            }

            // Update recovery state
            self.update_recovery_state(&recovery).await?;
            processed.push(recovery);
        }

        Ok(processed)
    }

    /// Update recovery state in database
    async fn update_recovery_state(&self, recovery: &PendingRecovery) -> Result<()> {
        // Update memory
        {
            let mut recoveries = self.pending_recoveries.write().await;
            recoveries.insert(recovery.id, recovery.clone());
        }

        // Update database
        if let Some(pool) = &self.db_pool {
            let state_str = match recovery.state {
                RecoveryState::Pending => "Pending",
                RecoveryState::InProgress => "InProgress",
                RecoveryState::Completed => "Completed",
                RecoveryState::Failed => "Failed",
                RecoveryState::ManuallyResolved => "ManuallyResolved",
            };

            sqlx::query(r#"
                UPDATE pending_recovery
                SET state = $1, recovery_attempts = $2, last_attempt = $3, error_message = $4, updated_at = NOW()
                WHERE id = $5
            "#)
            .bind(state_str)
            .bind(recovery.recovery_attempts as i32)
            .bind(recovery.last_attempt)
            .bind(&recovery.error_message)
            .bind(recovery.id)
            .execute(pool)
            .await
            .map_err(|e| ArbitrageError::Database(format!("Failed to update recovery: {}", e)))?;
        }

        Ok(())
    }

    /// Mark a recovery as manually resolved
    pub async fn mark_manually_resolved(&self, recovery_id: Uuid, notes: &str) -> Result<()> {
        let mut recovery = {
            let recoveries = self.pending_recoveries.read().await;
            recoveries.get(&recovery_id).cloned()
                .ok_or_else(|| ArbitrageError::Internal("Recovery not found".to_string()))?
        };

        recovery.state = RecoveryState::ManuallyResolved;
        recovery.error_message = Some(format!("Manually resolved: {}", notes));

        self.update_recovery_state(&recovery).await?;
        info!("Recovery {} marked as manually resolved: {}", recovery_id, notes);

        Ok(())
    }

    /// Get all pending recoveries
    pub async fn get_pending_recoveries(&self) -> Vec<PendingRecovery> {
        let recoveries = self.pending_recoveries.read().await;
        recoveries.values()
            .filter(|r| matches!(r.state, RecoveryState::Pending | RecoveryState::InProgress | RecoveryState::Failed))
            .cloned()
            .collect()
    }

    /// Get aggregated positions
    pub async fn get_aggregated_positions(&self) -> HashMap<String, AggregatedPosition> {
        self.positions.read().await.clone()
    }

    /// Update venue health status
    pub async fn update_venue_health(&self, venue: Venue, connected: bool, latency_ms: u64) {
        let mut health = self.venue_health.write().await;
        if let Some(h) = health.get_mut(&venue) {
            h.is_connected = connected;
            h.last_heartbeat = Utc::now();
            h.latency_ms = latency_ms;
        }
    }

    /// Check if a venue is healthy
    pub async fn is_venue_healthy(&self, venue: Venue) -> bool {
        let health = self.venue_health.read().await;
        health.get(&venue).map(|h| {
            h.is_connected &&
            h.latency_ms < 1000 &&
            h.order_success_rate > 0.9 &&
            (Utc::now() - h.last_heartbeat) < Duration::seconds(30)
        }).unwrap_or(false)
    }

    /// Get venue health summary
    pub async fn get_venue_health(&self) -> HashMap<Venue, VenueHealth> {
        self.venue_health.read().await.clone()
    }

    /// Start background reconciliation loop
    pub async fn start_reconciliation_loop(self: Arc<Self>) {
        let interval = tokio::time::Duration::from_secs(self.config.reconciliation_interval_secs);

        tokio::spawn(async move {
            loop {
                if let Err(e) = self.reconcile_positions().await {
                    error!("Position reconciliation failed: {}", e);
                }
                tokio::time::sleep(interval).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_exposure_calculation() {
        let pos1 = VenuePosition {
            venue: Venue::Polymarket,
            market_id: "btc-up".to_string(),
            token_id: "token1".to_string(),
            side: Side::Buy,
            outcome: Outcome::Yes,
            quantity: dec!(100),
            avg_entry_price: dec!(0.55),
            current_price: Some(dec!(0.57)),
            unrealized_pnl: dec!(2.0),
            realized_pnl: Decimal::ZERO,
            opened_at: Utc::now(),
            last_updated: Utc::now(),
        };

        let pos2 = VenuePosition {
            venue: Venue::Kalshi,
            market_id: "btc-up".to_string(),
            token_id: "token2".to_string(),
            side: Side::Sell,
            outcome: Outcome::Yes,
            quantity: dec!(95),
            avg_entry_price: dec!(0.56),
            current_price: Some(dec!(0.57)),
            unrealized_pnl: dec!(-0.95),
            realized_pnl: Decimal::ZERO,
            opened_at: Utc::now(),
            last_updated: Utc::now(),
        };

        // Exposure = qty * price
        let exposure1 = pos1.quantity * pos1.avg_entry_price;
        let exposure2 = pos2.quantity * pos2.avg_entry_price;

        assert_eq!(exposure1, dec!(55.0));
        assert_eq!(exposure2, dec!(53.2));

        // Hedge ratio = min/max
        let hedge_ratio = exposure2.min(exposure1) / exposure1.max(exposure2);
        assert!(hedge_ratio > dec!(0.95));  // Well hedged
    }
}
