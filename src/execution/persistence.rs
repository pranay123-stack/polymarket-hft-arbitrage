//! Database Persistence for Execution State
//!
//! Provides persistent storage and recovery for execution state across restarts.
//! Critical for HFT systems where crashes during execution could leave positions
//! in inconsistent states.
//!
//! ## Features:
//!
//! - **Execution Records**: Full state machine persistence
//! - **Order Tracking**: Links orders to executions for recovery
//! - **Trade History**: Audit trail of all fills
//! - **State Recovery**: Automatic resume of incomplete executions on restart
//! - **WAL Mode**: Write-ahead logging for crash safety
//!
//! ## Database Schema:
//!
//! - `executions`: Main execution record table
//! - `execution_legs`: Individual leg details
//! - `orders`: Order state and history
//! - `trades`: Fill/trade records
//! - `positions`: Current position snapshot
//! - `recovery_queue`: Pending recovery tasks

use crate::core::error::{ArbitrageError, Result};
use crate::core::types::{Order, OrderStatus, Outcome, Price, Quantity, Side, Trade};
use crate::execution::{
    ExecutionRecord, ExecutionState, FilledLegInfo, Venue,
    CrossPlatformLeg, CrossPlatformOpportunity, AbortReason,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Database configuration
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    /// PostgreSQL connection string
    pub database_url: String,
    /// Maximum connections in pool
    pub max_connections: u32,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Enable WAL mode for better crash safety
    pub wal_mode: bool,
    /// Checkpoint interval (seconds)
    pub checkpoint_interval_secs: u64,
    /// Enable auto-recovery on startup
    pub auto_recovery: bool,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            database_url: "postgresql://localhost/polymarket_hft".to_string(),
            max_connections: 10,
            connect_timeout: Duration::from_secs(30),
            wal_mode: true,
            checkpoint_interval_secs: 60,
            auto_recovery: true,
        }
    }
}

/// Execution persistence store
pub struct ExecutionPersistence {
    pool: PgPool,
    config: PersistenceConfig,
    /// In-memory cache for fast lookups
    cache: Arc<RwLock<std::collections::HashMap<Uuid, ExecutionRecord>>>,
}

impl ExecutionPersistence {
    /// Create a new persistence store
    pub async fn new(config: PersistenceConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.connect_timeout)
            .connect(&config.database_url)
            .await
            .map_err(|e| ArbitrageError::Database(format!("Failed to connect to database: {}", e)))?;

        let persistence = Self {
            pool,
            config,
            cache: Arc::new(RwLock::new(std::collections::HashMap::new())),
        };

        // Initialize schema
        persistence.init_schema().await?;

        info!("Execution persistence initialized");

        Ok(persistence)
    }

    /// Initialize database schema
    async fn init_schema(&self) -> Result<()> {
        // Create tables if they don't exist
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS executions (
                id UUID PRIMARY KEY,
                opportunity_id UUID NOT NULL,
                state_type VARCHAR(50) NOT NULL,
                state_data JSONB NOT NULL,
                leg_a_data JSONB NOT NULL,
                leg_b_data JSONB NOT NULL,
                started_at TIMESTAMPTZ NOT NULL,
                last_updated TIMESTAMPTZ NOT NULL,
                hedge_retry_count INTEGER NOT NULL DEFAULT 0,
                completed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_executions_state ON executions(state_type);
            CREATE INDEX IF NOT EXISTS idx_executions_opportunity ON executions(opportunity_id);
            CREATE INDEX IF NOT EXISTS idx_executions_started ON executions(started_at);

            CREATE TABLE IF NOT EXISTS execution_legs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                execution_id UUID NOT NULL REFERENCES executions(id),
                leg_type VARCHAR(20) NOT NULL, -- 'first', 'hedge', 'unwind'
                venue VARCHAR(20) NOT NULL,
                order_id VARCHAR(100),
                market_id VARCHAR(100) NOT NULL,
                token_id VARCHAR(100) NOT NULL,
                side VARCHAR(10) NOT NULL,
                outcome VARCHAR(10) NOT NULL,
                target_price DECIMAL NOT NULL,
                target_qty DECIMAL NOT NULL,
                filled_qty DECIMAL,
                avg_price DECIMAL,
                fees DECIMAL,
                status VARCHAR(20) NOT NULL,
                submitted_at TIMESTAMPTZ,
                filled_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_legs_execution ON execution_legs(execution_id);
            CREATE INDEX IF NOT EXISTS idx_legs_order ON execution_legs(order_id);

            CREATE TABLE IF NOT EXISTS orders (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                order_id VARCHAR(100) NOT NULL UNIQUE,
                execution_id UUID REFERENCES executions(id),
                venue VARCHAR(20) NOT NULL,
                market_id VARCHAR(100) NOT NULL,
                token_id VARCHAR(100) NOT NULL,
                side VARCHAR(10) NOT NULL,
                outcome VARCHAR(10) NOT NULL,
                price DECIMAL NOT NULL,
                quantity DECIMAL NOT NULL,
                order_type VARCHAR(20) NOT NULL,
                status VARCHAR(20) NOT NULL,
                filled_qty DECIMAL DEFAULT 0,
                avg_fill_price DECIMAL,
                fees DECIMAL DEFAULT 0,
                submitted_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_orders_execution ON orders(execution_id);
            CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
            CREATE INDEX IF NOT EXISTS idx_orders_venue ON orders(venue);

            CREATE TABLE IF NOT EXISTS trades (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                trade_id VARCHAR(100) NOT NULL UNIQUE,
                order_id VARCHAR(100) NOT NULL,
                execution_id UUID REFERENCES executions(id),
                venue VARCHAR(20) NOT NULL,
                market_id VARCHAR(100) NOT NULL,
                token_id VARCHAR(100) NOT NULL,
                side VARCHAR(10) NOT NULL,
                outcome VARCHAR(10) NOT NULL,
                price DECIMAL NOT NULL,
                quantity DECIMAL NOT NULL,
                fee DECIMAL NOT NULL DEFAULT 0,
                traded_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_trades_order ON trades(order_id);
            CREATE INDEX IF NOT EXISTS idx_trades_execution ON trades(execution_id);
            CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(traded_at);

            CREATE TABLE IF NOT EXISTS positions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                venue VARCHAR(20) NOT NULL,
                market_id VARCHAR(100) NOT NULL,
                token_id VARCHAR(100) NOT NULL,
                outcome VARCHAR(10) NOT NULL,
                quantity DECIMAL NOT NULL,
                avg_entry_price DECIMAL NOT NULL,
                realized_pnl DECIMAL NOT NULL DEFAULT 0,
                unrealized_pnl DECIMAL NOT NULL DEFAULT 0,
                last_updated TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(venue, market_id, token_id, outcome)
            );

            CREATE INDEX IF NOT EXISTS idx_positions_venue ON positions(venue);
            CREATE INDEX IF NOT EXISTS idx_positions_market ON positions(market_id);

            CREATE TABLE IF NOT EXISTS recovery_queue (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                execution_id UUID NOT NULL REFERENCES executions(id),
                recovery_type VARCHAR(50) NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                last_attempt TIMESTAMPTZ,
                next_attempt TIMESTAMPTZ,
                error_message TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_recovery_next ON recovery_queue(next_attempt);
            CREATE INDEX IF NOT EXISTS idx_recovery_execution ON recovery_queue(execution_id);
        "#)
        .execute(&self.pool)
        .await
        .map_err(|e| ArbitrageError::Database(format!("Failed to initialize schema: {}", e)))?;

        Ok(())
    }

    /// Save an execution record
    pub async fn save_execution(&self, record: &ExecutionRecord) -> Result<()> {
        let state_type = self.state_type_string(&record.state);
        let state_data = serde_json::to_value(&record.state)
            .map_err(|e| ArbitrageError::Database(format!("Failed to serialize state: {}", e)))?;
        let leg_a_data = serde_json::to_value(&record.leg_a)
            .map_err(|e| ArbitrageError::Database(format!("Failed to serialize leg_a: {}", e)))?;
        let leg_b_data = serde_json::to_value(&record.leg_b)
            .map_err(|e| ArbitrageError::Database(format!("Failed to serialize leg_b: {}", e)))?;

        sqlx::query(r#"
            INSERT INTO executions (
                id, opportunity_id, state_type, state_data, leg_a_data, leg_b_data,
                started_at, last_updated, hedge_retry_count
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (id) DO UPDATE SET
                state_type = EXCLUDED.state_type,
                state_data = EXCLUDED.state_data,
                last_updated = EXCLUDED.last_updated,
                hedge_retry_count = EXCLUDED.hedge_retry_count
        "#)
        .bind(record.id)
        .bind(record.opportunity_id)
        .bind(&state_type)
        .bind(&state_data)
        .bind(&leg_a_data)
        .bind(&leg_b_data)
        .bind(record.started_at)
        .bind(record.last_updated)
        .bind(record.hedge_retry_count as i32)
        .execute(&self.pool)
        .await
        .map_err(|e| ArbitrageError::Database(format!("Failed to save execution: {}", e)))?;

        // Update cache
        self.cache.write().await.insert(record.id, record.clone());

        debug!("Saved execution {} in state {}", record.id, state_type);

        Ok(())
    }

    /// Load an execution record
    pub async fn load_execution(&self, id: Uuid) -> Result<Option<ExecutionRecord>> {
        // Check cache first
        if let Some(record) = self.cache.read().await.get(&id) {
            return Ok(Some(record.clone()));
        }

        let row = sqlx::query(r#"
            SELECT id, opportunity_id, state_data, leg_a_data, leg_b_data,
                   started_at, last_updated, hedge_retry_count
            FROM executions WHERE id = $1
        "#)
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ArbitrageError::Database(format!("Failed to load execution: {}", e)))?;

        match row {
            Some(row) => {
                let record = self.row_to_record(&row)?;
                self.cache.write().await.insert(id, record.clone());
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Load all executions that need recovery
    pub async fn load_pending_executions(&self) -> Result<Vec<ExecutionRecord>> {
        let rows = sqlx::query(r#"
            SELECT id, opportunity_id, state_data, leg_a_data, leg_b_data,
                   started_at, last_updated, hedge_retry_count
            FROM executions
            WHERE state_type NOT IN ('Completed', 'Aborted', 'Failed', 'Cancelled', 'UnwindCompleted')
            ORDER BY started_at ASC
        "#)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ArbitrageError::Database(format!("Failed to load pending executions: {}", e)))?;

        let mut records = Vec::new();
        for row in rows {
            match self.row_to_record(&row) {
                Ok(record) => records.push(record),
                Err(e) => warn!("Failed to parse execution record: {}", e),
            }
        }

        info!("Loaded {} pending executions for recovery", records.len());

        Ok(records)
    }

    /// Save an order
    pub async fn save_order(
        &self,
        order_id: &str,
        execution_id: Option<Uuid>,
        venue: Venue,
        order: &Order,
        status: OrderStatus,
    ) -> Result<()> {
        sqlx::query(r#"
            INSERT INTO orders (
                order_id, execution_id, venue, market_id, token_id,
                side, outcome, price, quantity, order_type, status,
                submitted_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (order_id) DO UPDATE SET
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at
        "#)
        .bind(order_id)
        .bind(execution_id)
        .bind(venue.to_string())
        .bind(&order.market_id)
        .bind(&order.token_id)
        .bind(order.side.to_string())
        .bind(order.outcome.to_string())
        .bind(order.price.as_decimal())
        .bind(order.quantity.as_decimal())
        .bind(format!("{:?}", order.order_type))
        .bind(format!("{:?}", status))
        .bind(order.created_at)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(|e| ArbitrageError::Database(format!("Failed to save order: {}", e)))?;

        Ok(())
    }

    /// Update order status with fill information
    pub async fn update_order_fill(
        &self,
        order_id: &str,
        status: OrderStatus,
        filled_qty: Decimal,
        avg_price: Option<Decimal>,
        fees: Decimal,
    ) -> Result<()> {
        sqlx::query(r#"
            UPDATE orders SET
                status = $1,
                filled_qty = $2,
                avg_fill_price = $3,
                fees = $4,
                updated_at = $5
            WHERE order_id = $6
        "#)
        .bind(format!("{:?}", status))
        .bind(filled_qty)
        .bind(avg_price)
        .bind(fees)
        .bind(Utc::now())
        .bind(order_id)
        .execute(&self.pool)
        .await
        .map_err(|e| ArbitrageError::Database(format!("Failed to update order: {}", e)))?;

        Ok(())
    }

    /// Save a trade
    pub async fn save_trade(
        &self,
        trade: &Trade,
        execution_id: Option<Uuid>,
        venue: Venue,
    ) -> Result<()> {
        sqlx::query(r#"
            INSERT INTO trades (
                trade_id, order_id, execution_id, venue, market_id, token_id,
                side, outcome, price, quantity, fee, traded_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (trade_id) DO NOTHING
        "#)
        .bind(&trade.id)
        .bind(&trade.order_id)
        .bind(execution_id)
        .bind(venue.to_string())
        .bind(&trade.market_id)
        .bind(&trade.token_id)
        .bind(trade.side.to_string())
        .bind(trade.outcome.to_string())
        .bind(trade.price.as_decimal())
        .bind(trade.quantity.as_decimal())
        .bind(trade.fee)
        .bind(trade.timestamp)
        .execute(&self.pool)
        .await
        .map_err(|e| ArbitrageError::Database(format!("Failed to save trade: {}", e)))?;

        Ok(())
    }

    /// Update position from trade
    pub async fn update_position(
        &self,
        venue: Venue,
        trade: &Trade,
    ) -> Result<()> {
        // Upsert position
        sqlx::query(r#"
            INSERT INTO positions (
                venue, market_id, token_id, outcome, quantity, avg_entry_price, last_updated
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (venue, market_id, token_id, outcome) DO UPDATE SET
                quantity = positions.quantity +
                    CASE WHEN $8 = 'Buy' THEN $5 ELSE -$5 END,
                avg_entry_price = CASE
                    WHEN $8 = 'Buy' THEN
                        (positions.avg_entry_price * positions.quantity + $6 * $5)
                        / (positions.quantity + $5)
                    ELSE positions.avg_entry_price
                END,
                last_updated = $7
        "#)
        .bind(venue.to_string())
        .bind(&trade.market_id)
        .bind(&trade.token_id)
        .bind(trade.outcome.to_string())
        .bind(trade.quantity.as_decimal())
        .bind(trade.price.as_decimal())
        .bind(Utc::now())
        .bind(trade.side.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| ArbitrageError::Database(format!("Failed to update position: {}", e)))?;

        Ok(())
    }

    /// Queue an execution for recovery
    pub async fn queue_recovery(
        &self,
        execution_id: Uuid,
        recovery_type: &str,
        priority: i32,
    ) -> Result<()> {
        sqlx::query(r#"
            INSERT INTO recovery_queue (execution_id, recovery_type, priority, next_attempt)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT DO NOTHING
        "#)
        .bind(execution_id)
        .bind(recovery_type)
        .bind(priority)
        .execute(&self.pool)
        .await
        .map_err(|e| ArbitrageError::Database(format!("Failed to queue recovery: {}", e)))?;

        Ok(())
    }

    /// Get next recovery task
    pub async fn get_next_recovery(&self) -> Result<Option<RecoveryTask>> {
        let row = sqlx::query(r#"
            SELECT r.id, r.execution_id, r.recovery_type, r.attempts, r.max_attempts
            FROM recovery_queue r
            WHERE r.next_attempt <= NOW() AND r.attempts < r.max_attempts
            ORDER BY r.priority DESC, r.next_attempt ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        "#)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ArbitrageError::Database(format!("Failed to get recovery task: {}", e)))?;

        match row {
            Some(row) => Ok(Some(RecoveryTask {
                id: row.get("id"),
                execution_id: row.get("execution_id"),
                recovery_type: row.get("recovery_type"),
                attempts: row.get::<i32, _>("attempts") as u32,
                max_attempts: row.get::<i32, _>("max_attempts") as u32,
            })),
            None => Ok(None),
        }
    }

    /// Mark recovery task as attempted
    pub async fn update_recovery_attempt(
        &self,
        task_id: Uuid,
        success: bool,
        error_message: Option<&str>,
    ) -> Result<()> {
        if success {
            // Remove from queue on success
            sqlx::query("DELETE FROM recovery_queue WHERE id = $1")
                .bind(task_id)
                .execute(&self.pool)
                .await
                .map_err(|e| ArbitrageError::Database(format!("Failed to complete recovery: {}", e)))?;
        } else {
            // Update attempt count and schedule next attempt
            sqlx::query(r#"
                UPDATE recovery_queue SET
                    attempts = attempts + 1,
                    last_attempt = NOW(),
                    next_attempt = NOW() + INTERVAL '30 seconds' * attempts,
                    error_message = $2
                WHERE id = $1
            "#)
            .bind(task_id)
            .bind(error_message)
            .execute(&self.pool)
            .await
            .map_err(|e| ArbitrageError::Database(format!("Failed to update recovery: {}", e)))?;
        }

        Ok(())
    }

    /// Get execution statistics
    pub async fn get_stats(&self) -> Result<ExecutionStats> {
        let row = sqlx::query(r#"
            SELECT
                COUNT(*) as total_executions,
                COUNT(*) FILTER (WHERE state_type = 'Completed') as completed,
                COUNT(*) FILTER (WHERE state_type = 'Failed') as failed,
                COUNT(*) FILTER (WHERE state_type = 'Aborted') as aborted,
                COUNT(*) FILTER (WHERE state_type NOT IN ('Completed', 'Failed', 'Aborted', 'Cancelled', 'UnwindCompleted')) as pending
            FROM executions
            WHERE started_at >= NOW() - INTERVAL '24 hours'
        "#)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| ArbitrageError::Database(format!("Failed to get stats: {}", e)))?;

        Ok(ExecutionStats {
            total_executions: row.get::<i64, _>("total_executions") as u64,
            completed: row.get::<i64, _>("completed") as u64,
            failed: row.get::<i64, _>("failed") as u64,
            aborted: row.get::<i64, _>("aborted") as u64,
            pending: row.get::<i64, _>("pending") as u64,
        })
    }

    /// Clean up old records
    pub async fn cleanup_old_records(&self, days: i32) -> Result<u64> {
        let result = sqlx::query(r#"
            DELETE FROM executions
            WHERE state_type IN ('Completed', 'Failed', 'Aborted', 'Cancelled', 'UnwindCompleted')
            AND completed_at < NOW() - INTERVAL '1 day' * $1
        "#)
        .bind(days)
        .execute(&self.pool)
        .await
        .map_err(|e| ArbitrageError::Database(format!("Failed to cleanup: {}", e)))?;

        Ok(result.rows_affected())
    }

    /// Convert state to string for indexing
    fn state_type_string(&self, state: &ExecutionState) -> String {
        match state {
            ExecutionState::Pending => "Pending",
            ExecutionState::FirstLegSubmitted { .. } => "FirstLegSubmitted",
            ExecutionState::FirstLegPartialFill { .. } => "FirstLegPartialFill",
            ExecutionState::FirstLegFilled { .. } => "FirstLegFilled",
            ExecutionState::HedgeLegSubmitted { .. } => "HedgeLegSubmitted",
            ExecutionState::HedgeLegPartialFill { .. } => "HedgeLegPartialFill",
            ExecutionState::Completed { .. } => "Completed",
            ExecutionState::Aborted { .. } => "Aborted",
            ExecutionState::Unwinding { .. } => "Unwinding",
            ExecutionState::UnwindCompleted { .. } => "UnwindCompleted",
            ExecutionState::Failed { .. } => "Failed",
        }.to_string()
    }

    /// Convert database row to ExecutionRecord
    fn row_to_record(&self, row: &sqlx::postgres::PgRow) -> Result<ExecutionRecord> {
        let state_data: serde_json::Value = row.get("state_data");
        let leg_a_data: serde_json::Value = row.get("leg_a_data");
        let leg_b_data: serde_json::Value = row.get("leg_b_data");

        let state: ExecutionState = serde_json::from_value(state_data)
            .map_err(|e| ArbitrageError::Database(format!("Failed to deserialize state: {}", e)))?;
        let leg_a: CrossPlatformLeg = serde_json::from_value(leg_a_data)
            .map_err(|e| ArbitrageError::Database(format!("Failed to deserialize leg_a: {}", e)))?;
        let leg_b: CrossPlatformLeg = serde_json::from_value(leg_b_data)
            .map_err(|e| ArbitrageError::Database(format!("Failed to deserialize leg_b: {}", e)))?;

        Ok(ExecutionRecord {
            id: row.get("id"),
            opportunity_id: row.get("opportunity_id"),
            state,
            leg_a,
            leg_b,
            started_at: row.get("started_at"),
            last_updated: row.get("last_updated"),
            hedge_retry_count: row.get::<i32, _>("hedge_retry_count") as u32,
        })
    }
}

/// Recovery task from queue
#[derive(Debug, Clone)]
pub struct RecoveryTask {
    pub id: Uuid,
    pub execution_id: Uuid,
    pub recovery_type: String,
    pub attempts: u32,
    pub max_attempts: u32,
}

/// Execution statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExecutionStats {
    pub total_executions: u64,
    pub completed: u64,
    pub failed: u64,
    pub aborted: u64,
    pub pending: u64,
}

impl ExecutionStats {
    pub fn success_rate(&self) -> f64 {
        let total = self.completed + self.failed + self.aborted;
        if total == 0 {
            0.0
        } else {
            self.completed as f64 / total as f64
        }
    }
}

/// Helper trait for Side string conversion
trait SideExt {
    fn to_string(&self) -> String;
}

impl SideExt for Side {
    fn to_string(&self) -> String {
        match self {
            Side::Buy => "Buy".to_string(),
            Side::Sell => "Sell".to_string(),
        }
    }
}

/// Helper trait for Outcome string conversion
trait OutcomeExt {
    fn to_string(&self) -> String;
}

impl OutcomeExt for Outcome {
    fn to_string(&self) -> String {
        match self {
            Outcome::Yes => "Yes".to_string(),
            Outcome::No => "No".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_stats() {
        let stats = ExecutionStats {
            total_executions: 100,
            completed: 80,
            failed: 10,
            aborted: 5,
            pending: 5,
        };

        let rate = stats.success_rate();
        assert!((rate - 0.842).abs() < 0.01);  // 80 / 95 = 0.842
    }
}
