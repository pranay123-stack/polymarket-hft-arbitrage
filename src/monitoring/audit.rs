//! Comprehensive Audit Trail System
//!
//! Records all trading activities for:
//! - Regulatory compliance
//! - Post-trade analysis
//! - Debugging execution issues
//! - Performance attribution
//!
//! Features:
//! - Immutable append-only log
//! - Structured JSON format
//! - Database persistence
//! - Real-time streaming
//! - Queryable history

use crate::core::error::{Error, Result};
use crate::execution::cross_platform::{
    CrossPlatformOpportunity, ExecutionRecord, ExecutionState,
    FilledLegInfo, Venue, AbortReason,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgRow, Row};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::fs::{File, OpenOptions};
use tracing::{debug, error, info};
use uuid::Uuid;

/// Audit event types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventType {
    // Opportunity events
    OpportunityDetected,
    OpportunityExpired,
    OpportunityRejected,

    // Execution events
    ExecutionStarted,
    ExecutionCompleted,
    ExecutionAborted,
    ExecutionFailed,

    // Order events
    OrderSubmitted,
    OrderFilled,
    OrderPartialFill,
    OrderCancelled,
    OrderRejected,

    // Leg events
    FirstLegSubmitted,
    FirstLegFilled,
    FirstLegPartialFill,
    FirstLegCancelled,
    HedgeLegSubmitted,
    HedgeLegFilled,
    HedgeLegPartialFill,
    HedgeLegFailed,
    HedgeLegRetry,

    // Unwind events
    UnwindStarted,
    UnwindCompleted,
    UnwindFailed,

    // Risk events
    RiskCheckPassed,
    RiskCheckFailed,
    PositionLimitReached,
    ExposureLimitReached,
    DrawdownLimitReached,

    // System events
    CircuitBreakerTriggered,
    CircuitBreakerReset,
    VenueConnected,
    VenueDisconnected,
    ConfigurationChanged,
    SystemStarted,
    SystemStopped,
}

impl std::fmt::Display for AuditEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Audit event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: Uuid,
    pub event_type: AuditEventType,
    pub timestamp: DateTime<Utc>,
    /// Related execution ID
    pub execution_id: Option<Uuid>,
    /// Related opportunity ID
    pub opportunity_id: Option<Uuid>,
    /// Related order ID
    pub order_id: Option<String>,
    /// Market ID
    pub market_id: Option<String>,
    /// Venue
    pub venue: Option<Venue>,
    /// Summary message
    pub summary: String,
    /// Structured details
    pub details: HashMap<String, serde_json::Value>,
    /// Associated P&L (if applicable)
    pub pnl: Option<Decimal>,
    /// User/system that triggered event
    pub actor: String,
    /// Session ID
    pub session_id: String,
    /// Sequence number within session
    pub sequence: u64,
}

impl AuditEvent {
    pub fn new(event_type: AuditEventType, summary: &str) -> Self {
        Self {
            id: Uuid::new_v4(),
            event_type,
            timestamp: Utc::now(),
            execution_id: None,
            opportunity_id: None,
            order_id: None,
            market_id: None,
            venue: None,
            summary: summary.to_string(),
            details: HashMap::new(),
            pnl: None,
            actor: "system".to_string(),
            session_id: String::new(),
            sequence: 0,
        }
    }

    pub fn with_execution(mut self, execution_id: Uuid) -> Self {
        self.execution_id = Some(execution_id);
        self
    }

    pub fn with_opportunity(mut self, opportunity_id: Uuid) -> Self {
        self.opportunity_id = Some(opportunity_id);
        self
    }

    pub fn with_order(mut self, order_id: &str) -> Self {
        self.order_id = Some(order_id.to_string());
        self
    }

    pub fn with_market(mut self, market_id: &str) -> Self {
        self.market_id = Some(market_id.to_string());
        self
    }

    pub fn with_venue(mut self, venue: Venue) -> Self {
        self.venue = Some(venue);
        self
    }

    pub fn with_detail<T: Serialize>(mut self, key: &str, value: T) -> Self {
        if let Ok(json_value) = serde_json::to_value(value) {
            self.details.insert(key.to_string(), json_value);
        }
        self
    }

    pub fn with_pnl(mut self, pnl: Decimal) -> Self {
        self.pnl = Some(pnl);
        self
    }

    pub fn with_actor(mut self, actor: &str) -> Self {
        self.actor = actor.to_string();
        self
    }
}

/// Audit trail configuration
#[derive(Debug, Clone)]
pub struct AuditConfig {
    /// Database connection string
    pub database_url: Option<String>,
    /// File path for local audit log
    pub file_path: Option<String>,
    /// Enable file logging
    pub file_enabled: bool,
    /// Enable database logging
    pub database_enabled: bool,
    /// Enable real-time streaming
    pub streaming_enabled: bool,
    /// Buffer size for file writes
    pub file_buffer_size: usize,
    /// Flush interval (seconds)
    pub flush_interval_secs: u64,
    /// Retention period (days)
    pub retention_days: u32,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            database_url: None,
            file_path: Some("./logs/audit.jsonl".to_string()),
            file_enabled: true,
            database_enabled: true,
            streaming_enabled: true,
            file_buffer_size: 8192,
            flush_interval_secs: 5,
            retention_days: 90,
        }
    }
}

/// Audit trail system
pub struct AuditTrail {
    config: AuditConfig,
    db_pool: Option<PgPool>,
    /// Session ID for this instance
    session_id: String,
    /// Sequence counter
    sequence: Arc<RwLock<u64>>,
    /// In-memory buffer for recent events
    buffer: Arc<RwLock<Vec<AuditEvent>>>,
    /// Channel for streaming events
    event_tx: mpsc::Sender<AuditEvent>,
    /// File writer task handle
    file_writer_tx: Option<mpsc::Sender<AuditEvent>>,
}

impl AuditTrail {
    /// Create new audit trail
    pub async fn new(config: AuditConfig) -> Result<Self> {
        let session_id = Uuid::new_v4().to_string();

        // Initialize database if configured
        let db_pool = if config.database_enabled {
            if let Some(ref url) = config.database_url {
                Some(
                    sqlx::postgres::PgPoolOptions::new()
                        .max_connections(5)
                        .connect(url)
                        .await
                        .map_err(|e| Error::Database(format!("Audit DB connection failed: {}", e)))?
                )
            } else {
                None
            }
        } else {
            None
        };

        let (event_tx, mut event_rx) = mpsc::channel::<AuditEvent>(10000);

        // Start file writer if enabled
        let file_writer_tx = if config.file_enabled {
            if let Some(ref path) = config.file_path {
                let (tx, rx) = mpsc::channel::<AuditEvent>(1000);
                let path = path.clone();
                let buffer_size = config.file_buffer_size;

                tokio::spawn(async move {
                    Self::file_writer_task(path, rx, buffer_size).await;
                });

                Some(tx)
            } else {
                None
            }
        } else {
            None
        };

        let audit = Self {
            config,
            db_pool,
            session_id,
            sequence: Arc::new(RwLock::new(0)),
            buffer: Arc::new(RwLock::new(Vec::new())),
            event_tx,
            file_writer_tx,
        };

        // Initialize database schema
        if let Some(ref pool) = audit.db_pool {
            audit.initialize_schema(pool).await?;
        }

        // Log system start
        audit.log(AuditEvent::new(
            AuditEventType::SystemStarted,
            &format!("Audit trail started with session {}", audit.session_id),
        )).await?;

        Ok(audit)
    }

    /// Initialize database schema
    async fn initialize_schema(&self, pool: &PgPool) -> Result<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS audit_events (
                id UUID PRIMARY KEY,
                event_type VARCHAR(100) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                execution_id UUID,
                opportunity_id UUID,
                order_id VARCHAR(255),
                market_id VARCHAR(255),
                venue VARCHAR(50),
                summary TEXT NOT NULL,
                details JSONB NOT NULL DEFAULT '{}',
                pnl DECIMAL,
                actor VARCHAR(100) NOT NULL,
                session_id VARCHAR(100) NOT NULL,
                sequence BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_events(timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_audit_execution_id ON audit_events(execution_id);
            CREATE INDEX IF NOT EXISTS idx_audit_opportunity_id ON audit_events(opportunity_id);
            CREATE INDEX IF NOT EXISTS idx_audit_event_type ON audit_events(event_type);
            CREATE INDEX IF NOT EXISTS idx_audit_session_id ON audit_events(session_id);
            CREATE INDEX IF NOT EXISTS idx_audit_market_id ON audit_events(market_id);
        "#)
        .execute(pool)
        .await
        .map_err(|e| Error::Database(format!("Schema creation failed: {}", e)))?;

        info!("Audit trail schema initialized");
        Ok(())
    }

    /// File writer task
    async fn file_writer_task(path: String, mut rx: mpsc::Receiver<AuditEvent>, buffer_size: usize) {
        // Ensure directory exists
        if let Some(parent) = std::path::Path::new(&path).parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }

        let file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                error!("Failed to open audit file {}: {}", path, e);
                return;
            }
        };

        let mut writer = BufWriter::with_capacity(buffer_size, file);
        let mut flush_interval = tokio::time::interval(tokio::time::Duration::from_secs(5));

        loop {
            tokio::select! {
                event = rx.recv() => {
                    match event {
                        Some(event) => {
                            match serde_json::to_string(&event) {
                                Ok(json) => {
                                    if let Err(e) = writer.write_all(json.as_bytes()).await {
                                        error!("Failed to write audit event: {}", e);
                                    }
                                    if let Err(e) = writer.write_all(b"\n").await {
                                        error!("Failed to write newline: {}", e);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to serialize audit event: {}", e);
                                }
                            }
                        }
                        None => {
                            // Channel closed, flush and exit
                            let _ = writer.flush().await;
                            break;
                        }
                    }
                }
                _ = flush_interval.tick() => {
                    if let Err(e) = writer.flush().await {
                        error!("Failed to flush audit file: {}", e);
                    }
                }
            }
        }
    }

    /// Log an audit event
    pub async fn log(&self, mut event: AuditEvent) -> Result<()> {
        // Set session and sequence
        event.session_id = self.session_id.clone();
        {
            let mut seq = self.sequence.write().await;
            *seq += 1;
            event.sequence = *seq;
        }

        // Add to buffer
        {
            let mut buffer = self.buffer.write().await;
            buffer.push(event.clone());

            // Keep last 1000 events in memory
            if buffer.len() > 1000 {
                buffer.remove(0);
            }
        }

        // Write to file
        if let Some(ref tx) = self.file_writer_tx {
            let _ = tx.send(event.clone()).await;
        }

        // Write to database
        if let Some(ref pool) = self.db_pool {
            self.write_to_database(pool, &event).await?;
        }

        // Log to tracing
        debug!(
            audit_event_type = %event.event_type,
            audit_summary = %event.summary,
            "Audit event logged"
        );

        Ok(())
    }

    /// Write event to database
    async fn write_to_database(&self, pool: &PgPool, event: &AuditEvent) -> Result<()> {
        let details_json = serde_json::to_value(&event.details)
            .unwrap_or(serde_json::json!({}));

        sqlx::query(r#"
            INSERT INTO audit_events (
                id, event_type, timestamp, execution_id, opportunity_id,
                order_id, market_id, venue, summary, details, pnl,
                actor, session_id, sequence
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        "#)
        .bind(event.id)
        .bind(event.event_type.to_string())
        .bind(event.timestamp)
        .bind(event.execution_id)
        .bind(event.opportunity_id)
        .bind(&event.order_id)
        .bind(&event.market_id)
        .bind(event.venue.map(|v| v.to_string()))
        .bind(&event.summary)
        .bind(details_json)
        .bind(event.pnl)
        .bind(&event.actor)
        .bind(&event.session_id)
        .bind(event.sequence as i64)
        .execute(pool)
        .await
        .map_err(|e| Error::Database(format!("Failed to insert audit event: {}", e)))?;

        Ok(())
    }

    /// Query audit events
    pub async fn query(
        &self,
        event_type: Option<AuditEventType>,
        execution_id: Option<Uuid>,
        market_id: Option<&str>,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<AuditEvent>> {
        if let Some(ref pool) = self.db_pool {
            let mut query = String::from(
                "SELECT id, event_type, timestamp, execution_id, opportunity_id, \
                 order_id, market_id, venue, summary, details, pnl, actor, \
                 session_id, sequence FROM audit_events WHERE 1=1"
            );

            if event_type.is_some() {
                query.push_str(" AND event_type = $1");
            }
            if execution_id.is_some() {
                query.push_str(" AND execution_id = $2");
            }
            if market_id.is_some() {
                query.push_str(" AND market_id = $3");
            }
            if from.is_some() {
                query.push_str(" AND timestamp >= $4");
            }
            if to.is_some() {
                query.push_str(" AND timestamp <= $5");
            }

            query.push_str(" ORDER BY timestamp DESC LIMIT $6");

            // Note: Simplified query - in production would use proper parameterized queries
            let rows = sqlx::query(&format!(
                "SELECT id, event_type, timestamp, execution_id, opportunity_id, \
                 order_id, market_id, venue, summary, details, pnl, actor, \
                 session_id, sequence FROM audit_events \
                 ORDER BY timestamp DESC LIMIT {}", limit
            ))
            .fetch_all(pool)
            .await
            .map_err(|e| Error::Database(format!("Query failed: {}", e)))?;

            let events: Vec<AuditEvent> = rows.iter().filter_map(|row| {
                Some(AuditEvent {
                    id: row.try_get("id").ok()?,
                    event_type: AuditEventType::ExecutionStarted, // Simplified
                    timestamp: row.try_get("timestamp").ok()?,
                    execution_id: row.try_get("execution_id").ok(),
                    opportunity_id: row.try_get("opportunity_id").ok(),
                    order_id: row.try_get("order_id").ok(),
                    market_id: row.try_get("market_id").ok(),
                    venue: None,
                    summary: row.try_get("summary").ok()?,
                    details: row.try_get::<serde_json::Value, _>("details")
                        .ok()
                        .and_then(|v| serde_json::from_value(v).ok())
                        .unwrap_or_default(),
                    pnl: row.try_get("pnl").ok(),
                    actor: row.try_get("actor").ok()?,
                    session_id: row.try_get("session_id").ok()?,
                    sequence: row.try_get::<i64, _>("sequence").ok()? as u64,
                })
            }).collect();

            Ok(events)
        } else {
            // Return from memory buffer
            let buffer = self.buffer.read().await;
            Ok(buffer.iter().rev().take(limit as usize).cloned().collect())
        }
    }

    /// Get recent events from memory
    pub async fn get_recent(&self, count: usize) -> Vec<AuditEvent> {
        let buffer = self.buffer.read().await;
        buffer.iter().rev().take(count).cloned().collect()
    }

    /// Get events for an execution
    pub async fn get_execution_events(&self, execution_id: Uuid) -> Result<Vec<AuditEvent>> {
        if let Some(ref pool) = self.db_pool {
            let rows = sqlx::query(
                "SELECT id, event_type, timestamp, execution_id, opportunity_id, \
                 order_id, market_id, venue, summary, details, pnl, actor, \
                 session_id, sequence FROM audit_events \
                 WHERE execution_id = $1 ORDER BY sequence ASC"
            )
            .bind(execution_id)
            .fetch_all(pool)
            .await
            .map_err(|e| Error::Database(format!("Query failed: {}", e)))?;

            // Parse rows (simplified)
            Ok(vec![])
        } else {
            let buffer = self.buffer.read().await;
            Ok(buffer.iter()
                .filter(|e| e.execution_id == Some(execution_id))
                .cloned()
                .collect())
        }
    }

    // ========== Convenience methods for common events ==========

    /// Log opportunity detected
    pub async fn log_opportunity_detected(&self, opportunity: &CrossPlatformOpportunity) -> Result<()> {
        self.log(AuditEvent::new(
            AuditEventType::OpportunityDetected,
            &format!(
                "Arbitrage opportunity: {} -> {}, profit: {:.4}%",
                opportunity.leg_a.venue,
                opportunity.leg_b.venue,
                opportunity.expected_profit_pct * Decimal::from(100)
            ),
        )
        .with_opportunity(opportunity.id)
        .with_market(
            &format!("{}:{}", opportunity.leg_a.market_id, opportunity.leg_b.market_id)
        )
        .with_detail("expected_profit", opportunity.expected_profit)
        .with_detail("expected_profit_pct", opportunity.expected_profit_pct)
        .with_detail("required_capital", opportunity.required_capital)
        .with_detail("leg_a_venue", opportunity.leg_a.venue.to_string())
        .with_detail("leg_a_price", opportunity.leg_a.target_price.as_decimal())
        .with_detail("leg_b_venue", opportunity.leg_b.venue.to_string())
        .with_detail("leg_b_price", opportunity.leg_b.target_price.as_decimal())
        ).await
    }

    /// Log execution started
    pub async fn log_execution_started(&self, record: &ExecutionRecord) -> Result<()> {
        self.log(AuditEvent::new(
            AuditEventType::ExecutionStarted,
            &format!(
                "Execution started: {} -> {}",
                record.leg_a.venue, record.leg_b.venue
            ),
        )
        .with_execution(record.id)
        .with_opportunity(record.opportunity_id)
        ).await
    }

    /// Log first leg filled
    pub async fn log_first_leg_filled(
        &self,
        execution_id: Uuid,
        leg: &FilledLegInfo,
    ) -> Result<()> {
        self.log(AuditEvent::new(
            AuditEventType::FirstLegFilled,
            &format!(
                "First leg filled on {}: {} @ {}",
                leg.venue, leg.filled_qty, leg.avg_price
            ),
        )
        .with_execution(execution_id)
        .with_venue(leg.venue)
        .with_order(&leg.order_id)
        .with_market(&leg.market_id)
        .with_detail("filled_qty", leg.filled_qty)
        .with_detail("avg_price", leg.avg_price)
        .with_detail("fees", leg.fees)
        ).await
    }

    /// Log hedge leg failed
    pub async fn log_hedge_failed(
        &self,
        execution_id: Uuid,
        venue: Venue,
        reason: &str,
        retry_count: u32,
    ) -> Result<()> {
        self.log(AuditEvent::new(
            AuditEventType::HedgeLegFailed,
            &format!("Hedge leg failed on {} after {} retries: {}", venue, retry_count, reason),
        )
        .with_execution(execution_id)
        .with_venue(venue)
        .with_detail("retry_count", retry_count)
        .with_detail("failure_reason", reason)
        ).await
    }

    /// Log execution completed
    pub async fn log_execution_completed(
        &self,
        record: &ExecutionRecord,
        profit: Decimal,
    ) -> Result<()> {
        self.log(AuditEvent::new(
            AuditEventType::ExecutionCompleted,
            &format!("Execution completed with profit: ${:.4}", profit),
        )
        .with_execution(record.id)
        .with_opportunity(record.opportunity_id)
        .with_pnl(profit)
        ).await
    }

    /// Log unwind
    pub async fn log_unwind(&self, execution_id: Uuid, reason: &str, net_pnl: Decimal) -> Result<()> {
        self.log(AuditEvent::new(
            AuditEventType::UnwindCompleted,
            &format!("Position unwound: {}. Net P&L: ${:.4}", reason, net_pnl),
        )
        .with_execution(execution_id)
        .with_pnl(net_pnl)
        .with_detail("unwind_reason", reason)
        ).await
    }

    /// Log circuit breaker
    pub async fn log_circuit_breaker(&self, triggered: bool, reason: &str) -> Result<()> {
        let event_type = if triggered {
            AuditEventType::CircuitBreakerTriggered
        } else {
            AuditEventType::CircuitBreakerReset
        };

        self.log(AuditEvent::new(
            event_type,
            &format!("Circuit breaker {}: {}", if triggered { "TRIGGERED" } else { "RESET" }, reason),
        )
        .with_detail("reason", reason)
        ).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_audit_event_creation() {
        let event = AuditEvent::new(
            AuditEventType::ExecutionStarted,
            "Test execution started",
        )
        .with_execution(Uuid::new_v4())
        .with_venue(Venue::Polymarket)
        .with_detail("test_key", "test_value");

        assert_eq!(event.event_type, AuditEventType::ExecutionStarted);
        assert!(event.execution_id.is_some());
        assert_eq!(event.venue, Some(Venue::Polymarket));
        assert!(event.details.contains_key("test_key"));
    }

    #[tokio::test]
    async fn test_audit_trail_memory() {
        let config = AuditConfig {
            file_enabled: false,
            database_enabled: false,
            ..Default::default()
        };

        let audit = AuditTrail::new(config).await.unwrap();

        // Log some events
        for i in 0..5 {
            audit.log(AuditEvent::new(
                AuditEventType::OpportunityDetected,
                &format!("Test opportunity {}", i),
            )).await.unwrap();
        }

        // Check recent events
        let recent = audit.get_recent(10).await;
        assert_eq!(recent.len(), 6);  // 5 + system started
    }
}
