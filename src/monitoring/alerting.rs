//! Production Alerting System
//!
//! Provides real-time alerts for critical events:
//! - Hedge failures and partial fills
//! - Circuit breaker triggers
//! - Venue disconnections
//! - Large P&L swings
//! - Position limit breaches
//!
//! Supports multiple notification channels:
//! - Slack
//! - Discord
//! - Telegram
//! - PagerDuty (for critical alerts)
//! - Email

use crate::core::error::{ArbitrageError, Result};
use crate::execution::cross_platform::{ExecutionState, FilledLegInfo, Venue, AbortReason};
use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

/// Alert severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum AlertSeverity {
    /// Informational (trade executed, etc.)
    Info,
    /// Warning (partial fill, high latency)
    Warning,
    /// Error (hedge failed, needs attention)
    Error,
    /// Critical (circuit breaker, kill switch, manual intervention required)
    Critical,
}

impl std::fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertSeverity::Info => write!(f, "INFO"),
            AlertSeverity::Warning => write!(f, "WARNING"),
            AlertSeverity::Error => write!(f, "ERROR"),
            AlertSeverity::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Alert category
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AlertCategory {
    Execution,
    HedgeFailure,
    PartialFill,
    VenueHealth,
    RiskLimit,
    CircuitBreaker,
    Position,
    PnL,
    System,
}

impl std::fmt::Display for AlertCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertCategory::Execution => write!(f, "Execution"),
            AlertCategory::HedgeFailure => write!(f, "Hedge Failure"),
            AlertCategory::PartialFill => write!(f, "Partial Fill"),
            AlertCategory::VenueHealth => write!(f, "Venue Health"),
            AlertCategory::RiskLimit => write!(f, "Risk Limit"),
            AlertCategory::CircuitBreaker => write!(f, "Circuit Breaker"),
            AlertCategory::Position => write!(f, "Position"),
            AlertCategory::PnL => write!(f, "P&L"),
            AlertCategory::System => write!(f, "System"),
        }
    }
}

/// Alert message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: String,
    pub severity: AlertSeverity,
    pub category: AlertCategory,
    pub title: String,
    pub message: String,
    pub details: HashMap<String, String>,
    pub timestamp: DateTime<Utc>,
    /// Affected venue(s)
    pub venues: Vec<Venue>,
    /// Related execution ID
    pub execution_id: Option<String>,
    /// Market ID
    pub market_id: Option<String>,
    /// Whether this alert requires acknowledgment
    pub requires_ack: bool,
    /// Has been acknowledged
    pub acknowledged: bool,
    pub acknowledged_by: Option<String>,
    pub acknowledged_at: Option<DateTime<Utc>>,
}

impl Alert {
    pub fn new(severity: AlertSeverity, category: AlertCategory, title: &str, message: &str) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            severity,
            category,
            title: title.to_string(),
            message: message.to_string(),
            details: HashMap::new(),
            timestamp: Utc::now(),
            venues: Vec::new(),
            execution_id: None,
            market_id: None,
            requires_ack: severity >= AlertSeverity::Error,
            acknowledged: false,
            acknowledged_by: None,
            acknowledged_at: None,
        }
    }

    pub fn with_detail(mut self, key: &str, value: &str) -> Self {
        self.details.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_venues(mut self, venues: Vec<Venue>) -> Self {
        self.venues = venues;
        self
    }

    pub fn with_execution_id(mut self, id: &str) -> Self {
        self.execution_id = Some(id.to_string());
        self
    }

    pub fn with_market_id(mut self, id: &str) -> Self {
        self.market_id = Some(id.to_string());
        self
    }
}

/// Alerting configuration
#[derive(Debug, Clone)]
pub struct AlertingConfig {
    /// Slack webhook URL
    pub slack_webhook: Option<String>,
    /// Discord webhook URL
    pub discord_webhook: Option<String>,
    /// Telegram bot token and chat ID
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    /// PagerDuty routing key (for critical alerts)
    pub pagerduty_routing_key: Option<String>,
    /// Email configuration
    pub email_smtp_host: Option<String>,
    pub email_from: Option<String>,
    pub email_to: Option<Vec<String>>,
    /// Minimum severity for each channel
    pub slack_min_severity: AlertSeverity,
    pub discord_min_severity: AlertSeverity,
    pub telegram_min_severity: AlertSeverity,
    pub pagerduty_min_severity: AlertSeverity,
    pub email_min_severity: AlertSeverity,
    /// Rate limiting (max alerts per minute)
    pub max_alerts_per_minute: u32,
    /// Deduplication window (seconds)
    pub dedup_window_secs: u64,
    /// Enable console logging of alerts
    pub log_to_console: bool,
}

impl Default for AlertingConfig {
    fn default() -> Self {
        Self {
            slack_webhook: None,
            discord_webhook: None,
            telegram_bot_token: None,
            telegram_chat_id: None,
            pagerduty_routing_key: None,
            email_smtp_host: None,
            email_from: None,
            email_to: None,
            slack_min_severity: AlertSeverity::Warning,
            discord_min_severity: AlertSeverity::Warning,
            telegram_min_severity: AlertSeverity::Error,
            pagerduty_min_severity: AlertSeverity::Critical,
            email_min_severity: AlertSeverity::Error,
            max_alerts_per_minute: 30,
            dedup_window_secs: 60,
            log_to_console: true,
        }
    }
}

/// Alerting system
pub struct AlertingSystem {
    config: AlertingConfig,
    client: Client,
    /// Alert history
    history: Arc<RwLock<Vec<Alert>>>,
    /// Recent alerts for deduplication (hash -> timestamp)
    recent_alerts: Arc<RwLock<HashMap<String, DateTime<Utc>>>>,
    /// Unacknowledged alerts
    unacknowledged: Arc<RwLock<Vec<Alert>>>,
    /// Alert counter for rate limiting
    alert_counts: Arc<RwLock<HashMap<i64, u32>>>,
}

impl AlertingSystem {
    pub fn new(config: AlertingConfig) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap();

        Self {
            config,
            client,
            history: Arc::new(RwLock::new(Vec::new())),
            recent_alerts: Arc::new(RwLock::new(HashMap::new())),
            unacknowledged: Arc::new(RwLock::new(Vec::new())),
            alert_counts: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Send an alert
    pub async fn alert(&self, alert: Alert) -> Result<()> {
        // Check rate limit
        if !self.check_rate_limit().await {
            warn!("Alert rate limit exceeded, dropping alert: {}", alert.title);
            return Ok(());
        }

        // Check deduplication
        if self.is_duplicate(&alert).await {
            debug!("Duplicate alert suppressed: {}", alert.title);
            return Ok(());
        }

        // Log to console
        if self.config.log_to_console {
            self.log_alert(&alert);
        }

        // Store in history
        {
            let mut history = self.history.write().await;
            history.push(alert.clone());

            // Keep last 1000 alerts
            if history.len() > 1000 {
                history.remove(0);
            }
        }

        // Track for deduplication
        {
            let hash = self.alert_hash(&alert);
            let mut recent = self.recent_alerts.write().await;
            recent.insert(hash, Utc::now());
        }

        // Track unacknowledged
        if alert.requires_ack {
            let mut unack = self.unacknowledged.write().await;
            unack.push(alert.clone());
        }

        // Send to channels
        let mut tasks = Vec::new();

        if alert.severity >= self.config.slack_min_severity {
            if let Some(ref webhook) = self.config.slack_webhook {
                let client = self.client.clone();
                let webhook = webhook.clone();
                let alert_clone = alert.clone();
                tasks.push(tokio::spawn(async move {
                    Self::send_slack(&client, &webhook, &alert_clone).await
                }));
            }
        }

        if alert.severity >= self.config.discord_min_severity {
            if let Some(ref webhook) = self.config.discord_webhook {
                let client = self.client.clone();
                let webhook = webhook.clone();
                let alert_clone = alert.clone();
                tasks.push(tokio::spawn(async move {
                    Self::send_discord(&client, &webhook, &alert_clone).await
                }));
            }
        }

        if alert.severity >= self.config.telegram_min_severity {
            if let (Some(ref token), Some(ref chat_id)) =
                (&self.config.telegram_bot_token, &self.config.telegram_chat_id)
            {
                let client = self.client.clone();
                let token = token.clone();
                let chat_id = chat_id.clone();
                let alert_clone = alert.clone();
                tasks.push(tokio::spawn(async move {
                    Self::send_telegram(&client, &token, &chat_id, &alert_clone).await
                }));
            }
        }

        if alert.severity >= self.config.pagerduty_min_severity {
            if let Some(ref routing_key) = self.config.pagerduty_routing_key {
                let client = self.client.clone();
                let routing_key = routing_key.clone();
                let alert_clone = alert.clone();
                tasks.push(tokio::spawn(async move {
                    Self::send_pagerduty(&client, &routing_key, &alert_clone).await
                }));
            }
        }

        // Wait for all sends
        for task in tasks {
            if let Err(e) = task.await {
                error!("Alert send task failed: {}", e);
            }
        }

        Ok(())
    }

    /// Check rate limit
    async fn check_rate_limit(&self) -> bool {
        let now = Utc::now();
        let minute = now.timestamp() / 60;

        let mut counts = self.alert_counts.write().await;

        // Clean old entries
        counts.retain(|&m, _| m >= minute - 1);

        let count = counts.entry(minute).or_insert(0);
        if *count >= self.config.max_alerts_per_minute {
            return false;
        }

        *count += 1;
        true
    }

    /// Check if alert is duplicate
    async fn is_duplicate(&self, alert: &Alert) -> bool {
        let hash = self.alert_hash(alert);
        let now = Utc::now();
        let window = chrono::Duration::seconds(self.config.dedup_window_secs as i64);

        let mut recent = self.recent_alerts.write().await;

        // Clean old entries
        recent.retain(|_, ts| now - *ts < window);

        recent.contains_key(&hash)
    }

    /// Generate hash for deduplication
    fn alert_hash(&self, alert: &Alert) -> String {
        format!(
            "{}:{}:{}:{}",
            alert.category,
            alert.title,
            alert.market_id.as_deref().unwrap_or(""),
            alert.execution_id.as_deref().unwrap_or("")
        )
    }

    /// Log alert to console
    fn log_alert(&self, alert: &Alert) {
        let emoji = match alert.severity {
            AlertSeverity::Info => "ℹ️",
            AlertSeverity::Warning => "⚠️",
            AlertSeverity::Error => "❌",
            AlertSeverity::Critical => "🚨",
        };

        let msg = format!(
            "{} [{}] {} - {}: {}",
            emoji,
            alert.severity,
            alert.category,
            alert.title,
            alert.message
        );

        match alert.severity {
            AlertSeverity::Info => info!("{}", msg),
            AlertSeverity::Warning => warn!("{}", msg),
            AlertSeverity::Error => error!("{}", msg),
            AlertSeverity::Critical => error!("{}", msg),
        }
    }

    /// Send Slack alert
    async fn send_slack(client: &Client, webhook: &str, alert: &Alert) -> Result<()> {
        let color = match alert.severity {
            AlertSeverity::Info => "#36a64f",
            AlertSeverity::Warning => "#ff9800",
            AlertSeverity::Error => "#f44336",
            AlertSeverity::Critical => "#9c27b0",
        };

        let mut fields: Vec<serde_json::Value> = Vec::new();

        if !alert.venues.is_empty() {
            fields.push(serde_json::json!({
                "title": "Venues",
                "value": alert.venues.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "),
                "short": true
            }));
        }

        if let Some(ref exec_id) = alert.execution_id {
            fields.push(serde_json::json!({
                "title": "Execution ID",
                "value": exec_id,
                "short": true
            }));
        }

        for (key, value) in &alert.details {
            fields.push(serde_json::json!({
                "title": key,
                "value": value,
                "short": true
            }));
        }

        let payload = serde_json::json!({
            "attachments": [{
                "color": color,
                "title": format!("[{}] {}", alert.category, alert.title),
                "text": alert.message,
                "fields": fields,
                "footer": "Cross-Platform Arbitrage Bot",
                "ts": alert.timestamp.timestamp()
            }]
        });

        let response = client.post(webhook)
            .json(&payload)
            .send()
            .await
            .map_err(|e| ArbitrageError::Internal(format!("Slack send failed: {}", e)))?;

        if !response.status().is_success() {
            warn!("Slack alert failed: {}", response.status());
        }

        Ok(())
    }

    /// Send Discord alert
    async fn send_discord(client: &Client, webhook: &str, alert: &Alert) -> Result<()> {
        let color = match alert.severity {
            AlertSeverity::Info => 0x36a64f,
            AlertSeverity::Warning => 0xff9800,
            AlertSeverity::Error => 0xf44336,
            AlertSeverity::Critical => 0x9c27b0,
        };

        let mut fields: Vec<serde_json::Value> = Vec::new();

        if !alert.venues.is_empty() {
            fields.push(serde_json::json!({
                "name": "Venues",
                "value": alert.venues.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "),
                "inline": true
            }));
        }

        for (key, value) in &alert.details {
            fields.push(serde_json::json!({
                "name": key,
                "value": value,
                "inline": true
            }));
        }

        let payload = serde_json::json!({
            "embeds": [{
                "title": format!("[{}] {} - {}", alert.severity, alert.category, alert.title),
                "description": alert.message,
                "color": color,
                "fields": fields,
                "timestamp": alert.timestamp.to_rfc3339()
            }]
        });

        let response = client.post(webhook)
            .json(&payload)
            .send()
            .await
            .map_err(|e| ArbitrageError::Internal(format!("Discord send failed: {}", e)))?;

        if !response.status().is_success() {
            warn!("Discord alert failed: {}", response.status());
        }

        Ok(())
    }

    /// Send Telegram alert
    async fn send_telegram(client: &Client, token: &str, chat_id: &str, alert: &Alert) -> Result<()> {
        let emoji = match alert.severity {
            AlertSeverity::Info => "ℹ️",
            AlertSeverity::Warning => "⚠️",
            AlertSeverity::Error => "❌",
            AlertSeverity::Critical => "🚨",
        };

        let mut text = format!(
            "{} *[{}] {} - {}*\n\n{}",
            emoji,
            alert.severity,
            alert.category,
            alert.title,
            alert.message
        );

        if !alert.details.is_empty() {
            text.push_str("\n\n*Details:*");
            for (key, value) in &alert.details {
                text.push_str(&format!("\n• {}: {}", key, value));
            }
        }

        let url = format!("https://api.telegram.org/bot{}/sendMessage", token);

        let payload = serde_json::json!({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown"
        });

        let response = client.post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| ArbitrageError::Internal(format!("Telegram send failed: {}", e)))?;

        if !response.status().is_success() {
            warn!("Telegram alert failed: {}", response.status());
        }

        Ok(())
    }

    /// Send PagerDuty alert (for critical issues)
    async fn send_pagerduty(client: &Client, routing_key: &str, alert: &Alert) -> Result<()> {
        let severity = match alert.severity {
            AlertSeverity::Info => "info",
            AlertSeverity::Warning => "warning",
            AlertSeverity::Error => "error",
            AlertSeverity::Critical => "critical",
        };

        let payload = serde_json::json!({
            "routing_key": routing_key,
            "event_action": "trigger",
            "dedup_key": alert.id,
            "payload": {
                "summary": format!("[{}] {} - {}", alert.category, alert.title, alert.message),
                "severity": severity,
                "source": "cross-platform-arbitrage-bot",
                "timestamp": alert.timestamp.to_rfc3339(),
                "custom_details": alert.details
            }
        });

        let response = client.post("https://events.pagerduty.com/v2/enqueue")
            .json(&payload)
            .send()
            .await
            .map_err(|e| ArbitrageError::Internal(format!("PagerDuty send failed: {}", e)))?;

        if !response.status().is_success() {
            warn!("PagerDuty alert failed: {}", response.status());
        }

        Ok(())
    }

    /// Acknowledge an alert
    pub async fn acknowledge(&self, alert_id: &str, by: &str) -> Result<()> {
        let mut unack = self.unacknowledged.write().await;

        if let Some(alert) = unack.iter_mut().find(|a| a.id == alert_id) {
            alert.acknowledged = true;
            alert.acknowledged_by = Some(by.to_string());
            alert.acknowledged_at = Some(Utc::now());

            // Remove from unacknowledged
            unack.retain(|a| a.id != alert_id);

            info!("Alert {} acknowledged by {}", alert_id, by);
        }

        Ok(())
    }

    /// Get unacknowledged alerts
    pub async fn get_unacknowledged(&self) -> Vec<Alert> {
        self.unacknowledged.read().await.clone()
    }

    /// Get alert history
    pub async fn get_history(&self, limit: usize) -> Vec<Alert> {
        let history = self.history.read().await;
        history.iter().rev().take(limit).cloned().collect()
    }

    // ========== Convenience methods for common alerts ==========

    /// Alert for hedge failure
    pub async fn alert_hedge_failure(
        &self,
        execution_id: &str,
        first_leg: &FilledLegInfo,
        reason: &str,
        estimated_loss: Decimal,
    ) {
        let alert = Alert::new(
            AlertSeverity::Error,
            AlertCategory::HedgeFailure,
            "Hedge Leg Failed",
            &format!(
                "Hedge failed after first leg filled. First leg: {} {} @ {}. Reason: {}",
                first_leg.side, first_leg.filled_qty, first_leg.avg_price, reason
            ),
        )
        .with_execution_id(execution_id)
        .with_venues(vec![first_leg.venue])
        .with_detail("First Leg Qty", &first_leg.filled_qty.to_string())
        .with_detail("First Leg Price", &first_leg.avg_price.to_string())
        .with_detail("Estimated Loss", &format!("${:.2}", estimated_loss))
        .with_detail("Failure Reason", reason);

        if let Err(e) = self.alert(alert).await {
            error!("Failed to send hedge failure alert: {}", e);
        }
    }

    /// Alert for partial fill
    pub async fn alert_partial_fill(
        &self,
        execution_id: &str,
        venue: Venue,
        order_id: &str,
        filled_pct: Decimal,
        filled_qty: Decimal,
        target_qty: Decimal,
    ) {
        let severity = if filled_pct < Decimal::from(50) {
            AlertSeverity::Warning
        } else {
            AlertSeverity::Info
        };

        let alert = Alert::new(
            severity,
            AlertCategory::PartialFill,
            "Order Partially Filled",
            &format!(
                "Order {} on {} filled {:.1}% ({} / {})",
                order_id, venue, filled_pct, filled_qty, target_qty
            ),
        )
        .with_execution_id(execution_id)
        .with_venues(vec![venue])
        .with_detail("Fill Percentage", &format!("{:.1}%", filled_pct))
        .with_detail("Filled Qty", &filled_qty.to_string())
        .with_detail("Target Qty", &target_qty.to_string());

        if let Err(e) = self.alert(alert).await {
            error!("Failed to send partial fill alert: {}", e);
        }
    }

    /// Alert for venue disconnection
    pub async fn alert_venue_disconnected(&self, venue: Venue, reason: &str) {
        let alert = Alert::new(
            AlertSeverity::Error,
            AlertCategory::VenueHealth,
            "Venue Disconnected",
            &format!("{} WebSocket disconnected: {}", venue, reason),
        )
        .with_venues(vec![venue])
        .with_detail("Reason", reason);

        if let Err(e) = self.alert(alert).await {
            error!("Failed to send venue disconnect alert: {}", e);
        }
    }

    /// Alert for circuit breaker trigger
    pub async fn alert_circuit_breaker(&self, reason: &str, cooldown_secs: u64) {
        let alert = Alert::new(
            AlertSeverity::Critical,
            AlertCategory::CircuitBreaker,
            "Circuit Breaker Triggered",
            &format!("Trading halted: {}. Cooldown: {} seconds.", reason, cooldown_secs),
        )
        .with_detail("Trigger Reason", reason)
        .with_detail("Cooldown", &format!("{}s", cooldown_secs));

        if let Err(e) = self.alert(alert).await {
            error!("Failed to send circuit breaker alert: {}", e);
        }
    }

    /// Alert for P&L threshold
    pub async fn alert_pnl_threshold(
        &self,
        current_pnl: Decimal,
        threshold: Decimal,
        is_loss: bool,
    ) {
        let (severity, title) = if is_loss {
            (AlertSeverity::Error, "Loss Threshold Reached")
        } else {
            (AlertSeverity::Info, "Profit Threshold Reached")
        };

        let alert = Alert::new(
            severity,
            AlertCategory::PnL,
            title,
            &format!(
                "Daily P&L: ${:.2} has crossed threshold ${:.2}",
                current_pnl, threshold
            ),
        )
        .with_detail("Current P&L", &format!("${:.2}", current_pnl))
        .with_detail("Threshold", &format!("${:.2}", threshold));

        if let Err(e) = self.alert(alert).await {
            error!("Failed to send P&L alert: {}", e);
        }
    }

    /// Alert for successful arbitrage execution
    pub async fn alert_execution_success(
        &self,
        execution_id: &str,
        market_id: &str,
        profit: Decimal,
        first_venue: Venue,
        hedge_venue: Venue,
    ) {
        let alert = Alert::new(
            AlertSeverity::Info,
            AlertCategory::Execution,
            "Arbitrage Executed Successfully",
            &format!(
                "Cross-platform arb completed: {} -> {}. Profit: ${:.4}",
                first_venue, hedge_venue, profit
            ),
        )
        .with_execution_id(execution_id)
        .with_market_id(market_id)
        .with_venues(vec![first_venue, hedge_venue])
        .with_detail("Profit", &format!("${:.4}", profit));

        if let Err(e) = self.alert(alert).await {
            error!("Failed to send execution success alert: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_alert_rate_limiting() {
        let config = AlertingConfig {
            max_alerts_per_minute: 2,
            log_to_console: false,
            ..Default::default()
        };

        let alerting = AlertingSystem::new(config);

        // First 2 should pass
        assert!(alerting.check_rate_limit().await);
        assert!(alerting.check_rate_limit().await);

        // Third should fail
        assert!(!alerting.check_rate_limit().await);
    }

    #[tokio::test]
    async fn test_alert_deduplication() {
        let config = AlertingConfig {
            dedup_window_secs: 60,
            log_to_console: false,
            ..Default::default()
        };

        let alerting = AlertingSystem::new(config);

        let alert1 = Alert::new(
            AlertSeverity::Warning,
            AlertCategory::VenueHealth,
            "Test Alert",
            "Test message",
        );

        // First should not be duplicate
        assert!(!alerting.is_duplicate(&alert1).await);

        // Track it
        {
            let hash = alerting.alert_hash(&alert1);
            let mut recent = alerting.recent_alerts.write().await;
            recent.insert(hash, Utc::now());
        }

        // Second identical should be duplicate
        let alert2 = Alert::new(
            AlertSeverity::Warning,
            AlertCategory::VenueHealth,
            "Test Alert",
            "Test message",
        );
        assert!(alerting.is_duplicate(&alert2).await);

        // Different alert should not be duplicate
        let alert3 = Alert::new(
            AlertSeverity::Warning,
            AlertCategory::VenueHealth,
            "Different Alert",
            "Different message",
        );
        assert!(!alerting.is_duplicate(&alert3).await);
    }
}
