//! Position drift detection for cross-venue arbitrage.
//!
//! Detects when positions drift from expected hedged state:
//! - Unbalanced exposure across venues
//! - Unexpected P&L variance
//! - Timing drift in hedge execution

use std::collections::HashMap;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use std::sync::Arc;
use chrono::{DateTime, Utc, Duration};

use super::position_tracker::{PositionTracker, Position, PositionSnapshot};

/// Severity of a drift alert.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DriftSeverity {
    /// Informational - within normal variance.
    Info,
    /// Warning - approaching limits.
    Warning,
    /// Critical - requires immediate attention.
    Critical,
    /// Emergency - automated response may be triggered.
    Emergency,
}

/// A drift alert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftAlert {
    /// Unique alert ID.
    pub id: String,
    /// Alert type.
    pub alert_type: DriftAlertType,
    /// Severity level.
    pub severity: DriftSeverity,
    /// Affected venues.
    pub venues: Vec<String>,
    /// Affected markets.
    pub markets: Vec<String>,
    /// Description of the drift.
    pub description: String,
    /// Current value of the metric.
    pub current_value: Decimal,
    /// Expected/threshold value.
    pub threshold_value: Decimal,
    /// Deviation from expected.
    pub deviation: Decimal,
    /// When the drift was detected.
    pub detected_at: DateTime<Utc>,
    /// How long the drift has persisted.
    pub duration_seconds: i64,
    /// Recommended action.
    pub recommended_action: String,
    /// Whether the alert has been acknowledged.
    pub acknowledged: bool,
    /// Whether the drift has been resolved.
    pub resolved: bool,
    /// Resolution time.
    pub resolved_at: Option<DateTime<Utc>>,
}

/// Type of drift alert.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DriftAlertType {
    /// Net exposure exceeds threshold.
    ExposureImbalance,
    /// Single venue exposure too high.
    VenueConcentration,
    /// Hedge ratio drifted from target.
    HedgeRatioDrift,
    /// P&L variance higher than expected.
    PnLVariance,
    /// Position age exceeds limit.
    StalePosition,
    /// Cross-venue positions not balanced.
    CrossVenueImbalance,
    /// Unrealized loss exceeds threshold.
    UnrealizedLossLimit,
    /// Total exposure exceeds limit.
    ExposureLimit,
}

/// Configuration for drift detection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftDetectorConfig {
    /// Maximum net exposure (in base currency).
    pub max_net_exposure: Decimal,
    /// Maximum exposure per venue.
    pub max_venue_exposure: Decimal,
    /// Maximum total exposure.
    pub max_total_exposure: Decimal,
    /// Target hedge ratio (1.0 = fully hedged).
    pub target_hedge_ratio: Decimal,
    /// Allowable hedge ratio deviation.
    pub hedge_ratio_tolerance: Decimal,
    /// Maximum unrealized loss.
    pub max_unrealized_loss: Decimal,
    /// Maximum position age in seconds.
    pub max_position_age_seconds: i64,
    /// P&L variance threshold (standard deviations).
    pub pnl_variance_threshold: Decimal,
    /// Check interval in milliseconds.
    pub check_interval_ms: u64,
    /// Thresholds for severity escalation.
    pub severity_thresholds: SeverityThresholds,
}

/// Thresholds for severity levels (as multipliers of base threshold).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeverityThresholds {
    pub warning_multiplier: Decimal,
    pub critical_multiplier: Decimal,
    pub emergency_multiplier: Decimal,
}

impl Default for SeverityThresholds {
    fn default() -> Self {
        Self {
            warning_multiplier: Decimal::new(8, 1),   // 0.8x = 80% of limit
            critical_multiplier: Decimal::new(95, 2), // 0.95x = 95% of limit
            emergency_multiplier: Decimal::ONE,        // 1.0x = at limit
        }
    }
}

impl Default for DriftDetectorConfig {
    fn default() -> Self {
        Self {
            max_net_exposure: Decimal::new(10000, 0),
            max_venue_exposure: Decimal::new(50000, 0),
            max_total_exposure: Decimal::new(100000, 0),
            target_hedge_ratio: Decimal::ONE,
            hedge_ratio_tolerance: Decimal::new(5, 2), // 5%
            max_unrealized_loss: Decimal::new(5000, 0),
            max_position_age_seconds: 86400, // 24 hours
            pnl_variance_threshold: Decimal::new(3, 0), // 3 std devs
            check_interval_ms: 10000, // 10 seconds
            severity_thresholds: SeverityThresholds::default(),
        }
    }
}

/// Position drift detector.
pub struct DriftDetector {
    /// Configuration.
    config: DriftDetectorConfig,
    /// Position tracker.
    tracker: Arc<PositionTracker>,
    /// Active alerts.
    alerts: Arc<RwLock<Vec<DriftAlert>>>,
    /// Alert history.
    alert_history: Arc<RwLock<Vec<DriftAlert>>>,
    /// P&L history for variance calculation.
    pnl_history: Arc<RwLock<Vec<(DateTime<Utc>, Decimal)>>>,
    /// First detection time for persistent drifts.
    drift_start_times: Arc<RwLock<HashMap<String, DateTime<Utc>>>>,
    /// Alert callbacks.
    alert_callbacks: Arc<RwLock<Vec<Box<dyn Fn(&DriftAlert) + Send + Sync>>>>,
    /// Running flag.
    running: Arc<RwLock<bool>>,
}

impl DriftDetector {
    /// Create a new drift detector.
    pub fn new(config: DriftDetectorConfig, tracker: Arc<PositionTracker>) -> Self {
        Self {
            config,
            tracker,
            alerts: Arc::new(RwLock::new(Vec::new())),
            alert_history: Arc::new(RwLock::new(Vec::new())),
            pnl_history: Arc::new(RwLock::new(Vec::new())),
            drift_start_times: Arc::new(RwLock::new(HashMap::new())),
            alert_callbacks: Arc::new(RwLock::new(Vec::new())),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Register an alert callback.
    pub async fn on_alert<F>(&self, callback: F)
    where
        F: Fn(&DriftAlert) + Send + Sync + 'static,
    {
        self.alert_callbacks.write().await.push(Box::new(callback));
    }

    /// Start the drift detection daemon.
    pub async fn start(&self) {
        *self.running.write().await = true;

        let running = self.running.clone();
        let config = self.config.clone();
        let tracker = self.tracker.clone();
        let alerts = self.alerts.clone();
        let pnl_history = self.pnl_history.clone();
        let drift_start_times = self.drift_start_times.clone();
        let callbacks = self.alert_callbacks.clone();

        tokio::spawn(async move {
            while *running.read().await {
                // Get current snapshot
                let snapshot = tracker.get_snapshot().await;
                let positions = tracker.get_all_positions().await;

                // Record P&L for variance tracking
                {
                    let mut history = pnl_history.write().await;
                    history.push((Utc::now(), snapshot.total_pnl));
                    if history.len() > 1000 {
                        history.remove(0);
                    }
                }

                // Run all drift checks
                let new_alerts = Self::check_all_drifts(
                    &config,
                    &snapshot,
                    &positions,
                    &pnl_history,
                    &drift_start_times,
                ).await;

                // Process new alerts
                for alert in &new_alerts {
                    // Notify callbacks
                    let callbacks = callbacks.read().await;
                    for callback in callbacks.iter() {
                        callback(alert);
                    }
                }

                // Update alert list
                {
                    let mut current_alerts = alerts.write().await;
                    // Remove resolved alerts
                    current_alerts.retain(|a| !a.resolved);
                    // Add new alerts
                    current_alerts.extend(new_alerts);
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(config.check_interval_ms)).await;
            }
        });
    }

    /// Stop the drift detector.
    pub async fn stop(&self) {
        *self.running.write().await = false;
    }

    /// Run drift checks.
    async fn check_all_drifts(
        config: &DriftDetectorConfig,
        snapshot: &PositionSnapshot,
        positions: &[Position],
        pnl_history: &Arc<RwLock<Vec<(DateTime<Utc>, Decimal)>>>,
        drift_start_times: &Arc<RwLock<HashMap<String, DateTime<Utc>>>>,
    ) -> Vec<DriftAlert> {
        let mut alerts = Vec::new();
        let now = Utc::now();

        // Check net exposure
        if let Some(alert) = Self::check_exposure_imbalance(config, snapshot, drift_start_times, now).await {
            alerts.push(alert);
        }

        // Check venue concentration
        for (venue, exposure) in &snapshot.venue_exposure {
            if let Some(alert) = Self::check_venue_concentration(config, venue, *exposure, drift_start_times, now).await {
                alerts.push(alert);
            }
        }

        // Check total exposure
        let total_exposure = snapshot.total_long_exposure + snapshot.total_short_exposure;
        if let Some(alert) = Self::check_total_exposure(config, total_exposure, drift_start_times, now).await {
            alerts.push(alert);
        }

        // Check unrealized loss
        if snapshot.total_unrealized_pnl < -config.max_unrealized_loss {
            let severity = Self::calculate_severity(
                snapshot.total_unrealized_pnl.abs(),
                config.max_unrealized_loss,
                &config.severity_thresholds,
            );

            let key = "unrealized_loss".to_string();
            let duration = Self::get_drift_duration(drift_start_times, &key, now).await;

            alerts.push(DriftAlert {
                id: uuid::Uuid::new_v4().to_string(),
                alert_type: DriftAlertType::UnrealizedLossLimit,
                severity,
                venues: snapshot.venue_exposure.keys().cloned().collect(),
                markets: Vec::new(),
                description: format!(
                    "Unrealized loss {} exceeds limit {}",
                    snapshot.total_unrealized_pnl, config.max_unrealized_loss
                ),
                current_value: snapshot.total_unrealized_pnl,
                threshold_value: -config.max_unrealized_loss,
                deviation: snapshot.total_unrealized_pnl.abs() - config.max_unrealized_loss,
                detected_at: now,
                duration_seconds: duration,
                recommended_action: "Review positions and consider reducing exposure".to_string(),
                acknowledged: false,
                resolved: false,
                resolved_at: None,
            });
        }

        // Check stale positions
        for position in positions {
            let age = (now - position.opened_at).num_seconds();
            if age > config.max_position_age_seconds {
                let severity = if age > config.max_position_age_seconds * 2 {
                    DriftSeverity::Critical
                } else {
                    DriftSeverity::Warning
                };

                alerts.push(DriftAlert {
                    id: uuid::Uuid::new_v4().to_string(),
                    alert_type: DriftAlertType::StalePosition,
                    severity,
                    venues: vec![position.venue.clone()],
                    markets: vec![position.market_id.clone()],
                    description: format!(
                        "Position {} age {} seconds exceeds limit {}",
                        position.key(), age, config.max_position_age_seconds
                    ),
                    current_value: Decimal::from(age),
                    threshold_value: Decimal::from(config.max_position_age_seconds),
                    deviation: Decimal::from(age - config.max_position_age_seconds),
                    detected_at: now,
                    duration_seconds: age - config.max_position_age_seconds,
                    recommended_action: "Review position and close if market has resolved".to_string(),
                    acknowledged: false,
                    resolved: false,
                    resolved_at: None,
                });
            }
        }

        // Check P&L variance
        if let Some(alert) = Self::check_pnl_variance(config, pnl_history, drift_start_times, now).await {
            alerts.push(alert);
        }

        alerts
    }

    /// Check for exposure imbalance.
    async fn check_exposure_imbalance(
        config: &DriftDetectorConfig,
        snapshot: &PositionSnapshot,
        drift_start_times: &Arc<RwLock<HashMap<String, DateTime<Utc>>>>,
        now: DateTime<Utc>,
    ) -> Option<DriftAlert> {
        let net_exposure = snapshot.net_exposure.abs();

        if net_exposure > config.max_net_exposure {
            let severity = Self::calculate_severity(
                net_exposure,
                config.max_net_exposure,
                &config.severity_thresholds,
            );

            let key = "net_exposure".to_string();
            let duration = Self::get_drift_duration(drift_start_times, &key, now).await;

            Some(DriftAlert {
                id: uuid::Uuid::new_v4().to_string(),
                alert_type: DriftAlertType::ExposureImbalance,
                severity,
                venues: snapshot.venue_exposure.keys().cloned().collect(),
                markets: Vec::new(),
                description: format!(
                    "Net exposure {} exceeds limit {}",
                    net_exposure, config.max_net_exposure
                ),
                current_value: net_exposure,
                threshold_value: config.max_net_exposure,
                deviation: net_exposure - config.max_net_exposure,
                detected_at: now,
                duration_seconds: duration,
                recommended_action: "Rebalance positions to reduce net exposure".to_string(),
                acknowledged: false,
                resolved: false,
                resolved_at: None,
            })
        } else {
            // Clear drift start time if within limits
            drift_start_times.write().await.remove("net_exposure");
            None
        }
    }

    /// Check for venue concentration.
    async fn check_venue_concentration(
        config: &DriftDetectorConfig,
        venue: &str,
        exposure: Decimal,
        drift_start_times: &Arc<RwLock<HashMap<String, DateTime<Utc>>>>,
        now: DateTime<Utc>,
    ) -> Option<DriftAlert> {
        if exposure > config.max_venue_exposure {
            let severity = Self::calculate_severity(
                exposure,
                config.max_venue_exposure,
                &config.severity_thresholds,
            );

            let key = format!("venue_{}", venue);
            let duration = Self::get_drift_duration(drift_start_times, &key, now).await;

            Some(DriftAlert {
                id: uuid::Uuid::new_v4().to_string(),
                alert_type: DriftAlertType::VenueConcentration,
                severity,
                venues: vec![venue.to_string()],
                markets: Vec::new(),
                description: format!(
                    "Venue {} exposure {} exceeds limit {}",
                    venue, exposure, config.max_venue_exposure
                ),
                current_value: exposure,
                threshold_value: config.max_venue_exposure,
                deviation: exposure - config.max_venue_exposure,
                detected_at: now,
                duration_seconds: duration,
                recommended_action: format!("Reduce exposure on venue {}", venue),
                acknowledged: false,
                resolved: false,
                resolved_at: None,
            })
        } else {
            drift_start_times.write().await.remove(&format!("venue_{}", venue));
            None
        }
    }

    /// Check total exposure.
    async fn check_total_exposure(
        config: &DriftDetectorConfig,
        total_exposure: Decimal,
        drift_start_times: &Arc<RwLock<HashMap<String, DateTime<Utc>>>>,
        now: DateTime<Utc>,
    ) -> Option<DriftAlert> {
        if total_exposure > config.max_total_exposure {
            let severity = Self::calculate_severity(
                total_exposure,
                config.max_total_exposure,
                &config.severity_thresholds,
            );

            let key = "total_exposure".to_string();
            let duration = Self::get_drift_duration(drift_start_times, &key, now).await;

            Some(DriftAlert {
                id: uuid::Uuid::new_v4().to_string(),
                alert_type: DriftAlertType::ExposureLimit,
                severity,
                venues: Vec::new(),
                markets: Vec::new(),
                description: format!(
                    "Total exposure {} exceeds limit {}",
                    total_exposure, config.max_total_exposure
                ),
                current_value: total_exposure,
                threshold_value: config.max_total_exposure,
                deviation: total_exposure - config.max_total_exposure,
                detected_at: now,
                duration_seconds: duration,
                recommended_action: "Reduce overall position size".to_string(),
                acknowledged: false,
                resolved: false,
                resolved_at: None,
            })
        } else {
            drift_start_times.write().await.remove("total_exposure");
            None
        }
    }

    /// Check P&L variance.
    async fn check_pnl_variance(
        config: &DriftDetectorConfig,
        pnl_history: &Arc<RwLock<Vec<(DateTime<Utc>, Decimal)>>>,
        drift_start_times: &Arc<RwLock<HashMap<String, DateTime<Utc>>>>,
        now: DateTime<Utc>,
    ) -> Option<DriftAlert> {
        let history = pnl_history.read().await;

        if history.len() < 10 {
            return None;
        }

        // Calculate mean and standard deviation
        let pnls: Vec<f64> = history.iter().map(|(_, p)| p.to_f64().unwrap_or(0.0)).collect();
        let mean = pnls.iter().sum::<f64>() / pnls.len() as f64;
        let variance = pnls.iter().map(|p| (p - mean).powi(2)).sum::<f64>() / pnls.len() as f64;
        let std_dev = variance.sqrt();

        if let Some((_, current_pnl)) = history.last() {
            let current_f64 = current_pnl.to_f64().unwrap_or(0.0);
            let z_score = if std_dev > 0.0 {
                (current_f64 - mean).abs() / std_dev
            } else {
                0.0
            };

            let threshold_f64 = config.pnl_variance_threshold.to_f64().unwrap_or(3.0);

            if z_score > threshold_f64 {
                let severity = if z_score > threshold_f64 * 2.0 {
                    DriftSeverity::Critical
                } else if z_score > threshold_f64 * 1.5 {
                    DriftSeverity::Warning
                } else {
                    DriftSeverity::Info
                };

                let key = "pnl_variance".to_string();
                let duration = Self::get_drift_duration(drift_start_times, &key, now).await;

                return Some(DriftAlert {
                    id: uuid::Uuid::new_v4().to_string(),
                    alert_type: DriftAlertType::PnLVariance,
                    severity,
                    venues: Vec::new(),
                    markets: Vec::new(),
                    description: format!(
                        "P&L variance {:.2} std devs exceeds threshold {:.2}",
                        z_score, threshold_f64
                    ),
                    current_value: Decimal::from_f64(z_score).unwrap_or(Decimal::ZERO),
                    threshold_value: config.pnl_variance_threshold,
                    deviation: Decimal::from_f64(z_score - threshold_f64).unwrap_or(Decimal::ZERO),
                    detected_at: now,
                    duration_seconds: duration,
                    recommended_action: "Investigate unusual P&L movement".to_string(),
                    acknowledged: false,
                    resolved: false,
                    resolved_at: None,
                });
            }
        }

        drift_start_times.write().await.remove("pnl_variance");
        None
    }

    /// Calculate severity based on value vs threshold.
    fn calculate_severity(
        current: Decimal,
        threshold: Decimal,
        thresholds: &SeverityThresholds,
    ) -> DriftSeverity {
        if threshold == Decimal::ZERO {
            return DriftSeverity::Critical;
        }

        let ratio = current / threshold;

        if ratio >= thresholds.emergency_multiplier {
            DriftSeverity::Emergency
        } else if ratio >= thresholds.critical_multiplier {
            DriftSeverity::Critical
        } else if ratio >= thresholds.warning_multiplier {
            DriftSeverity::Warning
        } else {
            DriftSeverity::Info
        }
    }

    /// Get drift duration, recording start time if new.
    async fn get_drift_duration(
        drift_start_times: &Arc<RwLock<HashMap<String, DateTime<Utc>>>>,
        key: &str,
        now: DateTime<Utc>,
    ) -> i64 {
        let mut times = drift_start_times.write().await;

        if let Some(start) = times.get(key) {
            (now - *start).num_seconds()
        } else {
            times.insert(key.to_string(), now);
            0
        }
    }

    /// Get active alerts.
    pub async fn get_alerts(&self) -> Vec<DriftAlert> {
        self.alerts.read().await.clone()
    }

    /// Get alerts by severity.
    pub async fn get_alerts_by_severity(&self, min_severity: DriftSeverity) -> Vec<DriftAlert> {
        self.alerts
            .read()
            .await
            .iter()
            .filter(|a| a.severity >= min_severity)
            .cloned()
            .collect()
    }

    /// Acknowledge an alert.
    pub async fn acknowledge_alert(&self, alert_id: &str) -> Result<(), String> {
        let mut alerts = self.alerts.write().await;
        let alert = alerts
            .iter_mut()
            .find(|a| a.id == alert_id)
            .ok_or("Alert not found")?;

        alert.acknowledged = true;
        Ok(())
    }

    /// Resolve an alert.
    pub async fn resolve_alert(&self, alert_id: &str) -> Result<(), String> {
        let mut alerts = self.alerts.write().await;
        let alert = alerts
            .iter_mut()
            .find(|a| a.id == alert_id)
            .ok_or("Alert not found")?;

        alert.resolved = true;
        alert.resolved_at = Some(Utc::now());

        // Move to history
        let resolved = alert.clone();
        self.alert_history.write().await.push(resolved);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_severity_calculation() {
        let thresholds = SeverityThresholds::default();

        let severity = DriftDetector::calculate_severity(
            Decimal::new(100, 0),
            Decimal::new(100, 0),
            &thresholds,
        );
        assert_eq!(severity, DriftSeverity::Emergency);

        let severity = DriftDetector::calculate_severity(
            Decimal::new(50, 0),
            Decimal::new(100, 0),
            &thresholds,
        );
        assert_eq!(severity, DriftSeverity::Info);
    }

    #[tokio::test]
    async fn test_drift_detector_creation() {
        let tracker = Arc::new(PositionTracker::new(100));
        let config = DriftDetectorConfig::default();
        let detector = DriftDetector::new(config, tracker);

        let alerts = detector.get_alerts().await;
        assert!(alerts.is_empty());
    }
}
