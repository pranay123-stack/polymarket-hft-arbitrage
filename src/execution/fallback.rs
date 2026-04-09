//! Graceful Degradation and Fallback Venue Selection
//!
//! Provides resilience mechanisms for handling venue failures and degraded
//! service conditions in cross-platform arbitrage.
//!
//! ## Features:
//!
//! - **Venue Health Monitoring**: Real-time health scores based on latency, errors, fills
//! - **Automatic Fallback**: Route orders to healthy venues when primary fails
//! - **Degraded Mode**: Reduce order size/frequency when venues are partially healthy
//! - **Circuit Breaker Integration**: Prevent overwhelming unhealthy services
//! - **Recovery Detection**: Automatically restore normal operation when venues recover

use crate::core::error::{Error, Result};
use crate::execution::Venue;
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Health status of a venue
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VenueHealthStatus {
    /// Fully operational
    Healthy,
    /// Experiencing issues but still usable
    Degraded,
    /// Temporarily unavailable
    Unhealthy,
    /// Completely offline
    Offline,
    /// Unknown status (no data)
    Unknown,
}

impl VenueHealthStatus {
    /// Whether the venue can accept orders
    pub fn can_trade(&self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded)
    }

    /// Whether to use reduced position sizes
    pub fn is_degraded(&self) -> bool {
        matches!(self, Self::Degraded)
    }
}

/// Venue health metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueHealthMetrics {
    /// Venue identifier
    pub venue: Venue,
    /// Current health status
    pub status: VenueHealthStatus,
    /// Health score (0.0 - 1.0)
    pub health_score: f64,
    /// Average latency (ms) in last window
    pub avg_latency_ms: f64,
    /// P99 latency (ms) in last window
    pub p99_latency_ms: f64,
    /// Error rate (0.0 - 1.0) in last window
    pub error_rate: f64,
    /// Fill rate (0.0 - 1.0) in last window
    pub fill_rate: f64,
    /// Consecutive failures count
    pub consecutive_failures: u32,
    /// Last successful request time
    pub last_success: Option<DateTime<Utc>>,
    /// Last failure time
    pub last_failure: Option<DateTime<Utc>>,
    /// Last health check time
    pub last_check: DateTime<Utc>,
}

impl VenueHealthMetrics {
    pub fn new(venue: Venue) -> Self {
        Self {
            venue,
            status: VenueHealthStatus::Unknown,
            health_score: 0.5,
            avg_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            error_rate: 0.0,
            fill_rate: 1.0,
            consecutive_failures: 0,
            last_success: None,
            last_failure: None,
            last_check: Utc::now(),
        }
    }

    /// Update status based on current metrics
    pub fn update_status(&mut self) {
        self.status = if self.consecutive_failures >= 10 {
            VenueHealthStatus::Offline
        } else if self.consecutive_failures >= 5 || self.error_rate > 0.5 {
            VenueHealthStatus::Unhealthy
        } else if self.consecutive_failures >= 2 || self.error_rate > 0.1 || self.avg_latency_ms > 5000.0 {
            VenueHealthStatus::Degraded
        } else if self.health_score > 0.7 {
            VenueHealthStatus::Healthy
        } else {
            VenueHealthStatus::Degraded
        };

        self.last_check = Utc::now();
    }
}

/// Configuration for fallback behavior
#[derive(Debug, Clone)]
pub struct FallbackConfig {
    /// Health check interval
    pub health_check_interval: std::time::Duration,
    /// Latency threshold for degraded status (ms)
    pub latency_threshold_degraded_ms: u64,
    /// Latency threshold for unhealthy status (ms)
    pub latency_threshold_unhealthy_ms: u64,
    /// Error rate threshold for degraded status
    pub error_rate_threshold_degraded: f64,
    /// Error rate threshold for unhealthy status
    pub error_rate_threshold_unhealthy: f64,
    /// Consecutive failures before marking offline
    pub consecutive_failures_offline: u32,
    /// Recovery check interval when unhealthy
    pub recovery_check_interval: std::time::Duration,
    /// Position size multiplier when degraded
    pub degraded_position_multiplier: Decimal,
    /// Minimum position size multiplier
    pub min_position_multiplier: Decimal,
    /// Enable automatic fallback
    pub auto_fallback_enabled: bool,
    /// Fallback venue preferences (ordered)
    pub fallback_preferences: HashMap<Venue, Vec<Venue>>,
}

impl Default for FallbackConfig {
    fn default() -> Self {
        let mut fallback_prefs = HashMap::new();
        fallback_prefs.insert(Venue::Polymarket, vec![Venue::Kalshi, Venue::Opinion]);
        fallback_prefs.insert(Venue::Kalshi, vec![Venue::Polymarket, Venue::Opinion]);
        fallback_prefs.insert(Venue::Opinion, vec![Venue::Kalshi, Venue::Polymarket]);

        Self {
            health_check_interval: std::time::Duration::from_secs(30),
            latency_threshold_degraded_ms: 2000,
            latency_threshold_unhealthy_ms: 5000,
            error_rate_threshold_degraded: 0.05,
            error_rate_threshold_unhealthy: 0.20,
            consecutive_failures_offline: 10,
            recovery_check_interval: std::time::Duration::from_secs(60),
            degraded_position_multiplier: dec!(0.5),
            min_position_multiplier: dec!(0.1),
            auto_fallback_enabled: true,
            fallback_preferences: fallback_prefs,
        }
    }
}

/// Rolling window for metrics calculation
#[derive(Debug)]
struct MetricsWindow {
    /// Window duration
    window: std::time::Duration,
    /// Latency samples
    latencies: Vec<(Instant, u64)>,
    /// Error count
    errors: u32,
    /// Success count
    successes: u32,
    /// Fill count
    fills: u32,
    /// Attempt count
    attempts: u32,
}

impl MetricsWindow {
    fn new(window: std::time::Duration) -> Self {
        Self {
            window,
            latencies: Vec::with_capacity(1000),
            errors: 0,
            successes: 0,
            fills: 0,
            attempts: 0,
        }
    }

    fn record_success(&mut self, latency_ms: u64) {
        let now = Instant::now();
        self.cleanup(now);
        self.latencies.push((now, latency_ms));
        self.successes += 1;
    }

    fn record_error(&mut self) {
        let now = Instant::now();
        self.cleanup(now);
        self.errors += 1;
    }

    fn record_fill(&mut self) {
        let now = Instant::now();
        self.cleanup(now);
        self.fills += 1;
        self.attempts += 1;
    }

    fn record_no_fill(&mut self) {
        let now = Instant::now();
        self.cleanup(now);
        self.attempts += 1;
    }

    fn cleanup(&mut self, now: Instant) {
        let cutoff = now - self.window;
        self.latencies.retain(|(t, _)| *t > cutoff);
    }

    fn avg_latency(&self) -> f64 {
        if self.latencies.is_empty() {
            return 0.0;
        }
        let sum: u64 = self.latencies.iter().map(|(_, l)| l).sum();
        sum as f64 / self.latencies.len() as f64
    }

    fn p99_latency(&self) -> f64 {
        if self.latencies.is_empty() {
            return 0.0;
        }
        let mut sorted: Vec<u64> = self.latencies.iter().map(|(_, l)| *l).collect();
        sorted.sort_unstable();
        let idx = (sorted.len() as f64 * 0.99) as usize;
        sorted.get(idx.min(sorted.len() - 1)).copied().unwrap_or(0) as f64
    }

    fn error_rate(&self) -> f64 {
        let total = self.successes + self.errors;
        if total == 0 {
            return 0.0;
        }
        self.errors as f64 / total as f64
    }

    fn fill_rate(&self) -> f64 {
        if self.attempts == 0 {
            return 1.0;
        }
        self.fills as f64 / self.attempts as f64
    }
}

/// Venue health tracker
struct VenueTracker {
    metrics: VenueHealthMetrics,
    window: MetricsWindow,
    consecutive_failures: u32,
}

impl VenueTracker {
    fn new(venue: Venue) -> Self {
        Self {
            metrics: VenueHealthMetrics::new(venue),
            window: MetricsWindow::new(std::time::Duration::from_secs(300)), // 5 min window
            consecutive_failures: 0,
        }
    }

    fn record_success(&mut self, latency_ms: u64) {
        self.window.record_success(latency_ms);
        self.consecutive_failures = 0;
        self.metrics.last_success = Some(Utc::now());
        self.update_metrics();
    }

    fn record_error(&mut self) {
        self.window.record_error();
        self.consecutive_failures += 1;
        self.metrics.last_failure = Some(Utc::now());
        self.update_metrics();
    }

    fn record_fill(&mut self) {
        self.window.record_fill();
        self.update_metrics();
    }

    fn record_no_fill(&mut self) {
        self.window.record_no_fill();
        self.update_metrics();
    }

    fn update_metrics(&mut self) {
        self.metrics.avg_latency_ms = self.window.avg_latency();
        self.metrics.p99_latency_ms = self.window.p99_latency();
        self.metrics.error_rate = self.window.error_rate();
        self.metrics.fill_rate = self.window.fill_rate();
        self.metrics.consecutive_failures = self.consecutive_failures;

        // Calculate health score (weighted combination of metrics)
        let latency_score = 1.0 - (self.metrics.avg_latency_ms / 10000.0).min(1.0);
        let error_score = 1.0 - self.metrics.error_rate;
        let fill_score = self.metrics.fill_rate;
        let failure_score = 1.0 - (self.consecutive_failures as f64 / 10.0).min(1.0);

        self.metrics.health_score = (latency_score * 0.2
            + error_score * 0.4
            + fill_score * 0.2
            + failure_score * 0.2)
            .max(0.0)
            .min(1.0);

        self.metrics.update_status();
    }
}

/// Fallback Manager
///
/// Manages venue health and provides fallback routing decisions.
pub struct FallbackManager {
    config: FallbackConfig,
    trackers: Arc<RwLock<HashMap<Venue, VenueTracker>>>,
}

impl FallbackManager {
    pub fn new(config: FallbackConfig) -> Self {
        let mut trackers = HashMap::new();
        trackers.insert(Venue::Polymarket, VenueTracker::new(Venue::Polymarket));
        trackers.insert(Venue::Kalshi, VenueTracker::new(Venue::Kalshi));
        trackers.insert(Venue::Opinion, VenueTracker::new(Venue::Opinion));

        Self {
            config,
            trackers: Arc::new(RwLock::new(trackers)),
        }
    }

    /// Record a successful API call
    pub async fn record_success(&self, venue: Venue, latency_ms: u64) {
        let mut trackers = self.trackers.write().await;
        if let Some(tracker) = trackers.get_mut(&venue) {
            tracker.record_success(latency_ms);
        }
    }

    /// Record a failed API call
    pub async fn record_error(&self, venue: Venue) {
        let mut trackers = self.trackers.write().await;
        if let Some(tracker) = trackers.get_mut(&venue) {
            tracker.record_error();
        }
    }

    /// Record an order fill
    pub async fn record_fill(&self, venue: Venue) {
        let mut trackers = self.trackers.write().await;
        if let Some(tracker) = trackers.get_mut(&venue) {
            tracker.record_fill();
        }
    }

    /// Record an order that didn't fill
    pub async fn record_no_fill(&self, venue: Venue) {
        let mut trackers = self.trackers.write().await;
        if let Some(tracker) = trackers.get_mut(&venue) {
            tracker.record_no_fill();
        }
    }

    /// Get current health metrics for a venue
    pub async fn get_health(&self, venue: Venue) -> VenueHealthMetrics {
        let trackers = self.trackers.read().await;
        trackers
            .get(&venue)
            .map(|t| t.metrics.clone())
            .unwrap_or_else(|| VenueHealthMetrics::new(venue))
    }

    /// Get health metrics for all venues
    pub async fn get_all_health(&self) -> Vec<VenueHealthMetrics> {
        let trackers = self.trackers.read().await;
        trackers.values().map(|t| t.metrics.clone()).collect()
    }

    /// Check if a venue can accept orders
    pub async fn can_trade(&self, venue: Venue) -> bool {
        let trackers = self.trackers.read().await;
        trackers
            .get(&venue)
            .map(|t| t.metrics.status.can_trade())
            .unwrap_or(false)
    }

    /// Get position size multiplier for a venue
    pub async fn get_position_multiplier(&self, venue: Venue) -> Decimal {
        let trackers = self.trackers.read().await;
        let tracker = match trackers.get(&venue) {
            Some(t) => t,
            None => return Decimal::ONE,
        };

        match tracker.metrics.status {
            VenueHealthStatus::Healthy => Decimal::ONE,
            VenueHealthStatus::Degraded => self.config.degraded_position_multiplier,
            VenueHealthStatus::Unhealthy => self.config.min_position_multiplier,
            VenueHealthStatus::Offline => Decimal::ZERO,
            VenueHealthStatus::Unknown => self.config.degraded_position_multiplier,
        }
    }

    /// Select best venue for an order
    pub async fn select_venue(&self, preferred: Venue) -> Option<Venue> {
        let trackers = self.trackers.read().await;

        // Check if preferred venue is healthy
        if let Some(tracker) = trackers.get(&preferred) {
            if tracker.metrics.status.can_trade() {
                return Some(preferred);
            }
        }

        // Fallback to alternatives if enabled
        if !self.config.auto_fallback_enabled {
            return None;
        }

        // Try fallback preferences
        if let Some(fallbacks) = self.config.fallback_preferences.get(&preferred) {
            for fallback in fallbacks {
                if let Some(tracker) = trackers.get(fallback) {
                    if tracker.metrics.status.can_trade() {
                        info!(
                            "Falling back from {:?} to {:?} due to health status",
                            preferred, fallback
                        );
                        return Some(*fallback);
                    }
                }
            }
        }

        // Last resort: any healthy venue
        for (venue, tracker) in trackers.iter() {
            if tracker.metrics.status.can_trade() {
                warn!(
                    "Using last-resort fallback venue {:?} for preferred {:?}",
                    venue, preferred
                );
                return Some(*venue);
            }
        }

        error!("No healthy venues available!");
        None
    }

    /// Get ordered list of venues by health score
    pub async fn get_venues_by_health(&self) -> Vec<(Venue, f64)> {
        let trackers = self.trackers.read().await;
        let mut venues: Vec<_> = trackers
            .iter()
            .filter(|(_, t)| t.metrics.status.can_trade())
            .map(|(v, t)| (*v, t.metrics.health_score))
            .collect();

        venues.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        venues
    }

    /// Check if system should enter degraded mode
    pub async fn is_degraded_mode(&self) -> bool {
        let trackers = self.trackers.read().await;
        let healthy_count = trackers
            .values()
            .filter(|t| t.metrics.status == VenueHealthStatus::Healthy)
            .count();

        // Degraded if less than half of venues are healthy
        healthy_count < trackers.len() / 2
    }

    /// Get summary of current system state
    pub async fn get_system_status(&self) -> SystemStatus {
        let trackers = self.trackers.read().await;

        let healthy = trackers
            .values()
            .filter(|t| t.metrics.status == VenueHealthStatus::Healthy)
            .count();
        let degraded = trackers
            .values()
            .filter(|t| t.metrics.status == VenueHealthStatus::Degraded)
            .count();
        let unhealthy = trackers
            .values()
            .filter(|t| {
                matches!(
                    t.metrics.status,
                    VenueHealthStatus::Unhealthy | VenueHealthStatus::Offline
                )
            })
            .count();

        let mode = if unhealthy == trackers.len() {
            SystemMode::Offline
        } else if healthy == 0 {
            SystemMode::Emergency
        } else if degraded > 0 || unhealthy > 0 {
            SystemMode::Degraded
        } else {
            SystemMode::Normal
        };

        SystemStatus {
            mode,
            healthy_venues: healthy,
            degraded_venues: degraded,
            unhealthy_venues: unhealthy,
            timestamp: Utc::now(),
        }
    }
}

/// System operating mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemMode {
    /// All systems operational
    Normal,
    /// Some venues degraded
    Degraded,
    /// Critical: Only emergency operations
    Emergency,
    /// No venues available
    Offline,
}

/// System status summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatus {
    pub mode: SystemMode,
    pub healthy_venues: usize,
    pub degraded_venues: usize,
    pub unhealthy_venues: usize,
    pub timestamp: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_venue_health_status() {
        assert!(VenueHealthStatus::Healthy.can_trade());
        assert!(VenueHealthStatus::Degraded.can_trade());
        assert!(!VenueHealthStatus::Unhealthy.can_trade());
        assert!(!VenueHealthStatus::Offline.can_trade());
    }

    #[tokio::test]
    async fn test_health_tracking() {
        let manager = FallbackManager::new(FallbackConfig::default());

        // Record successes
        for _ in 0..10 {
            manager.record_success(Venue::Polymarket, 100).await;
        }

        let health = manager.get_health(Venue::Polymarket).await;
        assert_eq!(health.status, VenueHealthStatus::Healthy);
        assert!(health.health_score > 0.7);
    }

    #[tokio::test]
    async fn test_consecutive_failures() {
        let manager = FallbackManager::new(FallbackConfig::default());

        // Record failures
        for _ in 0..5 {
            manager.record_error(Venue::Kalshi).await;
        }

        let health = manager.get_health(Venue::Kalshi).await;
        assert_eq!(health.status, VenueHealthStatus::Unhealthy);
        assert!(!health.status.can_trade());
    }

    #[tokio::test]
    async fn test_fallback_selection() {
        let manager = FallbackManager::new(FallbackConfig::default());

        // Make Polymarket healthy
        for _ in 0..10 {
            manager.record_success(Venue::Polymarket, 50).await;
        }

        // Make Kalshi unhealthy
        for _ in 0..10 {
            manager.record_error(Venue::Kalshi).await;
        }

        // Should fallback from Kalshi to Polymarket
        let selected = manager.select_venue(Venue::Kalshi).await;
        assert!(selected.is_some());
        assert_ne!(selected.unwrap(), Venue::Kalshi);
    }

    #[tokio::test]
    async fn test_position_multiplier() {
        let manager = FallbackManager::new(FallbackConfig::default());

        // Make venue degraded
        for _ in 0..3 {
            manager.record_error(Venue::Opinion).await;
        }

        let multiplier = manager.get_position_multiplier(Venue::Opinion).await;
        assert!(multiplier < Decimal::ONE);
    }
}
