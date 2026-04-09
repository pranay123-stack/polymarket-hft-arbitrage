//! Venue-level analytics for cross-platform arbitrage.
//!
//! Tracks:
//! - Per-venue performance
//! - Venue comparison
//! - Latency and availability metrics
//! - Fee analysis

use std::collections::HashMap;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use std::sync::Arc;
use chrono::{DateTime, Utc, Duration};

/// Health status of a venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VenueStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Offline,
}

/// Venue health metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueHealth {
    /// Venue identifier.
    pub venue_id: String,
    /// Current status.
    pub status: VenueStatus,
    /// Uptime percentage (last 24h).
    pub uptime_pct: f64,
    /// Average latency (milliseconds).
    pub avg_latency_ms: f64,
    /// P99 latency (milliseconds).
    pub p99_latency_ms: f64,
    /// Last successful response time.
    pub last_response: DateTime<Utc>,
    /// Current error rate.
    pub error_rate: f64,
    /// Consecutive errors.
    pub consecutive_errors: u32,
    /// API rate limit remaining.
    pub rate_limit_remaining: Option<u32>,
    /// WebSocket connection status.
    pub ws_connected: bool,
    /// Last updated.
    pub last_updated: DateTime<Utc>,
}

impl VenueHealth {
    pub fn is_tradeable(&self) -> bool {
        matches!(self.status, VenueStatus::Healthy | VenueStatus::Degraded)
            && self.error_rate < 0.1
            && self.consecutive_errors < 3
    }
}

/// Venue comparison data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueComparison {
    /// Timestamp.
    pub timestamp: DateTime<Utc>,
    /// Venue metrics.
    pub venues: Vec<VenueMetrics>,
    /// Best venue by P&L.
    pub best_by_pnl: Option<String>,
    /// Best venue by fill rate.
    pub best_by_fill_rate: Option<String>,
    /// Best venue by latency.
    pub best_by_latency: Option<String>,
    /// Best venue for liquidity.
    pub best_by_liquidity: Option<String>,
}

/// Metrics for a single venue.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VenueMetrics {
    /// Venue identifier.
    pub venue_id: String,
    /// Venue name.
    pub name: String,
    /// Total trades.
    pub total_trades: u64,
    /// Successful fills.
    pub successful_fills: u64,
    /// Fill rate.
    pub fill_rate: f64,
    /// Total volume (notional).
    pub total_volume: Decimal,
    /// Total P&L from this venue.
    pub total_pnl: Decimal,
    /// Total fees paid.
    pub total_fees: Decimal,
    /// Average fee rate (bps).
    pub avg_fee_bps: Decimal,
    /// Average slippage (bps).
    pub avg_slippage_bps: f64,
    /// Average order latency (ms).
    pub avg_latency_ms: f64,
    /// P95 order latency (ms).
    pub p95_latency_ms: f64,
    /// Average spread available.
    pub avg_spread_bps: f64,
    /// Average depth at touch.
    pub avg_depth_at_touch: Decimal,
    /// Number of markets active.
    pub active_markets: u32,
    /// Current health status.
    pub health: VenueHealth,
    /// Last updated.
    pub last_updated: DateTime<Utc>,
}

/// Venue analytics service.
pub struct VenueAnalytics {
    /// Per-venue metrics.
    metrics: Arc<RwLock<HashMap<String, VenueMetrics>>>,
    /// Health history.
    health_history: Arc<RwLock<HashMap<String, Vec<VenueHealth>>>>,
    /// Latency samples.
    latency_samples: Arc<RwLock<HashMap<String, Vec<(DateTime<Utc>, f64)>>>>,
    /// Max history size.
    max_history_size: usize,
}

impl VenueAnalytics {
    /// Create new venue analytics.
    pub fn new(max_history_size: usize) -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            health_history: Arc::new(RwLock::new(HashMap::new())),
            latency_samples: Arc::new(RwLock::new(HashMap::new())),
            max_history_size,
        }
    }

    /// Register a venue.
    pub async fn register_venue(&self, venue_id: &str, name: &str) {
        let now = Utc::now();
        let health = VenueHealth {
            venue_id: venue_id.to_string(),
            status: VenueStatus::Healthy,
            uptime_pct: 100.0,
            avg_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            last_response: now,
            error_rate: 0.0,
            consecutive_errors: 0,
            rate_limit_remaining: None,
            ws_connected: false,
            last_updated: now,
        };

        let metrics = VenueMetrics {
            venue_id: venue_id.to_string(),
            name: name.to_string(),
            health,
            last_updated: now,
            ..Default::default()
        };

        self.metrics.write().await.insert(venue_id.to_string(), metrics);
    }

    /// Record a trade on a venue.
    pub async fn record_trade(
        &self,
        venue_id: &str,
        volume: Decimal,
        pnl: Decimal,
        fees: Decimal,
        slippage_bps: f64,
        latency_ms: f64,
        success: bool,
    ) {
        if let Some(metrics) = self.metrics.write().await.get_mut(venue_id) {
            metrics.total_trades += 1;
            if success {
                metrics.successful_fills += 1;
            }
            metrics.total_volume += volume;
            metrics.total_pnl += pnl;
            metrics.total_fees += fees;

            // Update fill rate
            if metrics.total_trades > 0 {
                metrics.fill_rate = metrics.successful_fills as f64 / metrics.total_trades as f64;
            }

            // Update average fee
            if metrics.total_volume > Decimal::ZERO {
                metrics.avg_fee_bps = metrics.total_fees / metrics.total_volume * Decimal::new(10000, 0);
            }

            // Running average for slippage
            let n = metrics.total_trades as f64;
            metrics.avg_slippage_bps = metrics.avg_slippage_bps * (n - 1.0) / n + slippage_bps / n;

            // Running average for latency
            metrics.avg_latency_ms = metrics.avg_latency_ms * (n - 1.0) / n + latency_ms / n;

            metrics.last_updated = Utc::now();
        }

        // Record latency sample
        let mut samples = self.latency_samples.write().await;
        let venue_samples = samples.entry(venue_id.to_string()).or_insert_with(Vec::new);
        venue_samples.push((Utc::now(), latency_ms));
        if venue_samples.len() > self.max_history_size {
            venue_samples.remove(0);
        }
    }

    /// Update venue health.
    pub async fn update_health(
        &self,
        venue_id: &str,
        status: VenueStatus,
        latency_ms: Option<f64>,
        error: bool,
        ws_connected: bool,
    ) {
        let now = Utc::now();

        if let Some(metrics) = self.metrics.write().await.get_mut(venue_id) {
            metrics.health.status = status;
            metrics.health.ws_connected = ws_connected;

            if let Some(lat) = latency_ms {
                metrics.health.last_response = now;
                // Update avg latency
                metrics.health.avg_latency_ms =
                    metrics.health.avg_latency_ms * 0.9 + lat * 0.1;
            }

            if error {
                metrics.health.consecutive_errors += 1;
                // Update error rate (exponential moving average)
                metrics.health.error_rate = metrics.health.error_rate * 0.9 + 0.1;
            } else {
                metrics.health.consecutive_errors = 0;
                metrics.health.error_rate *= 0.9;
            }

            metrics.health.last_updated = now;
            metrics.last_updated = now;
        }

        // Record health history
        if let Some(metrics) = self.metrics.read().await.get(venue_id) {
            let mut history = self.health_history.write().await;
            let venue_history = history.entry(venue_id.to_string()).or_insert_with(Vec::new);
            venue_history.push(metrics.health.clone());
            if venue_history.len() > self.max_history_size {
                venue_history.remove(0);
            }
        }
    }

    /// Update market depth info.
    pub async fn update_market_info(
        &self,
        venue_id: &str,
        spread_bps: f64,
        depth_at_touch: Decimal,
        active_markets: u32,
    ) {
        if let Some(metrics) = self.metrics.write().await.get_mut(venue_id) {
            // Running average for spread
            metrics.avg_spread_bps = metrics.avg_spread_bps * 0.95 + spread_bps * 0.05;
            // Running average for depth
            let new_depth = metrics.avg_depth_at_touch * Decimal::new(95, 2) +
                depth_at_touch * Decimal::new(5, 2);
            metrics.avg_depth_at_touch = new_depth;
            metrics.active_markets = active_markets;
            metrics.last_updated = Utc::now();
        }
    }

    /// Get metrics for a venue.
    pub async fn get_venue_metrics(&self, venue_id: &str) -> Option<VenueMetrics> {
        self.metrics.read().await.get(venue_id).cloned()
    }

    /// Get all venue metrics.
    pub async fn get_all_metrics(&self) -> Vec<VenueMetrics> {
        self.metrics.read().await.values().cloned().collect()
    }

    /// Get venue health.
    pub async fn get_health(&self, venue_id: &str) -> Option<VenueHealth> {
        self.metrics.read().await.get(venue_id).map(|m| m.health.clone())
    }

    /// Get all venue health.
    pub async fn get_all_health(&self) -> Vec<VenueHealth> {
        self.metrics.read().await.values().map(|m| m.health.clone()).collect()
    }

    /// Get venue comparison.
    pub async fn get_comparison(&self) -> VenueComparison {
        let venues = self.get_all_metrics().await;

        let best_by_pnl = venues.iter()
            .max_by_key(|v| v.total_pnl)
            .map(|v| v.venue_id.clone());

        let best_by_fill_rate = venues.iter()
            .max_by(|a, b| a.fill_rate.partial_cmp(&b.fill_rate).unwrap_or(std::cmp::Ordering::Equal))
            .map(|v| v.venue_id.clone());

        let best_by_latency = venues.iter()
            .min_by(|a, b| a.avg_latency_ms.partial_cmp(&b.avg_latency_ms).unwrap_or(std::cmp::Ordering::Equal))
            .map(|v| v.venue_id.clone());

        let best_by_liquidity = venues.iter()
            .max_by_key(|v| v.avg_depth_at_touch)
            .map(|v| v.venue_id.clone());

        VenueComparison {
            timestamp: Utc::now(),
            venues,
            best_by_pnl,
            best_by_fill_rate,
            best_by_latency,
            best_by_liquidity,
        }
    }

    /// Get latency percentiles for a venue.
    pub async fn get_latency_percentiles(&self, venue_id: &str) -> LatencyPercentiles {
        let samples = self.latency_samples.read().await;

        if let Some(venue_samples) = samples.get(venue_id) {
            let mut latencies: Vec<f64> = venue_samples.iter().map(|(_, lat)| *lat).collect();
            latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

            let n = latencies.len();
            if n == 0 {
                return LatencyPercentiles::default();
            }

            let p50_idx = n / 2;
            let p90_idx = (n as f64 * 0.90) as usize;
            let p95_idx = (n as f64 * 0.95) as usize;
            let p99_idx = (n as f64 * 0.99) as usize;

            LatencyPercentiles {
                venue_id: venue_id.to_string(),
                sample_count: n,
                min_ms: *latencies.first().unwrap_or(&0.0),
                max_ms: *latencies.last().unwrap_or(&0.0),
                avg_ms: latencies.iter().sum::<f64>() / n as f64,
                p50_ms: latencies.get(p50_idx).copied().unwrap_or(0.0),
                p90_ms: latencies.get(p90_idx).copied().unwrap_or(0.0),
                p95_ms: latencies.get(p95_idx).copied().unwrap_or(0.0),
                p99_ms: latencies.get(p99_idx).copied().unwrap_or(0.0),
            }
        } else {
            LatencyPercentiles {
                venue_id: venue_id.to_string(),
                ..Default::default()
            }
        }
    }

    /// Get tradeable venues.
    pub async fn get_tradeable_venues(&self) -> Vec<String> {
        self.metrics
            .read()
            .await
            .iter()
            .filter(|(_, m)| m.health.is_tradeable())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Check if venue is tradeable.
    pub async fn is_tradeable(&self, venue_id: &str) -> bool {
        self.metrics
            .read()
            .await
            .get(venue_id)
            .map(|m| m.health.is_tradeable())
            .unwrap_or(false)
    }
}

/// Latency percentiles.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub venue_id: String,
    pub sample_count: usize,
    pub min_ms: f64,
    pub max_ms: f64,
    pub avg_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_venue_analytics_creation() {
        let analytics = VenueAnalytics::new(1000);
        analytics.register_venue("kalshi", "Kalshi Exchange").await;

        let metrics = analytics.get_venue_metrics("kalshi").await;
        assert!(metrics.is_some());
        assert_eq!(metrics.unwrap().name, "Kalshi Exchange");
    }

    #[tokio::test]
    async fn test_trade_recording() {
        let analytics = VenueAnalytics::new(1000);
        analytics.register_venue("kalshi", "Kalshi").await;

        analytics.record_trade(
            "kalshi",
            Decimal::new(1000, 0),
            Decimal::new(50, 0),
            Decimal::new(3, 0),
            2.0,
            25.0,
            true,
        ).await;

        let metrics = analytics.get_venue_metrics("kalshi").await.unwrap();
        assert_eq!(metrics.total_trades, 1);
        assert_eq!(metrics.successful_fills, 1);
        assert_eq!(metrics.total_volume, Decimal::new(1000, 0));
    }

    #[tokio::test]
    async fn test_health_update() {
        let analytics = VenueAnalytics::new(1000);
        analytics.register_venue("kalshi", "Kalshi").await;

        analytics.update_health("kalshi", VenueStatus::Healthy, Some(20.0), false, true).await;

        let health = analytics.get_health("kalshi").await.unwrap();
        assert_eq!(health.status, VenueStatus::Healthy);
        assert!(health.ws_connected);
    }
}
