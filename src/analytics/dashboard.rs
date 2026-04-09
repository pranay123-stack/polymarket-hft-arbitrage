//! Dashboard data endpoints for cross-platform arbitrage monitoring.
//!
//! Provides:
//! - Real-time dashboard data
//! - Historical charts
//! - Summary statistics
//! - Alert status

use std::sync::Arc;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc, Duration};

use super::performance_tracker::{PnLTracker, PnLSnapshot, PnLSeries, RollingStats};
use super::strategy_analytics::{StrategyAnalytics, StrategyMetrics, StrategyComparison};
use super::venue_analytics::{VenueAnalytics, VenueMetrics, VenueHealth, VenueComparison};

/// Time range for dashboard queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeRange {
    /// Last 1 hour.
    Hour1,
    /// Last 6 hours.
    Hour6,
    /// Last 24 hours.
    Hour24,
    /// Last 7 days.
    Day7,
    /// Last 30 days.
    Day30,
    /// All time.
    AllTime,
}

impl TimeRange {
    pub fn to_duration(&self) -> Duration {
        match self {
            TimeRange::Hour1 => Duration::hours(1),
            TimeRange::Hour6 => Duration::hours(6),
            TimeRange::Hour24 => Duration::hours(24),
            TimeRange::Day7 => Duration::days(7),
            TimeRange::Day30 => Duration::days(30),
            TimeRange::AllTime => Duration::days(365 * 10), // Effectively all time
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            TimeRange::Hour1 => "1h",
            TimeRange::Hour6 => "6h",
            TimeRange::Hour24 => "24h",
            TimeRange::Day7 => "7d",
            TimeRange::Day30 => "30d",
            TimeRange::AllTime => "all",
        }
    }
}

/// Complete dashboard data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardData {
    /// Timestamp.
    pub timestamp: DateTime<Utc>,
    /// Summary cards.
    pub summary: SummaryData,
    /// Current P&L snapshot.
    pub pnl: PnLSnapshot,
    /// P&L chart data.
    pub pnl_chart: PnLChartData,
    /// Strategy summary.
    pub strategies: StrategySummary,
    /// Venue summary.
    pub venues: VenueSummary,
    /// Active alerts.
    pub alerts: Vec<AlertSummary>,
    /// Recent trades.
    pub recent_trades: Vec<TradeSummary>,
    /// System status.
    pub system_status: SystemStatus,
}

/// Summary cards for dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryData {
    /// Total net P&L.
    pub total_pnl: Decimal,
    /// P&L change (vs previous period).
    pub pnl_change: Decimal,
    /// P&L change percentage.
    pub pnl_change_pct: f64,
    /// Current equity.
    pub equity: Decimal,
    /// Current drawdown.
    pub drawdown: Decimal,
    /// Drawdown percentage.
    pub drawdown_pct: f64,
    /// Total trades today.
    pub trades_today: u64,
    /// Win rate today.
    pub win_rate_today: f64,
    /// Active positions.
    pub active_positions: u32,
    /// Total exposure.
    pub total_exposure: Decimal,
    /// Sharpe ratio (rolling).
    pub sharpe_ratio: f64,
    /// Opportunities detected (last hour).
    pub opportunities_1h: u64,
}

/// P&L chart data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnLChartData {
    /// Equity curve.
    pub equity_curve: Vec<ChartPoint>,
    /// Realized P&L curve.
    pub realized_pnl: Vec<ChartPoint>,
    /// Drawdown curve.
    pub drawdown: Vec<ChartPoint>,
    /// Daily P&L bars.
    pub daily_pnl: Vec<(String, Decimal)>,
}

/// A point on a chart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChartPoint {
    pub timestamp: DateTime<Utc>,
    pub value: f64,
}

/// Strategy summary for dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategySummary {
    /// Number of active strategies.
    pub active_count: usize,
    /// Best performing strategy.
    pub best_strategy: Option<String>,
    /// Per-strategy P&L.
    pub by_strategy: Vec<StrategyPnL>,
    /// Strategy comparison.
    pub comparison: StrategyComparison,
}

/// Strategy P&L summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyPnL {
    pub strategy_id: String,
    pub name: String,
    pub pnl: Decimal,
    pub trades: u64,
    pub win_rate: f64,
}

/// Venue summary for dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueSummary {
    /// Number of connected venues.
    pub connected_count: usize,
    /// Venue health status.
    pub health: Vec<VenueHealth>,
    /// Per-venue P&L.
    pub by_venue: Vec<VenuePnL>,
    /// Venue comparison.
    pub comparison: VenueComparison,
}

/// Venue P&L summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenuePnL {
    pub venue_id: String,
    pub name: String,
    pub pnl: Decimal,
    pub volume: Decimal,
    pub fill_rate: f64,
    pub avg_latency_ms: f64,
}

/// Alert summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertSummary {
    pub id: String,
    pub severity: String,
    pub message: String,
    pub timestamp: DateTime<Utc>,
    pub acknowledged: bool,
}

/// Trade summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeSummary {
    pub trade_id: String,
    pub timestamp: DateTime<Utc>,
    pub strategy: String,
    pub venue_a: String,
    pub venue_b: String,
    pub pnl: Decimal,
    pub success: bool,
}

/// System status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatus {
    /// Overall status.
    pub status: String,
    /// Trading enabled.
    pub trading_enabled: bool,
    /// Connected venues.
    pub connected_venues: u32,
    /// Active strategies.
    pub active_strategies: u32,
    /// Uptime seconds.
    pub uptime_seconds: u64,
    /// Memory usage (MB).
    pub memory_mb: f64,
    /// CPU usage percentage.
    pub cpu_pct: f64,
    /// Last health check.
    pub last_health_check: DateTime<Utc>,
}

/// Dashboard endpoint service.
pub struct DashboardEndpoint {
    /// P&L tracker.
    pnl_tracker: Arc<PnLTracker>,
    /// Strategy analytics.
    strategy_analytics: Arc<StrategyAnalytics>,
    /// Venue analytics.
    venue_analytics: Arc<VenueAnalytics>,
    /// Start time (for uptime).
    start_time: DateTime<Utc>,
    /// Trading enabled flag.
    trading_enabled: Arc<tokio::sync::RwLock<bool>>,
}

impl DashboardEndpoint {
    /// Create a new dashboard endpoint.
    pub fn new(
        pnl_tracker: Arc<PnLTracker>,
        strategy_analytics: Arc<StrategyAnalytics>,
        venue_analytics: Arc<VenueAnalytics>,
    ) -> Self {
        Self {
            pnl_tracker,
            strategy_analytics,
            venue_analytics,
            start_time: Utc::now(),
            trading_enabled: Arc::new(tokio::sync::RwLock::new(true)),
        }
    }

    /// Get complete dashboard data.
    pub async fn get_dashboard(&self, time_range: TimeRange) -> DashboardData {
        let duration = time_range.to_duration();
        let pnl_snapshot = self.pnl_tracker.get_snapshot().await;
        let strategy_comparison = self.strategy_analytics.get_comparison(time_range.label()).await;
        let venue_comparison = self.venue_analytics.get_comparison().await;

        // Build summary
        let rolling_stats = self.pnl_tracker.get_rolling_stats(duration).await;
        let summary = SummaryData {
            total_pnl: pnl_snapshot.net_pnl,
            pnl_change: rolling_stats.pnl,
            pnl_change_pct: if pnl_snapshot.equity > Decimal::ZERO {
                rolling_stats.pnl.to_f64().unwrap_or(0.0) /
                    pnl_snapshot.equity.to_f64().unwrap_or(1.0) * 100.0
            } else {
                0.0
            },
            equity: pnl_snapshot.equity,
            drawdown: pnl_snapshot.current_drawdown,
            drawdown_pct: pnl_snapshot.current_drawdown_pct,
            trades_today: rolling_stats.trades,
            win_rate_today: rolling_stats.win_rate,
            active_positions: 0, // Would need position tracker
            total_exposure: Decimal::ZERO,
            sharpe_ratio: rolling_stats.sharpe_ratio,
            opportunities_1h: strategy_comparison.aggregate.opportunities_detected,
        };

        // Build P&L chart
        let equity_series = self.pnl_tracker.get_series("equity", duration).await;
        let realized_series = self.pnl_tracker.get_series("realized_pnl", duration).await;
        let drawdown_series = self.pnl_tracker.get_series("drawdown", duration).await;
        let daily_pnl = self.pnl_tracker.get_daily_pnl(30).await;

        let pnl_chart = PnLChartData {
            equity_curve: equity_series.data.iter()
                .map(|(ts, v)| ChartPoint { timestamp: *ts, value: v.to_f64().unwrap_or(0.0) })
                .collect(),
            realized_pnl: realized_series.data.iter()
                .map(|(ts, v)| ChartPoint { timestamp: *ts, value: v.to_f64().unwrap_or(0.0) })
                .collect(),
            drawdown: drawdown_series.data.iter()
                .map(|(ts, v)| ChartPoint { timestamp: *ts, value: v.to_f64().unwrap_or(0.0) })
                .collect(),
            daily_pnl,
        };

        // Build strategy summary
        let strategy_metrics = self.strategy_analytics.get_all_metrics().await;
        let strategies = StrategySummary {
            active_count: strategy_metrics.len(),
            best_strategy: strategy_comparison.best_by_pnl.clone(),
            by_strategy: strategy_metrics.iter().map(|m| StrategyPnL {
                strategy_id: m.strategy_id.clone(),
                name: m.name.clone(),
                pnl: m.net_pnl,
                trades: m.total_trades,
                win_rate: m.win_rate,
            }).collect(),
            comparison: strategy_comparison,
        };

        // Build venue summary
        let venue_metrics = self.venue_analytics.get_all_metrics().await;
        let venue_health = self.venue_analytics.get_all_health().await;
        let connected = venue_health.iter().filter(|h| h.is_tradeable()).count();

        let venues = VenueSummary {
            connected_count: connected,
            health: venue_health,
            by_venue: venue_metrics.iter().map(|m| VenuePnL {
                venue_id: m.venue_id.clone(),
                name: m.name.clone(),
                pnl: m.total_pnl,
                volume: m.total_volume,
                fill_rate: m.fill_rate,
                avg_latency_ms: m.avg_latency_ms,
            }).collect(),
            comparison: venue_comparison,
        };

        // System status
        let uptime = (Utc::now() - self.start_time).num_seconds() as u64;
        let system_status = SystemStatus {
            status: "healthy".to_string(),
            trading_enabled: *self.trading_enabled.read().await,
            connected_venues: connected as u32,
            active_strategies: strategy_metrics.len() as u32,
            uptime_seconds: uptime,
            memory_mb: 0.0, // Would need system metrics
            cpu_pct: 0.0,
            last_health_check: Utc::now(),
        };

        DashboardData {
            timestamp: Utc::now(),
            summary,
            pnl: pnl_snapshot,
            pnl_chart,
            strategies,
            venues,
            alerts: Vec::new(), // Would integrate with alerting system
            recent_trades: Vec::new(), // Would need trade history
            system_status,
        }
    }

    /// Get P&L data only.
    pub async fn get_pnl(&self, time_range: TimeRange) -> PnLSnapshot {
        self.pnl_tracker.get_snapshot().await
    }

    /// Get rolling stats.
    pub async fn get_rolling_stats(&self, time_range: TimeRange) -> RollingStats {
        self.pnl_tracker.get_rolling_stats(time_range.to_duration()).await
    }

    /// Get strategy data only.
    pub async fn get_strategies(&self) -> StrategySummary {
        let metrics = self.strategy_analytics.get_all_metrics().await;
        let comparison = self.strategy_analytics.get_comparison("24h").await;

        StrategySummary {
            active_count: metrics.len(),
            best_strategy: comparison.best_by_pnl.clone(),
            by_strategy: metrics.iter().map(|m| StrategyPnL {
                strategy_id: m.strategy_id.clone(),
                name: m.name.clone(),
                pnl: m.net_pnl,
                trades: m.total_trades,
                win_rate: m.win_rate,
            }).collect(),
            comparison,
        }
    }

    /// Get venue data only.
    pub async fn get_venues(&self) -> VenueSummary {
        let metrics = self.venue_analytics.get_all_metrics().await;
        let health = self.venue_analytics.get_all_health().await;
        let comparison = self.venue_analytics.get_comparison().await;
        let connected = health.iter().filter(|h| h.is_tradeable()).count();

        VenueSummary {
            connected_count: connected,
            health,
            by_venue: metrics.iter().map(|m| VenuePnL {
                venue_id: m.venue_id.clone(),
                name: m.name.clone(),
                pnl: m.total_pnl,
                volume: m.total_volume,
                fill_rate: m.fill_rate,
                avg_latency_ms: m.avg_latency_ms,
            }).collect(),
            comparison,
        }
    }

    /// Enable/disable trading.
    pub async fn set_trading_enabled(&self, enabled: bool) {
        *self.trading_enabled.write().await = enabled;
    }

    /// Check if trading is enabled.
    pub async fn is_trading_enabled(&self) -> bool {
        *self.trading_enabled.read().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_range() {
        assert_eq!(TimeRange::Hour1.to_duration(), Duration::hours(1));
        assert_eq!(TimeRange::Day7.to_duration(), Duration::days(7));
        assert_eq!(TimeRange::Hour24.label(), "24h");
    }

    #[tokio::test]
    async fn test_dashboard_endpoint() {
        let pnl = Arc::new(PnLTracker::new(Decimal::new(10000, 0)));
        let strategies = Arc::new(StrategyAnalytics::new(1000));
        let venues = Arc::new(VenueAnalytics::new(1000));

        let endpoint = DashboardEndpoint::new(pnl, strategies, venues);
        let data = endpoint.get_dashboard(TimeRange::Hour24).await;

        assert!(data.pnl.equity >= Decimal::ZERO);
    }
}
