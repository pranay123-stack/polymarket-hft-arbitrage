//! Real-time P&L tracking and analytics.
//!
//! Tracks:
//! - Realized and unrealized P&L
//! - Rolling window statistics
//! - Drawdown metrics
//! - Attribution analysis

use std::collections::{HashMap, VecDeque};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use std::sync::Arc;
use chrono::{DateTime, Utc, Duration};

/// A P&L snapshot at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnLSnapshot {
    /// Timestamp.
    pub timestamp: DateTime<Utc>,
    /// Realized P&L.
    pub realized_pnl: Decimal,
    /// Unrealized P&L.
    pub unrealized_pnl: Decimal,
    /// Total P&L (realized + unrealized).
    pub total_pnl: Decimal,
    /// Total fees paid.
    pub fees_paid: Decimal,
    /// Net P&L (total - fees).
    pub net_pnl: Decimal,
    /// Number of trades.
    pub trade_count: u64,
    /// Number of winning trades.
    pub winning_trades: u64,
    /// Number of losing trades.
    pub losing_trades: u64,
    /// Current equity.
    pub equity: Decimal,
    /// Peak equity.
    pub peak_equity: Decimal,
    /// Current drawdown.
    pub current_drawdown: Decimal,
    /// Current drawdown percentage.
    pub current_drawdown_pct: f64,
}

/// Time series of P&L data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnLSeries {
    /// Series name (e.g., "total_pnl", "realized_pnl").
    pub name: String,
    /// Data points: (timestamp, value).
    pub data: Vec<(DateTime<Utc>, Decimal)>,
    /// Time range.
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
}

impl PnLSeries {
    /// Calculate returns.
    pub fn returns(&self) -> Vec<(DateTime<Utc>, f64)> {
        if self.data.len() < 2 {
            return Vec::new();
        }

        self.data.windows(2)
            .map(|w| {
                let prev = w[0].1.to_f64().unwrap_or(0.0);
                let curr = w[1].1.to_f64().unwrap_or(0.0);
                let ret = if prev != 0.0 { (curr - prev) / prev } else { 0.0 };
                (w[1].0, ret)
            })
            .collect()
    }

    /// Calculate cumulative returns.
    pub fn cumulative_returns(&self) -> Vec<(DateTime<Utc>, f64)> {
        if self.data.is_empty() {
            return Vec::new();
        }

        let initial = self.data[0].1.to_f64().unwrap_or(1.0);
        self.data.iter()
            .map(|(ts, val)| {
                let curr = val.to_f64().unwrap_or(1.0);
                let cum_ret = if initial != 0.0 { (curr - initial) / initial } else { 0.0 };
                (*ts, cum_ret)
            })
            .collect()
    }
}

/// P&L attribution by source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnLAttribution {
    pub timestamp: DateTime<Utc>,
    pub by_strategy: HashMap<String, Decimal>,
    pub by_venue: HashMap<String, Decimal>,
    pub by_market: HashMap<String, Decimal>,
    pub total: Decimal,
}

/// Rolling statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RollingStats {
    pub period: String,
    pub pnl: Decimal,
    pub trades: u64,
    pub win_rate: f64,
    pub avg_win: Decimal,
    pub avg_loss: Decimal,
    pub profit_factor: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: Decimal,
    pub max_drawdown_pct: f64,
    pub volatility: f64,
}

/// Real-time P&L tracker.
pub struct PnLTracker {
    /// Initial capital.
    initial_capital: Decimal,
    /// Current equity.
    current_equity: Arc<RwLock<Decimal>>,
    /// Peak equity.
    peak_equity: Arc<RwLock<Decimal>>,
    /// Realized P&L.
    realized_pnl: Arc<RwLock<Decimal>>,
    /// Unrealized P&L.
    unrealized_pnl: Arc<RwLock<Decimal>>,
    /// Total fees paid.
    fees_paid: Arc<RwLock<Decimal>>,
    /// Trade count.
    trade_count: Arc<RwLock<u64>>,
    /// Winning trades.
    winning_trades: Arc<RwLock<u64>>,
    /// Losing trades.
    losing_trades: Arc<RwLock<u64>>,
    /// P&L history (for time series).
    pnl_history: Arc<RwLock<VecDeque<PnLSnapshot>>>,
    /// Max history size.
    max_history_size: usize,
    /// P&L by strategy.
    by_strategy: Arc<RwLock<HashMap<String, Decimal>>>,
    /// P&L by venue.
    by_venue: Arc<RwLock<HashMap<String, Decimal>>>,
    /// P&L by market.
    by_market: Arc<RwLock<HashMap<String, Decimal>>>,
    /// Daily P&L snapshots.
    daily_snapshots: Arc<RwLock<HashMap<String, PnLSnapshot>>>,
}

impl PnLTracker {
    /// Create a new P&L tracker.
    pub fn new(initial_capital: Decimal) -> Self {
        Self {
            initial_capital,
            current_equity: Arc::new(RwLock::new(initial_capital)),
            peak_equity: Arc::new(RwLock::new(initial_capital)),
            realized_pnl: Arc::new(RwLock::new(Decimal::ZERO)),
            unrealized_pnl: Arc::new(RwLock::new(Decimal::ZERO)),
            fees_paid: Arc::new(RwLock::new(Decimal::ZERO)),
            trade_count: Arc::new(RwLock::new(0)),
            winning_trades: Arc::new(RwLock::new(0)),
            losing_trades: Arc::new(RwLock::new(0)),
            pnl_history: Arc::new(RwLock::new(VecDeque::new())),
            max_history_size: 86400, // 24 hours at 1 second intervals
            by_strategy: Arc::new(RwLock::new(HashMap::new())),
            by_venue: Arc::new(RwLock::new(HashMap::new())),
            by_market: Arc::new(RwLock::new(HashMap::new())),
            daily_snapshots: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record a trade.
    pub async fn record_trade(
        &self,
        pnl: Decimal,
        fees: Decimal,
        strategy: &str,
        venue: &str,
        market: &str,
    ) {
        // Update totals
        *self.realized_pnl.write().await += pnl;
        *self.fees_paid.write().await += fees;
        *self.trade_count.write().await += 1;

        let net_pnl = pnl - fees;
        *self.current_equity.write().await += net_pnl;

        // Update win/loss counts
        if net_pnl > Decimal::ZERO {
            *self.winning_trades.write().await += 1;
        } else if net_pnl < Decimal::ZERO {
            *self.losing_trades.write().await += 1;
        }

        // Update peak
        let equity = *self.current_equity.read().await;
        let mut peak = self.peak_equity.write().await;
        if equity > *peak {
            *peak = equity;
        }

        // Update attribution
        *self.by_strategy.write().await.entry(strategy.to_string()).or_insert(Decimal::ZERO) += net_pnl;
        *self.by_venue.write().await.entry(venue.to_string()).or_insert(Decimal::ZERO) += net_pnl;
        *self.by_market.write().await.entry(market.to_string()).or_insert(Decimal::ZERO) += net_pnl;

        // Record snapshot
        self.record_snapshot().await;
    }

    /// Update unrealized P&L.
    pub async fn update_unrealized(&self, unrealized: Decimal) {
        *self.unrealized_pnl.write().await = unrealized;

        // Update equity (initial + realized + unrealized - fees)
        let realized = *self.realized_pnl.read().await;
        let fees = *self.fees_paid.read().await;
        *self.current_equity.write().await = self.initial_capital + realized + unrealized - fees;

        // Update peak
        let equity = *self.current_equity.read().await;
        let mut peak = self.peak_equity.write().await;
        if equity > *peak {
            *peak = equity;
        }
    }

    /// Record a P&L snapshot.
    async fn record_snapshot(&self) {
        let snapshot = self.get_snapshot().await;

        let mut history = self.pnl_history.write().await;
        history.push_back(snapshot.clone());

        if history.len() > self.max_history_size {
            history.pop_front();
        }

        // Update daily snapshot
        let day_key = snapshot.timestamp.format("%Y-%m-%d").to_string();
        self.daily_snapshots.write().await.insert(day_key, snapshot);
    }

    /// Get current snapshot.
    pub async fn get_snapshot(&self) -> PnLSnapshot {
        let realized = *self.realized_pnl.read().await;
        let unrealized = *self.unrealized_pnl.read().await;
        let fees = *self.fees_paid.read().await;
        let equity = *self.current_equity.read().await;
        let peak = *self.peak_equity.read().await;
        let trade_count = *self.trade_count.read().await;
        let winning = *self.winning_trades.read().await;
        let losing = *self.losing_trades.read().await;

        let drawdown = peak - equity;
        let drawdown_pct = if peak > Decimal::ZERO {
            drawdown.to_f64().unwrap_or(0.0) / peak.to_f64().unwrap_or(1.0) * 100.0
        } else {
            0.0
        };

        PnLSnapshot {
            timestamp: Utc::now(),
            realized_pnl: realized,
            unrealized_pnl: unrealized,
            total_pnl: realized + unrealized,
            fees_paid: fees,
            net_pnl: realized + unrealized - fees,
            trade_count,
            winning_trades: winning,
            losing_trades: losing,
            equity,
            peak_equity: peak,
            current_drawdown: drawdown,
            current_drawdown_pct: drawdown_pct,
        }
    }

    /// Get P&L time series.
    pub async fn get_series(&self, name: &str, duration: Duration) -> PnLSeries {
        let history = self.pnl_history.read().await;
        let cutoff = Utc::now() - duration;

        let data: Vec<(DateTime<Utc>, Decimal)> = history
            .iter()
            .filter(|s| s.timestamp >= cutoff)
            .map(|s| {
                let value = match name {
                    "realized_pnl" => s.realized_pnl,
                    "unrealized_pnl" => s.unrealized_pnl,
                    "total_pnl" => s.total_pnl,
                    "net_pnl" => s.net_pnl,
                    "equity" => s.equity,
                    "drawdown" => s.current_drawdown,
                    _ => s.total_pnl,
                };
                (s.timestamp, value)
            })
            .collect();

        let (start, end) = if data.is_empty() {
            (Utc::now(), Utc::now())
        } else {
            (data[0].0, data.last().unwrap().0)
        };

        PnLSeries {
            name: name.to_string(),
            data,
            start_time: start,
            end_time: end,
        }
    }

    /// Get P&L attribution.
    pub async fn get_attribution(&self) -> PnLAttribution {
        PnLAttribution {
            timestamp: Utc::now(),
            by_strategy: self.by_strategy.read().await.clone(),
            by_venue: self.by_venue.read().await.clone(),
            by_market: self.by_market.read().await.clone(),
            total: *self.realized_pnl.read().await,
        }
    }

    /// Calculate rolling statistics.
    pub async fn get_rolling_stats(&self, period: Duration) -> RollingStats {
        let history = self.pnl_history.read().await;
        let cutoff = Utc::now() - period;

        let snapshots: Vec<_> = history
            .iter()
            .filter(|s| s.timestamp >= cutoff)
            .collect();

        if snapshots.is_empty() {
            return RollingStats {
                period: format!("{}h", period.num_hours()),
                ..Default::default()
            };
        }

        let first = snapshots.first().unwrap();
        let last = snapshots.last().unwrap();

        let pnl = last.realized_pnl - first.realized_pnl;
        let trades = last.trade_count - first.trade_count;
        let wins = last.winning_trades - first.winning_trades;
        let losses = last.losing_trades - first.losing_trades;

        let win_rate = if trades > 0 {
            wins as f64 / trades as f64
        } else {
            0.0
        };

        // Calculate returns for Sharpe
        let returns: Vec<f64> = snapshots.windows(2)
            .map(|w| {
                let prev = w[0].equity.to_f64().unwrap_or(1.0);
                let curr = w[1].equity.to_f64().unwrap_or(1.0);
                if prev != 0.0 { (curr - prev) / prev } else { 0.0 }
            })
            .collect();

        let mean_return = if !returns.is_empty() {
            returns.iter().sum::<f64>() / returns.len() as f64
        } else {
            0.0
        };

        let volatility = if returns.len() > 1 {
            let variance = returns.iter()
                .map(|r| (r - mean_return).powi(2))
                .sum::<f64>() / (returns.len() - 1) as f64;
            variance.sqrt()
        } else {
            0.0
        };

        let sharpe = if volatility > 0.0 {
            mean_return / volatility * (252.0_f64).sqrt() // Annualized
        } else {
            0.0
        };

        // Max drawdown in period
        let mut max_dd = Decimal::ZERO;
        let mut max_dd_pct = 0.0;
        let mut peak = first.equity;

        for snapshot in &snapshots {
            if snapshot.equity > peak {
                peak = snapshot.equity;
            }
            let dd = peak - snapshot.equity;
            if dd > max_dd {
                max_dd = dd;
                if peak > Decimal::ZERO {
                    max_dd_pct = dd.to_f64().unwrap_or(0.0) / peak.to_f64().unwrap_or(1.0) * 100.0;
                }
            }
        }

        RollingStats {
            period: format!("{}h", period.num_hours()),
            pnl,
            trades,
            win_rate,
            avg_win: Decimal::ZERO, // Would need more detailed tracking
            avg_loss: Decimal::ZERO,
            profit_factor: 0.0,
            sharpe_ratio: sharpe,
            max_drawdown: max_dd,
            max_drawdown_pct: max_dd_pct,
            volatility,
        }
    }

    /// Get daily P&L.
    pub async fn get_daily_pnl(&self, days: u32) -> Vec<(String, Decimal)> {
        let snapshots = self.daily_snapshots.read().await;
        let mut result: Vec<_> = snapshots
            .iter()
            .map(|(day, snapshot)| (day.clone(), snapshot.net_pnl))
            .collect();

        result.sort_by(|a, b| a.0.cmp(&b.0));
        result.into_iter().rev().take(days as usize).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pnl_tracker_creation() {
        let tracker = PnLTracker::new(Decimal::new(10000, 0));
        let snapshot = tracker.get_snapshot().await;

        assert_eq!(snapshot.equity, Decimal::new(10000, 0));
        assert_eq!(snapshot.realized_pnl, Decimal::ZERO);
    }

    #[tokio::test]
    async fn test_trade_recording() {
        let tracker = PnLTracker::new(Decimal::new(10000, 0));

        tracker.record_trade(
            Decimal::new(100, 0),
            Decimal::new(2, 0),
            "cross_platform",
            "kalshi",
            "MKT1",
        ).await;

        let snapshot = tracker.get_snapshot().await;
        assert_eq!(snapshot.realized_pnl, Decimal::new(100, 0));
        assert_eq!(snapshot.fees_paid, Decimal::new(2, 0));
        assert_eq!(snapshot.equity, Decimal::new(10098, 0)); // 10000 + 100 - 2
        assert_eq!(snapshot.trade_count, 1);
        assert_eq!(snapshot.winning_trades, 1);
    }
}
