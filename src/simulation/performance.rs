//! Performance metrics and trade recording for backtesting.
//!
//! Provides:
//! - Trade-by-trade recording
//! - P&L calculation (realized/unrealized)
//! - Risk metrics (Sharpe, Sortino, max drawdown)
//! - Execution quality analysis

use std::collections::{HashMap, VecDeque};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};

use super::latency_model::Venue;
use super::fill_simulator::OrderSide;

/// A recorded trade for performance analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    /// Unique trade ID.
    pub trade_id: String,
    /// Strategy that generated the trade.
    pub strategy_id: String,
    /// Timestamp when trade was initiated.
    pub entry_time_ms: u64,
    /// Timestamp when trade was completed (both legs).
    pub exit_time_ms: u64,
    /// First leg details.
    pub leg_a: LegRecord,
    /// Second leg details (hedge).
    pub leg_b: LegRecord,
    /// Gross profit/loss.
    pub gross_pnl: Decimal,
    /// Fees paid.
    pub total_fees: Decimal,
    /// Net profit/loss after fees.
    pub net_pnl: Decimal,
    /// Slippage cost.
    pub slippage_cost: Decimal,
    /// Whether trade was successful (both legs filled).
    pub success: bool,
    /// Reason for failure if any.
    pub failure_reason: Option<String>,
    /// Total execution latency.
    pub total_latency_ms: f64,
    /// Opportunity spread at entry (in bps).
    pub entry_spread_bps: Decimal,
    /// Realized spread after execution (in bps).
    pub realized_spread_bps: Decimal,
}

/// Record of a single leg execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegRecord {
    pub venue: Venue,
    pub market_id: String,
    pub side: OrderSide,
    pub intended_price: Decimal,
    pub fill_price: Decimal,
    pub intended_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub fill_ratio: Decimal,
    pub fees: Decimal,
    pub latency_ms: f64,
}

/// Performance metrics for a backtest.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    // Trade counts
    pub total_trades: u64,
    pub winning_trades: u64,
    pub losing_trades: u64,
    pub breakeven_trades: u64,
    pub failed_trades: u64,

    // P&L metrics
    pub gross_pnl: Decimal,
    pub net_pnl: Decimal,
    pub total_fees: Decimal,
    pub total_slippage: Decimal,

    // Win/loss analysis
    pub win_rate: f64,
    pub avg_win: Decimal,
    pub avg_loss: Decimal,
    pub profit_factor: f64,
    pub expectancy: Decimal,

    // Risk metrics
    pub max_drawdown: Decimal,
    pub max_drawdown_pct: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,

    // Execution quality
    pub avg_latency_ms: f64,
    pub avg_slippage_bps: f64,
    pub fill_rate: f64,
    pub avg_spread_capture: f64,

    // Time analysis
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    pub trading_days: u32,
    pub trades_per_day: f64,

    // Capital metrics
    pub initial_capital: Decimal,
    pub final_capital: Decimal,
    pub peak_capital: Decimal,
    pub return_pct: f64,
    pub annualized_return_pct: f64,
}

/// Result of a complete backtest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResult {
    /// Overall performance metrics.
    pub metrics: PerformanceMetrics,
    /// Per-strategy metrics.
    pub strategy_metrics: HashMap<String, PerformanceMetrics>,
    /// Per-venue metrics.
    pub venue_metrics: HashMap<Venue, VenueMetrics>,
    /// Equity curve (timestamp, equity value).
    pub equity_curve: Vec<(u64, Decimal)>,
    /// Drawdown curve (timestamp, drawdown amount).
    pub drawdown_curve: Vec<(u64, Decimal)>,
    /// Daily P&L.
    pub daily_pnl: Vec<(u64, Decimal)>,
    /// All trade records.
    pub trades: Vec<TradeRecord>,
    /// Configuration used.
    pub config_summary: String,
}

/// Per-venue performance metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VenueMetrics {
    pub venue: Venue,
    pub total_trades: u64,
    pub total_volume: Decimal,
    pub total_fees: Decimal,
    pub avg_latency_ms: f64,
    pub avg_fill_rate: f64,
    pub avg_slippage_bps: f64,
}

impl Default for Venue {
    fn default() -> Self {
        Venue::Polymarket
    }
}

/// Performance tracker for accumulating trade data.
pub struct PerformanceTracker {
    /// All recorded trades.
    trades: Vec<TradeRecord>,
    /// Equity curve points.
    equity_curve: Vec<(u64, Decimal)>,
    /// Current equity.
    current_equity: Decimal,
    /// Peak equity (for drawdown).
    peak_equity: Decimal,
    /// Initial capital.
    initial_capital: Decimal,
    /// Daily P&L accumulator.
    daily_pnl: HashMap<u64, Decimal>,
    /// Per-strategy trade counts.
    strategy_trades: HashMap<String, Vec<TradeRecord>>,
    /// Per-venue statistics.
    venue_stats: HashMap<Venue, VenueAccumulator>,
}

/// Accumulator for venue statistics.
#[derive(Debug, Default)]
struct VenueAccumulator {
    trade_count: u64,
    total_volume: Decimal,
    total_fees: Decimal,
    latency_sum: f64,
    fill_ratio_sum: f64,
    slippage_sum: f64,
}

impl PerformanceTracker {
    /// Create a new performance tracker.
    pub fn new(initial_capital: Decimal) -> Self {
        Self {
            trades: Vec::new(),
            equity_curve: vec![(0, initial_capital)],
            current_equity: initial_capital,
            peak_equity: initial_capital,
            initial_capital,
            daily_pnl: HashMap::new(),
            strategy_trades: HashMap::new(),
            venue_stats: HashMap::new(),
        }
    }

    /// Record a completed trade.
    pub fn record_trade(&mut self, trade: TradeRecord) {
        // Update equity
        self.current_equity = self.current_equity + trade.net_pnl;
        if self.current_equity > self.peak_equity {
            self.peak_equity = self.current_equity;
        }

        // Record equity point
        self.equity_curve.push((trade.exit_time_ms, self.current_equity));

        // Accumulate daily P&L
        let day = trade.exit_time_ms / (24 * 60 * 60 * 1000);
        *self.daily_pnl.entry(day).or_insert(Decimal::ZERO) += trade.net_pnl;

        // Record by strategy
        self.strategy_trades
            .entry(trade.strategy_id.clone())
            .or_insert_with(Vec::new)
            .push(trade.clone());

        // Accumulate venue stats for leg A
        let venue_a = self.venue_stats.entry(trade.leg_a.venue).or_default();
        venue_a.trade_count += 1;
        venue_a.total_volume += trade.leg_a.filled_quantity * trade.leg_a.fill_price;
        venue_a.total_fees += trade.leg_a.fees;
        venue_a.latency_sum += trade.leg_a.latency_ms;
        venue_a.fill_ratio_sum += trade.leg_a.fill_ratio.to_f64().unwrap_or(0.0);

        // Accumulate venue stats for leg B
        let venue_b = self.venue_stats.entry(trade.leg_b.venue).or_default();
        venue_b.trade_count += 1;
        venue_b.total_volume += trade.leg_b.filled_quantity * trade.leg_b.fill_price;
        venue_b.total_fees += trade.leg_b.fees;
        venue_b.latency_sum += trade.leg_b.latency_ms;
        venue_b.fill_ratio_sum += trade.leg_b.fill_ratio.to_f64().unwrap_or(0.0);

        self.trades.push(trade);
    }

    /// Calculate final performance metrics.
    pub fn calculate_metrics(&self) -> PerformanceMetrics {
        if self.trades.is_empty() {
            return PerformanceMetrics {
                initial_capital: self.initial_capital,
                final_capital: self.current_equity,
                ..Default::default()
            };
        }

        let mut metrics = PerformanceMetrics::default();

        // Basic counts
        metrics.total_trades = self.trades.len() as u64;
        metrics.winning_trades = self.trades.iter().filter(|t| t.net_pnl > Decimal::ZERO).count() as u64;
        metrics.losing_trades = self.trades.iter().filter(|t| t.net_pnl < Decimal::ZERO).count() as u64;
        metrics.breakeven_trades = self.trades.iter().filter(|t| t.net_pnl == Decimal::ZERO).count() as u64;
        metrics.failed_trades = self.trades.iter().filter(|t| !t.success).count() as u64;

        // P&L metrics
        metrics.gross_pnl = self.trades.iter().map(|t| t.gross_pnl).sum();
        metrics.net_pnl = self.trades.iter().map(|t| t.net_pnl).sum();
        metrics.total_fees = self.trades.iter().map(|t| t.total_fees).sum();
        metrics.total_slippage = self.trades.iter().map(|t| t.slippage_cost).sum();

        // Win/loss analysis
        metrics.win_rate = if metrics.total_trades > 0 {
            metrics.winning_trades as f64 / metrics.total_trades as f64
        } else {
            0.0
        };

        let wins: Vec<Decimal> = self.trades.iter()
            .filter(|t| t.net_pnl > Decimal::ZERO)
            .map(|t| t.net_pnl)
            .collect();
        let losses: Vec<Decimal> = self.trades.iter()
            .filter(|t| t.net_pnl < Decimal::ZERO)
            .map(|t| t.net_pnl.abs())
            .collect();

        metrics.avg_win = if !wins.is_empty() {
            wins.iter().sum::<Decimal>() / Decimal::from(wins.len())
        } else {
            Decimal::ZERO
        };

        metrics.avg_loss = if !losses.is_empty() {
            losses.iter().sum::<Decimal>() / Decimal::from(losses.len())
        } else {
            Decimal::ZERO
        };

        let total_wins: Decimal = wins.iter().sum();
        let total_losses: Decimal = losses.iter().sum();
        metrics.profit_factor = if total_losses > Decimal::ZERO {
            total_wins.to_f64().unwrap_or(0.0) / total_losses.to_f64().unwrap_or(1.0)
        } else if total_wins > Decimal::ZERO {
            f64::INFINITY
        } else {
            0.0
        };

        metrics.expectancy = if metrics.total_trades > 0 {
            metrics.net_pnl / Decimal::from(metrics.total_trades)
        } else {
            Decimal::ZERO
        };

        // Calculate drawdown
        let (max_dd, max_dd_pct) = self.calculate_max_drawdown();
        metrics.max_drawdown = max_dd;
        metrics.max_drawdown_pct = max_dd_pct;

        // Risk-adjusted returns
        let daily_returns = self.calculate_daily_returns();
        metrics.sharpe_ratio = self.calculate_sharpe(&daily_returns);
        metrics.sortino_ratio = self.calculate_sortino(&daily_returns);
        metrics.calmar_ratio = if max_dd_pct > 0.0 {
            metrics.return_pct / max_dd_pct
        } else {
            0.0
        };

        // Execution quality
        let successful_trades: Vec<_> = self.trades.iter().filter(|t| t.success).collect();
        if !successful_trades.is_empty() {
            let n = successful_trades.len() as f64;
            metrics.avg_latency_ms = successful_trades.iter().map(|t| t.total_latency_ms).sum::<f64>() / n;
            metrics.avg_slippage_bps = successful_trades.iter()
                .map(|t| t.slippage_cost.to_f64().unwrap_or(0.0))
                .sum::<f64>() / n;

            // Spread capture: realized_spread / entry_spread
            let spread_captures: Vec<f64> = successful_trades.iter()
                .filter(|t| t.entry_spread_bps > Decimal::ZERO)
                .map(|t| {
                    t.realized_spread_bps.to_f64().unwrap_or(0.0) /
                    t.entry_spread_bps.to_f64().unwrap_or(1.0)
                })
                .collect();
            metrics.avg_spread_capture = if !spread_captures.is_empty() {
                spread_captures.iter().sum::<f64>() / spread_captures.len() as f64
            } else {
                0.0
            };
        }

        metrics.fill_rate = if metrics.total_trades > 0 {
            (metrics.total_trades - metrics.failed_trades) as f64 / metrics.total_trades as f64
        } else {
            0.0
        };

        // Time analysis
        if let (Some(first), Some(last)) = (self.trades.first(), self.trades.last()) {
            metrics.start_time_ms = first.entry_time_ms;
            metrics.end_time_ms = last.exit_time_ms;

            let duration_days = (last.exit_time_ms - first.entry_time_ms) as f64 / (24.0 * 60.0 * 60.0 * 1000.0);
            metrics.trading_days = duration_days.ceil() as u32;
            metrics.trades_per_day = if duration_days > 0.0 {
                metrics.total_trades as f64 / duration_days
            } else {
                metrics.total_trades as f64
            };
        }

        // Capital metrics
        metrics.initial_capital = self.initial_capital;
        metrics.final_capital = self.current_equity;
        metrics.peak_capital = self.peak_equity;
        metrics.return_pct = if self.initial_capital > Decimal::ZERO {
            ((self.current_equity - self.initial_capital) / self.initial_capital)
                .to_f64().unwrap_or(0.0) * 100.0
        } else {
            0.0
        };

        // Annualized return
        let years = metrics.trading_days as f64 / 365.0;
        if years > 0.0 {
            metrics.annualized_return_pct = ((1.0 + metrics.return_pct / 100.0).powf(1.0 / years) - 1.0) * 100.0;
        }

        metrics
    }

    /// Calculate maximum drawdown.
    fn calculate_max_drawdown(&self) -> (Decimal, f64) {
        let mut max_dd = Decimal::ZERO;
        let mut max_dd_pct = 0.0;
        let mut peak = self.initial_capital;

        for (_, equity) in &self.equity_curve {
            if *equity > peak {
                peak = *equity;
            }
            let dd = peak - *equity;
            if dd > max_dd {
                max_dd = dd;
                if peak > Decimal::ZERO {
                    max_dd_pct = dd.to_f64().unwrap_or(0.0) / peak.to_f64().unwrap_or(1.0) * 100.0;
                }
            }
        }

        (max_dd, max_dd_pct)
    }

    /// Calculate daily returns from equity curve.
    fn calculate_daily_returns(&self) -> Vec<f64> {
        let daily_equity: Vec<_> = {
            let mut daily: HashMap<u64, Decimal> = HashMap::new();
            for (ts, equity) in &self.equity_curve {
                let day = ts / (24 * 60 * 60 * 1000);
                daily.insert(day, *equity);
            }
            let mut sorted: Vec<_> = daily.into_iter().collect();
            sorted.sort_by_key(|(day, _)| *day);
            sorted.into_iter().map(|(_, e)| e).collect()
        };

        if daily_equity.len() < 2 {
            return Vec::new();
        }

        daily_equity.windows(2)
            .map(|w| {
                if w[0] > Decimal::ZERO {
                    ((w[1] - w[0]) / w[0]).to_f64().unwrap_or(0.0)
                } else {
                    0.0
                }
            })
            .collect()
    }

    /// Calculate Sharpe ratio (assuming 0% risk-free rate).
    fn calculate_sharpe(&self, returns: &[f64]) -> f64 {
        if returns.is_empty() {
            return 0.0;
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;
        let std_dev = variance.sqrt();

        if std_dev > 0.0 {
            // Annualize: mean * sqrt(252), std_dev * sqrt(252)
            (mean * 252.0_f64.sqrt()) / (std_dev * 252.0_f64.sqrt())
        } else {
            0.0
        }
    }

    /// Calculate Sortino ratio (downside deviation only).
    fn calculate_sortino(&self, returns: &[f64]) -> f64 {
        if returns.is_empty() {
            return 0.0;
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;

        // Downside deviation: only negative returns
        let downside: Vec<f64> = returns.iter()
            .filter(|&&r| r < 0.0)
            .map(|r| r.powi(2))
            .collect();

        let downside_dev = if !downside.is_empty() {
            (downside.iter().sum::<f64>() / downside.len() as f64).sqrt()
        } else {
            return f64::INFINITY; // No downside risk
        };

        if downside_dev > 0.0 {
            (mean * 252.0_f64.sqrt()) / (downside_dev * 252.0_f64.sqrt())
        } else {
            0.0
        }
    }

    /// Generate drawdown curve.
    fn generate_drawdown_curve(&self) -> Vec<(u64, Decimal)> {
        let mut result = Vec::new();
        let mut peak = self.initial_capital;

        for (ts, equity) in &self.equity_curve {
            if *equity > peak {
                peak = *equity;
            }
            let dd = peak - *equity;
            result.push((*ts, dd));
        }

        result
    }

    /// Generate full backtest result.
    pub fn generate_result(&self, config_summary: String) -> BacktestResult {
        let metrics = self.calculate_metrics();

        // Per-strategy metrics
        let mut strategy_metrics = HashMap::new();
        for (strategy_id, trades) in &self.strategy_trades {
            let mut tracker = PerformanceTracker::new(self.initial_capital);
            for trade in trades {
                tracker.record_trade(trade.clone());
            }
            strategy_metrics.insert(strategy_id.clone(), tracker.calculate_metrics());
        }

        // Per-venue metrics
        let mut venue_metrics = HashMap::new();
        for (venue, acc) in &self.venue_stats {
            venue_metrics.insert(*venue, VenueMetrics {
                venue: *venue,
                total_trades: acc.trade_count,
                total_volume: acc.total_volume,
                total_fees: acc.total_fees,
                avg_latency_ms: if acc.trade_count > 0 {
                    acc.latency_sum / acc.trade_count as f64
                } else {
                    0.0
                },
                avg_fill_rate: if acc.trade_count > 0 {
                    acc.fill_ratio_sum / acc.trade_count as f64
                } else {
                    0.0
                },
                avg_slippage_bps: if acc.trade_count > 0 {
                    acc.slippage_sum / acc.trade_count as f64
                } else {
                    0.0
                },
            });
        }

        // Daily P&L
        let mut daily_pnl: Vec<_> = self.daily_pnl
            .iter()
            .map(|(day, pnl)| (*day * 24 * 60 * 60 * 1000, *pnl))
            .collect();
        daily_pnl.sort_by_key(|(day, _)| *day);

        BacktestResult {
            metrics,
            strategy_metrics,
            venue_metrics,
            equity_curve: self.equity_curve.clone(),
            drawdown_curve: self.generate_drawdown_curve(),
            daily_pnl,
            trades: self.trades.clone(),
            config_summary,
        }
    }

    /// Get all trades.
    pub fn trades(&self) -> &[TradeRecord] {
        &self.trades
    }

    /// Get current equity.
    pub fn current_equity(&self) -> Decimal {
        self.current_equity
    }

    /// Get trade count.
    pub fn trade_count(&self) -> usize {
        self.trades.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_trade(pnl: Decimal, success: bool) -> TradeRecord {
        TradeRecord {
            trade_id: "test".to_string(),
            strategy_id: "arb".to_string(),
            entry_time_ms: 1000,
            exit_time_ms: 2000,
            leg_a: LegRecord {
                venue: Venue::Kalshi,
                market_id: "MKT1".to_string(),
                side: OrderSide::Buy,
                intended_price: Decimal::new(50, 2),
                fill_price: Decimal::new(50, 2),
                intended_quantity: Decimal::new(100, 0),
                filled_quantity: Decimal::new(100, 0),
                fill_ratio: Decimal::ONE,
                fees: Decimal::new(1, 0),
                latency_ms: 50.0,
            },
            leg_b: LegRecord {
                venue: Venue::Polymarket,
                market_id: "MKT1".to_string(),
                side: OrderSide::Sell,
                intended_price: Decimal::new(52, 2),
                fill_price: Decimal::new(52, 2),
                intended_quantity: Decimal::new(100, 0),
                filled_quantity: Decimal::new(100, 0),
                fill_ratio: Decimal::ONE,
                fees: Decimal::new(1, 0),
                latency_ms: 60.0,
            },
            gross_pnl: pnl + Decimal::new(2, 0),
            total_fees: Decimal::new(2, 0),
            net_pnl: pnl,
            slippage_cost: Decimal::ZERO,
            success,
            failure_reason: if success { None } else { Some("Test failure".to_string()) },
            total_latency_ms: 110.0,
            entry_spread_bps: Decimal::new(200, 0),
            realized_spread_bps: Decimal::new(150, 0),
        }
    }

    #[test]
    fn test_tracker_creation() {
        let tracker = PerformanceTracker::new(Decimal::new(10000, 0));
        assert_eq!(tracker.trade_count(), 0);
        assert_eq!(tracker.current_equity(), Decimal::new(10000, 0));
    }

    #[test]
    fn test_trade_recording() {
        let mut tracker = PerformanceTracker::new(Decimal::new(10000, 0));

        tracker.record_trade(create_test_trade(Decimal::new(50, 0), true));
        tracker.record_trade(create_test_trade(Decimal::new(-20, 0), true));
        tracker.record_trade(create_test_trade(Decimal::new(30, 0), true));

        assert_eq!(tracker.trade_count(), 3);
        assert_eq!(tracker.current_equity(), Decimal::new(10060, 0)); // 10000 + 50 - 20 + 30
    }

    #[test]
    fn test_metrics_calculation() {
        let mut tracker = PerformanceTracker::new(Decimal::new(10000, 0));

        for _ in 0..7 {
            tracker.record_trade(create_test_trade(Decimal::new(100, 0), true));
        }
        for _ in 0..3 {
            tracker.record_trade(create_test_trade(Decimal::new(-50, 0), true));
        }

        let metrics = tracker.calculate_metrics();

        assert_eq!(metrics.total_trades, 10);
        assert_eq!(metrics.winning_trades, 7);
        assert_eq!(metrics.losing_trades, 3);
        assert!(metrics.win_rate > 0.69 && metrics.win_rate < 0.71);
        assert_eq!(metrics.net_pnl, Decimal::new(550, 0)); // 7*100 - 3*50
    }
}
