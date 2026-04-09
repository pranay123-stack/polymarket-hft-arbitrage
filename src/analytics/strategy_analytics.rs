//! Strategy-level analytics for cross-platform arbitrage.
//!
//! Tracks:
//! - Per-strategy performance
//! - Strategy comparison
//! - Opportunity analysis
//! - Execution quality by strategy

use std::collections::HashMap;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use std::sync::Arc;
use chrono::{DateTime, Utc, Duration};

/// Metrics for a single strategy.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StrategyMetrics {
    /// Strategy identifier.
    pub strategy_id: String,
    /// Strategy name/description.
    pub name: String,
    /// Total trades executed.
    pub total_trades: u64,
    /// Successful trades (both legs filled).
    pub successful_trades: u64,
    /// Failed trades.
    pub failed_trades: u64,
    /// Gross P&L.
    pub gross_pnl: Decimal,
    /// Net P&L (after fees).
    pub net_pnl: Decimal,
    /// Total fees paid.
    pub total_fees: Decimal,
    /// Win rate.
    pub win_rate: f64,
    /// Average profit per winning trade.
    pub avg_win: Decimal,
    /// Average loss per losing trade.
    pub avg_loss: Decimal,
    /// Profit factor (gross wins / gross losses).
    pub profit_factor: f64,
    /// Sharpe ratio.
    pub sharpe_ratio: f64,
    /// Sortino ratio.
    pub sortino_ratio: f64,
    /// Maximum drawdown.
    pub max_drawdown: Decimal,
    /// Average trade duration in milliseconds.
    pub avg_trade_duration_ms: f64,
    /// Average slippage in basis points.
    pub avg_slippage_bps: f64,
    /// Opportunities detected.
    pub opportunities_detected: u64,
    /// Opportunities executed.
    pub opportunities_executed: u64,
    /// Execution rate.
    pub execution_rate: f64,
    /// Average spread captured (realized vs expected).
    pub avg_spread_capture: f64,
    /// Last update time.
    pub last_updated: DateTime<Utc>,
}

/// Comparison between strategies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyComparison {
    /// Timestamp of comparison.
    pub timestamp: DateTime<Utc>,
    /// Time period for comparison.
    pub period: String,
    /// Strategy metrics.
    pub strategies: Vec<StrategyMetrics>,
    /// Best performing strategy (by net P&L).
    pub best_by_pnl: Option<String>,
    /// Best performing strategy (by Sharpe).
    pub best_by_sharpe: Option<String>,
    /// Best performing strategy (by win rate).
    pub best_by_win_rate: Option<String>,
    /// Aggregate metrics.
    pub aggregate: StrategyMetrics,
}

/// Opportunity record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunityRecord {
    pub opportunity_id: String,
    pub strategy_id: String,
    pub detected_at: DateTime<Utc>,
    pub expected_spread_bps: Decimal,
    pub executed: bool,
    pub execution_result: Option<ExecutionResult>,
}

/// Execution result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub success: bool,
    pub realized_spread_bps: Decimal,
    pub pnl: Decimal,
    pub fees: Decimal,
    pub duration_ms: f64,
    pub slippage_bps: Decimal,
    pub failure_reason: Option<String>,
}

/// Strategy analytics service.
pub struct StrategyAnalytics {
    /// Per-strategy metrics.
    metrics: Arc<RwLock<HashMap<String, StrategyMetrics>>>,
    /// Trade history by strategy.
    trade_history: Arc<RwLock<HashMap<String, Vec<TradeRecord>>>>,
    /// Opportunity history.
    opportunities: Arc<RwLock<Vec<OpportunityRecord>>>,
    /// Max history size.
    max_history_size: usize,
}

/// Trade record for analytics.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TradeRecord {
    pub timestamp: DateTime<Utc>,
    pub pnl: Decimal,
    pub fees: Decimal,
    pub is_winner: bool,
    pub duration_ms: f64,
    pub slippage_bps: Decimal,
    pub spread_capture: f64,
}

impl StrategyAnalytics {
    /// Create new strategy analytics.
    pub fn new(max_history_size: usize) -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            trade_history: Arc::new(RwLock::new(HashMap::new())),
            opportunities: Arc::new(RwLock::new(Vec::new())),
            max_history_size,
        }
    }

    /// Register a strategy.
    pub async fn register_strategy(&self, strategy_id: &str, name: &str) {
        let mut metrics = self.metrics.write().await;
        metrics.entry(strategy_id.to_string()).or_insert_with(|| StrategyMetrics {
            strategy_id: strategy_id.to_string(),
            name: name.to_string(),
            last_updated: Utc::now(),
            ..Default::default()
        });
    }

    /// Record an opportunity.
    pub async fn record_opportunity(
        &self,
        opportunity_id: &str,
        strategy_id: &str,
        expected_spread_bps: Decimal,
    ) {
        // Update opportunity count
        if let Some(metrics) = self.metrics.write().await.get_mut(strategy_id) {
            metrics.opportunities_detected += 1;
            metrics.last_updated = Utc::now();
        }

        // Record opportunity
        let record = OpportunityRecord {
            opportunity_id: opportunity_id.to_string(),
            strategy_id: strategy_id.to_string(),
            detected_at: Utc::now(),
            expected_spread_bps,
            executed: false,
            execution_result: None,
        };

        let mut opportunities = self.opportunities.write().await;
        opportunities.push(record);
        if opportunities.len() > self.max_history_size {
            opportunities.remove(0);
        }
    }

    /// Record a trade execution.
    pub async fn record_execution(
        &self,
        opportunity_id: &str,
        strategy_id: &str,
        result: ExecutionResult,
    ) {
        let now = Utc::now();

        // Update opportunity record
        {
            let mut opportunities = self.opportunities.write().await;
            if let Some(opp) = opportunities.iter_mut().find(|o| o.opportunity_id == opportunity_id) {
                opp.executed = true;
                opp.execution_result = Some(result.clone());
            }
        }

        // Update metrics
        if let Some(metrics) = self.metrics.write().await.get_mut(strategy_id) {
            metrics.total_trades += 1;
            metrics.opportunities_executed += 1;

            if result.success {
                metrics.successful_trades += 1;
                metrics.gross_pnl += result.pnl;
            } else {
                metrics.failed_trades += 1;
            }

            metrics.total_fees += result.fees;
            metrics.net_pnl = metrics.gross_pnl - metrics.total_fees;

            // Update win rate
            let is_winner = result.pnl > result.fees;
            if metrics.total_trades > 0 {
                metrics.win_rate = if is_winner {
                    (metrics.win_rate * (metrics.total_trades - 1) as f64 + 1.0) / metrics.total_trades as f64
                } else {
                    metrics.win_rate * (metrics.total_trades - 1) as f64 / metrics.total_trades as f64
                };
            }

            // Update execution rate
            if metrics.opportunities_detected > 0 {
                metrics.execution_rate = metrics.opportunities_executed as f64 / metrics.opportunities_detected as f64;
            }

            // Running average for slippage
            let n = metrics.total_trades as f64;
            metrics.avg_slippage_bps = metrics.avg_slippage_bps * (n - 1.0) / n +
                result.slippage_bps.to_f64().unwrap_or(0.0) / n;

            // Running average for duration
            metrics.avg_trade_duration_ms = metrics.avg_trade_duration_ms * (n - 1.0) / n +
                result.duration_ms / n;

            metrics.last_updated = now;
        }

        // Record trade
        let trade = TradeRecord {
            timestamp: now,
            pnl: result.pnl,
            fees: result.fees,
            is_winner: result.pnl > result.fees,
            duration_ms: result.duration_ms,
            slippage_bps: result.slippage_bps.to_f64().unwrap_or(0.0) as Decimal,
            spread_capture: 0.0, // Would calculate from expected vs realized
        };

        let mut history = self.trade_history.write().await;
        let strategy_history = history.entry(strategy_id.to_string()).or_insert_with(Vec::new);
        strategy_history.push(trade);
        if strategy_history.len() > self.max_history_size {
            strategy_history.remove(0);
        }
    }

    /// Get metrics for a strategy.
    pub async fn get_strategy_metrics(&self, strategy_id: &str) -> Option<StrategyMetrics> {
        self.metrics.read().await.get(strategy_id).cloned()
    }

    /// Get all strategy metrics.
    pub async fn get_all_metrics(&self) -> Vec<StrategyMetrics> {
        self.metrics.read().await.values().cloned().collect()
    }

    /// Get strategy comparison.
    pub async fn get_comparison(&self, period: &str) -> StrategyComparison {
        let strategies = self.get_all_metrics().await;

        let best_by_pnl = strategies.iter()
            .max_by_key(|s| s.net_pnl)
            .map(|s| s.strategy_id.clone());

        let best_by_sharpe = strategies.iter()
            .max_by(|a, b| a.sharpe_ratio.partial_cmp(&b.sharpe_ratio).unwrap_or(std::cmp::Ordering::Equal))
            .map(|s| s.strategy_id.clone());

        let best_by_win_rate = strategies.iter()
            .max_by(|a, b| a.win_rate.partial_cmp(&b.win_rate).unwrap_or(std::cmp::Ordering::Equal))
            .map(|s| s.strategy_id.clone());

        // Calculate aggregate
        let aggregate = self.calculate_aggregate(&strategies);

        StrategyComparison {
            timestamp: Utc::now(),
            period: period.to_string(),
            strategies,
            best_by_pnl,
            best_by_sharpe,
            best_by_win_rate,
            aggregate,
        }
    }

    /// Calculate aggregate metrics.
    fn calculate_aggregate(&self, strategies: &[StrategyMetrics]) -> StrategyMetrics {
        let mut aggregate = StrategyMetrics {
            strategy_id: "aggregate".to_string(),
            name: "All Strategies".to_string(),
            last_updated: Utc::now(),
            ..Default::default()
        };

        for strategy in strategies {
            aggregate.total_trades += strategy.total_trades;
            aggregate.successful_trades += strategy.successful_trades;
            aggregate.failed_trades += strategy.failed_trades;
            aggregate.gross_pnl += strategy.gross_pnl;
            aggregate.net_pnl += strategy.net_pnl;
            aggregate.total_fees += strategy.total_fees;
            aggregate.opportunities_detected += strategy.opportunities_detected;
            aggregate.opportunities_executed += strategy.opportunities_executed;

            if strategy.max_drawdown > aggregate.max_drawdown {
                aggregate.max_drawdown = strategy.max_drawdown;
            }
        }

        if aggregate.total_trades > 0 {
            aggregate.win_rate = aggregate.successful_trades as f64 / aggregate.total_trades as f64;
        }

        if aggregate.opportunities_detected > 0 {
            aggregate.execution_rate = aggregate.opportunities_executed as f64 / aggregate.opportunities_detected as f64;
        }

        aggregate
    }

    /// Get opportunity analysis.
    pub async fn get_opportunity_analysis(&self, strategy_id: Option<&str>) -> OpportunityAnalysis {
        let opportunities = self.opportunities.read().await;

        let filtered: Vec<_> = match strategy_id {
            Some(id) => opportunities.iter().filter(|o| o.strategy_id == id).collect(),
            None => opportunities.iter().collect(),
        };

        let total = filtered.len();
        let executed = filtered.iter().filter(|o| o.executed).count();
        let successful = filtered.iter()
            .filter(|o| o.execution_result.as_ref().map(|r| r.success).unwrap_or(false))
            .count();

        let avg_expected_spread = if !filtered.is_empty() {
            filtered.iter().map(|o| o.expected_spread_bps).sum::<Decimal>() / Decimal::from(filtered.len())
        } else {
            Decimal::ZERO
        };

        let avg_realized_spread = if executed > 0 {
            let sum: Decimal = filtered.iter()
                .filter_map(|o| o.execution_result.as_ref())
                .map(|r| r.realized_spread_bps)
                .sum();
            sum / Decimal::from(executed)
        } else {
            Decimal::ZERO
        };

        OpportunityAnalysis {
            timestamp: Utc::now(),
            strategy_id: strategy_id.map(String::from),
            total_opportunities: total,
            executed_opportunities: executed,
            successful_executions: successful,
            execution_rate: if total > 0 { executed as f64 / total as f64 } else { 0.0 },
            success_rate: if executed > 0 { successful as f64 / executed as f64 } else { 0.0 },
            avg_expected_spread_bps: avg_expected_spread,
            avg_realized_spread_bps: avg_realized_spread,
            spread_capture_rate: if avg_expected_spread > Decimal::ZERO {
                (avg_realized_spread / avg_expected_spread).to_f64().unwrap_or(0.0)
            } else {
                0.0
            },
        }
    }
}

/// Opportunity analysis results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunityAnalysis {
    pub timestamp: DateTime<Utc>,
    pub strategy_id: Option<String>,
    pub total_opportunities: usize,
    pub executed_opportunities: usize,
    pub successful_executions: usize,
    pub execution_rate: f64,
    pub success_rate: f64,
    pub avg_expected_spread_bps: Decimal,
    pub avg_realized_spread_bps: Decimal,
    pub spread_capture_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_strategy_analytics_creation() {
        let analytics = StrategyAnalytics::new(1000);
        analytics.register_strategy("cross_platform", "Cross-Platform Arbitrage").await;

        let metrics = analytics.get_strategy_metrics("cross_platform").await;
        assert!(metrics.is_some());
        assert_eq!(metrics.unwrap().name, "Cross-Platform Arbitrage");
    }

    #[tokio::test]
    async fn test_trade_recording() {
        let analytics = StrategyAnalytics::new(1000);
        analytics.register_strategy("test", "Test Strategy").await;

        analytics.record_opportunity("opp1", "test", Decimal::new(10, 0)).await;

        let result = ExecutionResult {
            success: true,
            realized_spread_bps: Decimal::new(8, 0),
            pnl: Decimal::new(100, 0),
            fees: Decimal::new(2, 0),
            duration_ms: 150.0,
            slippage_bps: Decimal::new(2, 0),
            failure_reason: None,
        };

        analytics.record_execution("opp1", "test", result).await;

        let metrics = analytics.get_strategy_metrics("test").await.unwrap();
        assert_eq!(metrics.total_trades, 1);
        assert_eq!(metrics.successful_trades, 1);
        assert_eq!(metrics.gross_pnl, Decimal::new(100, 0));
    }
}
