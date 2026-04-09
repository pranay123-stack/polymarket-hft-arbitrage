//! Simulated execution engine for cross-platform arbitrage backtesting.
//!
//! Integrates:
//! - Market replay
//! - Latency modeling
//! - Fill simulation
//! - Performance tracking

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use tokio::sync::mpsc;
use serde::{Deserialize, Serialize};

use super::latency_model::{LatencyModel, LatencyProfile, Venue, VenueLatencyConfig};
use super::fill_simulator::{
    FillSimulator, SimulatedOrder, SimulatedOrderBook, OrderSide, OrderType,
    CrossPlatformFillResult,
};
use super::market_replay::{MarketReplayEngine, ReplayConfig, ReplayEvent, HistoricalTick};
use super::performance::{
    PerformanceTracker, TradeRecord, LegRecord, BacktestResult, PerformanceMetrics,
};

/// Configuration for the simulated executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatorConfig {
    /// Initial capital for the simulation.
    pub initial_capital: Decimal,
    /// Maximum position size per market.
    pub max_position_size: Decimal,
    /// Minimum profit threshold to execute (in bps).
    pub min_profit_threshold_bps: Decimal,
    /// Maximum number of concurrent positions.
    pub max_concurrent_positions: u32,
    /// Hedge timeout in milliseconds.
    pub hedge_timeout_ms: u64,
    /// Use conservative fill model.
    pub conservative_mode: bool,
    /// Fee schedule per venue (in bps).
    pub venue_fees_bps: HashMap<Venue, Decimal>,
    /// Latency profile to use.
    pub latency_profile: LatencyProfile,
    /// Enable detailed logging.
    pub verbose_logging: bool,
    /// Market replay configuration.
    pub replay_config: ReplayConfig,
}

impl Default for SimulatorConfig {
    fn default() -> Self {
        let mut venue_fees = HashMap::new();
        venue_fees.insert(Venue::Polymarket, Decimal::new(20, 1)); // 2.0 bps
        venue_fees.insert(Venue::Kalshi, Decimal::new(35, 1));     // 3.5 bps
        venue_fees.insert(Venue::Opinion, Decimal::new(25, 1));    // 2.5 bps

        Self {
            initial_capital: Decimal::new(100000, 0),
            max_position_size: Decimal::new(10000, 0),
            min_profit_threshold_bps: Decimal::new(5, 0), // 5 bps
            max_concurrent_positions: 10,
            hedge_timeout_ms: 500,
            conservative_mode: false,
            venue_fees_bps: venue_fees,
            latency_profile: LatencyProfile::Normal,
            verbose_logging: false,
            replay_config: ReplayConfig::default(),
        }
    }
}

/// A detected arbitrage opportunity.
#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub id: String,
    pub timestamp_ms: u64,
    pub venue_a: Venue,
    pub market_a: String,
    pub side_a: OrderSide,
    pub price_a: Decimal,
    pub venue_b: Venue,
    pub market_b: String,
    pub side_b: OrderSide,
    pub price_b: Decimal,
    pub spread_bps: Decimal,
    pub max_size: Decimal,
    pub estimated_profit: Decimal,
}

/// State of a simulated position.
#[derive(Debug, Clone)]
pub struct SimulatedPosition {
    pub venue: Venue,
    pub market_id: String,
    pub side: OrderSide,
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub entry_time_ms: u64,
    pub unrealized_pnl: Decimal,
}

/// Simulated executor for backtesting cross-platform arbitrage.
pub struct SimulatedExecutor {
    /// Configuration.
    config: SimulatorConfig,
    /// Latency model.
    latency_model: LatencyModel,
    /// Fill simulator.
    fill_simulator: FillSimulator,
    /// Performance tracker.
    performance: Arc<RwLock<PerformanceTracker>>,
    /// Current positions.
    positions: Arc<RwLock<HashMap<(Venue, String), SimulatedPosition>>>,
    /// Current capital.
    available_capital: Arc<RwLock<Decimal>>,
    /// Trade counter.
    trade_counter: Arc<RwLock<u64>>,
    /// Opportunity detector callback.
    opportunity_detector: Option<Box<dyn Fn(&HashMap<(Venue, String), SimulatedOrderBook>) -> Vec<ArbitrageOpportunity> + Send + Sync>>,
}

impl SimulatedExecutor {
    /// Create a new simulated executor.
    pub fn new(config: SimulatorConfig) -> Self {
        let mut latency_model = LatencyModel::new();
        latency_model.set_profile(config.latency_profile);

        let fill_simulator = if config.conservative_mode {
            FillSimulator::conservative()
        } else {
            FillSimulator::new()
        };

        Self {
            performance: Arc::new(RwLock::new(PerformanceTracker::new(config.initial_capital))),
            positions: Arc::new(RwLock::new(HashMap::new())),
            available_capital: Arc::new(RwLock::new(config.initial_capital)),
            trade_counter: Arc::new(RwLock::new(0)),
            opportunity_detector: None,
            config,
            latency_model,
            fill_simulator,
        }
    }

    /// Set a custom opportunity detector.
    pub fn set_opportunity_detector<F>(&mut self, detector: F)
    where
        F: Fn(&HashMap<(Venue, String), SimulatedOrderBook>) -> Vec<ArbitrageOpportunity> + Send + Sync + 'static,
    {
        self.opportunity_detector = Some(Box::new(detector));
    }

    /// Run a full backtest.
    pub async fn run_backtest(&mut self, ticks: Vec<HistoricalTick>) -> Result<BacktestResult, String> {
        // Create replay engine
        let mut replay_engine = MarketReplayEngine::new(self.config.replay_config.clone());
        replay_engine.load_ticks(ticks);

        // Create channel for events
        let (tx, mut rx) = mpsc::channel::<ReplayEvent>(1000);

        // Spawn replay task
        let replay_handle = {
            let mut engine = replay_engine;
            tokio::spawn(async move {
                engine.run(tx).await
            })
        };

        // Process events
        while let Some(event) = rx.recv().await {
            self.process_event(&event).await;
        }

        // Wait for replay to complete
        let _stats = replay_handle.await.map_err(|e| format!("Replay failed: {}", e))?;

        // Generate results
        let config_summary = format!(
            "Capital: {}, Min Profit: {} bps, Conservative: {}",
            self.config.initial_capital,
            self.config.min_profit_threshold_bps,
            self.config.conservative_mode
        );

        Ok(self.performance.read().generate_result(config_summary))
    }

    /// Process a single replay event.
    async fn process_event(&mut self, event: &ReplayEvent) {
        // Update positions with current prices
        self.update_position_pnl(&event.books);

        // Detect opportunities
        let opportunities = if let Some(ref detector) = self.opportunity_detector {
            detector(&event.books)
        } else {
            self.detect_opportunities(&event.books, event.replay_time_ms)
        };

        // Execute profitable opportunities
        for opp in opportunities {
            if opp.spread_bps >= self.config.min_profit_threshold_bps {
                self.execute_opportunity(&opp, &event.books).await;
            }
        }
    }

    /// Detect arbitrage opportunities from current order books.
    fn detect_opportunities(
        &self,
        books: &HashMap<(Venue, String), SimulatedOrderBook>,
        timestamp_ms: u64,
    ) -> Vec<ArbitrageOpportunity> {
        let mut opportunities = Vec::new();

        // Group books by market concept (same underlying)
        let mut market_groups: HashMap<String, Vec<&SimulatedOrderBook>> = HashMap::new();
        for ((_, market_id), book) in books {
            // Extract base market identifier (strip venue prefix if any)
            let base_id = market_id.split(':').last().unwrap_or(market_id);
            market_groups.entry(base_id.to_string())
                .or_insert_with(Vec::new)
                .push(book);
        }

        // Find cross-venue opportunities
        for (base_market, venue_books) in &market_groups {
            if venue_books.len() < 2 {
                continue;
            }

            // Compare all pairs
            for i in 0..venue_books.len() {
                for j in (i + 1)..venue_books.len() {
                    let book_a = venue_books[i];
                    let book_b = venue_books[j];

                    // Check for buy A / sell B opportunity
                    if let (Some(ask_a), Some(bid_b)) = (book_a.best_ask(), book_b.best_bid()) {
                        if bid_b > ask_a {
                            let spread = bid_b - ask_a;
                            let mid = (bid_b + ask_a) / Decimal::new(2, 0);
                            let spread_bps = if mid > Decimal::ZERO {
                                spread / mid * Decimal::new(10000, 0)
                            } else {
                                Decimal::ZERO
                            };

                            // Calculate max executable size
                            let max_size_a = book_a.asks.first().map(|l| l.quantity).unwrap_or(Decimal::ZERO);
                            let max_size_b = book_b.bids.first().map(|l| l.quantity).unwrap_or(Decimal::ZERO);
                            let max_size = max_size_a.min(max_size_b).min(self.config.max_position_size);

                            if spread_bps >= self.config.min_profit_threshold_bps && max_size > Decimal::ZERO {
                                opportunities.push(ArbitrageOpportunity {
                                    id: format!("{}_{}", base_market, timestamp_ms),
                                    timestamp_ms,
                                    venue_a: book_a.venue,
                                    market_a: book_a.market_id.clone(),
                                    side_a: OrderSide::Buy,
                                    price_a: ask_a,
                                    venue_b: book_b.venue,
                                    market_b: book_b.market_id.clone(),
                                    side_b: OrderSide::Sell,
                                    price_b: bid_b,
                                    spread_bps,
                                    max_size,
                                    estimated_profit: spread * max_size,
                                });
                            }
                        }
                    }

                    // Check for sell A / buy B opportunity
                    if let (Some(bid_a), Some(ask_b)) = (book_a.best_bid(), book_b.best_ask()) {
                        if bid_a > ask_b {
                            let spread = bid_a - ask_b;
                            let mid = (bid_a + ask_b) / Decimal::new(2, 0);
                            let spread_bps = if mid > Decimal::ZERO {
                                spread / mid * Decimal::new(10000, 0)
                            } else {
                                Decimal::ZERO
                            };

                            let max_size_a = book_a.bids.first().map(|l| l.quantity).unwrap_or(Decimal::ZERO);
                            let max_size_b = book_b.asks.first().map(|l| l.quantity).unwrap_or(Decimal::ZERO);
                            let max_size = max_size_a.min(max_size_b).min(self.config.max_position_size);

                            if spread_bps >= self.config.min_profit_threshold_bps && max_size > Decimal::ZERO {
                                opportunities.push(ArbitrageOpportunity {
                                    id: format!("{}_{}_rev", base_market, timestamp_ms),
                                    timestamp_ms,
                                    venue_a: book_a.venue,
                                    market_a: book_a.market_id.clone(),
                                    side_a: OrderSide::Sell,
                                    price_a: bid_a,
                                    venue_b: book_b.venue,
                                    market_b: book_b.market_id.clone(),
                                    side_b: OrderSide::Buy,
                                    price_b: ask_b,
                                    spread_bps,
                                    max_size,
                                    estimated_profit: spread * max_size,
                                });
                            }
                        }
                    }
                }
            }
        }

        // Sort by estimated profit
        opportunities.sort_by(|a, b| b.estimated_profit.cmp(&a.estimated_profit));
        opportunities
    }

    /// Execute an arbitrage opportunity.
    async fn execute_opportunity(
        &mut self,
        opp: &ArbitrageOpportunity,
        books: &HashMap<(Venue, String), SimulatedOrderBook>,
    ) {
        // Check capital
        let required_capital = opp.max_size * opp.price_a.max(opp.price_b);
        if *self.available_capital.read() < required_capital {
            return;
        }

        // Check concurrent positions
        if self.positions.read().len() as u32 >= self.config.max_concurrent_positions {
            return;
        }

        // Get order books
        let book_a = match books.get(&(opp.venue_a, opp.market_a.clone())) {
            Some(b) => b.clone(),
            None => return,
        };
        let book_b = match books.get(&(opp.venue_b, opp.market_b.clone())) {
            Some(b) => b.clone(),
            None => return,
        };

        // Create orders
        let order_a = SimulatedOrder {
            id: format!("{}_A", opp.id),
            venue: opp.venue_a,
            side: opp.side_a,
            order_type: OrderType::ImmediateOrCancel,
            price: opp.price_a,
            quantity: opp.max_size,
            timestamp_ms: opp.timestamp_ms,
        };

        let order_b = SimulatedOrder {
            id: format!("{}_B", opp.id),
            venue: opp.venue_b,
            side: opp.side_b,
            order_type: OrderType::ImmediateOrCancel,
            price: opp.price_b,
            quantity: opp.max_size,
            timestamp_ms: opp.timestamp_ms,
        };

        // Simulate execution with latency
        let latency_a = self.latency_model.sample_total_order_latency(opp.venue_a);
        let latency_b = self.latency_model.sample_total_order_latency(opp.venue_b);

        // Execute cross-platform trade
        let result = self.fill_simulator.simulate_cross_platform_execution(
            &order_a,
            &book_a,
            &order_b,
            &book_b,
            self.config.hedge_timeout_ms,
        );

        // Calculate fees
        let fee_a = self.calculate_fee(opp.venue_a, result.leg_a_fill.filled_quantity, result.leg_a_fill.average_fill_price);
        let fee_b = result.leg_b_fill.as_ref().map(|f| {
            self.calculate_fee(opp.venue_b, f.filled_quantity, f.average_fill_price)
        }).unwrap_or(Decimal::ZERO);

        let total_fees = fee_a + fee_b;
        let net_pnl = result.net_profit - total_fees;

        // Calculate slippage
        let expected_value_a = opp.max_size * opp.price_a;
        let actual_value_a = result.leg_a_fill.filled_quantity * result.leg_a_fill.average_fill_price;
        let slippage_a = (actual_value_a - expected_value_a).abs();

        let slippage_b = result.leg_b_fill.as_ref().map(|f| {
            let expected = opp.max_size * opp.price_b;
            let actual = f.filled_quantity * f.average_fill_price;
            (actual - expected).abs()
        }).unwrap_or(Decimal::ZERO);

        let total_slippage = slippage_a + slippage_b;

        // Create trade record
        let trade_id = {
            let mut counter = self.trade_counter.write();
            *counter += 1;
            format!("trade_{}", *counter)
        };

        let leg_b_fill = result.leg_b_fill.clone().unwrap_or_else(|| super::fill_simulator::SimulatedFill {
            order_id: format!("{}_B", opp.id),
            filled_quantity: Decimal::ZERO,
            remaining_quantity: opp.max_size,
            average_fill_price: Decimal::ZERO,
            slippage_bps: Decimal::ZERO,
            fill_latency_ms: 0.0,
            was_full_fill: false,
            fills: Vec::new(),
        });

        let realized_spread = if result.leg_a_fill.average_fill_price > Decimal::ZERO {
            let mid = (result.leg_a_fill.average_fill_price + leg_b_fill.average_fill_price) / Decimal::new(2, 0);
            if mid > Decimal::ZERO {
                (leg_b_fill.average_fill_price - result.leg_a_fill.average_fill_price).abs() / mid * Decimal::new(10000, 0)
            } else {
                Decimal::ZERO
            }
        } else {
            Decimal::ZERO
        };

        let trade = TradeRecord {
            trade_id,
            strategy_id: "cross_platform_arb".to_string(),
            entry_time_ms: opp.timestamp_ms,
            exit_time_ms: opp.timestamp_ms + (result.total_latency_ms as u64),
            leg_a: LegRecord {
                venue: opp.venue_a,
                market_id: opp.market_a.clone(),
                side: opp.side_a,
                intended_price: opp.price_a,
                fill_price: result.leg_a_fill.average_fill_price,
                intended_quantity: opp.max_size,
                filled_quantity: result.leg_a_fill.filled_quantity,
                fill_ratio: if opp.max_size > Decimal::ZERO {
                    result.leg_a_fill.filled_quantity / opp.max_size
                } else {
                    Decimal::ZERO
                },
                fees: fee_a,
                latency_ms: latency_a.as_secs_f64() * 1000.0,
            },
            leg_b: LegRecord {
                venue: opp.venue_b,
                market_id: opp.market_b.clone(),
                side: opp.side_b,
                intended_price: opp.price_b,
                fill_price: leg_b_fill.average_fill_price,
                intended_quantity: opp.max_size,
                filled_quantity: leg_b_fill.filled_quantity,
                fill_ratio: if opp.max_size > Decimal::ZERO {
                    leg_b_fill.filled_quantity / opp.max_size
                } else {
                    Decimal::ZERO
                },
                fees: fee_b,
                latency_ms: latency_b.as_secs_f64() * 1000.0,
            },
            gross_pnl: result.net_profit,
            total_fees,
            net_pnl,
            slippage_cost: total_slippage,
            success: result.execution_success,
            failure_reason: result.failure_reason,
            total_latency_ms: result.total_latency_ms,
            entry_spread_bps: opp.spread_bps,
            realized_spread_bps: realized_spread,
        };

        // Record trade
        self.performance.write().record_trade(trade);

        // Update capital
        *self.available_capital.write() += net_pnl;

        if self.config.verbose_logging {
            println!(
                "[{}] Executed {} @ {} bps, PnL: {} (net: {})",
                opp.timestamp_ms,
                opp.id,
                opp.spread_bps,
                result.net_profit,
                net_pnl
            );
        }
    }

    /// Calculate fee for a trade.
    fn calculate_fee(&self, venue: Venue, quantity: Decimal, price: Decimal) -> Decimal {
        let fee_bps = self.config.venue_fees_bps.get(&venue).copied().unwrap_or(Decimal::new(30, 1));
        let notional = quantity * price;
        notional * fee_bps / Decimal::new(10000, 0)
    }

    /// Update unrealized P&L for open positions.
    fn update_position_pnl(&self, books: &HashMap<(Venue, String), SimulatedOrderBook>) {
        let mut positions = self.positions.write();
        for ((venue, market_id), position) in positions.iter_mut() {
            if let Some(book) = books.get(&(*venue, market_id.clone())) {
                let current_price = match position.side {
                    OrderSide::Buy => book.best_bid().unwrap_or(position.entry_price),
                    OrderSide::Sell => book.best_ask().unwrap_or(position.entry_price),
                };

                position.unrealized_pnl = match position.side {
                    OrderSide::Buy => (current_price - position.entry_price) * position.quantity,
                    OrderSide::Sell => (position.entry_price - current_price) * position.quantity,
                };
            }
        }
    }

    /// Get current performance metrics.
    pub fn current_metrics(&self) -> PerformanceMetrics {
        self.performance.read().calculate_metrics()
    }

    /// Get current equity.
    pub fn current_equity(&self) -> Decimal {
        self.performance.read().current_equity()
    }

    /// Get trade count.
    pub fn trade_count(&self) -> usize {
        self.performance.read().trade_count()
    }

    /// Get latency statistics.
    pub fn latency_stats(&self, venue: Venue) -> super::latency_model::VenueLatencyStats {
        self.latency_model.get_venue_stats(venue)
    }

    /// Get fill statistics.
    pub fn fill_stats(&self) -> super::fill_simulator::FillStatistics {
        self.fill_simulator.get_fill_stats()
    }
}

/// Helper to run a quick backtest with default settings.
pub async fn quick_backtest(
    ticks: Vec<HistoricalTick>,
    initial_capital: Decimal,
    min_profit_bps: Decimal,
) -> Result<BacktestResult, String> {
    let config = SimulatorConfig {
        initial_capital,
        min_profit_threshold_bps: min_profit_bps,
        ..Default::default()
    };

    let mut executor = SimulatedExecutor::new(config);
    executor.run_backtest(ticks).await
}

/// Helper to run a conservative backtest (worst-case analysis).
pub async fn conservative_backtest(
    ticks: Vec<HistoricalTick>,
    initial_capital: Decimal,
) -> Result<BacktestResult, String> {
    let config = SimulatorConfig {
        initial_capital,
        conservative_mode: true,
        latency_profile: LatencyProfile::HighVolatility,
        min_profit_threshold_bps: Decimal::new(10, 0), // Higher threshold for conservative
        ..Default::default()
    };

    let mut executor = SimulatedExecutor::new(config);
    executor.run_backtest(ticks).await
}

/// Helper to run a stress test backtest.
pub async fn stress_test_backtest(
    ticks: Vec<HistoricalTick>,
    initial_capital: Decimal,
) -> Result<BacktestResult, String> {
    let config = SimulatorConfig {
        initial_capital,
        conservative_mode: true,
        latency_profile: LatencyProfile::StressTest,
        min_profit_threshold_bps: Decimal::new(15, 0),
        ..Default::default()
    };

    let mut executor = SimulatedExecutor::new(config);
    executor.run_backtest(ticks).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::market_replay::TickBuilder;

    #[tokio::test]
    async fn test_executor_creation() {
        let config = SimulatorConfig::default();
        let executor = SimulatedExecutor::new(config);
        assert_eq!(executor.trade_count(), 0);
    }

    #[tokio::test]
    async fn test_opportunity_detection() {
        let config = SimulatorConfig::default();
        let executor = SimulatedExecutor::new(config);

        // Create books with an arbitrage opportunity
        let mut books = HashMap::new();

        books.insert(
            (Venue::Kalshi, "MKT1".to_string()),
            SimulatedOrderBook {
                venue: Venue::Kalshi,
                market_id: "MKT1".to_string(),
                bids: vec![super::super::fill_simulator::SimulatedBookLevel {
                    price: Decimal::new(48, 2),
                    quantity: Decimal::new(1000, 0),
                    order_count: 5,
                }],
                asks: vec![super::super::fill_simulator::SimulatedBookLevel {
                    price: Decimal::new(49, 2),
                    quantity: Decimal::new(1000, 0),
                    order_count: 5,
                }],
                last_update_ms: 0,
            },
        );

        books.insert(
            (Venue::Polymarket, "MKT1".to_string()),
            SimulatedOrderBook {
                venue: Venue::Polymarket,
                market_id: "MKT1".to_string(),
                bids: vec![super::super::fill_simulator::SimulatedBookLevel {
                    price: Decimal::new(52, 2),
                    quantity: Decimal::new(1000, 0),
                    order_count: 5,
                }],
                asks: vec![super::super::fill_simulator::SimulatedBookLevel {
                    price: Decimal::new(53, 2),
                    quantity: Decimal::new(1000, 0),
                    order_count: 5,
                }],
                last_update_ms: 0,
            },
        );

        let opportunities = executor.detect_opportunities(&books, 1000);

        // Should detect buy Kalshi @ 49, sell Polymarket @ 52
        assert!(!opportunities.is_empty());
        let best = &opportunities[0];
        assert_eq!(best.venue_a, Venue::Kalshi);
        assert_eq!(best.side_a, OrderSide::Buy);
        assert_eq!(best.venue_b, Venue::Polymarket);
        assert_eq!(best.side_b, OrderSide::Sell);
    }
}
