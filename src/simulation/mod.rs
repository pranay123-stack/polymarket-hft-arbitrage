//! Simulation and backtesting module for cross-platform arbitrage.
//!
//! Provides realistic simulation of:
//! - Order execution with latency modeling
//! - Partial fills and slippage
//! - Market impact modeling
//! - Cross-venue coordination delays
//! - Historical replay for strategy validation

pub mod executor;
pub mod market_replay;
pub mod latency_model;
pub mod fill_simulator;
pub mod performance;

pub use executor::SimulatedExecutor;
pub use market_replay::{MarketReplayEngine, HistoricalTick, ReplayConfig};
pub use latency_model::{LatencyModel, LatencyProfile, VenueLatencyConfig};
pub use fill_simulator::{FillSimulator, FillModel, SlippageModel};
pub use performance::{BacktestResult, PerformanceMetrics, TradeRecord};
