//! High-performance orderbook engine.
//!
//! Features:
//! - Lock-free orderbook updates
//! - L2/L3 depth tracking
//! - Efficient spread calculation
//! - VWAP computation
//! - Liquidity analysis

pub mod engine;
pub mod aggregator;
pub mod analyzer;

pub use engine::OrderbookEngine;
pub use aggregator::OrderbookAggregator;
pub use analyzer::LiquidityAnalyzer;
