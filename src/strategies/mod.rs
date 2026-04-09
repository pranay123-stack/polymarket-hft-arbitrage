//! Arbitrage detection strategies.
//!
//! Implements multiple arbitrage detection algorithms:
//! - Binary market mispricing (single venue)
//! - Multi-outcome mispricing (single venue)
//! - Cross-market arbitrage (single venue)
//! - Temporal arbitrage (5-min markets)
//! - Cross-platform arbitrage (Polymarket ↔ Kalshi/Opinion)
//! - Signal generation framework for alpha capture

pub mod binary;
pub mod multi_outcome;
pub mod cross_market;
pub mod temporal;
pub mod detector;
pub mod cross_platform;
pub mod signals;

pub use binary::BinaryArbitrageStrategy;
pub use multi_outcome::MultiOutcomeStrategy;
pub use cross_market::CrossMarketStrategy;
pub use temporal::TemporalStrategy;
pub use detector::ArbitrageDetector;
pub use cross_platform::{
    CrossPlatformStrategy, CrossPlatformStrategyConfig, MarketMapping,
    PolymarketMarketInfo, KalshiMarketInfo, OpinionMarketInfo,
    CrossVenuePriceSnapshot, VenuePrices,
};
pub use signals::{
    Signal, SignalAggregator, SignalConfig, SignalGenerator, SignalType,
    SignalStats, CombinedSignal, CombineStrategy, Direction, MarketData,
    PricingSignalGenerator, OrderFlowSignalGenerator,
};
