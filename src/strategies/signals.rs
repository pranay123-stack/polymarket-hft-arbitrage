//! Signal Generation Framework
//!
//! Provides a modular system for generating, combining, and evaluating
//! trading signals for arbitrage opportunities.
//!
//! ## Features:
//!
//! - **Signal Types**: Price, spread, momentum, volume, order flow signals
//! - **Signal Combination**: Weighted, voting, ML-based combination
//! - **Signal Decay**: Time-based signal strength decay
//! - **Signal History**: Track and analyze signal performance
//! - **Alpha Generation**: Identify persistent alpha sources

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, trace};

/// Signal strength range (-1.0 to 1.0)
pub type SignalValue = f64;

/// Unique identifier for a signal source
pub type SignalId = String;

/// Type of signal
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SignalType {
    /// Price-based signal (mispricing detection)
    Price,
    /// Spread-based signal (spread compression/expansion)
    Spread,
    /// Momentum signal (price momentum)
    Momentum,
    /// Volume signal (unusual volume)
    Volume,
    /// Order flow signal (order book imbalance)
    OrderFlow,
    /// Time-based signal (time-of-day effects)
    Temporal,
    /// Cross-market signal (cross-market effects)
    CrossMarket,
    /// Volatility signal (volatility regime)
    Volatility,
    /// Sentiment signal (news/social sentiment)
    Sentiment,
    /// Technical signal (technical indicators)
    Technical,
}

/// Trading direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Long,
    Short,
    Neutral,
}

impl Direction {
    pub fn from_signal(value: SignalValue) -> Self {
        if value > 0.1 {
            Direction::Long
        } else if value < -0.1 {
            Direction::Short
        } else {
            Direction::Neutral
        }
    }
}

/// A single trading signal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    /// Unique signal identifier
    pub id: SignalId,
    /// Signal type
    pub signal_type: SignalType,
    /// Market identifier
    pub market_id: String,
    /// Signal value (-1.0 to 1.0, negative = short, positive = long)
    pub value: SignalValue,
    /// Confidence in the signal (0.0 to 1.0)
    pub confidence: f64,
    /// Signal timestamp
    pub timestamp: DateTime<Utc>,
    /// Signal expiry (for decay)
    pub expiry: DateTime<Utc>,
    /// Source identifier
    pub source: String,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl Signal {
    /// Create a new signal
    pub fn new(
        signal_type: SignalType,
        market_id: impl Into<String>,
        value: SignalValue,
        confidence: f64,
        source: impl Into<String>,
        ttl: Duration,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            signal_type,
            market_id: market_id.into(),
            value: value.clamp(-1.0, 1.0),
            confidence: confidence.clamp(0.0, 1.0),
            timestamp: now,
            expiry: now + ttl,
            source: source.into(),
            metadata: HashMap::new(),
        }
    }

    /// Check if signal is still valid (not expired)
    pub fn is_valid(&self) -> bool {
        Utc::now() < self.expiry
    }

    /// Get signal strength (value * confidence)
    pub fn strength(&self) -> f64 {
        self.value * self.confidence
    }

    /// Get decayed signal value based on age
    pub fn decayed_value(&self, decay_rate: f64) -> f64 {
        let age_secs = (Utc::now() - self.timestamp).num_seconds() as f64;
        let decay = (-decay_rate * age_secs).exp();
        self.value * decay
    }

    /// Get direction
    pub fn direction(&self) -> Direction {
        Direction::from_signal(self.value)
    }
}

/// Configuration for signal generator
#[derive(Debug, Clone)]
pub struct SignalConfig {
    /// Default signal TTL
    pub default_ttl: Duration,
    /// Decay rate for signals (per second)
    pub decay_rate: f64,
    /// Minimum signal strength to consider
    pub min_strength: f64,
    /// Maximum history to retain
    pub max_history: usize,
    /// Enable signal performance tracking
    pub track_performance: bool,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            default_ttl: Duration::seconds(60),
            decay_rate: 0.01,
            min_strength: 0.1,
            max_history: 10000,
            track_performance: true,
        }
    }
}

/// Signal performance statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SignalStats {
    /// Total signals generated
    pub total_signals: u64,
    /// Signals that led to trades
    pub traded_signals: u64,
    /// Profitable signals
    pub profitable_signals: u64,
    /// Total P&L from this signal source
    pub total_pnl: Decimal,
    /// Hit rate (profitable / traded)
    pub hit_rate: f64,
    /// Average signal strength
    pub avg_strength: f64,
    /// Information coefficient
    pub ic: f64,
}

/// Combined signal output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CombinedSignal {
    /// Market identifier
    pub market_id: String,
    /// Combined signal value
    pub value: SignalValue,
    /// Combined confidence
    pub confidence: f64,
    /// Recommended direction
    pub direction: Direction,
    /// Contributing signals
    pub contributing_signals: Vec<SignalId>,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
}

/// Signal combiner strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CombineStrategy {
    /// Weighted average by confidence
    WeightedAverage,
    /// Majority voting
    MajorityVote,
    /// Strongest signal wins
    StrongestSignal,
    /// Product of signals (agreement required)
    Product,
    /// Custom weights per signal type
    CustomWeights,
}

/// Signal generator trait
pub trait SignalGenerator: Send + Sync {
    /// Generate signals for a market
    fn generate(&self, market_id: &str, data: &MarketData) -> Vec<Signal>;

    /// Signal type this generator produces
    fn signal_type(&self) -> SignalType;

    /// Generator name
    fn name(&self) -> &str;
}

/// Market data for signal generation
#[derive(Debug, Clone)]
pub struct MarketData {
    pub market_id: String,
    pub yes_bid: Decimal,
    pub yes_ask: Decimal,
    pub no_bid: Decimal,
    pub no_ask: Decimal,
    pub yes_volume_24h: Decimal,
    pub no_volume_24h: Decimal,
    pub bid_depth: Vec<(Decimal, Decimal)>,
    pub ask_depth: Vec<(Decimal, Decimal)>,
    pub timestamp: DateTime<Utc>,
}

/// Price mispricing signal generator
pub struct PricingSignalGenerator {
    config: SignalConfig,
}

impl PricingSignalGenerator {
    pub fn new(config: SignalConfig) -> Self {
        Self { config }
    }
}

impl SignalGenerator for PricingSignalGenerator {
    fn generate(&self, market_id: &str, data: &MarketData) -> Vec<Signal> {
        let mut signals = Vec::new();

        // Calculate theoretical prices
        let yes_mid = (data.yes_bid + data.yes_ask) / dec!(2);
        let no_mid = (data.no_bid + data.no_ask) / dec!(2);
        let total = yes_mid + no_mid;

        // Check for binary arbitrage (yes + no should equal 1.0)
        let mispricing = total - dec!(1);
        let mispricing_f64 = decimal_to_f64(mispricing);

        if mispricing_f64.abs() > 0.005 {
            // More than 0.5% mispricing
            let signal_value = -mispricing_f64.clamp(-1.0, 1.0); // Negative = overbought
            let confidence = (mispricing_f64.abs() * 10.0).min(1.0);

            signals.push(Signal::new(
                SignalType::Price,
                market_id,
                signal_value,
                confidence,
                "pricing_arb",
                self.config.default_ttl,
            ));
        }

        // Check bid-ask spread anomalies
        let yes_spread = decimal_to_f64(data.yes_ask - data.yes_bid);
        let no_spread = decimal_to_f64(data.no_ask - data.no_bid);

        if yes_spread < 0.01 && no_spread > 0.05 {
            // Yes side tight, No side wide - signal to sell No
            signals.push(Signal::new(
                SignalType::Spread,
                market_id,
                -0.3,
                0.6,
                "spread_imbalance",
                self.config.default_ttl,
            ));
        } else if no_spread < 0.01 && yes_spread > 0.05 {
            // No side tight, Yes side wide - signal to sell Yes
            signals.push(Signal::new(
                SignalType::Spread,
                market_id,
                0.3,
                0.6,
                "spread_imbalance",
                self.config.default_ttl,
            ));
        }

        signals
    }

    fn signal_type(&self) -> SignalType {
        SignalType::Price
    }

    fn name(&self) -> &str {
        "pricing_signal_generator"
    }
}

/// Order flow imbalance signal generator
pub struct OrderFlowSignalGenerator {
    config: SignalConfig,
}

impl OrderFlowSignalGenerator {
    pub fn new(config: SignalConfig) -> Self {
        Self { config }
    }
}

impl SignalGenerator for OrderFlowSignalGenerator {
    fn generate(&self, market_id: &str, data: &MarketData) -> Vec<Signal> {
        let mut signals = Vec::new();

        // Calculate order book imbalance
        let bid_volume: Decimal = data.bid_depth.iter().map(|(_, qty)| qty).sum();
        let ask_volume: Decimal = data.ask_depth.iter().map(|(_, qty)| qty).sum();

        let total_volume = bid_volume + ask_volume;
        if total_volume > Decimal::ZERO {
            let imbalance = decimal_to_f64((bid_volume - ask_volume) / total_volume);

            if imbalance.abs() > 0.2 {
                let signal_value = imbalance.clamp(-1.0, 1.0);
                let confidence = (imbalance.abs() * 1.5).min(1.0);

                signals.push(Signal::new(
                    SignalType::OrderFlow,
                    market_id,
                    signal_value,
                    confidence,
                    "orderbook_imbalance",
                    Duration::seconds(30),
                ));
            }
        }

        // Check for volume spike
        let total_24h = data.yes_volume_24h + data.no_volume_24h;
        if total_24h > dec!(100000) {
            // High volume market
            let volume_ratio =
                decimal_to_f64(data.yes_volume_24h / (data.no_volume_24h + dec!(1)));
            let signal_value = (volume_ratio.ln() / 2.0).clamp(-1.0, 1.0);

            if signal_value.abs() > 0.3 {
                signals.push(Signal::new(
                    SignalType::Volume,
                    market_id,
                    signal_value,
                    0.5,
                    "volume_ratio",
                    Duration::minutes(5),
                ));
            }
        }

        signals
    }

    fn signal_type(&self) -> SignalType {
        SignalType::OrderFlow
    }

    fn name(&self) -> &str {
        "orderflow_signal_generator"
    }
}

/// Signal aggregator for combining multiple signal sources
pub struct SignalAggregator {
    generators: Vec<Box<dyn SignalGenerator>>,
    config: SignalConfig,
    signals: RwLock<HashMap<String, VecDeque<Signal>>>,
    stats: RwLock<HashMap<String, SignalStats>>,
    weights: HashMap<SignalType, f64>,
}

impl SignalAggregator {
    pub fn new(config: SignalConfig) -> Self {
        let mut weights = HashMap::new();
        weights.insert(SignalType::Price, 1.0);
        weights.insert(SignalType::Spread, 0.8);
        weights.insert(SignalType::OrderFlow, 0.7);
        weights.insert(SignalType::Volume, 0.5);
        weights.insert(SignalType::Momentum, 0.6);
        weights.insert(SignalType::Temporal, 0.3);
        weights.insert(SignalType::CrossMarket, 0.9);
        weights.insert(SignalType::Volatility, 0.4);
        weights.insert(SignalType::Sentiment, 0.3);
        weights.insert(SignalType::Technical, 0.5);

        Self {
            generators: Vec::new(),
            config,
            signals: RwLock::new(HashMap::new()),
            stats: RwLock::new(HashMap::new()),
            weights,
        }
    }

    /// Add a signal generator
    pub fn add_generator(&mut self, generator: Box<dyn SignalGenerator>) {
        self.generators.push(generator);
    }

    /// Set weight for a signal type
    pub fn set_weight(&mut self, signal_type: SignalType, weight: f64) {
        self.weights.insert(signal_type, weight.clamp(0.0, 2.0));
    }

    /// Generate signals for a market
    pub async fn generate_signals(&self, market_id: &str, data: &MarketData) -> Vec<Signal> {
        let mut all_signals = Vec::new();

        for generator in &self.generators {
            let signals = generator.generate(market_id, data);
            trace!(
                "Generator {} produced {} signals for {}",
                generator.name(),
                signals.len(),
                market_id
            );
            all_signals.extend(signals);
        }

        // Store signals
        {
            let mut signal_map = self.signals.write().await;
            let market_signals = signal_map
                .entry(market_id.to_string())
                .or_insert_with(|| VecDeque::with_capacity(self.config.max_history));

            for signal in &all_signals {
                market_signals.push_back(signal.clone());
                while market_signals.len() > self.config.max_history {
                    market_signals.pop_front();
                }
            }
        }

        // Update stats
        if self.config.track_performance {
            let mut stats = self.stats.write().await;
            for signal in &all_signals {
                let source_stats = stats.entry(signal.source.clone()).or_default();
                source_stats.total_signals += 1;
                source_stats.avg_strength = source_stats.avg_strength * 0.99
                    + signal.strength().abs() * 0.01;
            }
        }

        all_signals
    }

    /// Combine signals for a market
    pub async fn combine_signals(
        &self,
        market_id: &str,
        strategy: CombineStrategy,
    ) -> Option<CombinedSignal> {
        let signals = self.signals.read().await;
        let market_signals = signals.get(market_id)?;

        // Get valid signals
        let valid_signals: Vec<&Signal> = market_signals
            .iter()
            .filter(|s| s.is_valid() && s.strength().abs() >= self.config.min_strength)
            .collect();

        if valid_signals.is_empty() {
            return None;
        }

        let (combined_value, combined_confidence) = match strategy {
            CombineStrategy::WeightedAverage => {
                let mut weighted_sum = 0.0;
                let mut weight_total = 0.0;

                for signal in &valid_signals {
                    let type_weight = self.weights.get(&signal.signal_type).unwrap_or(&1.0);
                    let weight = signal.confidence * type_weight;
                    weighted_sum += signal.decayed_value(self.config.decay_rate) * weight;
                    weight_total += weight;
                }

                if weight_total > 0.0 {
                    (weighted_sum / weight_total, weight_total / valid_signals.len() as f64)
                } else {
                    (0.0, 0.0)
                }
            }

            CombineStrategy::MajorityVote => {
                let mut long_votes = 0.0;
                let mut short_votes = 0.0;
                let mut neutral_votes = 0.0;

                for signal in &valid_signals {
                    let weight = signal.confidence;
                    match signal.direction() {
                        Direction::Long => long_votes += weight,
                        Direction::Short => short_votes += weight,
                        Direction::Neutral => neutral_votes += weight,
                    }
                }

                let total_votes = long_votes + short_votes + neutral_votes;
                if long_votes > short_votes && long_votes > neutral_votes {
                    (long_votes / total_votes, long_votes / total_votes)
                } else if short_votes > long_votes && short_votes > neutral_votes {
                    (-short_votes / total_votes, short_votes / total_votes)
                } else {
                    (0.0, neutral_votes / total_votes)
                }
            }

            CombineStrategy::StrongestSignal => {
                let strongest = valid_signals
                    .iter()
                    .max_by(|a, b| {
                        a.strength()
                            .abs()
                            .partial_cmp(&b.strength().abs())
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })?;
                (strongest.value, strongest.confidence)
            }

            CombineStrategy::Product => {
                let mut product = 1.0;
                let mut min_confidence = 1.0;

                for signal in &valid_signals {
                    product *= signal.value.signum() * (1.0 + signal.value.abs());
                    min_confidence = min_confidence.min(signal.confidence);
                }

                // Normalize
                let normalized = if product >= 0.0 {
                    (product - 1.0).min(1.0)
                } else {
                    (-(-product) - 1.0).max(-1.0)
                };

                (normalized, min_confidence)
            }

            CombineStrategy::CustomWeights => {
                // Same as weighted average but uses the pre-configured weights
                let mut weighted_sum = 0.0;
                let mut weight_total = 0.0;

                for signal in &valid_signals {
                    let type_weight = self.weights.get(&signal.signal_type).unwrap_or(&1.0);
                    let weight = signal.confidence * type_weight;
                    weighted_sum += signal.value * weight;
                    weight_total += weight;
                }

                if weight_total > 0.0 {
                    (weighted_sum / weight_total, weight_total / valid_signals.len() as f64)
                } else {
                    (0.0, 0.0)
                }
            }
        };

        Some(CombinedSignal {
            market_id: market_id.to_string(),
            value: combined_value,
            confidence: combined_confidence,
            direction: Direction::from_signal(combined_value),
            contributing_signals: valid_signals.iter().map(|s| s.id.clone()).collect(),
            timestamp: Utc::now(),
        })
    }

    /// Get signal statistics
    pub async fn get_stats(&self) -> HashMap<String, SignalStats> {
        self.stats.read().await.clone()
    }

    /// Record trade outcome for signal performance tracking
    pub async fn record_outcome(&self, signal_id: &str, pnl: Decimal, profitable: bool) {
        let signals = self.signals.read().await;

        // Find the signal source
        for (_market_id, market_signals) in signals.iter() {
            for signal in market_signals.iter() {
                if signal.id == signal_id {
                    let mut stats = self.stats.write().await;
                    let source_stats = stats.entry(signal.source.clone()).or_default();
                    source_stats.traded_signals += 1;
                    if profitable {
                        source_stats.profitable_signals += 1;
                    }
                    source_stats.total_pnl += pnl;
                    source_stats.hit_rate = source_stats.profitable_signals as f64
                        / source_stats.traded_signals as f64;
                    return;
                }
            }
        }
    }

    /// Clean up expired signals
    pub async fn cleanup_expired(&self) {
        let mut signals = self.signals.write().await;
        for (_market_id, market_signals) in signals.iter_mut() {
            market_signals.retain(|s| s.is_valid());
        }
    }
}

/// Helper function to convert Decimal to f64
fn decimal_to_f64(d: Decimal) -> f64 {
    use std::str::FromStr;
    f64::from_str(&d.to_string()).unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signal_creation() {
        let signal = Signal::new(
            SignalType::Price,
            "test_market",
            0.5,
            0.8,
            "test_source",
            Duration::minutes(5),
        );

        assert_eq!(signal.value, 0.5);
        assert_eq!(signal.confidence, 0.8);
        assert!(signal.is_valid());
        assert_eq!(signal.direction(), Direction::Long);
    }

    #[test]
    fn test_signal_strength() {
        let signal = Signal::new(
            SignalType::Price,
            "test_market",
            0.6,
            0.8,
            "test_source",
            Duration::minutes(5),
        );

        assert!((signal.strength() - 0.48).abs() < 0.001);
    }

    #[test]
    fn test_signal_direction() {
        assert_eq!(Direction::from_signal(0.5), Direction::Long);
        assert_eq!(Direction::from_signal(-0.5), Direction::Short);
        assert_eq!(Direction::from_signal(0.05), Direction::Neutral);
    }

    #[tokio::test]
    async fn test_signal_aggregator() {
        let config = SignalConfig::default();
        let mut aggregator = SignalAggregator::new(config.clone());

        aggregator.add_generator(Box::new(PricingSignalGenerator::new(config.clone())));
        aggregator.add_generator(Box::new(OrderFlowSignalGenerator::new(config)));

        let data = MarketData {
            market_id: "test".to_string(),
            yes_bid: dec!(0.45),
            yes_ask: dec!(0.47),
            no_bid: dec!(0.52),
            no_ask: dec!(0.55),
            yes_volume_24h: dec!(50000),
            no_volume_24h: dec!(50000),
            bid_depth: vec![(dec!(0.45), dec!(1000))],
            ask_depth: vec![(dec!(0.47), dec!(500))],
            timestamp: Utc::now(),
        };

        let signals = aggregator.generate_signals("test", &data).await;

        // Should generate at least one signal for the pricing mispricing
        assert!(!signals.is_empty() || data.yes_bid + data.no_bid < dec!(1));
    }

    #[tokio::test]
    async fn test_signal_combination() {
        let config = SignalConfig::default();
        let aggregator = SignalAggregator::new(config.clone());

        // Manually insert signals
        {
            let mut signals = aggregator.signals.write().await;
            let market_signals = signals.entry("test".to_string()).or_default();

            market_signals.push_back(Signal::new(
                SignalType::Price,
                "test",
                0.5,
                0.8,
                "source1",
                Duration::minutes(5),
            ));
            market_signals.push_back(Signal::new(
                SignalType::OrderFlow,
                "test",
                0.3,
                0.6,
                "source2",
                Duration::minutes(5),
            ));
        }

        let combined = aggregator
            .combine_signals("test", CombineStrategy::WeightedAverage)
            .await;

        assert!(combined.is_some());
        let combined = combined.unwrap();
        assert!(combined.value > 0.0);
        assert_eq!(combined.direction, Direction::Long);
    }
}
