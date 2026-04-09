//! Liquidity and market microstructure analysis.
//!
//! Provides advanced analytics for:
//! - Market impact estimation
//! - Liquidity scoring
//! - Order flow analysis
//! - Volatility measurement

use crate::core::types::*;
use crate::orderbook::engine::OrderbookEngine;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::VecDeque;
use std::sync::Arc;
use chrono::{DateTime, Utc};

/// Analyzes market liquidity and microstructure
pub struct LiquidityAnalyzer {
    engine: Arc<OrderbookEngine>,
    /// Historical mid prices for volatility calculation
    price_history: parking_lot::RwLock<std::collections::HashMap<String, VecDeque<PriceSnapshot>>>,
    /// Maximum history length
    max_history_len: usize,
}

impl LiquidityAnalyzer {
    /// Create a new liquidity analyzer
    pub fn new(engine: Arc<OrderbookEngine>, max_history_len: usize) -> Self {
        Self {
            engine,
            price_history: parking_lot::RwLock::new(std::collections::HashMap::new()),
            max_history_len,
        }
    }

    /// Record a price snapshot for volatility analysis
    pub fn record_price(&self, token_id: &str, price: Price) {
        let mut history = self.price_history.write();
        let snapshots = history
            .entry(token_id.to_string())
            .or_insert_with(VecDeque::new);

        snapshots.push_back(PriceSnapshot {
            price,
            timestamp: Utc::now(),
        });

        // Trim old data
        while snapshots.len() > self.max_history_len {
            snapshots.pop_front();
        }
    }

    /// Calculate market impact for a given order size
    pub fn estimate_market_impact(
        &self,
        token_id: &str,
        side: Side,
        quantity: Quantity,
    ) -> Option<MarketImpact> {
        let ob = self.engine.get(token_id)?;
        let book = ob.read();

        let mid = book.mid_price()?;
        let vwap = book.vwap(side, quantity)?;

        let impact = match side {
            Side::Buy => vwap.0 - mid.0,
            Side::Sell => mid.0 - vwap.0,
        };

        let impact_bps = if mid.0 > Decimal::ZERO {
            (impact / mid.0) * dec!(10000)
        } else {
            Decimal::ZERO
        };

        Some(MarketImpact {
            quantity,
            side,
            mid_price: mid,
            vwap,
            impact,
            impact_bps,
            fill_probability: self.estimate_fill_probability(token_id, side, vwap),
        })
    }

    /// Estimate probability of order being filled
    fn estimate_fill_probability(&self, token_id: &str, side: Side, price: Price) -> f64 {
        // Simple estimation based on spread position
        if let Some((bid, ask)) = self.engine.get_bbo(token_id) {
            match (bid, ask, side) {
                (Some(bid), Some(ask), Side::Buy) => {
                    // Buy order: probability based on how aggressive the price is
                    if price.0 >= ask.0 {
                        0.95 // Marketable order
                    } else if price.0 >= bid.0 {
                        let spread = ask.0 - bid.0;
                        let position = price.0 - bid.0;
                        (0.3 + 0.5 * (position / spread).to_f64().unwrap_or(0.0)).min(0.9)
                    } else {
                        0.1 // Below bid
                    }
                }
                (Some(bid), Some(ask), Side::Sell) => {
                    // Sell order: probability based on how aggressive the price is
                    if price.0 <= bid.0 {
                        0.95 // Marketable order
                    } else if price.0 <= ask.0 {
                        let spread = ask.0 - bid.0;
                        let position = ask.0 - price.0;
                        (0.3 + 0.5 * (position / spread).to_f64().unwrap_or(0.0)).min(0.9)
                    } else {
                        0.1 // Above ask
                    }
                }
                _ => 0.5,
            }
        } else {
            0.5
        }
    }

    /// Calculate liquidity score for a market
    pub fn calculate_liquidity_score(&self, token_id: &str) -> Option<LiquidityScore> {
        let ob = self.engine.get(token_id)?;
        let book = ob.read();

        let (bid_liq, ask_liq) = book.total_liquidity();
        let spread = book.spread()?;
        let spread_bps = book.spread_bps()?;

        // Depth score: higher is better (log scale)
        let depth_score = (bid_liq + ask_liq + Decimal::ONE).ln() / dec!(10);

        // Spread score: lower spread is better
        let spread_score = if spread_bps > Decimal::ZERO {
            (dec!(100) / spread_bps).min(dec!(10))
        } else {
            dec!(10)
        };

        // Imbalance penalty
        let total_liq = bid_liq + ask_liq;
        let imbalance = if total_liq > Decimal::ZERO {
            ((bid_liq - ask_liq).abs() / total_liq).to_f64().unwrap_or(1.0)
        } else {
            1.0
        };
        let balance_score = Decimal::from_f64_retain(1.0 - imbalance * 0.5).unwrap_or(dec!(0.5));

        // Combined score (0-100)
        let total_score = (depth_score * dec!(30)
            + spread_score * dec!(50)
            + balance_score * dec!(20))
            .min(dec!(100))
            .max(Decimal::ZERO);

        Some(LiquidityScore {
            total_score,
            depth_score,
            spread_score,
            balance_score,
            bid_liquidity: bid_liq,
            ask_liquidity: ask_liq,
            spread_bps,
        })
    }

    /// Calculate realized volatility
    pub fn calculate_volatility(&self, token_id: &str, window_size: usize) -> Option<f64> {
        let history = self.price_history.read();
        let snapshots = history.get(token_id)?;

        if snapshots.len() < window_size.min(2) {
            return None;
        }

        let prices: Vec<f64> = snapshots
            .iter()
            .rev()
            .take(window_size)
            .filter_map(|s| s.price.0.to_f64())
            .collect();

        if prices.len() < 2 {
            return None;
        }

        // Calculate log returns
        let returns: Vec<f64> = prices
            .windows(2)
            .filter_map(|w| {
                if w[1] > 0.0 {
                    Some((w[0] / w[1]).ln())
                } else {
                    None
                }
            })
            .collect();

        if returns.is_empty() {
            return None;
        }

        // Calculate standard deviation of returns
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;

        Some(variance.sqrt())
    }

    /// Get order flow imbalance
    pub fn get_order_flow_imbalance(&self, token_id: &str) -> Option<OrderFlowImbalance> {
        let ob = self.engine.get(token_id)?;
        let book = ob.read();

        let (bids, asks) = book.top_levels(5);

        let bid_volume: Decimal = bids.iter().map(|l| l.quantity.0).sum();
        let ask_volume: Decimal = asks.iter().map(|l| l.quantity.0).sum();
        let total_volume = bid_volume + ask_volume;

        if total_volume <= Decimal::ZERO {
            return None;
        }

        let imbalance = (bid_volume - ask_volume) / total_volume;

        // Pressure indicator: positive = buy pressure, negative = sell pressure
        let pressure = if bid_volume > ask_volume {
            OrderFlowPressure::Buying
        } else if ask_volume > bid_volume {
            OrderFlowPressure::Selling
        } else {
            OrderFlowPressure::Neutral
        };

        Some(OrderFlowImbalance {
            bid_volume,
            ask_volume,
            imbalance,
            pressure,
            pressure_strength: imbalance.abs().to_f64().unwrap_or(0.0),
        })
    }

    /// Analyze spread dynamics
    pub fn analyze_spread(&self, token_id: &str) -> Option<SpreadAnalysis> {
        let ob = self.engine.get(token_id)?;
        let book = ob.read();

        let spread = book.spread()?;
        let spread_bps = book.spread_bps()?;
        let mid = book.mid_price()?;

        let best_bid = book.best_bid_level()?;
        let best_ask = book.best_ask_level()?;

        // Estimate "true" spread considering depth
        let depth_adjusted_spread = if best_bid.quantity.0 > dec!(10)
            && best_ask.quantity.0 > dec!(10)
        {
            spread * dec!(0.8) // Tighter effective spread with good depth
        } else {
            spread * dec!(1.2) // Wider effective spread with thin depth
        };

        Some(SpreadAnalysis {
            quoted_spread: spread,
            quoted_spread_bps: spread_bps,
            mid_price: mid,
            depth_adjusted_spread,
            bid_depth: best_bid.quantity,
            ask_depth: best_ask.quantity,
            is_tight: spread_bps < dec!(100), // Less than 1%
            is_deep: best_bid.quantity.0 > dec!(100) && best_ask.quantity.0 > dec!(100),
        })
    }
}

/// Price snapshot for history
#[derive(Debug, Clone)]
struct PriceSnapshot {
    price: Price,
    timestamp: DateTime<Utc>,
}

/// Market impact estimation
#[derive(Debug, Clone)]
pub struct MarketImpact {
    pub quantity: Quantity,
    pub side: Side,
    pub mid_price: Price,
    pub vwap: Price,
    pub impact: Decimal,
    pub impact_bps: Decimal,
    pub fill_probability: f64,
}

impl MarketImpact {
    /// Check if impact is acceptable
    pub fn is_acceptable(&self, max_impact_bps: Decimal) -> bool {
        self.impact_bps <= max_impact_bps
    }
}

/// Liquidity score for a market
#[derive(Debug, Clone)]
pub struct LiquidityScore {
    pub total_score: Decimal,
    pub depth_score: Decimal,
    pub spread_score: Decimal,
    pub balance_score: Decimal,
    pub bid_liquidity: Decimal,
    pub ask_liquidity: Decimal,
    pub spread_bps: Decimal,
}

impl LiquidityScore {
    /// Get liquidity tier
    pub fn tier(&self) -> LiquidityTier {
        if self.total_score >= dec!(80) {
            LiquidityTier::Excellent
        } else if self.total_score >= dec!(60) {
            LiquidityTier::Good
        } else if self.total_score >= dec!(40) {
            LiquidityTier::Fair
        } else if self.total_score >= dec!(20) {
            LiquidityTier::Poor
        } else {
            LiquidityTier::VeryPoor
        }
    }
}

/// Liquidity tier classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiquidityTier {
    Excellent,
    Good,
    Fair,
    Poor,
    VeryPoor,
}

/// Order flow imbalance analysis
#[derive(Debug, Clone)]
pub struct OrderFlowImbalance {
    pub bid_volume: Decimal,
    pub ask_volume: Decimal,
    pub imbalance: Decimal,
    pub pressure: OrderFlowPressure,
    pub pressure_strength: f64,
}

/// Order flow pressure direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderFlowPressure {
    Buying,
    Selling,
    Neutral,
}

/// Spread analysis results
#[derive(Debug, Clone)]
pub struct SpreadAnalysis {
    pub quoted_spread: Decimal,
    pub quoted_spread_bps: Decimal,
    pub mid_price: Price,
    pub depth_adjusted_spread: Decimal,
    pub bid_depth: Quantity,
    pub ask_depth: Quantity,
    pub is_tight: bool,
    pub is_deep: bool,
}

impl SpreadAnalysis {
    /// Check if market is suitable for trading
    pub fn is_tradeable(&self) -> bool {
        self.is_tight || self.is_deep
    }
}

impl std::fmt::Debug for LiquidityAnalyzer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiquidityAnalyzer")
            .field("max_history_len", &self.max_history_len)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::events::EventBus;

    fn setup_test_engine() -> Arc<OrderbookEngine> {
        let event_bus = EventBus::new(100);
        let engine = Arc::new(OrderbookEngine::new(event_bus, 5000));

        let mut ob = Orderbook::new("test_token".to_string(), Outcome::Yes);

        // Add multiple bid levels
        for i in 0..5 {
            ob.bids.levels.push(PriceLevel::new(
                Price::new(dec!(0.50) - Decimal::from(i) * dec!(0.01)).unwrap(),
                Quantity(dec!(100)),
                1,
            ));
        }

        // Add multiple ask levels
        for i in 0..5 {
            ob.asks.levels.push(PriceLevel::new(
                Price::new(dec!(0.52) + Decimal::from(i) * dec!(0.01)).unwrap(),
                Quantity(dec!(100)),
                1,
            ));
        }

        engine.update_snapshot(ob).unwrap();
        engine
    }

    #[test]
    fn test_market_impact() {
        let engine = setup_test_engine();
        let analyzer = LiquidityAnalyzer::new(engine, 100);

        let impact = analyzer
            .estimate_market_impact("test_token", Side::Buy, Quantity(dec!(50)))
            .unwrap();

        assert!(impact.vwap.0 >= impact.mid_price.0);
        assert!(impact.impact_bps >= Decimal::ZERO);
    }

    #[test]
    fn test_liquidity_score() {
        let engine = setup_test_engine();
        let analyzer = LiquidityAnalyzer::new(engine, 100);

        let score = analyzer.calculate_liquidity_score("test_token").unwrap();

        assert!(score.total_score >= Decimal::ZERO);
        assert!(score.total_score <= dec!(100));
        assert!(score.bid_liquidity > Decimal::ZERO);
        assert!(score.ask_liquidity > Decimal::ZERO);
    }

    #[test]
    fn test_order_flow_imbalance() {
        let engine = setup_test_engine();
        let analyzer = LiquidityAnalyzer::new(engine, 100);

        let imbalance = analyzer.get_order_flow_imbalance("test_token").unwrap();

        // Both sides have equal volume, so imbalance should be near zero
        assert!(imbalance.imbalance.abs() < dec!(0.1));
        assert_eq!(imbalance.pressure, OrderFlowPressure::Neutral);
    }

    #[test]
    fn test_spread_analysis() {
        let engine = setup_test_engine();
        let analyzer = LiquidityAnalyzer::new(engine, 100);

        let analysis = analyzer.analyze_spread("test_token").unwrap();

        assert_eq!(analysis.quoted_spread, dec!(0.02));
        assert!(analysis.is_tradeable());
    }
}
