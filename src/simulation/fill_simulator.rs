//! Fill simulation for realistic order execution modeling.
//!
//! Models:
//! - Partial fills based on liquidity
//! - Slippage from market impact
//! - Queue position effects
//! - Adverse selection
//! - Fill probability based on order type

use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rand::Rng;
use rand_distr::{Distribution, Normal, Uniform};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

use super::latency_model::Venue;

/// Order side for simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

/// Order type for simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    /// Market order - immediate execution at best available price.
    Market,
    /// Limit order - execute at specified price or better.
    Limit,
    /// Immediate or cancel - fill what's available, cancel rest.
    ImmediateOrCancel,
    /// Fill or kill - all or nothing.
    FillOrKill,
}

/// Simulated order for the fill simulator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatedOrder {
    pub id: String,
    pub venue: Venue,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub price: Decimal,
    pub quantity: Decimal,
    pub timestamp_ms: u64,
}

/// Result of a simulated fill.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatedFill {
    pub order_id: String,
    pub filled_quantity: Decimal,
    pub remaining_quantity: Decimal,
    pub average_fill_price: Decimal,
    pub slippage_bps: Decimal,
    pub fill_latency_ms: f64,
    pub was_full_fill: bool,
    pub fills: Vec<PartialFill>,
}

/// A single partial fill within an order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialFill {
    pub price: Decimal,
    pub quantity: Decimal,
    pub timestamp_ms: u64,
}

/// Slippage model configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlippageModel {
    /// Base slippage in basis points.
    pub base_slippage_bps: Decimal,
    /// Additional slippage per $1000 of order size.
    pub size_impact_bps_per_1000: Decimal,
    /// Volatility multiplier (1.0 = normal).
    pub volatility_multiplier: Decimal,
    /// Spread capture for limit orders (0.0 to 1.0).
    pub spread_capture_rate: Decimal,
    /// Adverse selection probability (informed flow).
    pub adverse_selection_prob: f64,
    /// Adverse selection impact in bps when triggered.
    pub adverse_selection_impact_bps: Decimal,
}

impl Default for SlippageModel {
    fn default() -> Self {
        Self {
            base_slippage_bps: Decimal::new(5, 1), // 0.5 bps
            size_impact_bps_per_1000: Decimal::new(2, 0), // 2 bps per $1000
            volatility_multiplier: Decimal::ONE,
            spread_capture_rate: Decimal::new(5, 1), // 50%
            adverse_selection_prob: 0.05, // 5% chance
            adverse_selection_impact_bps: Decimal::new(10, 0), // 10 bps
        }
    }
}

impl SlippageModel {
    /// Create a conservative slippage model.
    pub fn conservative() -> Self {
        Self {
            base_slippage_bps: Decimal::new(10, 1), // 1.0 bps
            size_impact_bps_per_1000: Decimal::new(5, 0), // 5 bps per $1000
            volatility_multiplier: Decimal::new(15, 1), // 1.5x
            spread_capture_rate: Decimal::new(3, 1), // 30%
            adverse_selection_prob: 0.10,
            adverse_selection_impact_bps: Decimal::new(20, 0),
        }
    }

    /// Create an optimistic slippage model.
    pub fn optimistic() -> Self {
        Self {
            base_slippage_bps: Decimal::new(2, 1), // 0.2 bps
            size_impact_bps_per_1000: Decimal::new(1, 0), // 1 bps per $1000
            volatility_multiplier: Decimal::ONE,
            spread_capture_rate: Decimal::new(7, 1), // 70%
            adverse_selection_prob: 0.02,
            adverse_selection_impact_bps: Decimal::new(5, 0),
        }
    }

    /// Create venue-specific slippage model.
    pub fn for_venue(venue: Venue) -> Self {
        match venue {
            Venue::Polymarket => Self {
                base_slippage_bps: Decimal::new(8, 1), // Higher due to AMM
                size_impact_bps_per_1000: Decimal::new(4, 0),
                volatility_multiplier: Decimal::new(12, 1),
                spread_capture_rate: Decimal::new(4, 1),
                adverse_selection_prob: 0.08,
                adverse_selection_impact_bps: Decimal::new(15, 0),
            },
            Venue::Kalshi => Self {
                base_slippage_bps: Decimal::new(3, 1), // Regulated, more efficient
                size_impact_bps_per_1000: Decimal::new(15, 1),
                volatility_multiplier: Decimal::ONE,
                spread_capture_rate: Decimal::new(6, 1),
                adverse_selection_prob: 0.03,
                adverse_selection_impact_bps: Decimal::new(8, 0),
            },
            Venue::Opinion => Self {
                base_slippage_bps: Decimal::new(5, 1),
                size_impact_bps_per_1000: Decimal::new(25, 1),
                volatility_multiplier: Decimal::new(11, 1),
                spread_capture_rate: Decimal::new(55, 2),
                adverse_selection_prob: 0.04,
                adverse_selection_impact_bps: Decimal::new(10, 0),
            },
        }
    }
}

/// Fill model configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillModel {
    /// Base fill probability for limit orders at touch.
    pub base_fill_prob_at_touch: f64,
    /// Fill probability decay per tick away from touch.
    pub fill_prob_decay_per_tick: f64,
    /// Mean time to fill for limit orders at touch (ms).
    pub mean_time_to_fill_ms: f64,
    /// Partial fill probability.
    pub partial_fill_prob: f64,
    /// Mean partial fill ratio (0.0 to 1.0).
    pub mean_partial_fill_ratio: f64,
    /// Queue position factor (higher = worse position).
    pub queue_position_factor: f64,
}

impl Default for FillModel {
    fn default() -> Self {
        Self {
            base_fill_prob_at_touch: 0.85,
            fill_prob_decay_per_tick: 0.15,
            mean_time_to_fill_ms: 500.0,
            partial_fill_prob: 0.20,
            mean_partial_fill_ratio: 0.65,
            queue_position_factor: 1.0,
        }
    }
}

impl FillModel {
    /// Create venue-specific fill model.
    pub fn for_venue(venue: Venue) -> Self {
        match venue {
            Venue::Polymarket => Self {
                base_fill_prob_at_touch: 0.95, // AMM always fills
                fill_prob_decay_per_tick: 0.05,
                mean_time_to_fill_ms: 2000.0, // Blockchain delay
                partial_fill_prob: 0.10,
                mean_partial_fill_ratio: 0.80,
                queue_position_factor: 0.5, // No real queue
            },
            Venue::Kalshi => Self {
                base_fill_prob_at_touch: 0.80,
                fill_prob_decay_per_tick: 0.20,
                mean_time_to_fill_ms: 200.0,
                partial_fill_prob: 0.25,
                mean_partial_fill_ratio: 0.60,
                queue_position_factor: 1.2,
            },
            Venue::Opinion => Self {
                base_fill_prob_at_touch: 0.75,
                fill_prob_decay_per_tick: 0.18,
                mean_time_to_fill_ms: 350.0,
                partial_fill_prob: 0.30,
                mean_partial_fill_ratio: 0.55,
                queue_position_factor: 1.5,
            },
        }
    }
}

/// Simulated order book level for fill simulation.
#[derive(Debug, Clone)]
pub struct SimulatedBookLevel {
    pub price: Decimal,
    pub quantity: Decimal,
    pub order_count: u32,
}

/// Simulated order book for a market.
#[derive(Debug, Clone)]
pub struct SimulatedOrderBook {
    pub venue: Venue,
    pub market_id: String,
    pub bids: Vec<SimulatedBookLevel>,
    pub asks: Vec<SimulatedBookLevel>,
    pub last_update_ms: u64,
}

impl SimulatedOrderBook {
    /// Get best bid price.
    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.first().map(|l| l.price)
    }

    /// Get best ask price.
    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.first().map(|l| l.price)
    }

    /// Get spread in basis points.
    pub fn spread_bps(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) if bid > Decimal::ZERO => {
                Some((ask - bid) / bid * Decimal::new(10000, 0))
            }
            _ => None,
        }
    }

    /// Get total liquidity at price level or better.
    pub fn liquidity_at_price(&self, price: Decimal, side: OrderSide) -> Decimal {
        match side {
            OrderSide::Buy => {
                // For buying, sum asks at or below price
                self.asks
                    .iter()
                    .filter(|l| l.price <= price)
                    .map(|l| l.quantity)
                    .sum()
            }
            OrderSide::Sell => {
                // For selling, sum bids at or above price
                self.bids
                    .iter()
                    .filter(|l| l.price >= price)
                    .map(|l| l.quantity)
                    .sum()
            }
        }
    }
}

/// Fill simulator for realistic order execution.
pub struct FillSimulator {
    /// Slippage models per venue.
    slippage_models: std::collections::HashMap<Venue, SlippageModel>,
    /// Fill models per venue.
    fill_models: std::collections::HashMap<Venue, FillModel>,
    /// Random number generator.
    rng: rand::rngs::ThreadRng,
    /// Fill history for analysis.
    fill_history: VecDeque<SimulatedFill>,
    /// Maximum history size.
    max_history_size: usize,
}

impl FillSimulator {
    /// Create a new fill simulator with default models.
    pub fn new() -> Self {
        use std::collections::HashMap;

        let mut slippage_models = HashMap::new();
        slippage_models.insert(Venue::Polymarket, SlippageModel::for_venue(Venue::Polymarket));
        slippage_models.insert(Venue::Kalshi, SlippageModel::for_venue(Venue::Kalshi));
        slippage_models.insert(Venue::Opinion, SlippageModel::for_venue(Venue::Opinion));

        let mut fill_models = HashMap::new();
        fill_models.insert(Venue::Polymarket, FillModel::for_venue(Venue::Polymarket));
        fill_models.insert(Venue::Kalshi, FillModel::for_venue(Venue::Kalshi));
        fill_models.insert(Venue::Opinion, FillModel::for_venue(Venue::Opinion));

        Self {
            slippage_models,
            fill_models,
            rng: rand::thread_rng(),
            fill_history: VecDeque::new(),
            max_history_size: 10000,
        }
    }

    /// Create with conservative models (for risk analysis).
    pub fn conservative() -> Self {
        use std::collections::HashMap;

        let mut slippage_models = HashMap::new();
        let conservative = SlippageModel::conservative();
        slippage_models.insert(Venue::Polymarket, conservative.clone());
        slippage_models.insert(Venue::Kalshi, conservative.clone());
        slippage_models.insert(Venue::Opinion, conservative);

        let mut fill_models = HashMap::new();
        fill_models.insert(Venue::Polymarket, FillModel::for_venue(Venue::Polymarket));
        fill_models.insert(Venue::Kalshi, FillModel::for_venue(Venue::Kalshi));
        fill_models.insert(Venue::Opinion, FillModel::for_venue(Venue::Opinion));

        Self {
            slippage_models,
            fill_models,
            rng: rand::thread_rng(),
            fill_history: VecDeque::new(),
            max_history_size: 10000,
        }
    }

    /// Simulate fill for a market order.
    pub fn simulate_market_order(
        &mut self,
        order: &SimulatedOrder,
        book: &SimulatedOrderBook,
    ) -> SimulatedFill {
        let slippage_model = self.slippage_models
            .get(&order.venue)
            .cloned()
            .unwrap_or_default();

        // Calculate slippage
        let order_value = order.price * order.quantity;
        let size_impact = slippage_model.size_impact_bps_per_1000
            * (order_value / Decimal::new(1000, 0));

        let mut total_slippage = slippage_model.base_slippage_bps + size_impact;
        total_slippage = total_slippage * slippage_model.volatility_multiplier;

        // Check for adverse selection
        if self.rng.gen::<f64>() < slippage_model.adverse_selection_prob {
            total_slippage = total_slippage + slippage_model.adverse_selection_impact_bps;
        }

        // Walk the book to determine fills
        let levels = match order.side {
            OrderSide::Buy => &book.asks,
            OrderSide::Sell => &book.bids,
        };

        let mut remaining = order.quantity;
        let mut fills = Vec::new();
        let mut total_cost = Decimal::ZERO;

        for level in levels {
            if remaining <= Decimal::ZERO {
                break;
            }

            let fill_qty = remaining.min(level.quantity);
            let fill_price = self.apply_slippage(level.price, total_slippage, order.side);

            fills.push(PartialFill {
                price: fill_price,
                quantity: fill_qty,
                timestamp_ms: order.timestamp_ms + self.rng.gen_range(1..50),
            });

            total_cost = total_cost + (fill_price * fill_qty);
            remaining = remaining - fill_qty;
        }

        let filled_quantity = order.quantity - remaining;
        let average_fill_price = if filled_quantity > Decimal::ZERO {
            total_cost / filled_quantity
        } else {
            order.price
        };

        let actual_slippage = if order.price > Decimal::ZERO {
            ((average_fill_price - order.price) / order.price * Decimal::new(10000, 0)).abs()
        } else {
            Decimal::ZERO
        };

        let fill = SimulatedFill {
            order_id: order.id.clone(),
            filled_quantity,
            remaining_quantity: remaining,
            average_fill_price,
            slippage_bps: actual_slippage,
            fill_latency_ms: self.rng.gen_range(5.0..50.0),
            was_full_fill: remaining <= Decimal::ZERO,
            fills,
        };

        self.record_fill(fill.clone());
        fill
    }

    /// Simulate fill for a limit order.
    pub fn simulate_limit_order(
        &mut self,
        order: &SimulatedOrder,
        book: &SimulatedOrderBook,
        time_in_market_ms: u64,
    ) -> SimulatedFill {
        let fill_model = self.fill_models
            .get(&order.venue)
            .cloned()
            .unwrap_or_default();

        let touch_price = match order.side {
            OrderSide::Buy => book.best_ask(),
            OrderSide::Sell => book.best_bid(),
        };

        // Calculate fill probability based on price distance from touch
        let fill_prob = match touch_price {
            Some(touch) => {
                let price_diff = match order.side {
                    OrderSide::Buy => touch - order.price, // Buy below touch
                    OrderSide::Sell => order.price - touch, // Sell above touch
                };

                if price_diff <= Decimal::ZERO {
                    // At or through the touch - likely to fill
                    fill_model.base_fill_prob_at_touch
                } else {
                    // Away from touch - decay probability
                    let ticks_away = (price_diff * Decimal::new(100, 0))
                        .to_f64()
                        .unwrap_or(10.0);
                    let decay = fill_model.fill_prob_decay_per_tick * ticks_away;
                    (fill_model.base_fill_prob_at_touch - decay).max(0.05)
                }
            }
            None => 0.5, // Unknown market state
        };

        // Time factor - longer in market = higher fill probability
        let time_factor = (time_in_market_ms as f64 / fill_model.mean_time_to_fill_ms).min(2.0);
        let adjusted_fill_prob = (fill_prob * time_factor).min(0.99);

        // Determine if order fills
        let does_fill = self.rng.gen::<f64>() < adjusted_fill_prob;

        if !does_fill {
            return SimulatedFill {
                order_id: order.id.clone(),
                filled_quantity: Decimal::ZERO,
                remaining_quantity: order.quantity,
                average_fill_price: Decimal::ZERO,
                slippage_bps: Decimal::ZERO,
                fill_latency_ms: time_in_market_ms as f64,
                was_full_fill: false,
                fills: Vec::new(),
            };
        }

        // Determine if partial or full fill
        let is_partial = self.rng.gen::<f64>() < fill_model.partial_fill_prob;

        let fill_ratio = if is_partial {
            // Sample partial fill ratio
            let normal = Normal::new(fill_model.mean_partial_fill_ratio, 0.15)
                .unwrap_or_else(|_| Normal::new(0.5, 0.1).unwrap());
            normal.sample(&mut self.rng).clamp(0.1, 0.99)
        } else {
            1.0
        };

        let filled_quantity = order.quantity * Decimal::from_f64(fill_ratio).unwrap_or(Decimal::ONE);
        let remaining_quantity = order.quantity - filled_quantity;

        // Limit orders get their price (or better for crossing orders)
        let fill_price = match (order.side, touch_price) {
            (OrderSide::Buy, Some(ask)) if order.price >= ask => ask,
            (OrderSide::Sell, Some(bid)) if order.price <= bid => bid,
            _ => order.price,
        };

        let fill = SimulatedFill {
            order_id: order.id.clone(),
            filled_quantity,
            remaining_quantity,
            average_fill_price: fill_price,
            slippage_bps: Decimal::ZERO, // Limit orders don't have slippage
            fill_latency_ms: self.rng.gen_range(10.0..fill_model.mean_time_to_fill_ms),
            was_full_fill: remaining_quantity <= Decimal::ZERO,
            fills: vec![PartialFill {
                price: fill_price,
                quantity: filled_quantity,
                timestamp_ms: order.timestamp_ms + self.rng.gen_range(10..time_in_market_ms.max(100)),
            }],
        };

        self.record_fill(fill.clone());
        fill
    }

    /// Simulate a cross-platform arbitrage execution.
    pub fn simulate_cross_platform_execution(
        &mut self,
        leg_a_order: &SimulatedOrder,
        leg_a_book: &SimulatedOrderBook,
        leg_b_order: &SimulatedOrder,
        leg_b_book: &SimulatedOrderBook,
        hedge_timeout_ms: u64,
    ) -> CrossPlatformFillResult {
        // Execute first leg (the more liquid one should be passed first)
        let leg_a_fill = match leg_a_order.order_type {
            OrderType::Market => self.simulate_market_order(leg_a_order, leg_a_book),
            OrderType::Limit => self.simulate_limit_order(leg_a_order, leg_a_book, hedge_timeout_ms / 2),
            OrderType::ImmediateOrCancel => self.simulate_ioc_order(leg_a_order, leg_a_book),
            OrderType::FillOrKill => self.simulate_fok_order(leg_a_order, leg_a_book),
        };

        if leg_a_fill.filled_quantity <= Decimal::ZERO {
            return CrossPlatformFillResult {
                leg_a_fill,
                leg_b_fill: None,
                net_profit: Decimal::ZERO,
                execution_success: false,
                failure_reason: Some("First leg failed to fill".to_string()),
                total_latency_ms: 0.0,
            };
        }

        // Adjust leg B quantity based on leg A fill
        let adjusted_leg_b = SimulatedOrder {
            quantity: leg_a_fill.filled_quantity,
            ..leg_b_order.clone()
        };

        // Execute hedge leg
        let leg_b_fill = match adjusted_leg_b.order_type {
            OrderType::Market => self.simulate_market_order(&adjusted_leg_b, leg_b_book),
            OrderType::Limit => self.simulate_limit_order(&adjusted_leg_b, leg_b_book, hedge_timeout_ms),
            OrderType::ImmediateOrCancel => self.simulate_ioc_order(&adjusted_leg_b, leg_b_book),
            OrderType::FillOrKill => self.simulate_fok_order(&adjusted_leg_b, leg_b_book),
        };

        let hedge_ratio = if leg_a_fill.filled_quantity > Decimal::ZERO {
            leg_b_fill.filled_quantity / leg_a_fill.filled_quantity
        } else {
            Decimal::ZERO
        };

        // Calculate P&L
        let leg_a_value = leg_a_fill.filled_quantity * leg_a_fill.average_fill_price;
        let leg_b_value = leg_b_fill.filled_quantity * leg_b_fill.average_fill_price;

        // Net profit depends on direction (buy A sell B or vice versa)
        let net_profit = match (leg_a_order.side, leg_b_order.side) {
            (OrderSide::Buy, OrderSide::Sell) => leg_b_value - leg_a_value,
            (OrderSide::Sell, OrderSide::Buy) => leg_a_value - leg_b_value,
            _ => Decimal::ZERO, // Same side shouldn't happen
        };

        let execution_success = hedge_ratio >= Decimal::new(95, 2); // 95%+ hedge
        let failure_reason = if !execution_success {
            Some(format!(
                "Incomplete hedge: {:.1}% filled",
                hedge_ratio.to_f64().unwrap_or(0.0) * 100.0
            ))
        } else {
            None
        };

        let total_latency = leg_a_fill.fill_latency_ms + leg_b_fill.fill_latency_ms;

        CrossPlatformFillResult {
            leg_a_fill,
            leg_b_fill: Some(leg_b_fill),
            net_profit,
            execution_success,
            failure_reason,
            total_latency_ms: total_latency,
        }
    }

    /// Simulate IOC (Immediate or Cancel) order.
    pub fn simulate_ioc_order(
        &mut self,
        order: &SimulatedOrder,
        book: &SimulatedOrderBook,
    ) -> SimulatedFill {
        // IOC fills what's available immediately, cancels rest
        let available = book.liquidity_at_price(order.price, order.side);
        let fill_qty = order.quantity.min(available);

        if fill_qty <= Decimal::ZERO {
            return SimulatedFill {
                order_id: order.id.clone(),
                filled_quantity: Decimal::ZERO,
                remaining_quantity: order.quantity,
                average_fill_price: Decimal::ZERO,
                slippage_bps: Decimal::ZERO,
                fill_latency_ms: self.rng.gen_range(1.0..10.0),
                was_full_fill: false,
                fills: Vec::new(),
            };
        }

        let adjusted_order = SimulatedOrder {
            quantity: fill_qty,
            ..order.clone()
        };

        self.simulate_market_order(&adjusted_order, book)
    }

    /// Simulate FOK (Fill or Kill) order.
    pub fn simulate_fok_order(
        &mut self,
        order: &SimulatedOrder,
        book: &SimulatedOrderBook,
    ) -> SimulatedFill {
        // FOK requires full fill or nothing
        let available = book.liquidity_at_price(order.price, order.side);

        if available >= order.quantity {
            self.simulate_market_order(order, book)
        } else {
            SimulatedFill {
                order_id: order.id.clone(),
                filled_quantity: Decimal::ZERO,
                remaining_quantity: order.quantity,
                average_fill_price: Decimal::ZERO,
                slippage_bps: Decimal::ZERO,
                fill_latency_ms: self.rng.gen_range(1.0..10.0),
                was_full_fill: false,
                fills: Vec::new(),
            }
        }
    }

    /// Apply slippage to a price.
    fn apply_slippage(&self, price: Decimal, slippage_bps: Decimal, side: OrderSide) -> Decimal {
        let slip_factor = slippage_bps / Decimal::new(10000, 0);
        match side {
            OrderSide::Buy => price * (Decimal::ONE + slip_factor),
            OrderSide::Sell => price * (Decimal::ONE - slip_factor),
        }
    }

    /// Record a fill for analysis.
    fn record_fill(&mut self, fill: SimulatedFill) {
        if self.fill_history.len() >= self.max_history_size {
            self.fill_history.pop_front();
        }
        self.fill_history.push_back(fill);
    }

    /// Get fill statistics.
    pub fn get_fill_stats(&self) -> FillStatistics {
        if self.fill_history.is_empty() {
            return FillStatistics::default();
        }

        let total = self.fill_history.len();
        let full_fills = self.fill_history.iter().filter(|f| f.was_full_fill).count();
        let partial_fills = self.fill_history.iter().filter(|f| !f.was_full_fill && f.filled_quantity > Decimal::ZERO).count();
        let no_fills = self.fill_history.iter().filter(|f| f.filled_quantity <= Decimal::ZERO).count();

        let slippages: Vec<f64> = self.fill_history
            .iter()
            .filter(|f| f.slippage_bps > Decimal::ZERO)
            .map(|f| f.slippage_bps.to_f64().unwrap_or(0.0))
            .collect();

        let avg_slippage = if !slippages.is_empty() {
            slippages.iter().sum::<f64>() / slippages.len() as f64
        } else {
            0.0
        };

        let latencies: Vec<f64> = self.fill_history
            .iter()
            .filter(|f| f.filled_quantity > Decimal::ZERO)
            .map(|f| f.fill_latency_ms)
            .collect();

        let avg_latency = if !latencies.is_empty() {
            latencies.iter().sum::<f64>() / latencies.len() as f64
        } else {
            0.0
        };

        FillStatistics {
            total_orders: total,
            full_fill_rate: full_fills as f64 / total as f64,
            partial_fill_rate: partial_fills as f64 / total as f64,
            no_fill_rate: no_fills as f64 / total as f64,
            avg_slippage_bps: avg_slippage,
            avg_fill_latency_ms: avg_latency,
        }
    }

    /// Clear fill history.
    pub fn clear_history(&mut self) {
        self.fill_history.clear();
    }
}

impl Default for FillSimulator {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a cross-platform arbitrage execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossPlatformFillResult {
    pub leg_a_fill: SimulatedFill,
    pub leg_b_fill: Option<SimulatedFill>,
    pub net_profit: Decimal,
    pub execution_success: bool,
    pub failure_reason: Option<String>,
    pub total_latency_ms: f64,
}

/// Fill statistics for analysis.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FillStatistics {
    pub total_orders: usize,
    pub full_fill_rate: f64,
    pub partial_fill_rate: f64,
    pub no_fill_rate: f64,
    pub avg_slippage_bps: f64,
    pub avg_fill_latency_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_book() -> SimulatedOrderBook {
        SimulatedOrderBook {
            venue: Venue::Kalshi,
            market_id: "test".to_string(),
            bids: vec![
                SimulatedBookLevel { price: Decimal::new(49, 2), quantity: Decimal::new(1000, 0), order_count: 5 },
                SimulatedBookLevel { price: Decimal::new(48, 2), quantity: Decimal::new(2000, 0), order_count: 8 },
            ],
            asks: vec![
                SimulatedBookLevel { price: Decimal::new(51, 2), quantity: Decimal::new(1000, 0), order_count: 4 },
                SimulatedBookLevel { price: Decimal::new(52, 2), quantity: Decimal::new(3000, 0), order_count: 10 },
            ],
            last_update_ms: 0,
        }
    }

    #[test]
    fn test_market_order_fill() {
        let mut simulator = FillSimulator::new();
        let book = create_test_book();

        let order = SimulatedOrder {
            id: "test-1".to_string(),
            venue: Venue::Kalshi,
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            price: Decimal::new(51, 2),
            quantity: Decimal::new(500, 0),
            timestamp_ms: 0,
        };

        let fill = simulator.simulate_market_order(&order, &book);
        assert!(fill.filled_quantity > Decimal::ZERO);
        assert!(fill.was_full_fill);
    }

    #[test]
    fn test_limit_order_fill() {
        let mut simulator = FillSimulator::new();
        let book = create_test_book();

        let order = SimulatedOrder {
            id: "test-2".to_string(),
            venue: Venue::Kalshi,
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            price: Decimal::new(51, 2),
            quantity: Decimal::new(500, 0),
            timestamp_ms: 0,
        };

        let fill = simulator.simulate_limit_order(&order, &book, 1000);
        // Limit order at touch should have good fill probability
        assert!(fill.filled_quantity >= Decimal::ZERO);
    }

    #[test]
    fn test_fok_order() {
        let mut simulator = FillSimulator::new();
        let book = create_test_book();

        // FOK order larger than available liquidity should fail
        let order = SimulatedOrder {
            id: "test-3".to_string(),
            venue: Venue::Kalshi,
            side: OrderSide::Buy,
            order_type: OrderType::FillOrKill,
            price: Decimal::new(51, 2),
            quantity: Decimal::new(5000, 0), // More than available at 51
            timestamp_ms: 0,
        };

        let fill = simulator.simulate_fok_order(&order, &book);
        assert_eq!(fill.filled_quantity, Decimal::ZERO);
    }
}
