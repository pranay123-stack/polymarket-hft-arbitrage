//! Smart order router for optimal execution.
//!
//! Features:
//! - Order splitting for large orders
//! - Price improvement detection
//! - Execution venue selection (when multiple exist)

use crate::core::types::*;
use crate::orderbook::OrderbookEngine;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tracing::debug;

/// Smart order router
pub struct SmartOrderRouter {
    orderbook_engine: Arc<OrderbookEngine>,
    max_single_order_pct: Decimal,
    price_improvement_threshold: Decimal,
}

impl SmartOrderRouter {
    /// Create a new smart order router
    pub fn new(orderbook_engine: Arc<OrderbookEngine>) -> Self {
        Self {
            orderbook_engine,
            max_single_order_pct: dec!(0.25), // Max 25% of available liquidity
            price_improvement_threshold: dec!(0.001), // 0.1% price improvement
        }
    }

    /// Route an order for optimal execution
    pub fn route(&self, order: &Order) -> RoutingDecision {
        // Get orderbook for the token
        let ob = match self.orderbook_engine.get(&order.token_id) {
            Some(ob) => ob,
            None => return RoutingDecision::Direct { order: order.clone() },
        };

        let book = ob.read();

        // Get available liquidity at the order price
        let available_qty = match order.side {
            Side::Buy => book.depth_at_price(Side::Sell, order.price),
            Side::Sell => book.depth_at_price(Side::Buy, order.price),
        };

        // Check if we should split the order
        if order.quantity.0 > available_qty.0 * self.max_single_order_pct {
            return self.create_split_decision(order, available_qty);
        }

        // Check for price improvement
        if let Some(improved_price) = self.check_price_improvement(order, &book) {
            return RoutingDecision::PriceImproved {
                order: order.clone(),
                improved_price,
            };
        }

        RoutingDecision::Direct { order: order.clone() }
    }

    /// Create a split order decision
    fn create_split_decision(&self, order: &Order, available_qty: Quantity) -> RoutingDecision {
        let max_slice = available_qty.0 * self.max_single_order_pct;
        let num_slices = (order.quantity.0 / max_slice).ceil().to_u32().unwrap_or(1);

        debug!(
            "Splitting order {} into {} slices",
            order.id, num_slices
        );

        let mut slices = Vec::new();
        let mut remaining = order.quantity.0;

        for i in 0..num_slices {
            let slice_qty = remaining.min(max_slice);
            remaining -= slice_qty;

            slices.push(OrderSlice {
                order: Order {
                    id: format!("{}-{}", order.id, i),
                    quantity: Quantity(slice_qty),
                    ..order.clone()
                },
                delay_ms: (i as u64) * 100, // Stagger by 100ms
            });

            if remaining <= Decimal::ZERO {
                break;
            }
        }

        RoutingDecision::Split { slices }
    }

    /// Check if we can get price improvement
    fn check_price_improvement(
        &self,
        order: &Order,
        book: &crate::orderbook::engine::ManagedOrderbook,
    ) -> Option<Price> {
        let (best_price, improved_price) = match order.side {
            Side::Buy => {
                let best = book.best_ask()?;
                let improved = Price::new(order.price.0 - self.price_improvement_threshold)?;
                (best, improved)
            }
            Side::Sell => {
                let best = book.best_bid()?;
                let improved = Price::new(order.price.0 + self.price_improvement_threshold)?;
                (best, improved)
            }
        };

        // Check if improved price is still competitive
        match order.side {
            Side::Buy if improved_price.0 >= best_price.0 => Some(improved_price),
            Side::Sell if improved_price.0 <= best_price.0 => Some(improved_price),
            _ => None,
        }
    }

    /// Estimate slippage for an order
    pub fn estimate_slippage(&self, order: &Order) -> Option<Decimal> {
        let ob = self.orderbook_engine.get(&order.token_id)?;
        let book = ob.read();

        let vwap = book.vwap(order.side, order.quantity)?;
        let mid = book.mid_price()?;

        Some((vwap.0 - mid.0).abs())
    }

    /// Get recommended order size based on liquidity
    pub fn recommended_size(&self, token_id: &str, side: Side) -> Option<Quantity> {
        let ob = self.orderbook_engine.get(token_id)?;
        let book = ob.read();

        let available = match side {
            Side::Buy => {
                let (_, ask_liq) = book.total_liquidity();
                ask_liq
            }
            Side::Sell => {
                let (bid_liq, _) = book.total_liquidity();
                bid_liq
            }
        };

        Some(Quantity(available * self.max_single_order_pct))
    }
}

/// Routing decision for an order
#[derive(Debug, Clone)]
pub enum RoutingDecision {
    /// Execute order directly
    Direct { order: Order },

    /// Split order into multiple slices
    Split { slices: Vec<OrderSlice> },

    /// Price can be improved
    PriceImproved { order: Order, improved_price: Price },

    /// Order rejected (insufficient liquidity, etc.)
    Rejected { reason: String },
}

/// A slice of a split order
#[derive(Debug, Clone)]
pub struct OrderSlice {
    pub order: Order,
    pub delay_ms: u64,
}

impl std::fmt::Debug for SmartOrderRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmartOrderRouter")
            .field("max_single_order_pct", &self.max_single_order_pct)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::events::EventBus;

    #[test]
    fn test_routing_decision() {
        let event_bus = EventBus::new(100);
        let engine = Arc::new(OrderbookEngine::new(event_bus, 5000));

        // Add orderbook with limited liquidity
        let mut ob = Orderbook::new("token1".to_string(), Outcome::Yes);
        ob.asks.levels.push(PriceLevel::new(
            Price::new(dec!(0.50)).unwrap(),
            Quantity(dec!(100)),
            1,
        ));
        engine.update_snapshot(ob).unwrap();

        let router = SmartOrderRouter::new(engine);

        // Small order should be direct
        let small_order = Order::new(
            "market1".to_string(),
            "token1".to_string(),
            Side::Buy,
            Outcome::Yes,
            Price::new(dec!(0.50)).unwrap(),
            Quantity(dec!(10)),
            OrderType::Gtc,
        );

        let decision = router.route(&small_order);
        assert!(matches!(decision, RoutingDecision::Direct { .. }));

        // Large order should be split
        let large_order = Order::new(
            "market1".to_string(),
            "token1".to_string(),
            Side::Buy,
            Outcome::Yes,
            Price::new(dec!(0.50)).unwrap(),
            Quantity(dec!(200)),
            OrderType::Gtc,
        );

        let decision = router.route(&large_order);
        assert!(matches!(decision, RoutingDecision::Split { .. }));
    }
}
