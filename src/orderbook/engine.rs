//! Orderbook engine for managing market depth.
//!
//! Provides a high-performance orderbook implementation with:
//! - O(log n) updates
//! - Lock-free reads where possible
//! - Automatic staleness detection
//! - Event emission on significant changes

use crate::core::error::{Error, Result};
use crate::core::events::{Event, EventBus};
use crate::core::types::*;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, warn};

/// High-performance orderbook engine
pub struct OrderbookEngine {
    /// Orderbooks by token ID
    orderbooks: DashMap<TokenId, Arc<RwLock<ManagedOrderbook>>>,

    /// Event bus for publishing updates
    event_bus: EventBus,

    /// Staleness threshold in milliseconds
    stale_threshold_ms: u64,

    /// Sequence counter
    sequence: AtomicU64,
}

impl OrderbookEngine {
    /// Create a new orderbook engine
    pub fn new(event_bus: EventBus, stale_threshold_ms: u64) -> Self {
        Self {
            orderbooks: DashMap::new(),
            event_bus,
            stale_threshold_ms,
            sequence: AtomicU64::new(0),
        }
    }

    /// Get or create an orderbook for a token
    pub fn get_or_create(&self, token_id: &str, outcome: Outcome) -> Arc<RwLock<ManagedOrderbook>> {
        self.orderbooks
            .entry(token_id.to_string())
            .or_insert_with(|| {
                Arc::new(RwLock::new(ManagedOrderbook::new(
                    token_id.to_string(),
                    outcome,
                )))
            })
            .clone()
    }

    /// Get an existing orderbook
    pub fn get(&self, token_id: &str) -> Option<Arc<RwLock<ManagedOrderbook>>> {
        self.orderbooks.get(token_id).map(|ob| ob.clone())
    }

    /// Update an orderbook with a new snapshot
    pub fn update_snapshot(&self, orderbook: Orderbook) -> Result<()> {
        let token_id = orderbook.token_id.clone();
        let outcome = orderbook.outcome;

        let managed = self.get_or_create(&token_id, outcome);
        let mut ob = managed.write();

        let old_mid = ob.mid_price();

        ob.apply_snapshot(orderbook);
        ob.sequence = self.sequence.fetch_add(1, Ordering::SeqCst);

        let new_mid = ob.mid_price();

        // Emit event if mid price changed significantly
        if let (Some(old), Some(new)) = (old_mid, new_mid) {
            let change = ((new.0 - old.0) / old.0).abs();
            if change > dec!(0.001) {
                // 0.1% change
                self.event_bus
                    .publish(Event::orderbook_update(ob.to_orderbook()));
            }
        }

        Ok(())
    }

    /// Apply an incremental update to an orderbook
    pub fn apply_delta(&self, token_id: &str, side: Side, price: Price, quantity: Quantity) -> Result<()> {
        let managed = self
            .get(token_id)
            .ok_or_else(|| Error::NotFound(format!("Orderbook not found: {}", token_id)))?;

        let mut ob = managed.write();
        ob.apply_delta(side, price, quantity);
        ob.sequence = self.sequence.fetch_add(1, Ordering::SeqCst);

        Ok(())
    }

    /// Get the best bid/ask for a token
    pub fn get_bbo(&self, token_id: &str) -> Option<(Option<Price>, Option<Price>)> {
        self.get(token_id).map(|managed| {
            let ob = managed.read();
            (ob.best_bid(), ob.best_ask())
        })
    }

    /// Get the mid price for a token
    pub fn get_mid_price(&self, token_id: &str) -> Option<Price> {
        self.get(token_id).and_then(|managed| {
            let ob = managed.read();
            ob.mid_price()
        })
    }

    /// Get the spread for a token
    pub fn get_spread(&self, token_id: &str) -> Option<Decimal> {
        self.get(token_id).and_then(|managed| {
            let ob = managed.read();
            ob.spread()
        })
    }

    /// Check if an orderbook is stale
    pub fn is_stale(&self, token_id: &str) -> bool {
        self.get(token_id)
            .map(|managed| {
                let ob = managed.read();
                ob.is_stale(self.stale_threshold_ms)
            })
            .unwrap_or(true)
    }

    /// Get depth at a price level
    pub fn get_depth(&self, token_id: &str, side: Side, price: Price) -> Option<Quantity> {
        self.get(token_id).map(|managed| {
            let ob = managed.read();
            ob.depth_at_price(side, price)
        })
    }

    /// Calculate VWAP for a quantity
    pub fn get_vwap(&self, token_id: &str, side: Side, quantity: Quantity) -> Option<Price> {
        self.get(token_id).and_then(|managed| {
            let ob = managed.read();
            ob.vwap(side, quantity)
        })
    }

    /// Get all active orderbooks
    pub fn all_orderbooks(&self) -> Vec<(TokenId, Orderbook)> {
        self.orderbooks
            .iter()
            .map(|entry| {
                let ob = entry.value().read();
                (entry.key().clone(), ob.to_orderbook())
            })
            .collect()
    }

    /// Clear all orderbooks
    pub fn clear(&self) {
        self.orderbooks.clear();
    }

    /// Remove a specific orderbook
    pub fn remove(&self, token_id: &str) {
        self.orderbooks.remove(token_id);
    }

    /// Get statistics
    pub fn stats(&self) -> OrderbookEngineStats {
        let mut total_levels = 0;
        let mut stale_count = 0;

        for entry in self.orderbooks.iter() {
            let ob = entry.value().read();
            total_levels += ob.bids.len() + ob.asks.len();
            if ob.is_stale(self.stale_threshold_ms) {
                stale_count += 1;
            }
        }

        OrderbookEngineStats {
            orderbook_count: self.orderbooks.len(),
            total_levels,
            stale_count,
            sequence: self.sequence.load(Ordering::Relaxed),
        }
    }
}

/// Managed orderbook with additional metadata
pub struct ManagedOrderbook {
    pub token_id: TokenId,
    pub outcome: Outcome,
    pub bids: BTreeMap<Price, PriceLevel>,
    pub asks: BTreeMap<Price, PriceLevel>,
    pub last_update: DateTime<Utc>,
    pub sequence: u64,
    pub update_count: u64,
}

impl ManagedOrderbook {
    /// Create a new managed orderbook
    pub fn new(token_id: TokenId, outcome: Outcome) -> Self {
        Self {
            token_id,
            outcome,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update: Utc::now(),
            sequence: 0,
            update_count: 0,
        }
    }

    /// Apply a full orderbook snapshot
    pub fn apply_snapshot(&mut self, orderbook: Orderbook) {
        self.bids.clear();
        self.asks.clear();

        for level in &orderbook.bids.levels {
            self.bids.insert(level.price, *level);
        }

        for level in &orderbook.asks.levels {
            self.asks.insert(level.price, *level);
        }

        self.last_update = Utc::now();
        self.update_count += 1;
    }

    /// Apply an incremental update
    pub fn apply_delta(&mut self, side: Side, price: Price, quantity: Quantity) {
        let book = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };

        if quantity.0 <= Decimal::ZERO {
            book.remove(&price);
        } else {
            book.insert(
                price,
                PriceLevel::new(price, quantity, 1),
            );
        }

        self.last_update = Utc::now();
        self.update_count += 1;
    }

    /// Get best bid price
    pub fn best_bid(&self) -> Option<Price> {
        self.bids.keys().next_back().copied()
    }

    /// Get best ask price
    pub fn best_ask(&self) -> Option<Price> {
        self.asks.keys().next().copied()
    }

    /// Get best bid level
    pub fn best_bid_level(&self) -> Option<&PriceLevel> {
        self.bids.values().next_back()
    }

    /// Get best ask level
    pub fn best_ask_level(&self) -> Option<&PriceLevel> {
        self.asks.values().next()
    }

    /// Calculate mid price
    pub fn mid_price(&self) -> Option<Price> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Price::new((bid.0 + ask.0) / Decimal::TWO),
            _ => None,
        }
    }

    /// Calculate spread
    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask.0 - bid.0),
            _ => None,
        }
    }

    /// Calculate spread in basis points
    pub fn spread_bps(&self) -> Option<Decimal> {
        match (self.mid_price(), self.spread()) {
            (Some(mid), Some(spread)) if mid.0 > Decimal::ZERO => {
                Some((spread / mid.0) * dec!(10000))
            }
            _ => None,
        }
    }

    /// Get depth at a specific price
    pub fn depth_at_price(&self, side: Side, price: Price) -> Quantity {
        let book = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        };

        let total: Decimal = book
            .iter()
            .filter(|(p, _)| match side {
                Side::Buy => **p >= price,
                Side::Sell => **p <= price,
            })
            .map(|(_, level)| level.quantity.0)
            .sum();

        Quantity(total)
    }

    /// Calculate VWAP for a given quantity
    pub fn vwap(&self, side: Side, quantity: Quantity) -> Option<Price> {
        let levels: Vec<_> = match side {
            Side::Buy => self.asks.values().collect(),
            Side::Sell => self.bids.values().rev().collect(),
        };

        let mut remaining = quantity.0;
        let mut notional = Decimal::ZERO;
        let mut filled = Decimal::ZERO;

        for level in levels {
            let fill_qty = remaining.min(level.quantity.0);
            notional += fill_qty * level.price.0;
            filled += fill_qty;
            remaining -= fill_qty;

            if remaining <= Decimal::ZERO {
                break;
            }
        }

        if filled > Decimal::ZERO {
            Price::new(notional / filled)
        } else {
            None
        }
    }

    /// Check if orderbook is stale
    pub fn is_stale(&self, threshold_ms: u64) -> bool {
        let age = Utc::now() - self.last_update;
        age > ChronoDuration::milliseconds(threshold_ms as i64)
    }

    /// Convert to Orderbook type
    pub fn to_orderbook(&self) -> Orderbook {
        let mut ob = Orderbook::new(self.token_id.clone(), self.outcome);

        // Bids: highest first
        for (_, level) in self.bids.iter().rev() {
            ob.bids.levels.push(*level);
        }

        // Asks: lowest first
        for (_, level) in self.asks.iter() {
            ob.asks.levels.push(*level);
        }

        ob.sequence = self.sequence;
        ob.timestamp = self.last_update;

        ob
    }

    /// Get top N levels
    pub fn top_levels(&self, n: usize) -> (Vec<PriceLevel>, Vec<PriceLevel>) {
        let bids: Vec<_> = self.bids.values().rev().take(n).copied().collect();
        let asks: Vec<_> = self.asks.values().take(n).copied().collect();
        (bids, asks)
    }

    /// Calculate total liquidity
    pub fn total_liquidity(&self) -> (Decimal, Decimal) {
        let bid_liquidity: Decimal = self.bids.values().map(|l| l.notional()).sum();
        let ask_liquidity: Decimal = self.asks.values().map(|l| l.notional()).sum();
        (bid_liquidity, ask_liquidity)
    }

    /// Check if there's sufficient liquidity
    pub fn has_liquidity(&self, min_bid: Quantity, min_ask: Quantity) -> bool {
        let bid_qty: Decimal = self.bids.values().map(|l| l.quantity.0).sum();
        let ask_qty: Decimal = self.asks.values().map(|l| l.quantity.0).sum();

        bid_qty >= min_bid.0 && ask_qty >= min_ask.0
    }
}

/// Orderbook engine statistics
#[derive(Debug, Clone)]
pub struct OrderbookEngineStats {
    pub orderbook_count: usize,
    pub total_levels: usize,
    pub stale_count: usize,
    pub sequence: u64,
}

impl std::fmt::Debug for OrderbookEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.stats();
        f.debug_struct("OrderbookEngine")
            .field("orderbook_count", &stats.orderbook_count)
            .field("total_levels", &stats.total_levels)
            .field("stale_count", &stats.stale_count)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_orderbook() -> ManagedOrderbook {
        let mut ob = ManagedOrderbook::new("test_token".to_string(), Outcome::Yes);

        // Add bids
        ob.bids.insert(
            Price::new(dec!(0.45)).unwrap(),
            PriceLevel::new(Price::new(dec!(0.45)).unwrap(), Quantity(dec!(100)), 1),
        );
        ob.bids.insert(
            Price::new(dec!(0.44)).unwrap(),
            PriceLevel::new(Price::new(dec!(0.44)).unwrap(), Quantity(dec!(200)), 1),
        );

        // Add asks
        ob.asks.insert(
            Price::new(dec!(0.55)).unwrap(),
            PriceLevel::new(Price::new(dec!(0.55)).unwrap(), Quantity(dec!(150)), 1),
        );
        ob.asks.insert(
            Price::new(dec!(0.56)).unwrap(),
            PriceLevel::new(Price::new(dec!(0.56)).unwrap(), Quantity(dec!(250)), 1),
        );

        ob
    }

    #[test]
    fn test_best_bid_ask() {
        let ob = create_test_orderbook();

        assert_eq!(ob.best_bid().map(|p| p.0), Some(dec!(0.45)));
        assert_eq!(ob.best_ask().map(|p| p.0), Some(dec!(0.55)));
    }

    #[test]
    fn test_mid_price() {
        let ob = create_test_orderbook();

        let mid = ob.mid_price().unwrap();
        assert_eq!(mid.0, dec!(0.50));
    }

    #[test]
    fn test_spread() {
        let ob = create_test_orderbook();

        let spread = ob.spread().unwrap();
        assert_eq!(spread, dec!(0.10));
    }

    #[test]
    fn test_vwap() {
        let ob = create_test_orderbook();

        // Buy 100 shares (fills from ask at 0.55)
        let vwap = ob.vwap(Side::Buy, Quantity(dec!(100))).unwrap();
        assert_eq!(vwap.0, dec!(0.55));

        // Buy 200 shares (fills 150 @ 0.55, 50 @ 0.56)
        let vwap = ob.vwap(Side::Buy, Quantity(dec!(200))).unwrap();
        // VWAP = (150 * 0.55 + 50 * 0.56) / 200 = (82.5 + 28) / 200 = 0.5525
        assert_eq!(vwap.0, dec!(0.5525));
    }

    #[test]
    fn test_depth_at_price() {
        let ob = create_test_orderbook();

        // Depth for buys at 0.44 and above
        let depth = ob.depth_at_price(Side::Buy, Price::new(dec!(0.44)).unwrap());
        assert_eq!(depth.0, dec!(300)); // 100 + 200
    }

    #[test]
    fn test_delta_update() {
        let mut ob = create_test_orderbook();

        // Update bid at 0.45 to 150
        ob.apply_delta(Side::Buy, Price::new(dec!(0.45)).unwrap(), Quantity(dec!(150)));
        assert_eq!(ob.bids.get(&Price::new(dec!(0.45)).unwrap()).unwrap().quantity.0, dec!(150));

        // Remove bid at 0.44
        ob.apply_delta(Side::Buy, Price::new(dec!(0.44)).unwrap(), Quantity::ZERO);
        assert!(ob.bids.get(&Price::new(dec!(0.44)).unwrap()).is_none());
    }

    #[test]
    fn test_engine() {
        let event_bus = EventBus::new(100);
        let engine = OrderbookEngine::new(event_bus, 5000);

        let mut orderbook = Orderbook::new("token1".to_string(), Outcome::Yes);
        orderbook.bids.levels.push(PriceLevel::new(
            Price::new(dec!(0.45)).unwrap(),
            Quantity(dec!(100)),
            1,
        ));
        orderbook.asks.levels.push(PriceLevel::new(
            Price::new(dec!(0.55)).unwrap(),
            Quantity(dec!(100)),
            1,
        ));

        engine.update_snapshot(orderbook).unwrap();

        assert_eq!(engine.get_mid_price("token1").map(|p| p.0), Some(dec!(0.50)));
        assert!(!engine.is_stale("token1"));
    }
}
