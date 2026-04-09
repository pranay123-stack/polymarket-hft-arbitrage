//! Position tracking for cross-venue arbitrage.
//!
//! Tracks:
//! - Individual positions per venue/market
//! - Aggregated exposure
//! - Realized/unrealized P&L
//! - Position history

use std::collections::HashMap;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use std::sync::Arc;
use chrono::{DateTime, Utc};

/// Position side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PositionSide {
    Long,
    Short,
    Flat,
}

impl PositionSide {
    pub fn from_quantity(qty: Decimal) -> Self {
        if qty > Decimal::ZERO {
            PositionSide::Long
        } else if qty < Decimal::ZERO {
            PositionSide::Short
        } else {
            PositionSide::Flat
        }
    }
}

/// A position in a specific market.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    /// Position ID.
    pub id: String,
    /// Venue identifier.
    pub venue: String,
    /// Market identifier.
    pub market_id: String,
    /// Outcome/token ID (for multi-outcome markets).
    pub outcome_id: Option<String>,
    /// Current quantity (positive = long, negative = short).
    pub quantity: Decimal,
    /// Average entry price.
    pub avg_entry_price: Decimal,
    /// Current market price.
    pub current_price: Decimal,
    /// Realized P&L.
    pub realized_pnl: Decimal,
    /// Unrealized P&L.
    pub unrealized_pnl: Decimal,
    /// Total fees paid.
    pub fees_paid: Decimal,
    /// When the position was opened.
    pub opened_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
    /// Number of trades.
    pub trade_count: u32,
    /// Position side.
    pub side: PositionSide,
}

impl Position {
    /// Create a new position.
    pub fn new(
        venue: &str,
        market_id: &str,
        outcome_id: Option<&str>,
        quantity: Decimal,
        price: Decimal,
        fees: Decimal,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            venue: venue.to_string(),
            market_id: market_id.to_string(),
            outcome_id: outcome_id.map(String::from),
            quantity,
            avg_entry_price: price,
            current_price: price,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            fees_paid: fees,
            opened_at: now,
            updated_at: now,
            trade_count: 1,
            side: PositionSide::from_quantity(quantity),
        }
    }

    /// Update position with a new fill.
    pub fn apply_fill(&mut self, quantity: Decimal, price: Decimal, fees: Decimal) {
        let new_quantity = self.quantity + quantity;

        // Check if position is being increased or reduced
        let is_increasing = (self.quantity >= Decimal::ZERO && quantity > Decimal::ZERO)
            || (self.quantity <= Decimal::ZERO && quantity < Decimal::ZERO);

        if is_increasing {
            // Increasing position - calculate new average price
            if new_quantity.abs() > Decimal::ZERO {
                let old_cost = self.quantity.abs() * self.avg_entry_price;
                let new_cost = quantity.abs() * price;
                self.avg_entry_price = (old_cost + new_cost) / new_quantity.abs();
            }
        } else {
            // Reducing position - realize P&L
            let closed_qty = quantity.abs().min(self.quantity.abs());
            let pnl = if self.quantity > Decimal::ZERO {
                // Long position being reduced
                (price - self.avg_entry_price) * closed_qty
            } else {
                // Short position being reduced
                (self.avg_entry_price - price) * closed_qty
            };
            self.realized_pnl += pnl;
        }

        self.quantity = new_quantity;
        self.fees_paid += fees;
        self.current_price = price;
        self.trade_count += 1;
        self.updated_at = Utc::now();
        self.side = PositionSide::from_quantity(self.quantity);

        // Recalculate unrealized P&L
        self.update_unrealized_pnl();
    }

    /// Update the current market price.
    pub fn update_price(&mut self, price: Decimal) {
        self.current_price = price;
        self.updated_at = Utc::now();
        self.update_unrealized_pnl();
    }

    /// Calculate unrealized P&L.
    fn update_unrealized_pnl(&mut self) {
        self.unrealized_pnl = if self.quantity > Decimal::ZERO {
            (self.current_price - self.avg_entry_price) * self.quantity
        } else if self.quantity < Decimal::ZERO {
            (self.avg_entry_price - self.current_price) * self.quantity.abs()
        } else {
            Decimal::ZERO
        };
    }

    /// Get total P&L (realized + unrealized - fees).
    pub fn total_pnl(&self) -> Decimal {
        self.realized_pnl + self.unrealized_pnl - self.fees_paid
    }

    /// Get notional value.
    pub fn notional_value(&self) -> Decimal {
        self.quantity.abs() * self.current_price
    }

    /// Check if position is flat.
    pub fn is_flat(&self) -> bool {
        self.quantity == Decimal::ZERO
    }

    /// Get unique position key.
    pub fn key(&self) -> String {
        match &self.outcome_id {
            Some(outcome) => format!("{}:{}:{}", self.venue, self.market_id, outcome),
            None => format!("{}:{}", self.venue, self.market_id),
        }
    }
}

/// Fill record for position updates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub fill_id: String,
    pub order_id: String,
    pub venue: String,
    pub market_id: String,
    pub outcome_id: Option<String>,
    pub side: FillSide,
    pub quantity: Decimal,
    pub price: Decimal,
    pub fees: Decimal,
    pub timestamp: DateTime<Utc>,
}

/// Fill side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FillSide {
    Buy,
    Sell,
}

/// Position tracker for managing all positions.
pub struct PositionTracker {
    /// Active positions indexed by key.
    positions: Arc<RwLock<HashMap<String, Position>>>,
    /// Closed positions history.
    closed_positions: Arc<RwLock<Vec<Position>>>,
    /// Fill history.
    fill_history: Arc<RwLock<Vec<Fill>>>,
    /// Maximum closed position history size.
    max_history_size: usize,
    /// Snapshot callbacks.
    snapshot_callbacks: Arc<RwLock<Vec<Box<dyn Fn(&PositionSnapshot) + Send + Sync>>>>,
}

impl PositionTracker {
    /// Create a new position tracker.
    pub fn new(max_history_size: usize) -> Self {
        Self {
            positions: Arc::new(RwLock::new(HashMap::new())),
            closed_positions: Arc::new(RwLock::new(Vec::new())),
            fill_history: Arc::new(RwLock::new(Vec::new())),
            max_history_size,
            snapshot_callbacks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Process a fill.
    pub async fn process_fill(&self, fill: Fill) {
        let key = match &fill.outcome_id {
            Some(outcome) => format!("{}:{}:{}", fill.venue, fill.market_id, outcome),
            None => format!("{}:{}", fill.venue, fill.market_id),
        };

        let quantity = match fill.side {
            FillSide::Buy => fill.quantity,
            FillSide::Sell => -fill.quantity,
        };

        let mut positions = self.positions.write().await;

        if let Some(position) = positions.get_mut(&key) {
            position.apply_fill(quantity, fill.price, fill.fees);

            // Check if position is now flat
            if position.is_flat() {
                let closed = position.clone();
                positions.remove(&key);

                // Add to closed positions
                let mut closed_positions = self.closed_positions.write().await;
                closed_positions.push(closed);
                if closed_positions.len() > self.max_history_size {
                    closed_positions.remove(0);
                }
            }
        } else {
            // New position
            let position = Position::new(
                &fill.venue,
                &fill.market_id,
                fill.outcome_id.as_deref(),
                quantity,
                fill.price,
                fill.fees,
            );
            positions.insert(key, position);
        }

        // Record fill
        let mut history = self.fill_history.write().await;
        history.push(fill);
        if history.len() > self.max_history_size {
            history.remove(0);
        }

        // Notify callbacks
        drop(positions);
        self.notify_snapshot().await;
    }

    /// Update price for a position.
    pub async fn update_price(&self, venue: &str, market_id: &str, outcome_id: Option<&str>, price: Decimal) {
        let key = match outcome_id {
            Some(outcome) => format!("{}:{}:{}", venue, market_id, outcome),
            None => format!("{}:{}", venue, market_id),
        };

        let mut positions = self.positions.write().await;
        if let Some(position) = positions.get_mut(&key) {
            position.update_price(price);
        }
    }

    /// Get a specific position.
    pub async fn get_position(&self, venue: &str, market_id: &str, outcome_id: Option<&str>) -> Option<Position> {
        let key = match outcome_id {
            Some(outcome) => format!("{}:{}:{}", venue, market_id, outcome),
            None => format!("{}:{}", venue, market_id),
        };

        self.positions.read().await.get(&key).cloned()
    }

    /// Get all active positions.
    pub async fn get_all_positions(&self) -> Vec<Position> {
        self.positions.read().await.values().cloned().collect()
    }

    /// Get positions for a specific venue.
    pub async fn get_positions_for_venue(&self, venue: &str) -> Vec<Position> {
        self.positions
            .read()
            .await
            .values()
            .filter(|p| p.venue == venue)
            .cloned()
            .collect()
    }

    /// Get positions for a specific market.
    pub async fn get_positions_for_market(&self, venue: &str, market_id: &str) -> Vec<Position> {
        self.positions
            .read()
            .await
            .values()
            .filter(|p| p.venue == venue && p.market_id == market_id)
            .cloned()
            .collect()
    }

    /// Get current position snapshot.
    pub async fn get_snapshot(&self) -> PositionSnapshot {
        let positions = self.positions.read().await;

        let total_long_exposure: Decimal = positions
            .values()
            .filter(|p| p.side == PositionSide::Long)
            .map(|p| p.notional_value())
            .sum();

        let total_short_exposure: Decimal = positions
            .values()
            .filter(|p| p.side == PositionSide::Short)
            .map(|p| p.notional_value())
            .sum();

        let total_unrealized_pnl: Decimal = positions.values().map(|p| p.unrealized_pnl).sum();
        let total_realized_pnl: Decimal = positions.values().map(|p| p.realized_pnl).sum();
        let total_fees: Decimal = positions.values().map(|p| p.fees_paid).sum();

        let mut venue_exposure: HashMap<String, Decimal> = HashMap::new();
        for position in positions.values() {
            *venue_exposure.entry(position.venue.clone()).or_insert(Decimal::ZERO) += position.notional_value();
        }

        PositionSnapshot {
            timestamp: Utc::now(),
            position_count: positions.len(),
            total_long_exposure,
            total_short_exposure,
            net_exposure: total_long_exposure - total_short_exposure,
            total_unrealized_pnl,
            total_realized_pnl,
            total_pnl: total_unrealized_pnl + total_realized_pnl - total_fees,
            total_fees,
            venue_exposure,
        }
    }

    /// Get closed positions.
    pub async fn get_closed_positions(&self) -> Vec<Position> {
        self.closed_positions.read().await.clone()
    }

    /// Get fill history.
    pub async fn get_fill_history(&self) -> Vec<Fill> {
        self.fill_history.read().await.clone()
    }

    /// Set position from external source (for reconciliation).
    pub async fn set_position(&self, position: Position) {
        let key = position.key();
        self.positions.write().await.insert(key, position);
    }

    /// Clear all positions (use with caution).
    pub async fn clear_all(&self) {
        self.positions.write().await.clear();
    }

    /// Register a snapshot callback.
    pub async fn on_snapshot<F>(&self, callback: F)
    where
        F: Fn(&PositionSnapshot) + Send + Sync + 'static,
    {
        self.snapshot_callbacks.write().await.push(Box::new(callback));
    }

    /// Notify all snapshot callbacks.
    async fn notify_snapshot(&self) {
        let snapshot = self.get_snapshot().await;
        let callbacks = self.snapshot_callbacks.read().await;
        for callback in callbacks.iter() {
            callback(&snapshot);
        }
    }
}

/// Position snapshot for reporting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub timestamp: DateTime<Utc>,
    pub position_count: usize,
    pub total_long_exposure: Decimal,
    pub total_short_exposure: Decimal,
    pub net_exposure: Decimal,
    pub total_unrealized_pnl: Decimal,
    pub total_realized_pnl: Decimal,
    pub total_pnl: Decimal,
    pub total_fees: Decimal,
    pub venue_exposure: HashMap<String, Decimal>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_creation() {
        let pos = Position::new(
            "kalshi",
            "MKT1",
            None,
            Decimal::new(100, 0),
            Decimal::new(50, 2),
            Decimal::new(1, 0),
        );

        assert_eq!(pos.venue, "kalshi");
        assert_eq!(pos.quantity, Decimal::new(100, 0));
        assert_eq!(pos.side, PositionSide::Long);
    }

    #[test]
    fn test_position_fill() {
        let mut pos = Position::new(
            "kalshi",
            "MKT1",
            None,
            Decimal::new(100, 0),
            Decimal::new(50, 2),
            Decimal::ZERO,
        );

        // Add to position
        pos.apply_fill(Decimal::new(100, 0), Decimal::new(55, 2), Decimal::ZERO);
        assert_eq!(pos.quantity, Decimal::new(200, 0));

        // Reduce position
        pos.apply_fill(Decimal::new(-50, 0), Decimal::new(60, 2), Decimal::ZERO);
        assert_eq!(pos.quantity, Decimal::new(150, 0));
        assert!(pos.realized_pnl > Decimal::ZERO); // Should have profit
    }

    #[tokio::test]
    async fn test_position_tracker() {
        let tracker = PositionTracker::new(100);

        let fill = Fill {
            fill_id: "fill1".to_string(),
            order_id: "order1".to_string(),
            venue: "kalshi".to_string(),
            market_id: "MKT1".to_string(),
            outcome_id: None,
            side: FillSide::Buy,
            quantity: Decimal::new(100, 0),
            price: Decimal::new(50, 2),
            fees: Decimal::new(1, 0),
            timestamp: Utc::now(),
        };

        tracker.process_fill(fill).await;

        let positions = tracker.get_all_positions().await;
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].quantity, Decimal::new(100, 0));
    }
}
