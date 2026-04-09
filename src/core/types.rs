//! Core types used throughout the HFT arbitrage bot.
//!
//! These types are designed for:
//! - Zero-copy where possible
//! - Cache-line alignment for hot paths
//! - Minimal heap allocations

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::fmt;
use strum::{Display, EnumString};
use uuid::Uuid;

/// Type alias for token IDs (ERC1155 token identifiers)
pub type TokenId = String;

/// Type alias for market condition IDs
pub type ConditionId = String;

/// Type alias for order IDs
pub type OrderId = String;

/// Type alias for trade IDs
pub type TradeId = String;

/// Price in the range [0, 1] representing probability
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Price(#[serde(with = "rust_decimal::serde::str")] pub Decimal);

impl Price {
    pub const ZERO: Price = Price(Decimal::ZERO);
    pub const ONE: Price = Price(Decimal::ONE);

    #[inline]
    pub fn new(value: Decimal) -> Option<Self> {
        if value >= Decimal::ZERO && value <= Decimal::ONE {
            Some(Price(value))
        } else {
            None
        }
    }

    #[inline]
    pub fn new_unchecked(value: Decimal) -> Self {
        Price(value)
    }

    #[inline]
    pub fn as_decimal(&self) -> Decimal {
        self.0
    }

    #[inline]
    pub fn complement(&self) -> Self {
        Price(Decimal::ONE - self.0)
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${:.4}", self.0)
    }
}

/// Quantity of shares/tokens
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Quantity(#[serde(with = "rust_decimal::serde::str")] pub Decimal);

impl Quantity {
    pub const ZERO: Quantity = Quantity(Decimal::ZERO);

    #[inline]
    pub fn new(value: Decimal) -> Option<Self> {
        if value >= Decimal::ZERO {
            Some(Quantity(value))
        } else {
            None
        }
    }

    #[inline]
    pub fn as_decimal(&self) -> Decimal {
        self.0
    }
}

impl fmt::Display for Quantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:.2}", self.0)
    }
}

/// Side of the trade
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
#[serde(rename_all = "UPPERCASE")]
#[strum(serialize_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    #[inline]
    pub fn opposite(&self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

/// Outcome type for binary markets
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Yes,
    No,
}

impl Outcome {
    #[inline]
    pub fn opposite(&self) -> Self {
        match self {
            Outcome::Yes => Outcome::No,
            Outcome::No => Outcome::Yes,
        }
    }

    #[inline]
    pub fn index(&self) -> usize {
        match self {
            Outcome::Yes => 0,
            Outcome::No => 1,
        }
    }
}

/// Order type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderType {
    /// Good till cancelled limit order
    Gtc,
    /// Fill or kill
    Fok,
    /// Good till date
    Gtd,
}

/// Order status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    Live,
    Matched,
    Cancelled,
    PartiallyFilled,
    Expired,
    Rejected,
}

/// Market status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
#[serde(rename_all = "lowercase")]
pub enum MarketStatus {
    Active,
    Closed,
    Resolved,
    Paused,
}

/// Market type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketType {
    Binary,
    MultiOutcome { num_outcomes: u8 },
    Scalar { min: Decimal, max: Decimal },
}

/// Represents a prediction market
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub id: String,
    pub condition_id: ConditionId,
    pub slug: String,
    pub question: String,
    pub description: Option<String>,
    pub market_type: MarketType,
    pub status: MarketStatus,
    pub outcomes: Vec<OutcomeInfo>,
    pub end_date: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub volume: Decimal,
    pub liquidity: Decimal,
    pub fee_rate: Decimal,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub related_markets: Vec<String>,
}

impl Market {
    /// Check if this is a binary market
    #[inline]
    pub fn is_binary(&self) -> bool {
        matches!(self.market_type, MarketType::Binary)
    }

    /// Check if market is tradeable
    #[inline]
    pub fn is_active(&self) -> bool {
        self.status == MarketStatus::Active
    }

    /// Get the YES token ID for binary markets
    pub fn yes_token_id(&self) -> Option<&str> {
        self.outcomes.iter()
            .find(|o| o.outcome == Outcome::Yes)
            .map(|o| o.token_id.as_str())
    }

    /// Get the NO token ID for binary markets
    pub fn no_token_id(&self) -> Option<&str> {
        self.outcomes.iter()
            .find(|o| o.outcome == Outcome::No)
            .map(|o| o.token_id.as_str())
    }
}

/// Information about a market outcome
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutcomeInfo {
    pub outcome: Outcome,
    pub token_id: TokenId,
    pub price: Price,
    pub name: String,
}

/// A price level in the orderbook
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(C)]
pub struct PriceLevel {
    pub price: Price,
    pub quantity: Quantity,
    pub order_count: u32,
}

impl PriceLevel {
    #[inline]
    pub fn new(price: Price, quantity: Quantity, order_count: u32) -> Self {
        Self { price, quantity, order_count }
    }

    #[inline]
    pub fn notional(&self) -> Decimal {
        self.price.0 * self.quantity.0
    }
}

/// Orderbook snapshot for a single outcome
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookSide {
    /// Sorted price levels (best first)
    pub levels: SmallVec<[PriceLevel; 16]>,
    pub timestamp: DateTime<Utc>,
}

impl OrderbookSide {
    pub fn new() -> Self {
        Self {
            levels: SmallVec::new(),
            timestamp: Utc::now(),
        }
    }

    #[inline]
    pub fn best_price(&self) -> Option<Price> {
        self.levels.first().map(|l| l.price)
    }

    #[inline]
    pub fn best_quantity(&self) -> Option<Quantity> {
        self.levels.first().map(|l| l.quantity)
    }

    /// Calculate total quantity available up to a price
    pub fn depth_at_price(&self, price: Price, side: Side) -> Quantity {
        let total: Decimal = self.levels.iter()
            .take_while(|l| match side {
                Side::Buy => l.price >= price,
                Side::Sell => l.price <= price,
            })
            .map(|l| l.quantity.0)
            .sum();
        Quantity(total)
    }

    /// Calculate VWAP for a given quantity
    pub fn vwap(&self, quantity: Quantity) -> Option<Price> {
        let mut remaining = quantity.0;
        let mut notional = Decimal::ZERO;
        let mut filled = Decimal::ZERO;

        for level in &self.levels {
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
}

impl Default for OrderbookSide {
    fn default() -> Self {
        Self::new()
    }
}

/// Complete orderbook for an outcome
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Orderbook {
    pub token_id: TokenId,
    pub outcome: Outcome,
    pub bids: OrderbookSide,
    pub asks: OrderbookSide,
    pub sequence: u64,
    pub timestamp: DateTime<Utc>,
}

impl Orderbook {
    pub fn new(token_id: TokenId, outcome: Outcome) -> Self {
        Self {
            token_id,
            outcome,
            bids: OrderbookSide::new(),
            asks: OrderbookSide::new(),
            sequence: 0,
            timestamp: Utc::now(),
        }
    }

    /// Get the mid price
    #[inline]
    pub fn mid_price(&self) -> Option<Price> {
        match (self.bids.best_price(), self.asks.best_price()) {
            (Some(bid), Some(ask)) => Price::new((bid.0 + ask.0) / Decimal::TWO),
            _ => None,
        }
    }

    /// Get the bid-ask spread
    #[inline]
    pub fn spread(&self) -> Option<Decimal> {
        match (self.bids.best_price(), self.asks.best_price()) {
            (Some(bid), Some(ask)) => Some(ask.0 - bid.0),
            _ => None,
        }
    }

    /// Get spread in basis points
    #[inline]
    pub fn spread_bps(&self) -> Option<Decimal> {
        match (self.mid_price(), self.spread()) {
            (Some(mid), Some(spread)) if mid.0 > Decimal::ZERO => {
                Some((spread / mid.0) * Decimal::from(10000))
            }
            _ => None,
        }
    }

    /// Check if orderbook has sufficient liquidity
    pub fn has_liquidity(&self, min_quantity: Quantity) -> bool {
        self.bids.best_quantity().map_or(false, |q| q >= min_quantity) &&
        self.asks.best_quantity().map_or(false, |q| q >= min_quantity)
    }
}

/// An order to be submitted
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub market_id: String,
    pub token_id: TokenId,
    pub side: Side,
    pub outcome: Outcome,
    pub price: Price,
    pub quantity: Quantity,
    pub order_type: OrderType,
    pub expiration: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub client_order_id: Option<String>,
}

impl Order {
    pub fn new(
        market_id: String,
        token_id: TokenId,
        side: Side,
        outcome: Outcome,
        price: Price,
        quantity: Quantity,
        order_type: OrderType,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            market_id,
            token_id,
            side,
            outcome,
            price,
            quantity,
            order_type,
            expiration: None,
            created_at: Utc::now(),
            client_order_id: Some(Uuid::new_v4().to_string()),
        }
    }

    /// Calculate notional value
    #[inline]
    pub fn notional(&self) -> Decimal {
        self.price.0 * self.quantity.0
    }
}

/// A filled or partially filled order
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub id: TradeId,
    pub order_id: OrderId,
    pub market_id: String,
    pub token_id: TokenId,
    pub side: Side,
    pub outcome: Outcome,
    pub price: Price,
    pub quantity: Quantity,
    pub fee: Decimal,
    pub timestamp: DateTime<Utc>,
    pub tx_hash: Option<String>,
}

impl Trade {
    /// Calculate net proceeds/cost
    pub fn net_amount(&self) -> Decimal {
        let gross = self.price.0 * self.quantity.0;
        match self.side {
            Side::Buy => -(gross + self.fee),
            Side::Sell => gross - self.fee,
        }
    }
}

/// Position in a market
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub market_id: String,
    pub token_id: TokenId,
    pub outcome: Outcome,
    pub quantity: Quantity,
    pub average_entry_price: Price,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub last_updated: DateTime<Utc>,
}

impl Position {
    pub fn new(market_id: String, token_id: TokenId, outcome: Outcome) -> Self {
        Self {
            market_id,
            token_id,
            outcome,
            quantity: Quantity::ZERO,
            average_entry_price: Price::ZERO,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            last_updated: Utc::now(),
        }
    }

    /// Update position with a trade
    pub fn apply_trade(&mut self, trade: &Trade) {
        let trade_qty = trade.quantity.0;
        let trade_price = trade.price.0;

        match trade.side {
            Side::Buy => {
                // Increasing position
                let total_cost = self.average_entry_price.0 * self.quantity.0 + trade_price * trade_qty;
                let new_qty = self.quantity.0 + trade_qty;
                self.quantity = Quantity(new_qty);
                if new_qty > Decimal::ZERO {
                    self.average_entry_price = Price::new_unchecked(total_cost / new_qty);
                }
            }
            Side::Sell => {
                // Decreasing position
                let pnl = (trade_price - self.average_entry_price.0) * trade_qty.min(self.quantity.0);
                self.realized_pnl += pnl - trade.fee;
                self.quantity = Quantity((self.quantity.0 - trade_qty).max(Decimal::ZERO));
            }
        }

        self.last_updated = Utc::now();
    }

    /// Update unrealized PnL based on current market price
    pub fn update_unrealized_pnl(&mut self, current_price: Price) {
        if self.quantity.0 > Decimal::ZERO {
            self.unrealized_pnl = (current_price.0 - self.average_entry_price.0) * self.quantity.0;
        } else {
            self.unrealized_pnl = Decimal::ZERO;
        }
        self.last_updated = Utc::now();
    }

    /// Total PnL
    #[inline]
    pub fn total_pnl(&self) -> Decimal {
        self.realized_pnl + self.unrealized_pnl
    }

    /// Notional value at current average price
    #[inline]
    pub fn notional(&self) -> Decimal {
        self.average_entry_price.0 * self.quantity.0
    }
}

/// Wallet balance information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletBalance {
    pub usdc_balance: Decimal,
    pub allowance: Decimal,
    pub positions: Vec<Position>,
    pub last_updated: DateTime<Utc>,
}

impl WalletBalance {
    pub fn new(usdc_balance: Decimal) -> Self {
        Self {
            usdc_balance,
            allowance: Decimal::ZERO,
            positions: Vec::new(),
            last_updated: Utc::now(),
        }
    }

    /// Total portfolio value including positions
    pub fn total_value(&self, market_prices: &[(TokenId, Price)]) -> Decimal {
        let positions_value: Decimal = self.positions.iter()
            .filter_map(|pos| {
                market_prices.iter()
                    .find(|(tid, _)| *tid == pos.token_id)
                    .map(|(_, price)| price.0 * pos.quantity.0)
            })
            .sum();

        self.usdc_balance + positions_value
    }
}

/// Arbitrage opportunity detected by the system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitrageOpportunity {
    pub id: Uuid,
    pub opportunity_type: ArbitrageType,
    pub market_ids: Vec<String>,
    pub expected_profit: Decimal,
    pub expected_profit_pct: Decimal,
    pub required_capital: Decimal,
    pub legs: Vec<ArbitrageLeg>,
    pub confidence: f64,
    pub expires_at: DateTime<Utc>,
    pub detected_at: DateTime<Utc>,
    pub risk_score: f64,
}

impl ArbitrageOpportunity {
    /// Check if opportunity is still valid
    pub fn is_valid(&self) -> bool {
        Utc::now() < self.expires_at && self.expected_profit > Decimal::ZERO
    }

    /// Risk-adjusted return
    pub fn risk_adjusted_return(&self) -> Decimal {
        if self.risk_score > 0.0 {
            self.expected_profit_pct / Decimal::from_f64_retain(self.risk_score).unwrap_or(Decimal::ONE)
        } else {
            self.expected_profit_pct
        }
    }
}

/// Type of arbitrage opportunity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
pub enum ArbitrageType {
    /// YES + NO prices sum to != 1.0
    BinaryMispricing,
    /// Multi-outcome prices don't sum to 1.0
    MultiOutcomeMispricing,
    /// Related markets have inconsistent pricing
    CrossMarket,
    /// Time-based arbitrage (e.g., 5-min BTC markets)
    Temporal,
    /// Statistical arbitrage based on historical patterns
    Statistical,
}

/// A single leg of an arbitrage trade
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitrageLeg {
    pub market_id: String,
    pub token_id: TokenId,
    pub side: Side,
    pub outcome: Outcome,
    pub price: Price,
    pub quantity: Quantity,
    pub order_type: OrderType,
    pub priority: u8,
}

impl ArbitrageLeg {
    pub fn to_order(&self) -> Order {
        Order::new(
            self.market_id.clone(),
            self.token_id.clone(),
            self.side,
            self.outcome,
            self.price,
            self.quantity,
            self.order_type,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_price_creation() {
        assert!(Price::new(dec!(0.5)).is_some());
        assert!(Price::new(dec!(0.0)).is_some());
        assert!(Price::new(dec!(1.0)).is_some());
        assert!(Price::new(dec!(1.1)).is_none());
        assert!(Price::new(dec!(-0.1)).is_none());
    }

    #[test]
    fn test_price_complement() {
        let p = Price::new(dec!(0.3)).unwrap();
        assert_eq!(p.complement().0, dec!(0.7));
    }

    #[test]
    fn test_orderbook_spread() {
        let mut ob = Orderbook::new("token1".to_string(), Outcome::Yes);
        ob.bids.levels.push(PriceLevel::new(
            Price::new(dec!(0.48)).unwrap(),
            Quantity::new(dec!(100)).unwrap(),
            1,
        ));
        ob.asks.levels.push(PriceLevel::new(
            Price::new(dec!(0.52)).unwrap(),
            Quantity::new(dec!(100)).unwrap(),
            1,
        ));

        assert_eq!(ob.spread(), Some(dec!(0.04)));
        assert_eq!(ob.mid_price().map(|p| p.0), Some(dec!(0.50)));
    }

    #[test]
    fn test_position_pnl() {
        let mut pos = Position::new(
            "market1".to_string(),
            "token1".to_string(),
            Outcome::Yes,
        );

        // Buy 10 @ 0.40
        let buy_trade = Trade {
            id: "t1".to_string(),
            order_id: "o1".to_string(),
            market_id: "market1".to_string(),
            token_id: "token1".to_string(),
            side: Side::Buy,
            outcome: Outcome::Yes,
            price: Price::new(dec!(0.40)).unwrap(),
            quantity: Quantity::new(dec!(10)).unwrap(),
            fee: dec!(0.02),
            timestamp: Utc::now(),
            tx_hash: None,
        };
        pos.apply_trade(&buy_trade);

        assert_eq!(pos.quantity.0, dec!(10));
        assert_eq!(pos.average_entry_price.0, dec!(0.40));

        // Sell 5 @ 0.50
        let sell_trade = Trade {
            id: "t2".to_string(),
            order_id: "o2".to_string(),
            market_id: "market1".to_string(),
            token_id: "token1".to_string(),
            side: Side::Sell,
            outcome: Outcome::Yes,
            price: Price::new(dec!(0.50)).unwrap(),
            quantity: Quantity::new(dec!(5)).unwrap(),
            fee: dec!(0.01),
            timestamp: Utc::now(),
            tx_hash: None,
        };
        pos.apply_trade(&sell_trade);

        assert_eq!(pos.quantity.0, dec!(5));
        // PnL = (0.50 - 0.40) * 5 - 0.01 = 0.49
        assert_eq!(pos.realized_pnl, dec!(0.49));
    }
}
