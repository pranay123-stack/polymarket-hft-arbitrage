//! Orderbook aggregator for combining market data.
//!
//! Provides:
//! - Multi-token orderbook aggregation
//! - Cross-market spread analysis
//! - Binary market price reconciliation

use crate::core::error::Result;
use crate::core::types::*;
use crate::orderbook::engine::OrderbookEngine;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tracing::debug;

/// Aggregates orderbook data across multiple tokens/markets
pub struct OrderbookAggregator {
    engine: Arc<OrderbookEngine>,
}

impl OrderbookAggregator {
    /// Create a new orderbook aggregator
    pub fn new(engine: Arc<OrderbookEngine>) -> Self {
        Self { engine }
    }

    /// Get combined view of YES and NO orderbooks for a binary market
    pub fn get_binary_market_view(
        &self,
        yes_token_id: &str,
        no_token_id: &str,
    ) -> Option<BinaryMarketView> {
        let yes_ob = self.engine.get(yes_token_id)?;
        let no_ob = self.engine.get(no_token_id)?;

        let yes = yes_ob.read();
        let no = no_ob.read();

        Some(BinaryMarketView {
            yes_bid: yes.best_bid(),
            yes_ask: yes.best_ask(),
            yes_mid: yes.mid_price(),
            no_bid: no.best_bid(),
            no_ask: no.best_ask(),
            no_mid: no.mid_price(),
            implied_yes_from_no: no.best_bid().map(|p| p.complement()),
            implied_no_from_yes: yes.best_bid().map(|p| p.complement()),
            yes_spread: yes.spread(),
            no_spread: no.spread(),
            price_sum: Self::calculate_price_sum(yes.mid_price(), no.mid_price()),
            mispricing: Self::calculate_mispricing(yes.mid_price(), no.mid_price()),
        })
    }

    /// Calculate the sum of YES and NO mid prices
    fn calculate_price_sum(yes_mid: Option<Price>, no_mid: Option<Price>) -> Option<Decimal> {
        match (yes_mid, no_mid) {
            (Some(y), Some(n)) => Some(y.0 + n.0),
            _ => None,
        }
    }

    /// Calculate mispricing (deviation from 1.0)
    fn calculate_mispricing(yes_mid: Option<Price>, no_mid: Option<Price>) -> Option<Decimal> {
        Self::calculate_price_sum(yes_mid, no_mid).map(|sum| (sum - Decimal::ONE).abs())
    }

    /// Find arbitrage opportunities in binary markets
    pub fn find_binary_arbitrage(
        &self,
        yes_token_id: &str,
        no_token_id: &str,
        min_profit_pct: Decimal,
    ) -> Option<BinaryArbitrageOpportunity> {
        let view = self.get_binary_market_view(yes_token_id, no_token_id)?;

        // Check for mispricing above threshold
        let mispricing = view.mispricing?;
        if mispricing < min_profit_pct / dec!(100) {
            return None;
        }

        let price_sum = view.price_sum?;

        // Determine arbitrage direction
        if price_sum < Decimal::ONE {
            // Prices sum to < 1.0: Buy both YES and NO
            let yes_ask = view.yes_ask?;
            let no_ask = view.no_ask?;
            let total_cost = yes_ask.0 + no_ask.0;
            let profit = Decimal::ONE - total_cost;

            if profit > Decimal::ZERO {
                return Some(BinaryArbitrageOpportunity {
                    direction: BinaryArbitrageDirection::BuyBoth,
                    yes_price: yes_ask,
                    no_price: no_ask,
                    expected_profit: profit,
                    profit_pct: (profit / total_cost) * dec!(100),
                    price_sum: total_cost,
                });
            }
        } else if price_sum > Decimal::ONE {
            // Prices sum to > 1.0: Sell both YES and NO
            let yes_bid = view.yes_bid?;
            let no_bid = view.no_bid?;
            let total_proceeds = yes_bid.0 + no_bid.0;
            let profit = total_proceeds - Decimal::ONE;

            if profit > Decimal::ZERO {
                return Some(BinaryArbitrageOpportunity {
                    direction: BinaryArbitrageDirection::SellBoth,
                    yes_price: yes_bid,
                    no_price: no_bid,
                    expected_profit: profit,
                    profit_pct: (profit / Decimal::ONE) * dec!(100),
                    price_sum: total_proceeds,
                });
            }
        }

        None
    }

    /// Get liquidity summary across multiple tokens
    pub fn get_liquidity_summary(&self, token_ids: &[&str]) -> LiquiditySummary {
        let mut total_bid_liquidity = Decimal::ZERO;
        let mut total_ask_liquidity = Decimal::ZERO;
        let mut min_spread = None;
        let mut max_spread = None;
        let mut orderbook_count = 0;

        for token_id in token_ids {
            if let Some(ob) = self.engine.get(token_id) {
                let book = ob.read();
                let (bid_liq, ask_liq) = book.total_liquidity();
                total_bid_liquidity += bid_liq;
                total_ask_liquidity += ask_liq;

                if let Some(spread) = book.spread() {
                    min_spread = Some(min_spread.map_or(spread, |m: Decimal| m.min(spread)));
                    max_spread = Some(max_spread.map_or(spread, |m: Decimal| m.max(spread)));
                }

                orderbook_count += 1;
            }
        }

        LiquiditySummary {
            total_bid_liquidity,
            total_ask_liquidity,
            min_spread,
            max_spread,
            avg_spread: if orderbook_count > 0 && min_spread.is_some() && max_spread.is_some() {
                Some((min_spread.unwrap() + max_spread.unwrap()) / Decimal::TWO)
            } else {
                None
            },
            orderbook_count,
        }
    }

    /// Get cross-market price comparison
    pub fn compare_markets(
        &self,
        token_pairs: &[(&str, &str)], // (token_id_1, token_id_2)
    ) -> Vec<MarketComparison> {
        token_pairs
            .iter()
            .filter_map(|(t1, t2)| {
                let ob1 = self.engine.get(t1)?;
                let ob2 = self.engine.get(t2)?;

                let b1 = ob1.read();
                let b2 = ob2.read();

                let mid1 = b1.mid_price()?;
                let mid2 = b2.mid_price()?;

                Some(MarketComparison {
                    token_id_1: t1.to_string(),
                    token_id_2: t2.to_string(),
                    mid_1: mid1,
                    mid_2: mid2,
                    price_diff: (mid1.0 - mid2.0).abs(),
                    price_diff_pct: ((mid1.0 - mid2.0) / mid1.0).abs() * dec!(100),
                    spread_1: b1.spread(),
                    spread_2: b2.spread(),
                })
            })
            .collect()
    }
}

/// View of a binary market's orderbooks
#[derive(Debug, Clone)]
pub struct BinaryMarketView {
    pub yes_bid: Option<Price>,
    pub yes_ask: Option<Price>,
    pub yes_mid: Option<Price>,
    pub no_bid: Option<Price>,
    pub no_ask: Option<Price>,
    pub no_mid: Option<Price>,
    pub implied_yes_from_no: Option<Price>,
    pub implied_no_from_yes: Option<Price>,
    pub yes_spread: Option<Decimal>,
    pub no_spread: Option<Decimal>,
    pub price_sum: Option<Decimal>,
    pub mispricing: Option<Decimal>,
}

impl BinaryMarketView {
    /// Check if the market has valid prices
    pub fn is_valid(&self) -> bool {
        self.yes_mid.is_some() && self.no_mid.is_some()
    }

    /// Get combined spread
    pub fn combined_spread(&self) -> Option<Decimal> {
        match (self.yes_spread, self.no_spread) {
            (Some(y), Some(n)) => Some(y + n),
            _ => None,
        }
    }
}

/// Binary arbitrage opportunity
#[derive(Debug, Clone)]
pub struct BinaryArbitrageOpportunity {
    pub direction: BinaryArbitrageDirection,
    pub yes_price: Price,
    pub no_price: Price,
    pub expected_profit: Decimal,
    pub profit_pct: Decimal,
    pub price_sum: Decimal,
}

/// Direction of binary arbitrage
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryArbitrageDirection {
    /// Buy both YES and NO (prices sum to < 1.0)
    BuyBoth,
    /// Sell both YES and NO (prices sum to > 1.0)
    SellBoth,
}

/// Summary of liquidity across orderbooks
#[derive(Debug, Clone)]
pub struct LiquiditySummary {
    pub total_bid_liquidity: Decimal,
    pub total_ask_liquidity: Decimal,
    pub min_spread: Option<Decimal>,
    pub max_spread: Option<Decimal>,
    pub avg_spread: Option<Decimal>,
    pub orderbook_count: usize,
}

/// Comparison between two markets
#[derive(Debug, Clone)]
pub struct MarketComparison {
    pub token_id_1: String,
    pub token_id_2: String,
    pub mid_1: Price,
    pub mid_2: Price,
    pub price_diff: Decimal,
    pub price_diff_pct: Decimal,
    pub spread_1: Option<Decimal>,
    pub spread_2: Option<Decimal>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::events::EventBus;

    fn setup_test_engine() -> Arc<OrderbookEngine> {
        let event_bus = EventBus::new(100);
        let engine = Arc::new(OrderbookEngine::new(event_bus, 5000));

        // Add YES orderbook
        let mut yes_ob = Orderbook::new("yes_token".to_string(), Outcome::Yes);
        yes_ob.bids.levels.push(PriceLevel::new(
            Price::new(dec!(0.55)).unwrap(),
            Quantity(dec!(100)),
            1,
        ));
        yes_ob.asks.levels.push(PriceLevel::new(
            Price::new(dec!(0.57)).unwrap(),
            Quantity(dec!(100)),
            1,
        ));
        engine.update_snapshot(yes_ob).unwrap();

        // Add NO orderbook
        let mut no_ob = Orderbook::new("no_token".to_string(), Outcome::No);
        no_ob.bids.levels.push(PriceLevel::new(
            Price::new(dec!(0.43)).unwrap(),
            Quantity(dec!(100)),
            1,
        ));
        no_ob.asks.levels.push(PriceLevel::new(
            Price::new(dec!(0.45)).unwrap(),
            Quantity(dec!(100)),
            1,
        ));
        engine.update_snapshot(no_ob).unwrap();

        engine
    }

    #[test]
    fn test_binary_market_view() {
        let engine = setup_test_engine();
        let aggregator = OrderbookAggregator::new(engine);

        let view = aggregator
            .get_binary_market_view("yes_token", "no_token")
            .unwrap();

        assert!(view.is_valid());
        assert!(view.price_sum.is_some());

        // YES mid = 0.56, NO mid = 0.44, sum = 1.00
        let sum = view.price_sum.unwrap();
        assert!((sum - dec!(1.00)).abs() < dec!(0.001));
    }

    #[test]
    fn test_binary_arbitrage_detection() {
        let event_bus = EventBus::new(100);
        let engine = Arc::new(OrderbookEngine::new(event_bus, 5000));

        // Create mispriced market (sum < 1.0)
        let mut yes_ob = Orderbook::new("yes_token".to_string(), Outcome::Yes);
        yes_ob.asks.levels.push(PriceLevel::new(
            Price::new(dec!(0.45)).unwrap(),
            Quantity(dec!(100)),
            1,
        ));
        engine.update_snapshot(yes_ob).unwrap();

        let mut no_ob = Orderbook::new("no_token".to_string(), Outcome::No);
        no_ob.asks.levels.push(PriceLevel::new(
            Price::new(dec!(0.45)).unwrap(),
            Quantity(dec!(100)),
            1,
        ));
        engine.update_snapshot(no_ob).unwrap();

        let aggregator = OrderbookAggregator::new(engine);

        let arb = aggregator.find_binary_arbitrage("yes_token", "no_token", dec!(1));

        assert!(arb.is_some());
        let arb = arb.unwrap();
        assert_eq!(arb.direction, BinaryArbitrageDirection::BuyBoth);
        assert_eq!(arb.price_sum, dec!(0.90));
        assert_eq!(arb.expected_profit, dec!(0.10));
    }
}
