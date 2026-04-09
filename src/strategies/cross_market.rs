//! Cross-market arbitrage strategy.
//!
//! Detects pricing inconsistencies between related markets.

use crate::core::constants::*;
use crate::core::types::*;
use crate::orderbook::OrderbookEngine;
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tracing::{debug, instrument};
use uuid::Uuid;

/// Cross-market arbitrage detection strategy
pub struct CrossMarketStrategy {
    orderbook_engine: Arc<OrderbookEngine>,
    threshold: Decimal,
}

impl CrossMarketStrategy {
    /// Create a new cross-market strategy
    pub fn new(orderbook_engine: Arc<OrderbookEngine>, threshold: Decimal) -> Self {
        Self {
            orderbook_engine,
            threshold,
        }
    }

    /// Detect arbitrage opportunities in a group of related markets
    #[instrument(skip(self, markets))]
    pub async fn detect_in_group(&self, markets: &[&Market]) -> Option<Vec<ArbitrageOpportunity>> {
        if markets.len() < 2 {
            return None;
        }

        let mut opportunities = Vec::new();

        // Compare each pair of markets
        for i in 0..markets.len() {
            for j in (i + 1)..markets.len() {
                if let Some(opp) = self.compare_markets(markets[i], markets[j]).await {
                    opportunities.push(opp);
                }
            }
        }

        if opportunities.is_empty() {
            None
        } else {
            Some(opportunities)
        }
    }

    /// Compare two related markets for arbitrage
    async fn compare_markets(&self, market_a: &Market, market_b: &Market) -> Option<ArbitrageOpportunity> {
        // Only compare binary markets for simplicity
        if !market_a.is_binary() || !market_b.is_binary() {
            return None;
        }

        // Get YES prices for both markets
        let a_yes = market_a.outcomes.iter().find(|o| o.outcome == Outcome::Yes)?;
        let b_yes = market_b.outcomes.iter().find(|o| o.outcome == Outcome::Yes)?;

        let a_ob = self.orderbook_engine.get(&a_yes.token_id)?;
        let b_ob = self.orderbook_engine.get(&b_yes.token_id)?;

        let a_book = a_ob.read();
        let b_book = b_ob.read();

        // Check staleness
        if a_book.is_stale(ORDERBOOK_STALE_THRESHOLD_MS)
            || b_book.is_stale(ORDERBOOK_STALE_THRESHOLD_MS)
        {
            return None;
        }

        let a_mid = a_book.mid_price()?;
        let b_mid = b_book.mid_price()?;

        // Calculate price difference
        let price_diff = (a_mid.0 - b_mid.0).abs();
        let price_diff_pct = price_diff / a_mid.0.max(b_mid.0) * dec!(100);

        if price_diff_pct < self.threshold * dec!(100) {
            return None;
        }

        // Determine which market to buy and which to sell
        let (buy_market, sell_market, buy_info, sell_info) = if a_mid.0 < b_mid.0 {
            (market_a, market_b, a_yes, b_yes)
        } else {
            (market_b, market_a, b_yes, a_yes)
        };

        // Get execution prices
        let buy_ob = self.orderbook_engine.get(&buy_info.token_id)?;
        let sell_ob = self.orderbook_engine.get(&sell_info.token_id)?;

        let buy_book = buy_ob.read();
        let sell_book = sell_ob.read();

        let buy_price = buy_book.best_ask()?;
        let sell_price = sell_book.best_bid()?;

        // Calculate profit
        let gross_profit = sell_price.0 - buy_price.0;
        let fees = (buy_price.0 + sell_price.0) * *FEE_RATE;
        let net_profit = gross_profit - fees;

        if net_profit <= Decimal::ZERO {
            return None;
        }

        // Get available quantity
        let buy_qty = buy_book.best_ask_level()?.quantity;
        let sell_qty = sell_book.best_bid_level()?.quantity;
        let max_qty = buy_qty.0.min(sell_qty.0);

        if max_qty < *MIN_ORDER_SIZE {
            return None;
        }

        let required_capital = buy_price.0 * max_qty;
        let expected_profit = net_profit * max_qty;
        let profit_pct = (net_profit / buy_price.0) * dec!(100);

        let legs = vec![
            ArbitrageLeg {
                market_id: buy_market.id.clone(),
                token_id: buy_info.token_id.clone(),
                side: Side::Buy,
                outcome: Outcome::Yes,
                price: buy_price,
                quantity: Quantity(max_qty),
                order_type: OrderType::Gtc,
                priority: 1,
            },
            ArbitrageLeg {
                market_id: sell_market.id.clone(),
                token_id: sell_info.token_id.clone(),
                side: Side::Sell,
                outcome: Outcome::Yes,
                price: sell_price,
                quantity: Quantity(max_qty),
                order_type: OrderType::Gtc,
                priority: 2,
            },
        ];

        Some(ArbitrageOpportunity {
            id: Uuid::new_v4(),
            opportunity_type: ArbitrageType::CrossMarket,
            market_ids: vec![buy_market.id.clone(), sell_market.id.clone()],
            expected_profit,
            expected_profit_pct: profit_pct,
            required_capital,
            legs,
            confidence: self.calculate_confidence(&buy_book, &sell_book),
            expires_at: Utc::now() + Duration::milliseconds(ARBITRAGE_EXPIRATION_MS as i64),
            detected_at: Utc::now(),
            risk_score: self.calculate_risk_score(price_diff_pct, max_qty),
        })
    }

    /// Calculate confidence score
    fn calculate_confidence(
        &self,
        buy_book: &crate::orderbook::engine::ManagedOrderbook,
        sell_book: &crate::orderbook::engine::ManagedOrderbook,
    ) -> f64 {
        let mut confidence = 0.8; // Base confidence for cross-market

        // Adjust for spread
        if let Some(spread) = buy_book.spread_bps() {
            if spread > dec!(300) {
                confidence *= 0.85;
            }
        }
        if let Some(spread) = sell_book.spread_bps() {
            if spread > dec!(300) {
                confidence *= 0.85;
            }
        }

        // Adjust for liquidity
        let (buy_bid_liq, buy_ask_liq) = buy_book.total_liquidity();
        let (sell_bid_liq, sell_ask_liq) = sell_book.total_liquidity();

        let min_liq = buy_ask_liq.min(sell_bid_liq);
        if min_liq < dec!(100) {
            confidence *= 0.8;
        }

        confidence.max(0.1)
    }

    /// Calculate risk score
    fn calculate_risk_score(&self, price_diff_pct: Decimal, quantity: Decimal) -> f64 {
        // Cross-market has inherent correlation risk
        let mut risk = 0.3;

        // Very large price differences might indicate stale data
        if price_diff_pct > dec!(20) {
            risk += 0.3;
        }

        // Larger quantities = more execution risk
        risk += (quantity / dec!(1000)).to_f64().unwrap_or(0.0) * 0.1;

        risk.min(1.0)
    }
}

impl std::fmt::Debug for CrossMarketStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrossMarketStrategy")
            .field("threshold", &self.threshold)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::events::EventBus;

    fn create_test_markets() -> (Market, Market) {
        let market_a = Market {
            id: "market-a".to_string(),
            condition_id: "0xa".to_string(),
            slug: "will-x-happen".to_string(),
            question: "Will X happen?".to_string(),
            description: None,
            market_type: MarketType::Binary,
            status: MarketStatus::Active,
            outcomes: vec![
                OutcomeInfo {
                    outcome: Outcome::Yes,
                    token_id: "a_yes".to_string(),
                    price: Price::new(dec!(0.40)).unwrap(),
                    name: "Yes".to_string(),
                },
                OutcomeInfo {
                    outcome: Outcome::No,
                    token_id: "a_no".to_string(),
                    price: Price::new(dec!(0.60)).unwrap(),
                    name: "No".to_string(),
                },
            ],
            end_date: None,
            created_at: Utc::now(),
            volume: dec!(10000),
            liquidity: dec!(5000),
            fee_rate: dec!(0.02),
            tags: vec!["topic".to_string()],
            related_markets: vec![],
        };

        let market_b = Market {
            id: "market-b".to_string(),
            condition_id: "0xb".to_string(),
            slug: "will-x-occur".to_string(),
            question: "Will X occur?".to_string(),
            description: None,
            market_type: MarketType::Binary,
            status: MarketStatus::Active,
            outcomes: vec![
                OutcomeInfo {
                    outcome: Outcome::Yes,
                    token_id: "b_yes".to_string(),
                    price: Price::new(dec!(0.50)).unwrap(),
                    name: "Yes".to_string(),
                },
                OutcomeInfo {
                    outcome: Outcome::No,
                    token_id: "b_no".to_string(),
                    price: Price::new(dec!(0.50)).unwrap(),
                    name: "No".to_string(),
                },
            ],
            end_date: None,
            created_at: Utc::now(),
            volume: dec!(10000),
            liquidity: dec!(5000),
            fee_rate: dec!(0.02),
            tags: vec!["topic".to_string()],
            related_markets: vec![],
        };

        (market_a, market_b)
    }

    #[tokio::test]
    async fn test_cross_market_detection() {
        let event_bus = EventBus::new(100);
        let engine = Arc::new(OrderbookEngine::new(event_bus, 5000));

        // Market A: YES at 0.40
        let mut a_yes_ob = Orderbook::new("a_yes".to_string(), Outcome::Yes);
        a_yes_ob.bids.levels.push(PriceLevel::new(
            Price::new(dec!(0.38)).unwrap(),
            Quantity(dec!(100)),
            1,
        ));
        a_yes_ob.asks.levels.push(PriceLevel::new(
            Price::new(dec!(0.40)).unwrap(),
            Quantity(dec!(100)),
            1,
        ));
        engine.update_snapshot(a_yes_ob).unwrap();

        // Market B: YES at 0.55 (10% higher)
        let mut b_yes_ob = Orderbook::new("b_yes".to_string(), Outcome::Yes);
        b_yes_ob.bids.levels.push(PriceLevel::new(
            Price::new(dec!(0.55)).unwrap(),
            Quantity(dec!(100)),
            1,
        ));
        b_yes_ob.asks.levels.push(PriceLevel::new(
            Price::new(dec!(0.57)).unwrap(),
            Quantity(dec!(100)),
            1,
        ));
        engine.update_snapshot(b_yes_ob).unwrap();

        let strategy = CrossMarketStrategy::new(engine, dec!(0.05));
        let (market_a, market_b) = create_test_markets();

        let opportunities = strategy.detect_in_group(&[&market_a, &market_b]).await;

        assert!(opportunities.is_some());
        let opps = opportunities.unwrap();
        assert!(!opps.is_empty());
        assert_eq!(opps[0].opportunity_type, ArbitrageType::CrossMarket);
    }
}
