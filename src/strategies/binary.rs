//! Binary market arbitrage strategy.
//!
//! Detects mispricing in binary markets where YES + NO prices should sum to 1.0.
//! Opportunities exist when:
//! - YES_ask + NO_ask < 1.0 (buy both, guaranteed $1 payout)
//! - YES_bid + NO_bid > 1.0 (sell both, receive > $1)

use crate::core::constants::*;
use crate::core::types::*;
use crate::orderbook::OrderbookEngine;
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tracing::{debug, instrument};
use uuid::Uuid;

/// Binary market arbitrage detection strategy
pub struct BinaryArbitrageStrategy {
    orderbook_engine: Arc<OrderbookEngine>,
    threshold: Decimal,
}

impl BinaryArbitrageStrategy {
    /// Create a new binary arbitrage strategy
    pub fn new(orderbook_engine: Arc<OrderbookEngine>, threshold: Decimal) -> Self {
        Self {
            orderbook_engine,
            threshold,
        }
    }

    /// Detect arbitrage opportunity in a binary market
    #[instrument(skip(self, market), fields(market_id = %market.id))]
    pub async fn detect(&self, market: &Market) -> Option<ArbitrageOpportunity> {
        if !market.is_binary() || market.outcomes.len() != 2 {
            return None;
        }

        let yes_info = market.outcomes.iter().find(|o| o.outcome == Outcome::Yes)?;
        let no_info = market.outcomes.iter().find(|o| o.outcome == Outcome::No)?;

        let yes_ob = self.orderbook_engine.get(&yes_info.token_id)?;
        let no_ob = self.orderbook_engine.get(&no_info.token_id)?;

        let yes_book = yes_ob.read();
        let no_book = no_ob.read();

        // Check for staleness
        if yes_book.is_stale(ORDERBOOK_STALE_THRESHOLD_MS)
            || no_book.is_stale(ORDERBOOK_STALE_THRESHOLD_MS)
        {
            debug!("Orderbook stale, skipping");
            return None;
        }

        // Get best prices
        let yes_bid = yes_book.best_bid()?;
        let yes_ask = yes_book.best_ask()?;
        let no_bid = no_book.best_bid()?;
        let no_ask = no_book.best_ask()?;

        // Check for buy-both arbitrage (asks sum to < 1.0)
        let ask_sum = yes_ask.0 + no_ask.0;
        if ask_sum < Decimal::ONE - self.threshold {
            return self.create_buy_both_opportunity(
                market,
                yes_info,
                no_info,
                yes_ask,
                no_ask,
                &yes_book,
                &no_book,
            );
        }

        // Check for sell-both arbitrage (bids sum to > 1.0)
        let bid_sum = yes_bid.0 + no_bid.0;
        if bid_sum > Decimal::ONE + self.threshold {
            return self.create_sell_both_opportunity(
                market,
                yes_info,
                no_info,
                yes_bid,
                no_bid,
                &yes_book,
                &no_book,
            );
        }

        None
    }

    /// Create a buy-both arbitrage opportunity
    fn create_buy_both_opportunity(
        &self,
        market: &Market,
        yes_info: &OutcomeInfo,
        no_info: &OutcomeInfo,
        yes_ask: Price,
        no_ask: Price,
        yes_book: &crate::orderbook::engine::ManagedOrderbook,
        no_book: &crate::orderbook::engine::ManagedOrderbook,
    ) -> Option<ArbitrageOpportunity> {
        let total_cost = yes_ask.0 + no_ask.0;
        let gross_profit = Decimal::ONE - total_cost;

        // Account for fees (2% per trade, so 4% total for 2 trades)
        let fees = total_cost * *FEE_RATE * dec!(2);
        let net_profit = gross_profit - fees;

        if net_profit <= Decimal::ZERO {
            debug!("Profit negative after fees: {}", net_profit);
            return None;
        }

        // Determine max quantity based on available liquidity
        let yes_qty = yes_book.best_ask_level()?.quantity;
        let no_qty = no_book.best_ask_level()?.quantity;
        let max_quantity = yes_qty.0.min(no_qty.0);

        if max_quantity < *MIN_ORDER_SIZE {
            debug!("Insufficient liquidity: {}", max_quantity);
            return None;
        }

        let required_capital = total_cost * max_quantity;
        let expected_profit = net_profit * max_quantity;
        let profit_pct = (net_profit / total_cost) * dec!(100);

        // Calculate risk score (lower is better)
        let risk_score = self.calculate_risk_score(
            yes_book.spread(),
            no_book.spread(),
            max_quantity,
        );

        let legs = vec![
            ArbitrageLeg {
                market_id: market.id.clone(),
                token_id: yes_info.token_id.clone(),
                side: Side::Buy,
                outcome: Outcome::Yes,
                price: yes_ask,
                quantity: Quantity(max_quantity),
                order_type: OrderType::Gtc,
                priority: 1,
            },
            ArbitrageLeg {
                market_id: market.id.clone(),
                token_id: no_info.token_id.clone(),
                side: Side::Buy,
                outcome: Outcome::No,
                price: no_ask,
                quantity: Quantity(max_quantity),
                order_type: OrderType::Gtc,
                priority: 1,
            },
        ];

        Some(ArbitrageOpportunity {
            id: Uuid::new_v4(),
            opportunity_type: ArbitrageType::BinaryMispricing,
            market_ids: vec![market.id.clone()],
            expected_profit,
            expected_profit_pct: profit_pct,
            required_capital,
            legs,
            confidence: self.calculate_confidence(yes_book, no_book),
            expires_at: Utc::now() + Duration::milliseconds(ARBITRAGE_EXPIRATION_MS as i64),
            detected_at: Utc::now(),
            risk_score,
        })
    }

    /// Create a sell-both arbitrage opportunity
    fn create_sell_both_opportunity(
        &self,
        market: &Market,
        yes_info: &OutcomeInfo,
        no_info: &OutcomeInfo,
        yes_bid: Price,
        no_bid: Price,
        yes_book: &crate::orderbook::engine::ManagedOrderbook,
        no_book: &crate::orderbook::engine::ManagedOrderbook,
    ) -> Option<ArbitrageOpportunity> {
        let total_proceeds = yes_bid.0 + no_bid.0;
        let gross_profit = total_proceeds - Decimal::ONE;

        // Account for fees
        let fees = total_proceeds * *FEE_RATE * dec!(2);
        let net_profit = gross_profit - fees;

        if net_profit <= Decimal::ZERO {
            debug!("Profit negative after fees: {}", net_profit);
            return None;
        }

        // Determine max quantity based on available liquidity
        let yes_qty = yes_book.best_bid_level()?.quantity;
        let no_qty = no_book.best_bid_level()?.quantity;
        let max_quantity = yes_qty.0.min(no_qty.0);

        if max_quantity < *MIN_ORDER_SIZE {
            debug!("Insufficient liquidity: {}", max_quantity);
            return None;
        }

        // For selling, we need to own the tokens first
        // This would require checking position or buying first
        let required_capital = max_quantity; // Need to own 1 share each
        let expected_profit = net_profit * max_quantity;
        let profit_pct = (net_profit / Decimal::ONE) * dec!(100);

        let risk_score = self.calculate_risk_score(
            yes_book.spread(),
            no_book.spread(),
            max_quantity,
        );

        let legs = vec![
            ArbitrageLeg {
                market_id: market.id.clone(),
                token_id: yes_info.token_id.clone(),
                side: Side::Sell,
                outcome: Outcome::Yes,
                price: yes_bid,
                quantity: Quantity(max_quantity),
                order_type: OrderType::Gtc,
                priority: 1,
            },
            ArbitrageLeg {
                market_id: market.id.clone(),
                token_id: no_info.token_id.clone(),
                side: Side::Sell,
                outcome: Outcome::No,
                price: no_bid,
                quantity: Quantity(max_quantity),
                order_type: OrderType::Gtc,
                priority: 1,
            },
        ];

        Some(ArbitrageOpportunity {
            id: Uuid::new_v4(),
            opportunity_type: ArbitrageType::BinaryMispricing,
            market_ids: vec![market.id.clone()],
            expected_profit,
            expected_profit_pct: profit_pct,
            required_capital,
            legs,
            confidence: self.calculate_confidence(yes_book, no_book),
            expires_at: Utc::now() + Duration::milliseconds(ARBITRAGE_EXPIRATION_MS as i64),
            detected_at: Utc::now(),
            risk_score,
        })
    }

    /// Calculate confidence score based on orderbook quality
    fn calculate_confidence(
        &self,
        yes_book: &crate::orderbook::engine::ManagedOrderbook,
        no_book: &crate::orderbook::engine::ManagedOrderbook,
    ) -> f64 {
        let mut confidence = 1.0;

        // Reduce confidence for wide spreads
        if let Some(yes_spread) = yes_book.spread_bps() {
            if yes_spread > dec!(500) {
                confidence *= 0.8;
            } else if yes_spread > dec!(200) {
                confidence *= 0.9;
            }
        }

        if let Some(no_spread) = no_book.spread_bps() {
            if no_spread > dec!(500) {
                confidence *= 0.8;
            } else if no_spread > dec!(200) {
                confidence *= 0.9;
            }
        }

        // Reduce confidence for thin books
        let (yes_bid_liq, yes_ask_liq) = yes_book.total_liquidity();
        let (no_bid_liq, no_ask_liq) = no_book.total_liquidity();

        let min_liq = yes_bid_liq
            .min(yes_ask_liq)
            .min(no_bid_liq)
            .min(no_ask_liq);

        if min_liq < dec!(50) {
            confidence *= 0.7;
        } else if min_liq < dec!(100) {
            confidence *= 0.85;
        }

        confidence.max(0.1)
    }

    /// Calculate risk score (0-1, lower is better)
    fn calculate_risk_score(
        &self,
        yes_spread: Option<Decimal>,
        no_spread: Option<Decimal>,
        quantity: Decimal,
    ) -> f64 {
        let mut risk = 0.0;

        // Spread risk
        if let Some(spread) = yes_spread {
            risk += spread.to_f64().unwrap_or(0.0) * 5.0;
        }
        if let Some(spread) = no_spread {
            risk += spread.to_f64().unwrap_or(0.0) * 5.0;
        }

        // Size risk (larger orders have more risk)
        risk += (quantity / dec!(1000)).to_f64().unwrap_or(0.0) * 0.2;

        risk.min(1.0)
    }
}

impl std::fmt::Debug for BinaryArbitrageStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinaryArbitrageStrategy")
            .field("threshold", &self.threshold)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::events::EventBus;

    fn create_test_market() -> Market {
        Market {
            id: "test-market".to_string(),
            condition_id: "0xabc".to_string(),
            slug: "test-binary".to_string(),
            question: "Test?".to_string(),
            description: None,
            market_type: MarketType::Binary,
            status: MarketStatus::Active,
            outcomes: vec![
                OutcomeInfo {
                    outcome: Outcome::Yes,
                    token_id: "yes_token".to_string(),
                    price: Price::new(dec!(0.45)).unwrap(),
                    name: "Yes".to_string(),
                },
                OutcomeInfo {
                    outcome: Outcome::No,
                    token_id: "no_token".to_string(),
                    price: Price::new(dec!(0.45)).unwrap(),
                    name: "No".to_string(),
                },
            ],
            end_date: None,
            created_at: Utc::now(),
            volume: dec!(10000),
            liquidity: dec!(5000),
            fee_rate: dec!(0.02),
            tags: vec![],
            related_markets: vec![],
        }
    }

    #[tokio::test]
    async fn test_buy_both_detection() {
        let event_bus = EventBus::new(100);
        let engine = Arc::new(OrderbookEngine::new(event_bus, 5000));

        // Create mispriced orderbooks (asks sum to 0.90 < 1.0)
        let mut yes_ob = Orderbook::new("yes_token".to_string(), Outcome::Yes);
        yes_ob.bids.levels.push(PriceLevel::new(
            Price::new(dec!(0.43)).unwrap(),
            Quantity(dec!(100)),
            1,
        ));
        yes_ob.asks.levels.push(PriceLevel::new(
            Price::new(dec!(0.45)).unwrap(),
            Quantity(dec!(100)),
            1,
        ));
        engine.update_snapshot(yes_ob).unwrap();

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

        let strategy = BinaryArbitrageStrategy::new(engine, dec!(0.02));
        let market = create_test_market();

        let opportunity = strategy.detect(&market).await;

        assert!(opportunity.is_some());
        let opp = opportunity.unwrap();
        assert_eq!(opp.opportunity_type, ArbitrageType::BinaryMispricing);
        assert_eq!(opp.legs.len(), 2);
        assert!(opp.expected_profit > Decimal::ZERO);
    }
}
