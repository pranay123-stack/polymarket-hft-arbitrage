//! Multi-outcome market arbitrage strategy.
//!
//! Detects mispricing in markets with more than 2 outcomes where
//! the sum of all outcome prices should equal 1.0.

use crate::core::constants::*;
use crate::core::types::*;
use crate::orderbook::OrderbookEngine;
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tracing::{debug, instrument};
use uuid::Uuid;

/// Multi-outcome arbitrage detection strategy
pub struct MultiOutcomeStrategy {
    orderbook_engine: Arc<OrderbookEngine>,
}

impl MultiOutcomeStrategy {
    /// Create a new multi-outcome strategy
    pub fn new(orderbook_engine: Arc<OrderbookEngine>) -> Self {
        Self { orderbook_engine }
    }

    /// Detect arbitrage opportunity in a multi-outcome market
    #[instrument(skip(self, market), fields(market_id = %market.id))]
    pub async fn detect(&self, market: &Market) -> Option<ArbitrageOpportunity> {
        // Only handle multi-outcome markets
        let num_outcomes = match market.market_type {
            MarketType::MultiOutcome { num_outcomes } if num_outcomes > 2 => num_outcomes,
            _ => return None,
        };

        // Collect orderbook data for all outcomes
        let mut outcome_data = Vec::new();

        for outcome_info in &market.outcomes {
            let ob = self.orderbook_engine.get(&outcome_info.token_id)?;
            let book = ob.read();

            if book.is_stale(ORDERBOOK_STALE_THRESHOLD_MS) {
                debug!("Orderbook stale for {}", outcome_info.token_id);
                return None;
            }

            let bid = book.best_bid();
            let ask = book.best_ask();
            let bid_qty = book.best_bid_level().map(|l| l.quantity);
            let ask_qty = book.best_ask_level().map(|l| l.quantity);

            outcome_data.push(OutcomeData {
                info: outcome_info.clone(),
                bid,
                ask,
                bid_qty,
                ask_qty,
            });
        }

        // Check for buy-all arbitrage (all asks sum to < 1.0)
        if let Some(opp) = self.check_buy_all(market, &outcome_data) {
            return Some(opp);
        }

        // Check for partial arbitrage (subset of outcomes mispriced)
        if let Some(opp) = self.check_partial_arbitrage(market, &outcome_data) {
            return Some(opp);
        }

        None
    }

    /// Check if buying all outcomes is profitable
    fn check_buy_all(
        &self,
        market: &Market,
        outcome_data: &[OutcomeData],
    ) -> Option<ArbitrageOpportunity> {
        // Sum of all ask prices
        let ask_sum: Decimal = outcome_data
            .iter()
            .filter_map(|d| d.ask.map(|p| p.0))
            .sum();

        // Check if we have prices for all outcomes
        if outcome_data.iter().any(|d| d.ask.is_none()) {
            return None;
        }

        // Calculate profit potential
        let gross_profit = Decimal::ONE - ask_sum;

        // Account for fees (2% per trade × number of outcomes)
        let num_trades = Decimal::from(outcome_data.len() as u32);
        let fees = ask_sum * *FEE_RATE * num_trades;
        let net_profit = gross_profit - fees;

        if net_profit <= dec!(0.01) {
            // Minimum profit threshold
            return None;
        }

        // Find minimum available quantity
        let min_qty = outcome_data
            .iter()
            .filter_map(|d| d.ask_qty.map(|q| q.0))
            .min()?;

        if min_qty < *MIN_ORDER_SIZE {
            debug!("Insufficient liquidity: {}", min_qty);
            return None;
        }

        let required_capital = ask_sum * min_qty;
        let expected_profit = net_profit * min_qty;
        let profit_pct = (net_profit / ask_sum) * dec!(100);

        // Create legs for all outcomes
        let legs: Vec<ArbitrageLeg> = outcome_data
            .iter()
            .enumerate()
            .map(|(i, data)| ArbitrageLeg {
                market_id: market.id.clone(),
                token_id: data.info.token_id.clone(),
                side: Side::Buy,
                outcome: data.info.outcome,
                price: data.ask.unwrap(),
                quantity: Quantity(min_qty),
                order_type: OrderType::Gtc,
                priority: (i + 1) as u8,
            })
            .collect();

        Some(ArbitrageOpportunity {
            id: Uuid::new_v4(),
            opportunity_type: ArbitrageType::MultiOutcomeMispricing,
            market_ids: vec![market.id.clone()],
            expected_profit,
            expected_profit_pct: profit_pct,
            required_capital,
            legs,
            confidence: self.calculate_confidence(outcome_data),
            expires_at: Utc::now() + Duration::milliseconds(ARBITRAGE_EXPIRATION_MS as i64),
            detected_at: Utc::now(),
            risk_score: self.calculate_risk_score(outcome_data),
        })
    }

    /// Check for partial arbitrage (complementary outcomes mispriced)
    fn check_partial_arbitrage(
        &self,
        market: &Market,
        outcome_data: &[OutcomeData],
    ) -> Option<ArbitrageOpportunity> {
        // For markets with many outcomes, check if any subset
        // that should sum to 1 is mispriced

        // This is more complex and market-specific
        // For now, we'll implement a simple check for "rest of field" scenarios

        // If one outcome has very high probability, others combined should equal complement
        let high_prob_threshold = dec!(0.7);

        for (i, data) in outcome_data.iter().enumerate() {
            if let Some(bid) = data.bid {
                if bid.0 >= high_prob_threshold {
                    // This outcome has high probability
                    // Check if we can profit by buying all others
                    let other_ask_sum: Decimal = outcome_data
                        .iter()
                        .enumerate()
                        .filter(|(j, _)| *j != i)
                        .filter_map(|(_, d)| d.ask.map(|p| p.0))
                        .sum();

                    let implied_other_prob = Decimal::ONE - bid.0;

                    if other_ask_sum < implied_other_prob - dec!(0.05) {
                        // Potential arbitrage: buy all other outcomes
                        return self.create_partial_opportunity(
                            market,
                            outcome_data,
                            i,
                            other_ask_sum,
                        );
                    }
                }
            }
        }

        None
    }

    /// Create opportunity for partial arbitrage
    fn create_partial_opportunity(
        &self,
        market: &Market,
        outcome_data: &[OutcomeData],
        exclude_idx: usize,
        total_cost: Decimal,
    ) -> Option<ArbitrageOpportunity> {
        let excluded = &outcome_data[exclude_idx];
        let implied_value = Decimal::ONE - excluded.bid?.0;

        let gross_profit = implied_value - total_cost;
        let num_trades = Decimal::from((outcome_data.len() - 1) as u32);
        let fees = total_cost * *FEE_RATE * num_trades;
        let net_profit = gross_profit - fees;

        if net_profit <= Decimal::ZERO {
            return None;
        }

        // Find minimum quantity
        let min_qty = outcome_data
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != exclude_idx)
            .filter_map(|(_, d)| d.ask_qty.map(|q| q.0))
            .min()?;

        if min_qty < *MIN_ORDER_SIZE {
            return None;
        }

        let required_capital = total_cost * min_qty;
        let expected_profit = net_profit * min_qty;
        let profit_pct = (net_profit / total_cost) * dec!(100);

        let legs: Vec<ArbitrageLeg> = outcome_data
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != exclude_idx)
            .filter_map(|(i, data)| {
                Some(ArbitrageLeg {
                    market_id: market.id.clone(),
                    token_id: data.info.token_id.clone(),
                    side: Side::Buy,
                    outcome: data.info.outcome,
                    price: data.ask?,
                    quantity: Quantity(min_qty),
                    order_type: OrderType::Gtc,
                    priority: (i + 1) as u8,
                })
            })
            .collect();

        Some(ArbitrageOpportunity {
            id: Uuid::new_v4(),
            opportunity_type: ArbitrageType::MultiOutcomeMispricing,
            market_ids: vec![market.id.clone()],
            expected_profit,
            expected_profit_pct: profit_pct,
            required_capital,
            legs,
            confidence: self.calculate_confidence(outcome_data) * 0.9, // Slightly lower for partial
            expires_at: Utc::now() + Duration::milliseconds(ARBITRAGE_EXPIRATION_MS as i64),
            detected_at: Utc::now(),
            risk_score: self.calculate_risk_score(outcome_data) * 1.1, // Slightly higher risk
        })
    }

    /// Calculate confidence based on orderbook quality
    fn calculate_confidence(&self, outcome_data: &[OutcomeData]) -> f64 {
        let mut confidence = 1.0;

        // Reduce for missing data
        let missing_count = outcome_data
            .iter()
            .filter(|d| d.ask.is_none() || d.bid.is_none())
            .count();

        confidence *= 1.0 - (missing_count as f64 * 0.1);

        // Reduce for low liquidity
        let min_liq = outcome_data
            .iter()
            .filter_map(|d| d.ask_qty.map(|q| q.0))
            .min()
            .unwrap_or(Decimal::ZERO);

        if min_liq < dec!(50) {
            confidence *= 0.7;
        } else if min_liq < dec!(100) {
            confidence *= 0.85;
        }

        confidence.max(0.1)
    }

    /// Calculate risk score
    fn calculate_risk_score(&self, outcome_data: &[OutcomeData]) -> f64 {
        // More outcomes = more risk (more legs to execute)
        let num_outcomes = outcome_data.len() as f64;
        let base_risk = 0.1 * num_outcomes;

        // Add risk for thin liquidity
        let min_liq = outcome_data
            .iter()
            .filter_map(|d| d.ask_qty.map(|q| q.0.to_f64().unwrap_or(0.0)))
            .min()
            .unwrap_or(0.0);

        let liq_risk = if min_liq < 50.0 {
            0.3
        } else if min_liq < 100.0 {
            0.15
        } else {
            0.0
        };

        (base_risk + liq_risk).min(1.0)
    }
}

/// Data for a single outcome
struct OutcomeData {
    info: OutcomeInfo,
    bid: Option<Price>,
    ask: Option<Price>,
    bid_qty: Option<Quantity>,
    ask_qty: Option<Quantity>,
}

impl std::fmt::Debug for MultiOutcomeStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiOutcomeStrategy").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::events::EventBus;

    fn create_multi_outcome_market() -> Market {
        Market {
            id: "test-multi".to_string(),
            condition_id: "0xdef".to_string(),
            slug: "test-multi-outcome".to_string(),
            question: "Who will win?".to_string(),
            description: None,
            market_type: MarketType::MultiOutcome { num_outcomes: 4 },
            status: MarketStatus::Active,
            outcomes: vec![
                OutcomeInfo {
                    outcome: Outcome::Yes, // Using Yes/No for simplicity
                    token_id: "token_a".to_string(),
                    price: Price::new(dec!(0.20)).unwrap(),
                    name: "A".to_string(),
                },
                OutcomeInfo {
                    outcome: Outcome::No,
                    token_id: "token_b".to_string(),
                    price: Price::new(dec!(0.20)).unwrap(),
                    name: "B".to_string(),
                },
                OutcomeInfo {
                    outcome: Outcome::Yes,
                    token_id: "token_c".to_string(),
                    price: Price::new(dec!(0.20)).unwrap(),
                    name: "C".to_string(),
                },
                OutcomeInfo {
                    outcome: Outcome::No,
                    token_id: "token_d".to_string(),
                    price: Price::new(dec!(0.20)).unwrap(),
                    name: "D".to_string(),
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
    async fn test_multi_outcome_detection() {
        let event_bus = EventBus::new(100);
        let engine = Arc::new(OrderbookEngine::new(event_bus, 5000));

        // Create orderbooks where sum of asks < 1.0
        for (token_id, price) in [
            ("token_a", dec!(0.20)),
            ("token_b", dec!(0.20)),
            ("token_c", dec!(0.20)),
            ("token_d", dec!(0.20)),
        ] {
            let mut ob = Orderbook::new(token_id.to_string(), Outcome::Yes);
            ob.bids.levels.push(PriceLevel::new(
                Price::new(price - dec!(0.02)).unwrap(),
                Quantity(dec!(100)),
                1,
            ));
            ob.asks.levels.push(PriceLevel::new(
                Price::new(price).unwrap(),
                Quantity(dec!(100)),
                1,
            ));
            engine.update_snapshot(ob).unwrap();
        }

        let strategy = MultiOutcomeStrategy::new(engine);
        let market = create_multi_outcome_market();

        let opportunity = strategy.detect(&market).await;

        assert!(opportunity.is_some());
        let opp = opportunity.unwrap();
        assert_eq!(opp.opportunity_type, ArbitrageType::MultiOutcomeMispricing);
        assert_eq!(opp.legs.len(), 4);
    }
}
