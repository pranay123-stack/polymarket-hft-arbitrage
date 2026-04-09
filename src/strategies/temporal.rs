//! Temporal arbitrage strategy for 5-minute markets.
//!
//! Exploits inefficiencies in short-duration prediction markets
//! like BTC/ETH 5-minute up/down markets.

use crate::core::constants::*;
use crate::core::types::*;
use crate::orderbook::OrderbookEngine;
use chrono::{DateTime, Duration, TimeZone, Utc};
use chrono_tz::America::New_York;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tracing::{debug, info, instrument};
use uuid::Uuid;

/// Temporal arbitrage strategy for short-duration markets
pub struct TemporalStrategy {
    orderbook_engine: Arc<OrderbookEngine>,
}

impl TemporalStrategy {
    /// Create a new temporal strategy
    pub fn new(orderbook_engine: Arc<OrderbookEngine>) -> Self {
        Self { orderbook_engine }
    }

    /// Detect arbitrage opportunity in a 5-minute market
    #[instrument(skip(self, market), fields(market_id = %market.id))]
    pub async fn detect(&self, market: &Market) -> Option<ArbitrageOpportunity> {
        // Verify this is a 5-minute market
        if !self.is_5min_market(&market.slug) {
            return None;
        }

        // Check if market is in the right trading window
        let window = self.get_trading_window(market)?;
        if !window.is_in_trading_period() {
            debug!("Market not in trading period");
            return None;
        }

        // Must be binary
        if !market.is_binary() {
            return None;
        }

        let up_info = market.outcomes.iter().find(|o|
            o.name.to_lowercase().contains("up") || o.outcome == Outcome::Yes
        )?;
        let down_info = market.outcomes.iter().find(|o|
            o.name.to_lowercase().contains("down") || o.outcome == Outcome::No
        )?;

        let up_ob = self.orderbook_engine.get(&up_info.token_id)?;
        let down_ob = self.orderbook_engine.get(&down_info.token_id)?;

        let up_book = up_ob.read();
        let down_book = down_ob.read();

        // Check staleness
        if up_book.is_stale(ORDERBOOK_STALE_THRESHOLD_MS)
            || down_book.is_stale(ORDERBOOK_STALE_THRESHOLD_MS)
        {
            return None;
        }

        // For 5-min markets, we look for:
        // 1. Both prices < 0.50 (buy both strategy)
        // 2. Large mispricing where sum deviates significantly from 1.0

        let up_ask = up_book.best_ask()?;
        let down_ask = down_book.best_ask()?;
        let up_bid = up_book.best_bid()?;
        let down_bid = down_book.best_bid()?;

        // Strategy 1: Buy both at < $0.50 each
        if up_ask.0 <= dec!(0.49) && down_ask.0 <= dec!(0.49) {
            return self.create_buy_both_strategy(
                market,
                up_info,
                down_info,
                up_ask,
                down_ask,
                &up_book,
                &down_book,
                &window,
            );
        }

        // Strategy 2: Classic binary mispricing
        let ask_sum = up_ask.0 + down_ask.0;
        if ask_sum < dec!(0.96) {
            return self.create_mispricing_strategy(
                market,
                up_info,
                down_info,
                up_ask,
                down_ask,
                &up_book,
                &down_book,
                &window,
            );
        }

        None
    }

    /// Check if this is a 5-minute market
    fn is_5min_market(&self, slug: &str) -> bool {
        slug.contains("-5m-")
            || slug.contains("5min")
            || slug.contains("updown-5m")
    }

    /// Get trading window for a 5-minute market
    fn get_trading_window(&self, market: &Market) -> Option<TradingWindow> {
        let end_time = market.end_date?;
        let start_time = end_time - Duration::seconds(FIVE_MIN_MARKET_DURATION_SECS as i64);
        let close_buffer = Duration::seconds(MARKET_CLOSE_BUFFER_SECS as i64);

        Some(TradingWindow {
            market_start: start_time,
            market_end: end_time,
            trading_cutoff: end_time - close_buffer,
        })
    }

    /// Create buy-both strategy for 5-minute markets
    fn create_buy_both_strategy(
        &self,
        market: &Market,
        up_info: &OutcomeInfo,
        down_info: &OutcomeInfo,
        up_ask: Price,
        down_ask: Price,
        up_book: &crate::orderbook::engine::ManagedOrderbook,
        down_book: &crate::orderbook::engine::ManagedOrderbook,
        window: &TradingWindow,
    ) -> Option<ArbitrageOpportunity> {
        let total_cost = up_ask.0 + down_ask.0;
        let gross_profit = Decimal::ONE - total_cost;

        // Account for fees
        let fees = total_cost * *FEE_RATE * dec!(2);
        let net_profit = gross_profit - fees;

        if net_profit <= dec!(0.01) {
            return None;
        }

        // Get available liquidity
        let up_qty = up_book.best_ask_level()?.quantity;
        let down_qty = down_book.best_ask_level()?.quantity;
        let max_qty = up_qty.0.min(down_qty.0);

        if max_qty < *MIN_ORDER_SIZE {
            return None;
        }

        let required_capital = total_cost * max_qty;
        let expected_profit = net_profit * max_qty;
        let profit_pct = (net_profit / total_cost) * dec!(100);

        // Calculate time-adjusted risk
        let time_to_expiry = window.time_to_trading_cutoff();
        let time_risk = if time_to_expiry < Duration::seconds(60) {
            0.5 // High risk if < 1 minute
        } else if time_to_expiry < Duration::seconds(120) {
            0.3
        } else {
            0.1
        };

        let legs = vec![
            ArbitrageLeg {
                market_id: market.id.clone(),
                token_id: up_info.token_id.clone(),
                side: Side::Buy,
                outcome: Outcome::Yes,
                price: up_ask,
                quantity: Quantity(max_qty),
                order_type: OrderType::Gtc,
                priority: 1,
            },
            ArbitrageLeg {
                market_id: market.id.clone(),
                token_id: down_info.token_id.clone(),
                side: Side::Buy,
                outcome: Outcome::No,
                price: down_ask,
                quantity: Quantity(max_qty),
                order_type: OrderType::Gtc,
                priority: 1,
            },
        ];

        // Expiry is shorter for temporal opportunities
        let expiry = Utc::now() + Duration::milliseconds((ARBITRAGE_EXPIRATION_MS / 2) as i64);

        Some(ArbitrageOpportunity {
            id: Uuid::new_v4(),
            opportunity_type: ArbitrageType::Temporal,
            market_ids: vec![market.id.clone()],
            expected_profit,
            expected_profit_pct: profit_pct,
            required_capital,
            legs,
            confidence: 0.85 - time_risk * 0.3,
            expires_at: expiry,
            detected_at: Utc::now(),
            risk_score: time_risk + 0.1,
        })
    }

    /// Create mispricing strategy
    fn create_mispricing_strategy(
        &self,
        market: &Market,
        up_info: &OutcomeInfo,
        down_info: &OutcomeInfo,
        up_ask: Price,
        down_ask: Price,
        up_book: &crate::orderbook::engine::ManagedOrderbook,
        down_book: &crate::orderbook::engine::ManagedOrderbook,
        window: &TradingWindow,
    ) -> Option<ArbitrageOpportunity> {
        let total_cost = up_ask.0 + down_ask.0;
        let gross_profit = Decimal::ONE - total_cost;

        let fees = total_cost * *FEE_RATE * dec!(2);
        let net_profit = gross_profit - fees;

        if net_profit <= Decimal::ZERO {
            return None;
        }

        let up_qty = up_book.best_ask_level()?.quantity;
        let down_qty = down_book.best_ask_level()?.quantity;
        let max_qty = up_qty.0.min(down_qty.0);

        if max_qty < *MIN_ORDER_SIZE {
            return None;
        }

        let required_capital = total_cost * max_qty;
        let expected_profit = net_profit * max_qty;
        let profit_pct = (net_profit / total_cost) * dec!(100);

        let time_to_expiry = window.time_to_trading_cutoff();
        let time_risk = if time_to_expiry < Duration::seconds(60) {
            0.4
        } else {
            0.2
        };

        let legs = vec![
            ArbitrageLeg {
                market_id: market.id.clone(),
                token_id: up_info.token_id.clone(),
                side: Side::Buy,
                outcome: Outcome::Yes,
                price: up_ask,
                quantity: Quantity(max_qty),
                order_type: OrderType::Gtc,
                priority: 1,
            },
            ArbitrageLeg {
                market_id: market.id.clone(),
                token_id: down_info.token_id.clone(),
                side: Side::Buy,
                outcome: Outcome::No,
                price: down_ask,
                quantity: Quantity(max_qty),
                order_type: OrderType::Gtc,
                priority: 1,
            },
        ];

        let expiry = Utc::now() + Duration::milliseconds((ARBITRAGE_EXPIRATION_MS / 2) as i64);

        Some(ArbitrageOpportunity {
            id: Uuid::new_v4(),
            opportunity_type: ArbitrageType::Temporal,
            market_ids: vec![market.id.clone()],
            expected_profit,
            expected_profit_pct: profit_pct,
            required_capital,
            legs,
            confidence: 0.75 - time_risk * 0.2,
            expires_at: expiry,
            detected_at: Utc::now(),
            risk_score: time_risk + 0.15,
        })
    }

    /// Build market slug for a given timestamp
    pub fn build_market_slug(asset: &str, timestamp: DateTime<Utc>) -> String {
        // Convert to Eastern Time for slug generation
        let et = timestamp.with_timezone(&New_York);

        // Round down to nearest 5-minute interval
        let minute = (et.minute() / 5) * 5;
        let rounded = et.date_naive().and_hms_opt(et.hour(), minute, 0).unwrap();

        // Format: btc-updown-5m-YYYYMMDD-HHMM
        format!(
            "{}-updown-5m-{}-{:02}{:02}",
            asset.to_lowercase(),
            rounded.format("%Y%m%d"),
            rounded.hour(),
            rounded.minute()
        )
    }

    /// Get the next 5-minute market start time
    pub fn next_market_start() -> DateTime<Utc> {
        let now = Utc::now();
        let now_et = now.with_timezone(&New_York);

        // Round up to next 5-minute interval
        let current_minute = now_et.minute();
        let next_interval = ((current_minute / 5) + 1) * 5;

        if next_interval >= 60 {
            // Move to next hour
            let next_hour = now_et
                .date_naive()
                .and_hms_opt((now_et.hour() + 1) % 24, 0, 0)
                .unwrap();
            New_York.from_local_datetime(&next_hour).unwrap().with_timezone(&Utc)
        } else {
            let next_time = now_et
                .date_naive()
                .and_hms_opt(now_et.hour(), next_interval, 0)
                .unwrap();
            New_York.from_local_datetime(&next_time).unwrap().with_timezone(&Utc)
        }
    }
}

/// Trading window for a 5-minute market
#[derive(Debug, Clone)]
pub struct TradingWindow {
    pub market_start: DateTime<Utc>,
    pub market_end: DateTime<Utc>,
    pub trading_cutoff: DateTime<Utc>,
}

impl TradingWindow {
    /// Check if we're in the active trading period
    pub fn is_in_trading_period(&self) -> bool {
        let now = Utc::now();
        now >= self.market_start && now < self.trading_cutoff
    }

    /// Get time remaining until trading cutoff
    pub fn time_to_trading_cutoff(&self) -> Duration {
        let now = Utc::now();
        if now >= self.trading_cutoff {
            Duration::zero()
        } else {
            self.trading_cutoff - now
        }
    }

    /// Get time remaining until market end
    pub fn time_to_market_end(&self) -> Duration {
        let now = Utc::now();
        if now >= self.market_end {
            Duration::zero()
        } else {
            self.market_end - now
        }
    }
}

impl std::fmt::Debug for TemporalStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemporalStrategy").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::events::EventBus;

    fn create_5min_market() -> Market {
        let now = Utc::now();
        let end_time = now + Duration::minutes(4);

        Market {
            id: "btc-5min-market".to_string(),
            condition_id: "0x5min".to_string(),
            slug: "btc-updown-5m-20240115-1400".to_string(),
            question: "Will BTC go up in the next 5 minutes?".to_string(),
            description: None,
            market_type: MarketType::Binary,
            status: MarketStatus::Active,
            outcomes: vec![
                OutcomeInfo {
                    outcome: Outcome::Yes,
                    token_id: "btc_up".to_string(),
                    price: Price::new(dec!(0.48)).unwrap(),
                    name: "Up".to_string(),
                },
                OutcomeInfo {
                    outcome: Outcome::No,
                    token_id: "btc_down".to_string(),
                    price: Price::new(dec!(0.48)).unwrap(),
                    name: "Down".to_string(),
                },
            ],
            end_date: Some(end_time),
            created_at: now,
            volume: dec!(5000),
            liquidity: dec!(2000),
            fee_rate: dec!(0.02),
            tags: vec!["btc".to_string(), "5min".to_string()],
            related_markets: vec![],
        }
    }

    #[test]
    fn test_is_5min_market() {
        let engine = Arc::new(OrderbookEngine::new(EventBus::new(100), 5000));
        let strategy = TemporalStrategy::new(engine);

        assert!(strategy.is_5min_market("btc-updown-5m-20240115-1400"));
        assert!(strategy.is_5min_market("eth-5min-market"));
        assert!(!strategy.is_5min_market("regular-market"));
    }

    #[test]
    fn test_build_market_slug() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 19, 0, 0).unwrap(); // 2pm ET
        let slug = TemporalStrategy::build_market_slug("BTC", timestamp);

        assert!(slug.starts_with("btc-updown-5m-"));
    }

    #[tokio::test]
    async fn test_temporal_detection() {
        let event_bus = EventBus::new(100);
        let engine = Arc::new(OrderbookEngine::new(event_bus, 5000));

        // Create orderbooks with prices < 0.50 each
        let mut up_ob = Orderbook::new("btc_up".to_string(), Outcome::Yes);
        up_ob.bids.levels.push(PriceLevel::new(
            Price::new(dec!(0.47)).unwrap(),
            Quantity(dec!(50)),
            1,
        ));
        up_ob.asks.levels.push(PriceLevel::new(
            Price::new(dec!(0.48)).unwrap(),
            Quantity(dec!(50)),
            1,
        ));
        engine.update_snapshot(up_ob).unwrap();

        let mut down_ob = Orderbook::new("btc_down".to_string(), Outcome::No);
        down_ob.bids.levels.push(PriceLevel::new(
            Price::new(dec!(0.47)).unwrap(),
            Quantity(dec!(50)),
            1,
        ));
        down_ob.asks.levels.push(PriceLevel::new(
            Price::new(dec!(0.48)).unwrap(),
            Quantity(dec!(50)),
            1,
        ));
        engine.update_snapshot(down_ob).unwrap();

        let strategy = TemporalStrategy::new(engine);
        let market = create_5min_market();

        let opportunity = strategy.detect(&market).await;

        assert!(opportunity.is_some());
        let opp = opportunity.unwrap();
        assert_eq!(opp.opportunity_type, ArbitrageType::Temporal);
        assert!(opp.expected_profit > Decimal::ZERO);
    }
}
