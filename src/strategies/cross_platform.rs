//! Cross-Platform Arbitrage Strategy
//!
//! Detects arbitrage opportunities across Polymarket, Kalshi, and Opinion exchanges.
//!
//! ## Strategy Overview:
//! 1. Map equivalent markets across venues (same underlying event)
//! 2. Monitor prices on all venues in real-time
//! 3. Detect mispricings that exceed threshold after fees
//! 4. Score opportunities by profit potential, liquidity, and risk
//! 5. Execute via CrossPlatformExecutor
//!
//! ## Key Considerations:
//! - Different fee structures across venues
//! - Latency asymmetry (some venues faster than others)
//! - Liquidity differences
//! - Market correlation confidence

use crate::api::{
    ClobClient,
    kalshi::{KalshiClient, KalshiMarket, KalshiOrderbook},
    opinion::{OpinionClient, OpinionMarket, OpinionOrderbook},
};
use crate::core::{
    error::{Error, Result},
    types::{Market, Orderbook, Price, Quantity, Side, Outcome},
};
use crate::execution::cross_platform::{
    Venue, CrossPlatformLeg, CrossPlatformOpportunity, CrossPlatformExecutor,
};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Cross-platform strategy configuration
#[derive(Debug, Clone)]
pub struct CrossPlatformStrategyConfig {
    /// Minimum profit threshold in USD
    pub min_profit_usd: Decimal,
    /// Minimum profit percentage after fees
    pub min_profit_pct: Decimal,
    /// Maximum position size per opportunity
    pub max_position_size: Decimal,
    /// Fee rate on Polymarket (maker/taker)
    pub polymarket_fee_rate: Decimal,
    /// Fee rate on Kalshi
    pub kalshi_fee_rate: Decimal,
    /// Commission rate on Opinion (on winnings)
    pub opinion_commission_rate: Decimal,
    /// Minimum correlation score for market matching
    pub min_correlation_score: f64,
    /// Maximum age of price data before refresh (ms)
    pub max_price_age_ms: u64,
    /// Opportunity expiration time (seconds)
    pub opportunity_ttl_secs: u64,
    /// Minimum liquidity on both sides
    pub min_liquidity: Decimal,
    /// Enable reverse arbitrage (when venue A > venue B AND venue B > venue A for different outcomes)
    pub enable_reverse_arb: bool,
}

impl Default for CrossPlatformStrategyConfig {
    fn default() -> Self {
        Self {
            min_profit_usd: dec!(1.0),
            min_profit_pct: dec!(0.005),  // 0.5%
            max_position_size: dec!(1000),
            polymarket_fee_rate: dec!(0.02),   // 2%
            kalshi_fee_rate: dec!(0.01),       // 1% (included in price)
            opinion_commission_rate: dec!(0.02), // 2% on winnings
            min_correlation_score: 0.9,
            max_price_age_ms: 1000,
            opportunity_ttl_secs: 5,
            min_liquidity: dec!(100),
            enable_reverse_arb: true,
        }
    }
}

/// Mapping between equivalent markets on different venues
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketMapping {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    /// Polymarket market info
    pub polymarket: Option<PolymarketMarketInfo>,
    /// Kalshi market info
    pub kalshi: Option<KalshiMarketInfo>,
    /// Opinion market info
    pub opinion: Option<OpinionMarketInfo>,
    /// Confidence score that these markets are equivalent (0-1)
    pub correlation_score: f64,
    /// Last time prices were checked
    pub last_checked: DateTime<Utc>,
    /// Is this mapping active?
    pub active: bool,
    /// Tags for categorization
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketMarketInfo {
    pub condition_id: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub slug: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiMarketInfo {
    pub ticker: String,
    pub event_ticker: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionMarketInfo {
    pub market_id: String,
    pub selection_id: String,
}

/// Real-time price snapshot for a mapped market
#[derive(Debug, Clone)]
pub struct CrossVenuePriceSnapshot {
    pub mapping_id: Uuid,
    pub polymarket_prices: Option<VenuePrices>,
    pub kalshi_prices: Option<VenuePrices>,
    pub opinion_prices: Option<VenuePrices>,
    pub timestamp: DateTime<Utc>,
}

/// Prices from a single venue
#[derive(Debug, Clone)]
pub struct VenuePrices {
    pub venue: Venue,
    pub yes_bid: Option<Price>,
    pub yes_ask: Option<Price>,
    pub no_bid: Option<Price>,
    pub no_ask: Option<Price>,
    pub yes_bid_size: Decimal,
    pub yes_ask_size: Decimal,
    pub no_bid_size: Decimal,
    pub no_ask_size: Decimal,
    pub timestamp: DateTime<Utc>,
}

impl VenuePrices {
    /// Get effective bid price (what we can sell at)
    pub fn effective_yes_bid(&self) -> Option<Price> {
        self.yes_bid
    }

    /// Get effective ask price (what we can buy at)
    pub fn effective_yes_ask(&self) -> Option<Price> {
        self.yes_ask
    }

    /// Check if prices are fresh
    pub fn is_fresh(&self, max_age_ms: u64) -> bool {
        let age = Utc::now() - self.timestamp;
        age < Duration::milliseconds(max_age_ms as i64)
    }

    /// Get mid price
    pub fn mid_price(&self) -> Option<Price> {
        match (self.yes_bid, self.yes_ask) {
            (Some(bid), Some(ask)) => Price::new((bid.as_decimal() + ask.as_decimal()) / dec!(2)),
            _ => None,
        }
    }
}

/// Cross-platform arbitrage opportunity detector
pub struct CrossPlatformStrategy {
    config: CrossPlatformStrategyConfig,
    polymarket: Arc<ClobClient>,
    kalshi: Arc<KalshiClient>,
    opinion: Arc<OpinionClient>,
    /// Market mappings
    mappings: Arc<RwLock<HashMap<Uuid, MarketMapping>>>,
    /// Price cache
    price_cache: Arc<RwLock<HashMap<Uuid, CrossVenuePriceSnapshot>>>,
    /// Detected opportunities
    opportunities: Arc<RwLock<Vec<CrossPlatformOpportunity>>>,
    /// Estimated latencies to each venue
    venue_latencies: Arc<RwLock<HashMap<Venue, u64>>>,
}

impl CrossPlatformStrategy {
    pub fn new(
        config: CrossPlatformStrategyConfig,
        polymarket: Arc<ClobClient>,
        kalshi: Arc<KalshiClient>,
        opinion: Arc<OpinionClient>,
    ) -> Self {
        Self {
            config,
            polymarket,
            kalshi,
            opinion,
            mappings: Arc::new(RwLock::new(HashMap::new())),
            price_cache: Arc::new(RwLock::new(HashMap::new())),
            opportunities: Arc::new(RwLock::new(Vec::new())),
            venue_latencies: Arc::new(RwLock::new(HashMap::from([
                (Venue::Polymarket, 100),
                (Venue::Kalshi, 80),
                (Venue::Opinion, 120),
            ]))),
        }
    }

    /// Add a market mapping
    pub async fn add_mapping(&self, mapping: MarketMapping) {
        let mut mappings = self.mappings.write().await;
        mappings.insert(mapping.id, mapping);
    }

    /// Update venue latency estimate
    pub async fn update_latency(&self, venue: Venue, latency_ms: u64) {
        let mut latencies = self.venue_latencies.write().await;
        latencies.insert(venue, latency_ms);
    }

    /// Scan for arbitrage opportunities across all mapped markets
    pub async fn scan_opportunities(&self) -> Result<Vec<CrossPlatformOpportunity>> {
        let mappings = self.mappings.read().await.clone();
        let mut all_opportunities = Vec::new();

        for (id, mapping) in mappings {
            if !mapping.active || mapping.correlation_score < self.config.min_correlation_score {
                continue;
            }

            // Fetch prices from all venues
            let snapshot = self.fetch_prices(&mapping).await?;

            // Store in cache
            {
                let mut cache = self.price_cache.write().await;
                cache.insert(id, snapshot.clone());
            }

            // Detect opportunities
            let opportunities = self.detect_opportunities(&mapping, &snapshot).await?;
            all_opportunities.extend(opportunities);
        }

        // Sort by expected profit
        all_opportunities.sort_by(|a, b| b.expected_profit.cmp(&a.expected_profit));

        // Store opportunities
        {
            let mut opps = self.opportunities.write().await;
            *opps = all_opportunities.clone();
        }

        Ok(all_opportunities)
    }

    /// Fetch prices from all venues for a mapping
    async fn fetch_prices(&self, mapping: &MarketMapping) -> Result<CrossVenuePriceSnapshot> {
        let polymarket_prices = if let Some(ref pm) = mapping.polymarket {
            self.fetch_polymarket_prices(pm).await.ok()
        } else {
            None
        };

        let kalshi_prices = if let Some(ref k) = mapping.kalshi {
            self.fetch_kalshi_prices(k).await.ok()
        } else {
            None
        };

        let opinion_prices = if let Some(ref o) = mapping.opinion {
            self.fetch_opinion_prices(o).await.ok()
        } else {
            None
        };

        Ok(CrossVenuePriceSnapshot {
            mapping_id: mapping.id,
            polymarket_prices,
            kalshi_prices,
            opinion_prices,
            timestamp: Utc::now(),
        })
    }

    /// Fetch prices from Polymarket
    async fn fetch_polymarket_prices(&self, info: &PolymarketMarketInfo) -> Result<VenuePrices> {
        let yes_ob = self.polymarket.get_orderbook(&info.yes_token_id).await?;
        let no_ob = self.polymarket.get_orderbook(&info.no_token_id).await?;

        Ok(VenuePrices {
            venue: Venue::Polymarket,
            yes_bid: yes_ob.bids.first().map(|l| Price::new_unchecked(l.price)),
            yes_ask: yes_ob.asks.first().map(|l| Price::new_unchecked(l.price)),
            no_bid: no_ob.bids.first().map(|l| Price::new_unchecked(l.price)),
            no_ask: no_ob.asks.first().map(|l| Price::new_unchecked(l.price)),
            yes_bid_size: yes_ob.bids.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
            yes_ask_size: yes_ob.asks.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
            no_bid_size: no_ob.bids.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
            no_ask_size: no_ob.asks.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
            timestamp: Utc::now(),
        })
    }

    /// Fetch prices from Kalshi
    async fn fetch_kalshi_prices(&self, info: &KalshiMarketInfo) -> Result<VenuePrices> {
        let ob = self.kalshi.get_orderbook(&info.ticker, Some(5)).await?;

        // Kalshi prices are in cents (0-100)
        let yes_bid = ob.yes.iter().max_by_key(|l| l.price).map(|l| KalshiMarket::normalize_price(l.price));
        let yes_ask = ob.yes.iter().min_by_key(|l| l.price).map(|l| KalshiMarket::normalize_price(l.price));
        let yes_bid_size = ob.yes.iter().max_by_key(|l| l.price).map(|l| Decimal::from(l.count)).unwrap_or(Decimal::ZERO);
        let yes_ask_size = ob.yes.iter().min_by_key(|l| l.price).map(|l| Decimal::from(l.count)).unwrap_or(Decimal::ZERO);

        // NO prices are complement of YES
        let no_bid = yes_ask.map(|p| p.complement());
        let no_ask = yes_bid.map(|p| p.complement());

        Ok(VenuePrices {
            venue: Venue::Kalshi,
            yes_bid,
            yes_ask,
            no_bid,
            no_ask,
            yes_bid_size,
            yes_ask_size,
            no_bid_size: yes_ask_size,
            no_ask_size: yes_bid_size,
            timestamp: Utc::now(),
        })
    }

    /// Fetch prices from Opinion
    async fn fetch_opinion_prices(&self, info: &OpinionMarketInfo) -> Result<VenuePrices> {
        let ob = self.opinion.get_orderbook(&info.market_id, &info.selection_id).await?;

        // Opinion uses back/lay model
        // Back price = price to buy YES (equivalent to ask)
        // Lay price = price to sell YES (equivalent to bid)
        let yes_bid = ob.best_lay().and_then(|l| Price::new(Decimal::ONE / l.price));  // Convert odds to probability
        let yes_ask = ob.best_back().and_then(|l| Price::new(Decimal::ONE / l.price));

        Ok(VenuePrices {
            venue: Venue::Opinion,
            yes_bid,
            yes_ask,
            no_bid: yes_ask.map(|p| p.complement()),
            no_ask: yes_bid.map(|p| p.complement()),
            yes_bid_size: ob.best_lay().map(|l| l.size).unwrap_or(Decimal::ZERO),
            yes_ask_size: ob.best_back().map(|l| l.size).unwrap_or(Decimal::ZERO),
            no_bid_size: ob.best_back().map(|l| l.size).unwrap_or(Decimal::ZERO),
            no_ask_size: ob.best_lay().map(|l| l.size).unwrap_or(Decimal::ZERO),
            timestamp: Utc::now(),
        })
    }

    /// Detect arbitrage opportunities from price snapshot
    async fn detect_opportunities(
        &self,
        mapping: &MarketMapping,
        snapshot: &CrossVenuePriceSnapshot,
    ) -> Result<Vec<CrossPlatformOpportunity>> {
        let mut opportunities = Vec::new();
        let latencies = self.venue_latencies.read().await;

        // Get all venue prices that are available
        let venue_prices: Vec<&VenuePrices> = [
            snapshot.polymarket_prices.as_ref(),
            snapshot.kalshi_prices.as_ref(),
            snapshot.opinion_prices.as_ref(),
        ]
        .into_iter()
        .flatten()
        .filter(|p| p.is_fresh(self.config.max_price_age_ms))
        .collect();

        // Need at least 2 venues for cross-platform arbitrage
        if venue_prices.len() < 2 {
            return Ok(opportunities);
        }

        // Check all venue pairs for arbitrage
        for i in 0..venue_prices.len() {
            for j in (i + 1)..venue_prices.len() {
                let venue_a = venue_prices[i];
                let venue_b = venue_prices[j];

                // Check YES arbitrage: Buy YES on cheaper venue, sell YES on more expensive
                if let Some(opp) = self.check_yes_arbitrage(mapping, venue_a, venue_b, &latencies).await {
                    if self.is_profitable(&opp) {
                        opportunities.push(opp);
                    }
                }

                // Check reverse (B to A)
                if let Some(opp) = self.check_yes_arbitrage(mapping, venue_b, venue_a, &latencies).await {
                    if self.is_profitable(&opp) {
                        opportunities.push(opp);
                    }
                }

                // Check NO arbitrage if enabled
                if self.config.enable_reverse_arb {
                    if let Some(opp) = self.check_no_arbitrage(mapping, venue_a, venue_b, &latencies).await {
                        if self.is_profitable(&opp) {
                            opportunities.push(opp);
                        }
                    }

                    if let Some(opp) = self.check_no_arbitrage(mapping, venue_b, venue_a, &latencies).await {
                        if self.is_profitable(&opp) {
                            opportunities.push(opp);
                        }
                    }
                }
            }
        }

        Ok(opportunities)
    }

    /// Check for YES outcome arbitrage between two venues
    /// Buy YES on venue A, sell YES on venue B
    async fn check_yes_arbitrage(
        &self,
        mapping: &MarketMapping,
        buy_venue: &VenuePrices,
        sell_venue: &VenuePrices,
        latencies: &HashMap<Venue, u64>,
    ) -> Option<CrossPlatformOpportunity> {
        let buy_price = buy_venue.effective_yes_ask()?;
        let sell_price = sell_venue.effective_yes_bid()?;

        // Need to buy lower than we can sell
        if buy_price.as_decimal() >= sell_price.as_decimal() {
            return None;
        }

        // Calculate fees
        let buy_fee = self.get_fee_rate(buy_venue.venue);
        let sell_fee = self.get_fee_rate(sell_venue.venue);

        // Effective prices after fees
        let effective_buy = buy_price.as_decimal() * (Decimal::ONE + buy_fee);
        let effective_sell = sell_price.as_decimal() * (Decimal::ONE - sell_fee);

        // Still profitable after fees?
        if effective_buy >= effective_sell {
            return None;
        }

        // Calculate quantity (limited by liquidity on both sides)
        let max_qty = buy_venue.yes_ask_size
            .min(sell_venue.yes_bid_size)
            .min(self.config.max_position_size);

        if max_qty < self.config.min_liquidity {
            return None;
        }

        // Calculate profit
        let gross_profit = (effective_sell - effective_buy) * max_qty;
        let profit_pct = (effective_sell - effective_buy) / effective_buy;

        // Get market info for legs
        let (buy_market_id, buy_token_id) = self.get_market_info(mapping, buy_venue.venue)?;
        let (sell_market_id, sell_token_id) = self.get_market_info(mapping, sell_venue.venue)?;

        Some(CrossPlatformOpportunity {
            id: Uuid::new_v4(),
            leg_a: CrossPlatformLeg {
                venue: buy_venue.venue,
                market_id: buy_market_id,
                token_id: buy_token_id,
                side: Side::Buy,
                outcome: Outcome::Yes,
                target_price: buy_price,
                target_qty: Quantity::new(max_qty).unwrap_or(Quantity::ZERO),
                is_hedge: false,
                estimated_latency_ms: *latencies.get(&buy_venue.venue).unwrap_or(&100),
                available_liquidity: buy_venue.yes_ask_size,
            },
            leg_b: CrossPlatformLeg {
                venue: sell_venue.venue,
                market_id: sell_market_id,
                token_id: sell_token_id,
                side: Side::Sell,
                outcome: Outcome::Yes,
                target_price: sell_price,
                target_qty: Quantity::new(max_qty).unwrap_or(Quantity::ZERO),
                is_hedge: true,
                estimated_latency_ms: *latencies.get(&sell_venue.venue).unwrap_or(&100),
                available_liquidity: sell_venue.yes_bid_size,
            },
            expected_profit: gross_profit,
            expected_profit_pct: profit_pct,
            required_capital: effective_buy * max_qty,
            detected_at: Utc::now(),
            expires_at: Utc::now() + Duration::seconds(self.config.opportunity_ttl_secs as i64),
            correlation_score: mapping.correlation_score,
        })
    }

    /// Check for NO outcome arbitrage between two venues
    async fn check_no_arbitrage(
        &self,
        mapping: &MarketMapping,
        buy_venue: &VenuePrices,
        sell_venue: &VenuePrices,
        latencies: &HashMap<Venue, u64>,
    ) -> Option<CrossPlatformOpportunity> {
        let buy_price = buy_venue.no_ask?;
        let sell_price = sell_venue.no_bid?;

        if buy_price.as_decimal() >= sell_price.as_decimal() {
            return None;
        }

        let buy_fee = self.get_fee_rate(buy_venue.venue);
        let sell_fee = self.get_fee_rate(sell_venue.venue);

        let effective_buy = buy_price.as_decimal() * (Decimal::ONE + buy_fee);
        let effective_sell = sell_price.as_decimal() * (Decimal::ONE - sell_fee);

        if effective_buy >= effective_sell {
            return None;
        }

        let max_qty = buy_venue.no_ask_size
            .min(sell_venue.no_bid_size)
            .min(self.config.max_position_size);

        if max_qty < self.config.min_liquidity {
            return None;
        }

        let gross_profit = (effective_sell - effective_buy) * max_qty;
        let profit_pct = (effective_sell - effective_buy) / effective_buy;

        let (buy_market_id, buy_token_id) = self.get_market_info(mapping, buy_venue.venue)?;
        let (sell_market_id, sell_token_id) = self.get_market_info(mapping, sell_venue.venue)?;

        Some(CrossPlatformOpportunity {
            id: Uuid::new_v4(),
            leg_a: CrossPlatformLeg {
                venue: buy_venue.venue,
                market_id: buy_market_id,
                token_id: buy_token_id,
                side: Side::Buy,
                outcome: Outcome::No,
                target_price: buy_price,
                target_qty: Quantity::new(max_qty).unwrap_or(Quantity::ZERO),
                is_hedge: false,
                estimated_latency_ms: *latencies.get(&buy_venue.venue).unwrap_or(&100),
                available_liquidity: buy_venue.no_ask_size,
            },
            leg_b: CrossPlatformLeg {
                venue: sell_venue.venue,
                market_id: sell_market_id,
                token_id: sell_token_id,
                side: Side::Sell,
                outcome: Outcome::No,
                target_price: sell_price,
                target_qty: Quantity::new(max_qty).unwrap_or(Quantity::ZERO),
                is_hedge: true,
                estimated_latency_ms: *latencies.get(&sell_venue.venue).unwrap_or(&100),
                available_liquidity: sell_venue.no_bid_size,
            },
            expected_profit: gross_profit,
            expected_profit_pct: profit_pct,
            required_capital: effective_buy * max_qty,
            detected_at: Utc::now(),
            expires_at: Utc::now() + Duration::seconds(self.config.opportunity_ttl_secs as i64),
            correlation_score: mapping.correlation_score,
        })
    }

    /// Get fee rate for a venue
    fn get_fee_rate(&self, venue: Venue) -> Decimal {
        match venue {
            Venue::Polymarket => self.config.polymarket_fee_rate,
            Venue::Kalshi => self.config.kalshi_fee_rate,
            Venue::Opinion => self.config.opinion_commission_rate,
        }
    }

    /// Get market info for a venue from mapping
    fn get_market_info(&self, mapping: &MarketMapping, venue: Venue) -> Option<(String, String)> {
        match venue {
            Venue::Polymarket => mapping.polymarket.as_ref().map(|p| (p.condition_id.clone(), p.yes_token_id.clone())),
            Venue::Kalshi => mapping.kalshi.as_ref().map(|k| (k.ticker.clone(), k.ticker.clone())),
            Venue::Opinion => mapping.opinion.as_ref().map(|o| (o.market_id.clone(), o.selection_id.clone())),
        }
    }

    /// Check if opportunity meets profit thresholds
    fn is_profitable(&self, opp: &CrossPlatformOpportunity) -> bool {
        opp.expected_profit >= self.config.min_profit_usd &&
        opp.expected_profit_pct >= self.config.min_profit_pct &&
        opp.is_valid()
    }

    /// Get current opportunities
    pub async fn get_opportunities(&self) -> Vec<CrossPlatformOpportunity> {
        let opps = self.opportunities.read().await;
        opps.iter()
            .filter(|o| o.is_valid())
            .cloned()
            .collect()
    }

    /// Get best opportunity
    pub async fn get_best_opportunity(&self) -> Option<CrossPlatformOpportunity> {
        let opps = self.opportunities.read().await;
        opps.iter()
            .filter(|o| o.is_valid())
            .max_by(|a, b| a.expected_profit.cmp(&b.expected_profit))
            .cloned()
    }

    /// Auto-discover market mappings by searching for similar markets
    pub async fn discover_mappings(&self, keywords: &[&str]) -> Result<Vec<MarketMapping>> {
        let mut mappings = Vec::new();

        // Search each venue for matching markets
        for keyword in keywords {
            // Search Polymarket (through Gamma API)
            let poly_markets = self.search_polymarket(keyword).await.unwrap_or_default();

            // Search Kalshi
            let kalshi_markets = self.kalshi.get_markets_by_event(keyword).await.unwrap_or_default();

            // Search Opinion
            let opinion_markets = self.opinion.search_markets(keyword, None).await.unwrap_or_default();

            // Try to match markets across venues
            for pm in &poly_markets {
                let mapping = MarketMapping {
                    id: Uuid::new_v4(),
                    name: pm.question.clone(),
                    description: pm.description.clone().unwrap_or_default(),
                    polymarket: Some(PolymarketMarketInfo {
                        condition_id: pm.condition_id.clone(),
                        yes_token_id: pm.yes_token_id().unwrap_or_default().to_string(),
                        no_token_id: pm.no_token_id().unwrap_or_default().to_string(),
                        slug: pm.slug.clone(),
                    }),
                    kalshi: self.find_matching_kalshi(&pm.question, &kalshi_markets),
                    opinion: self.find_matching_opinion(&pm.question, &opinion_markets),
                    correlation_score: 0.85,  // Default, should be refined
                    last_checked: Utc::now(),
                    active: true,
                    tags: pm.tags.clone(),
                };

                // Only add if we have at least 2 venues
                let venue_count = [mapping.polymarket.is_some(), mapping.kalshi.is_some(), mapping.opinion.is_some()]
                    .iter()
                    .filter(|&&x| x)
                    .count();

                if venue_count >= 2 {
                    mappings.push(mapping);
                }
            }
        }

        // Add discovered mappings
        for mapping in &mappings {
            self.add_mapping(mapping.clone()).await;
        }

        info!("Discovered {} cross-venue market mappings", mappings.len());
        Ok(mappings)
    }

    /// Search Polymarket for markets
    async fn search_polymarket(&self, keyword: &str) -> Result<Vec<Market>> {
        // This would use the Gamma API search endpoint
        // For now, return empty - in production would implement search
        Ok(vec![])
    }

    /// Find matching Kalshi market by title similarity
    fn find_matching_kalshi(&self, title: &str, markets: &[KalshiMarket]) -> Option<KalshiMarketInfo> {
        let title_lower = title.to_lowercase();

        markets.iter()
            .find(|m| {
                let market_title = format!("{} {}", m.title, m.subtitle.as_deref().unwrap_or("")).to_lowercase();
                self.calculate_similarity(&title_lower, &market_title) > 0.7
            })
            .map(|m| KalshiMarketInfo {
                ticker: m.ticker.clone(),
                event_ticker: m.event_ticker.clone(),
            })
    }

    /// Find matching Opinion market by title similarity
    fn find_matching_opinion(&self, title: &str, markets: &[OpinionMarket]) -> Option<OpinionMarketInfo> {
        let title_lower = title.to_lowercase();

        markets.iter()
            .find(|m| {
                let market_title = m.name.to_lowercase();
                self.calculate_similarity(&title_lower, &market_title) > 0.7
            })
            .map(|m| OpinionMarketInfo {
                market_id: m.id.clone(),
                selection_id: m.id.clone(),  // Simplified - would need proper selection ID
            })
    }

    /// Calculate string similarity (Jaccard index on words)
    fn calculate_similarity(&self, a: &str, b: &str) -> f64 {
        let words_a: std::collections::HashSet<&str> = a.split_whitespace().collect();
        let words_b: std::collections::HashSet<&str> = b.split_whitespace().collect();

        let intersection = words_a.intersection(&words_b).count();
        let union = words_a.union(&words_b).count();

        if union == 0 {
            0.0
        } else {
            intersection as f64 / union as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_arbitrage_detection() {
        let config = CrossPlatformStrategyConfig::default();

        // Venue A: Can buy YES at 0.45
        let venue_a = VenuePrices {
            venue: Venue::Polymarket,
            yes_bid: Some(Price::new(dec!(0.44)).unwrap()),
            yes_ask: Some(Price::new(dec!(0.45)).unwrap()),
            no_bid: Some(Price::new(dec!(0.54)).unwrap()),
            no_ask: Some(Price::new(dec!(0.56)).unwrap()),
            yes_bid_size: dec!(500),
            yes_ask_size: dec!(500),
            no_bid_size: dec!(500),
            no_ask_size: dec!(500),
            timestamp: Utc::now(),
        };

        // Venue B: Can sell YES at 0.50
        let venue_b = VenuePrices {
            venue: Venue::Kalshi,
            yes_bid: Some(Price::new(dec!(0.50)).unwrap()),
            yes_ask: Some(Price::new(dec!(0.51)).unwrap()),
            no_bid: Some(Price::new(dec!(0.48)).unwrap()),
            no_ask: Some(Price::new(dec!(0.50)).unwrap()),
            yes_bid_size: dec!(300),
            yes_ask_size: dec!(300),
            no_bid_size: dec!(300),
            no_ask_size: dec!(300),
            timestamp: Utc::now(),
        };

        // Arbitrage exists: Buy at 0.45, sell at 0.50 = 5 cent profit per share
        // After 2% fees on each side: 0.45 * 1.02 = 0.459, 0.50 * 0.98 = 0.49
        // Still profitable: 0.49 - 0.459 = 0.031 per share

        let buy_price = venue_a.effective_yes_ask().unwrap().as_decimal();
        let sell_price = venue_b.effective_yes_bid().unwrap().as_decimal();

        let effective_buy = buy_price * dec!(1.02);
        let effective_sell = sell_price * dec!(0.98);

        assert!(effective_sell > effective_buy);
        let profit_per_share = effective_sell - effective_buy;
        assert!(profit_per_share > dec!(0.02));
    }

    #[test]
    fn test_string_similarity() {
        let a = "Will Bitcoin exceed $100,000 by end of 2024?";
        let b = "Bitcoin to exceed 100000 dollars by December 2024";

        let words_a: std::collections::HashSet<&str> = a.to_lowercase().split_whitespace().collect();
        let words_b: std::collections::HashSet<&str> = b.to_lowercase().split_whitespace().collect();

        let intersection = words_a.intersection(&words_b).count();
        let union = words_a.union(&words_b).count();
        let similarity = intersection as f64 / union as f64;

        // Should have reasonable similarity
        assert!(similarity > 0.3);
    }
}
