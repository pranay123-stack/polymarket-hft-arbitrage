//! Cross-Venue Order Book Aggregator
//!
//! Aggregates real-time order book data from multiple venues into a unified view.
//! This is critical for:
//! - Accurate cross-platform arbitrage detection
//! - Latency-adjusted price comparison
//! - Liquidity analysis across venues
//!
//! Features:
//! - Real-time price normalization across different formats
//! - Staleness detection and handling
//! - Weighted pricing based on liquidity
//! - Spread analysis across venues

use crate::api::{
    kalshi_ws::{KalshiRealtimeOrderbook, KalshiWsEvent},
    opinion_ws::{OpinionPriceLadder, OpinionWsEvent},
};
use crate::core::{
    error::{ArbitrageError, Result},
    types::{Price, Quantity, Side, Outcome},
};
use crate::execution::cross_platform::Venue;
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, warn};

/// Configuration for the cross-venue aggregator
#[derive(Debug, Clone)]
pub struct CrossVenueAggregatorConfig {
    /// Maximum age of price data before considered stale (ms)
    pub max_price_age_ms: u64,
    /// Minimum liquidity to consider a price valid
    pub min_liquidity: Decimal,
    /// Whether to adjust for latency in comparisons
    pub latency_adjustment_enabled: bool,
    /// Default latency assumption if not measured (ms)
    pub default_latency_ms: u64,
}

impl Default for CrossVenueAggregatorConfig {
    fn default() -> Self {
        Self {
            max_price_age_ms: 500,  // 500ms max staleness
            min_liquidity: dec!(10),
            latency_adjustment_enabled: true,
            default_latency_ms: 100,
        }
    }
}

/// Unified price level across venues
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedPriceLevel {
    pub venue: Venue,
    pub price: Price,
    pub size: Quantity,
    /// Timestamp of this price
    pub timestamp: DateTime<Utc>,
    /// Measured latency to this venue (ms)
    pub latency_ms: u64,
    /// Is this price considered fresh?
    pub is_fresh: bool,
    /// Confidence score (0-1) based on liquidity and freshness
    pub confidence: f64,
}

/// Unified order book view for a market across venues
#[derive(Debug, Clone)]
pub struct UnifiedOrderbook {
    /// Unique identifier for the underlying market
    pub market_id: String,
    /// Human-readable name
    pub market_name: String,
    /// Best bids across all venues (sorted by price desc)
    pub bids: Vec<UnifiedPriceLevel>,
    /// Best asks across all venues (sorted by price asc)
    pub asks: Vec<UnifiedPriceLevel>,
    /// Per-venue detailed books
    pub venue_books: HashMap<Venue, VenueOrderbook>,
    /// Last update time
    pub last_update: DateTime<Utc>,
    /// Aggregation metadata
    pub metadata: AggregationMetadata,
}

/// Order book data from a single venue
#[derive(Debug, Clone)]
pub struct VenueOrderbook {
    pub venue: Venue,
    pub market_id: String,
    pub token_id: String,
    pub best_bid: Option<(Price, Quantity)>,
    pub best_ask: Option<(Price, Quantity)>,
    pub mid_price: Option<Price>,
    pub spread: Option<Decimal>,
    pub total_bid_liquidity: Decimal,
    pub total_ask_liquidity: Decimal,
    pub last_update: DateTime<Utc>,
    pub latency_ms: u64,
    pub is_stale: bool,
}

/// Metadata about the aggregation
#[derive(Debug, Clone)]
pub struct AggregationMetadata {
    /// Number of venues with valid data
    pub venue_count: u32,
    /// Venues with stale data
    pub stale_venues: Vec<Venue>,
    /// Best cross-venue bid (highest across all venues)
    pub best_cross_venue_bid: Option<(Venue, Price, Quantity)>,
    /// Best cross-venue ask (lowest across all venues)
    pub best_cross_venue_ask: Option<(Venue, Price, Quantity)>,
    /// Cross-venue spread (if arbitrage exists, this is negative)
    pub cross_venue_spread: Option<Decimal>,
    /// Is there an arbitrage opportunity?
    pub has_arbitrage: bool,
    /// Arbitrage profit (if any)
    pub arbitrage_profit_pct: Option<Decimal>,
}

/// Cross-venue arbitrage opportunity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossVenueArbitrageSignal {
    pub market_id: String,
    /// Buy venue (where to buy)
    pub buy_venue: Venue,
    pub buy_price: Price,
    pub buy_size: Quantity,
    pub buy_latency_ms: u64,
    /// Sell venue (where to sell)
    pub sell_venue: Venue,
    pub sell_price: Price,
    pub sell_size: Quantity,
    pub sell_latency_ms: u64,
    /// Expected profit
    pub gross_profit_pct: Decimal,
    pub net_profit_pct: Decimal,  // After estimated fees
    /// Maximum executable size
    pub max_size: Quantity,
    /// Confidence score
    pub confidence: f64,
    /// Timestamp
    pub detected_at: DateTime<Utc>,
    /// Estimated time to execute (based on latencies)
    pub estimated_execution_time_ms: u64,
}

/// Event from the aggregator
#[derive(Debug, Clone)]
pub enum AggregatorEvent {
    /// Order book updated
    OrderbookUpdate(UnifiedOrderbook),
    /// Arbitrage signal detected
    ArbitrageSignal(CrossVenueArbitrageSignal),
    /// Venue went stale
    VenueStale { venue: Venue, market_id: String, last_update: DateTime<Utc> },
    /// Venue recovered
    VenueRecovered { venue: Venue, market_id: String },
}

/// Cross-Venue Order Book Aggregator
pub struct CrossVenueAggregator {
    config: CrossVenueAggregatorConfig,
    /// Unified order books by market_id
    unified_books: Arc<RwLock<HashMap<String, UnifiedOrderbook>>>,
    /// Per-venue raw order books
    polymarket_books: Arc<RwLock<HashMap<String, VenueOrderbook>>>,
    kalshi_books: Arc<RwLock<HashMap<String, VenueOrderbook>>>,
    opinion_books: Arc<RwLock<HashMap<String, VenueOrderbook>>>,
    /// Venue latencies
    venue_latencies: Arc<RwLock<HashMap<Venue, u64>>>,
    /// Event broadcast
    event_tx: broadcast::Sender<AggregatorEvent>,
    /// Market mappings (unified_id -> venue-specific ids)
    market_mappings: Arc<RwLock<HashMap<String, MarketVenueMapping>>>,
    /// Fee rates per venue
    fee_rates: HashMap<Venue, Decimal>,
}

/// Mapping from unified market ID to venue-specific identifiers
#[derive(Debug, Clone)]
pub struct MarketVenueMapping {
    pub unified_id: String,
    pub name: String,
    pub polymarket_token_id: Option<String>,
    pub kalshi_ticker: Option<String>,
    pub opinion_market_id: Option<String>,
    pub opinion_selection_id: Option<i64>,
}

impl CrossVenueAggregator {
    pub fn new(config: CrossVenueAggregatorConfig) -> Self {
        let (event_tx, _) = broadcast::channel(1000);

        let mut fee_rates = HashMap::new();
        fee_rates.insert(Venue::Polymarket, dec!(0.02));  // 2%
        fee_rates.insert(Venue::Kalshi, dec!(0.01));       // 1%
        fee_rates.insert(Venue::Opinion, dec!(0.02));      // 2%

        Self {
            config,
            unified_books: Arc::new(RwLock::new(HashMap::new())),
            polymarket_books: Arc::new(RwLock::new(HashMap::new())),
            kalshi_books: Arc::new(RwLock::new(HashMap::new())),
            opinion_books: Arc::new(RwLock::new(HashMap::new())),
            venue_latencies: Arc::new(RwLock::new(HashMap::from([
                (Venue::Polymarket, 100),
                (Venue::Kalshi, 80),
                (Venue::Opinion, 120),
            ]))),
            event_tx,
            market_mappings: Arc::new(RwLock::new(HashMap::new())),
            fee_rates,
        }
    }

    /// Subscribe to aggregator events
    pub fn subscribe(&self) -> broadcast::Receiver<AggregatorEvent> {
        self.event_tx.subscribe()
    }

    /// Add a market mapping
    pub async fn add_market_mapping(&self, mapping: MarketVenueMapping) {
        let mut mappings = self.market_mappings.write().await;
        mappings.insert(mapping.unified_id.clone(), mapping);
    }

    /// Update venue latency
    pub async fn update_latency(&self, venue: Venue, latency_ms: u64) {
        let mut latencies = self.venue_latencies.write().await;
        latencies.insert(venue, latency_ms);
    }

    /// Process Polymarket order book update
    pub async fn process_polymarket_update(
        &self,
        token_id: &str,
        bids: Vec<(Decimal, Decimal)>,  // (price, size)
        asks: Vec<(Decimal, Decimal)>,
        timestamp: DateTime<Utc>,
    ) {
        let latency = *self.venue_latencies.read().await.get(&Venue::Polymarket).unwrap_or(&100);

        let best_bid = bids.first().map(|(p, s)| (
            Price::new(*p).unwrap_or(Price::ZERO),
            Quantity::new(*s).unwrap_or(Quantity::ZERO),
        ));
        let best_ask = asks.first().map(|(p, s)| (
            Price::new(*p).unwrap_or(Price::ZERO),
            Quantity::new(*s).unwrap_or(Quantity::ZERO),
        ));

        let mid_price = match (&best_bid, &best_ask) {
            (Some((bid, _)), Some((ask, _))) => {
                Price::new((bid.as_decimal() + ask.as_decimal()) / dec!(2))
            }
            _ => None,
        };

        let spread = match (&best_bid, &best_ask) {
            (Some((bid, _)), Some((ask, _))) => Some(ask.as_decimal() - bid.as_decimal()),
            _ => None,
        };

        let venue_book = VenueOrderbook {
            venue: Venue::Polymarket,
            market_id: token_id.to_string(),
            token_id: token_id.to_string(),
            best_bid,
            best_ask,
            mid_price,
            spread,
            total_bid_liquidity: bids.iter().map(|(_, s)| s).sum(),
            total_ask_liquidity: asks.iter().map(|(_, s)| s).sum(),
            last_update: timestamp,
            latency_ms: latency,
            is_stale: false,
        };

        {
            let mut books = self.polymarket_books.write().await;
            books.insert(token_id.to_string(), venue_book);
        }

        // Find unified market and update
        self.update_unified_book(token_id).await;
    }

    /// Process Kalshi order book update
    pub async fn process_kalshi_update(&self, orderbook: &KalshiRealtimeOrderbook) {
        let latency = *self.venue_latencies.read().await.get(&Venue::Kalshi).unwrap_or(&80);

        let best_bid = orderbook.best_yes_bid().map(|(p, c)| (
            Price::new(Decimal::from(p) / dec!(100)).unwrap_or(Price::ZERO),
            Quantity::new(Decimal::from(c)).unwrap_or(Quantity::ZERO),
        ));
        let best_ask = orderbook.best_yes_ask().map(|(p, c)| (
            Price::new(Decimal::from(p) / dec!(100)).unwrap_or(Price::ZERO),
            Quantity::new(Decimal::from(c)).unwrap_or(Quantity::ZERO),
        ));

        let mid_price = match (&best_bid, &best_ask) {
            (Some((bid, _)), Some((ask, _))) => {
                Price::new((bid.as_decimal() + ask.as_decimal()) / dec!(2))
            }
            _ => None,
        };

        let spread = match (&best_bid, &best_ask) {
            (Some((bid, _)), Some((ask, _))) => Some(ask.as_decimal() - bid.as_decimal()),
            _ => None,
        };

        let venue_book = VenueOrderbook {
            venue: Venue::Kalshi,
            market_id: orderbook.market_ticker.clone(),
            token_id: orderbook.market_ticker.clone(),
            best_bid,
            best_ask,
            mid_price,
            spread,
            total_bid_liquidity: orderbook.yes_bids.values().map(|&c| Decimal::from(c)).sum(),
            total_ask_liquidity: orderbook.no_bids.values().map(|&c| Decimal::from(c)).sum(),
            last_update: orderbook.last_update,
            latency_ms: latency,
            is_stale: false,
        };

        {
            let mut books = self.kalshi_books.write().await;
            books.insert(orderbook.market_ticker.clone(), venue_book);
        }

        self.update_unified_book(&orderbook.market_ticker).await;
    }

    /// Process Opinion price ladder update
    pub async fn process_opinion_update(&self, ladder: &OpinionPriceLadder) {
        let latency = *self.venue_latencies.read().await.get(&Venue::Opinion).unwrap_or(&120);

        // Convert odds to probability (price)
        let best_bid = ladder.best_lay().map(|(odds, size)| {
            let prob = Decimal::ONE / odds;
            (
                Price::new(prob).unwrap_or(Price::ZERO),
                Quantity::new(size).unwrap_or(Quantity::ZERO),
            )
        });
        let best_ask = ladder.best_back().map(|(odds, size)| {
            let prob = Decimal::ONE / odds;
            (
                Price::new(prob).unwrap_or(Price::ZERO),
                Quantity::new(size).unwrap_or(Quantity::ZERO),
            )
        });

        let mid_price = match (&best_bid, &best_ask) {
            (Some((bid, _)), Some((ask, _))) => {
                Price::new((bid.as_decimal() + ask.as_decimal()) / dec!(2))
            }
            _ => None,
        };

        let spread = match (&best_bid, &best_ask) {
            (Some((bid, _)), Some((ask, _))) => Some(ask.as_decimal() - bid.as_decimal()),
            _ => None,
        };

        let key = format!("{}:{}", ladder.market_id, ladder.selection_id);

        let venue_book = VenueOrderbook {
            venue: Venue::Opinion,
            market_id: ladder.market_id.clone(),
            token_id: key.clone(),
            best_bid,
            best_ask,
            mid_price,
            spread,
            total_bid_liquidity: ladder.lay_prices.iter().map(|(_, s)| *s).sum(),
            total_ask_liquidity: ladder.back_prices.iter().map(|(_, s)| *s).sum(),
            last_update: ladder.last_update,
            latency_ms: latency,
            is_stale: false,
        };

        {
            let mut books = self.opinion_books.write().await;
            books.insert(key.clone(), venue_book);
        }

        self.update_unified_book(&ladder.market_id).await;
    }

    /// Update unified order book after a venue update
    async fn update_unified_book(&self, market_id: &str) {
        let mappings = self.market_mappings.read().await;

        // Find all venue books for this market
        let mapping = mappings.iter()
            .find(|(_, m)| {
                m.polymarket_token_id.as_deref() == Some(market_id) ||
                m.kalshi_ticker.as_deref() == Some(market_id) ||
                m.opinion_market_id.as_deref() == Some(market_id)
            })
            .map(|(id, m)| (id.clone(), m.clone()));

        let (unified_id, mapping) = match mapping {
            Some((id, m)) => (id, m),
            None => return,  // No mapping found
        };

        drop(mappings);

        let now = Utc::now();
        let max_age = Duration::milliseconds(self.config.max_price_age_ms as i64);

        // Collect venue books
        let mut venue_books = HashMap::new();
        let mut bids = Vec::new();
        let mut asks = Vec::new();

        // Polymarket
        if let Some(token_id) = &mapping.polymarket_token_id {
            let books = self.polymarket_books.read().await;
            if let Some(book) = books.get(token_id) {
                let is_stale = (now - book.last_update) > max_age;
                let mut book = book.clone();
                book.is_stale = is_stale;

                if !is_stale {
                    if let Some((price, size)) = &book.best_bid {
                        bids.push(UnifiedPriceLevel {
                            venue: Venue::Polymarket,
                            price: *price,
                            size: *size,
                            timestamp: book.last_update,
                            latency_ms: book.latency_ms,
                            is_fresh: true,
                            confidence: self.calculate_confidence(&book),
                        });
                    }
                    if let Some((price, size)) = &book.best_ask {
                        asks.push(UnifiedPriceLevel {
                            venue: Venue::Polymarket,
                            price: *price,
                            size: *size,
                            timestamp: book.last_update,
                            latency_ms: book.latency_ms,
                            is_fresh: true,
                            confidence: self.calculate_confidence(&book),
                        });
                    }
                }

                venue_books.insert(Venue::Polymarket, book);
            }
        }

        // Kalshi
        if let Some(ticker) = &mapping.kalshi_ticker {
            let books = self.kalshi_books.read().await;
            if let Some(book) = books.get(ticker) {
                let is_stale = (now - book.last_update) > max_age;
                let mut book = book.clone();
                book.is_stale = is_stale;

                if !is_stale {
                    if let Some((price, size)) = &book.best_bid {
                        bids.push(UnifiedPriceLevel {
                            venue: Venue::Kalshi,
                            price: *price,
                            size: *size,
                            timestamp: book.last_update,
                            latency_ms: book.latency_ms,
                            is_fresh: true,
                            confidence: self.calculate_confidence(&book),
                        });
                    }
                    if let Some((price, size)) = &book.best_ask {
                        asks.push(UnifiedPriceLevel {
                            venue: Venue::Kalshi,
                            price: *price,
                            size: *size,
                            timestamp: book.last_update,
                            latency_ms: book.latency_ms,
                            is_fresh: true,
                            confidence: self.calculate_confidence(&book),
                        });
                    }
                }

                venue_books.insert(Venue::Kalshi, book);
            }
        }

        // Opinion
        if let Some(opinion_market_id) = &mapping.opinion_market_id {
            let key = if let Some(sel_id) = mapping.opinion_selection_id {
                format!("{}:{}", opinion_market_id, sel_id)
            } else {
                format!("{}:0", opinion_market_id)
            };

            let books = self.opinion_books.read().await;
            if let Some(book) = books.get(&key) {
                let is_stale = (now - book.last_update) > max_age;
                let mut book = book.clone();
                book.is_stale = is_stale;

                if !is_stale {
                    if let Some((price, size)) = &book.best_bid {
                        bids.push(UnifiedPriceLevel {
                            venue: Venue::Opinion,
                            price: *price,
                            size: *size,
                            timestamp: book.last_update,
                            latency_ms: book.latency_ms,
                            is_fresh: true,
                            confidence: self.calculate_confidence(&book),
                        });
                    }
                    if let Some((price, size)) = &book.best_ask {
                        asks.push(UnifiedPriceLevel {
                            venue: Venue::Opinion,
                            price: *price,
                            size: *size,
                            timestamp: book.last_update,
                            latency_ms: book.latency_ms,
                            is_fresh: true,
                            confidence: self.calculate_confidence(&book),
                        });
                    }
                }

                venue_books.insert(Venue::Opinion, book);
            }
        }

        // Sort: bids by price desc, asks by price asc
        bids.sort_by(|a, b| b.price.as_decimal().cmp(&a.price.as_decimal()));
        asks.sort_by(|a, b| a.price.as_decimal().cmp(&b.price.as_decimal()));

        // Build metadata
        let stale_venues: Vec<Venue> = venue_books.iter()
            .filter(|(_, b)| b.is_stale)
            .map(|(v, _)| *v)
            .collect();

        let best_cross_venue_bid = bids.first().map(|b| (b.venue, b.price, b.size));
        let best_cross_venue_ask = asks.first().map(|a| (a.venue, a.price, a.size));

        // Check for arbitrage: can we buy on one venue and sell on another for profit?
        let (has_arbitrage, arbitrage_profit_pct, cross_venue_spread) =
            self.calculate_arbitrage(&bids, &asks);

        let metadata = AggregationMetadata {
            venue_count: venue_books.len() as u32,
            stale_venues,
            best_cross_venue_bid,
            best_cross_venue_ask,
            cross_venue_spread,
            has_arbitrage,
            arbitrage_profit_pct,
        };

        let unified = UnifiedOrderbook {
            market_id: unified_id.clone(),
            market_name: mapping.name.clone(),
            bids,
            asks,
            venue_books,
            last_update: now,
            metadata,
        };

        // Emit arbitrage signal if detected
        if has_arbitrage {
            if let Some(signal) = self.create_arbitrage_signal(&unified) {
                let _ = self.event_tx.send(AggregatorEvent::ArbitrageSignal(signal));
            }
        }

        // Update unified book
        {
            let mut books = self.unified_books.write().await;
            books.insert(unified_id, unified.clone());
        }

        let _ = self.event_tx.send(AggregatorEvent::OrderbookUpdate(unified));
    }

    /// Calculate confidence score for a venue book
    fn calculate_confidence(&self, book: &VenueOrderbook) -> f64 {
        let mut score = 1.0;

        // Penalize low liquidity
        let total_liquidity = book.total_bid_liquidity + book.total_ask_liquidity;
        if total_liquidity < self.config.min_liquidity * dec!(2) {
            score *= 0.5;
        }

        // Penalize wide spreads
        if let Some(spread) = book.spread {
            if spread > dec!(0.05) {
                score *= 0.7;
            } else if spread > dec!(0.10) {
                score *= 0.4;
            }
        }

        // Penalize high latency
        if book.latency_ms > 200 {
            score *= 0.9;
        } else if book.latency_ms > 500 {
            score *= 0.7;
        }

        score
    }

    /// Calculate arbitrage opportunity
    fn calculate_arbitrage(
        &self,
        bids: &[UnifiedPriceLevel],
        asks: &[UnifiedPriceLevel],
    ) -> (bool, Option<Decimal>, Option<Decimal>) {
        // Find best bid and best ask on DIFFERENT venues
        let mut best_cross_bid: Option<&UnifiedPriceLevel> = None;
        let mut best_cross_ask: Option<&UnifiedPriceLevel> = None;

        for bid in bids {
            for ask in asks {
                if bid.venue != ask.venue {
                    // Check if this is an arbitrage (can buy lower than sell)
                    if ask.price.as_decimal() < bid.price.as_decimal() {
                        if best_cross_bid.is_none() ||
                           bid.price.as_decimal() > best_cross_bid.unwrap().price.as_decimal() {
                            best_cross_bid = Some(bid);
                            best_cross_ask = Some(ask);
                        }
                    }
                }
            }
        }

        match (best_cross_bid, best_cross_ask) {
            (Some(bid), Some(ask)) => {
                let spread = bid.price.as_decimal() - ask.price.as_decimal();

                // Calculate profit after fees
                let buy_fee = self.fee_rates.get(&ask.venue).unwrap_or(&dec!(0.02));
                let sell_fee = self.fee_rates.get(&bid.venue).unwrap_or(&dec!(0.02));

                let effective_buy = ask.price.as_decimal() * (Decimal::ONE + buy_fee);
                let effective_sell = bid.price.as_decimal() * (Decimal::ONE - sell_fee);

                let profit_pct = if effective_buy > Decimal::ZERO {
                    (effective_sell - effective_buy) / effective_buy
                } else {
                    Decimal::ZERO
                };

                let has_arb = profit_pct > Decimal::ZERO;

                (has_arb, Some(profit_pct), Some(spread))
            }
            _ => {
                // Calculate normal spread (not arbitrage)
                let spread = match (bids.first(), asks.first()) {
                    (Some(bid), Some(ask)) => {
                        Some(ask.price.as_decimal() - bid.price.as_decimal())
                    }
                    _ => None,
                };
                (false, None, spread)
            }
        }
    }

    /// Create arbitrage signal from unified order book
    fn create_arbitrage_signal(&self, unified: &UnifiedOrderbook) -> Option<CrossVenueArbitrageSignal> {
        // Find the best cross-venue arbitrage
        let mut best_signal: Option<CrossVenueArbitrageSignal> = None;
        let mut best_profit = Decimal::ZERO;

        for bid in &unified.bids {
            for ask in &unified.asks {
                if bid.venue != ask.venue && ask.price.as_decimal() < bid.price.as_decimal() {
                    let buy_fee = self.fee_rates.get(&ask.venue).unwrap_or(&dec!(0.02));
                    let sell_fee = self.fee_rates.get(&bid.venue).unwrap_or(&dec!(0.02));

                    let effective_buy = ask.price.as_decimal() * (Decimal::ONE + buy_fee);
                    let effective_sell = bid.price.as_decimal() * (Decimal::ONE - sell_fee);

                    if effective_sell > effective_buy {
                        let gross_profit_pct = (bid.price.as_decimal() - ask.price.as_decimal())
                            / ask.price.as_decimal();
                        let net_profit_pct = (effective_sell - effective_buy) / effective_buy;

                        let max_size = bid.size.as_decimal().min(ask.size.as_decimal());

                        if net_profit_pct > best_profit && max_size >= self.config.min_liquidity {
                            best_profit = net_profit_pct;

                            best_signal = Some(CrossVenueArbitrageSignal {
                                market_id: unified.market_id.clone(),
                                buy_venue: ask.venue,
                                buy_price: ask.price,
                                buy_size: ask.size,
                                buy_latency_ms: ask.latency_ms,
                                sell_venue: bid.venue,
                                sell_price: bid.price,
                                sell_size: bid.size,
                                sell_latency_ms: bid.latency_ms,
                                gross_profit_pct,
                                net_profit_pct,
                                max_size: Quantity::new(max_size).unwrap_or(Quantity::ZERO),
                                confidence: (bid.confidence + ask.confidence) / 2.0,
                                detected_at: Utc::now(),
                                estimated_execution_time_ms: ask.latency_ms + bid.latency_ms + 50, // +50ms for processing
                            });
                        }
                    }
                }
            }
        }

        best_signal
    }

    /// Get unified order book
    pub async fn get_unified_book(&self, market_id: &str) -> Option<UnifiedOrderbook> {
        self.unified_books.read().await.get(market_id).cloned()
    }

    /// Get all unified order books
    pub async fn get_all_unified_books(&self) -> HashMap<String, UnifiedOrderbook> {
        self.unified_books.read().await.clone()
    }

    /// Check for stale venues across all markets
    pub async fn check_staleness(&self) -> Vec<(String, Venue)> {
        let now = Utc::now();
        let max_age = Duration::milliseconds(self.config.max_price_age_ms as i64);
        let mut stale = Vec::new();

        let books = self.unified_books.read().await;
        for (market_id, book) in books.iter() {
            for (venue, venue_book) in &book.venue_books {
                if (now - venue_book.last_update) > max_age {
                    stale.push((market_id.clone(), *venue));
                }
            }
        }

        stale
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_arbitrage_detection() {
        let aggregator = CrossVenueAggregator::new(CrossVenueAggregatorConfig::default());

        let bids = vec![
            UnifiedPriceLevel {
                venue: Venue::Kalshi,
                price: Price::new(dec!(0.55)).unwrap(),
                size: Quantity::new(dec!(100)).unwrap(),
                timestamp: Utc::now(),
                latency_ms: 50,
                is_fresh: true,
                confidence: 0.9,
            },
        ];

        let asks = vec![
            UnifiedPriceLevel {
                venue: Venue::Polymarket,
                price: Price::new(dec!(0.50)).unwrap(),
                size: Quantity::new(dec!(100)).unwrap(),
                timestamp: Utc::now(),
                latency_ms: 60,
                is_fresh: true,
                confidence: 0.9,
            },
        ];

        let (has_arb, profit_pct, spread) = aggregator.calculate_arbitrage(&bids, &asks);

        // Buy at 0.50 on Polymarket, sell at 0.55 on Kalshi = 10% gross profit
        // After fees (~3% total): ~7% net profit
        assert!(has_arb);
        assert!(profit_pct.unwrap() > dec!(0.05));
        assert_eq!(spread, Some(dec!(0.05)));
    }

    #[test]
    fn test_no_arbitrage_same_venue() {
        let aggregator = CrossVenueAggregator::new(CrossVenueAggregatorConfig::default());

        let bids = vec![
            UnifiedPriceLevel {
                venue: Venue::Polymarket,
                price: Price::new(dec!(0.55)).unwrap(),
                size: Quantity::new(dec!(100)).unwrap(),
                timestamp: Utc::now(),
                latency_ms: 50,
                is_fresh: true,
                confidence: 0.9,
            },
        ];

        let asks = vec![
            UnifiedPriceLevel {
                venue: Venue::Polymarket,  // Same venue
                price: Price::new(dec!(0.50)).unwrap(),
                size: Quantity::new(dec!(100)).unwrap(),
                timestamp: Utc::now(),
                latency_ms: 60,
                is_fresh: true,
                confidence: 0.9,
            },
        ];

        let (has_arb, _, _) = aggregator.calculate_arbitrage(&bids, &asks);

        // No arbitrage on same venue
        assert!(!has_arb);
    }
}
