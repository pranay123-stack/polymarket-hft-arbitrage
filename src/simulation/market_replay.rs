//! Market replay engine for historical backtesting.
//!
//! Provides:
//! - Historical tick data replay
//! - Multi-venue synchronized playback
//! - Event-driven simulation
//! - Gap handling and time scaling

use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::cmp::Ordering;
use std::sync::Arc;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::latency_model::Venue;
use super::fill_simulator::{SimulatedOrderBook, SimulatedBookLevel};

/// Configuration for market replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    /// Start timestamp in milliseconds.
    pub start_time_ms: u64,
    /// End timestamp in milliseconds.
    pub end_time_ms: u64,
    /// Playback speed multiplier (1.0 = real-time).
    pub playback_speed: f64,
    /// Maximum events per second (rate limiting).
    pub max_events_per_second: u32,
    /// Skip gaps longer than this (milliseconds).
    pub max_gap_ms: u64,
    /// Venues to replay.
    pub venues: Vec<Venue>,
    /// Market IDs to include (empty = all).
    pub market_ids: Vec<String>,
    /// Enable cross-venue time synchronization.
    pub sync_across_venues: bool,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            start_time_ms: 0,
            end_time_ms: u64::MAX,
            playback_speed: 1.0,
            max_events_per_second: 10000,
            max_gap_ms: 60000, // 1 minute
            venues: vec![Venue::Polymarket, Venue::Kalshi, Venue::Opinion],
            market_ids: Vec::new(),
            sync_across_venues: true,
        }
    }
}

/// A historical market tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalTick {
    /// Timestamp in milliseconds since epoch.
    pub timestamp_ms: u64,
    /// Venue this tick is from.
    pub venue: Venue,
    /// Market identifier.
    pub market_id: String,
    /// Tick type.
    pub tick_type: TickType,
    /// Tick data.
    pub data: TickData,
}

impl PartialEq for HistoricalTick {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp_ms == other.timestamp_ms
    }
}

impl Eq for HistoricalTick {}

impl PartialOrd for HistoricalTick {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HistoricalTick {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap (earliest first)
        other.timestamp_ms.cmp(&self.timestamp_ms)
    }
}

/// Type of tick event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TickType {
    /// Order book update.
    BookUpdate,
    /// Trade execution.
    Trade,
    /// Quote (BBO) update.
    Quote,
    /// Market status change.
    MarketStatus,
    /// Snapshot (full book).
    Snapshot,
}

/// Data contained in a tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TickData {
    /// Order book update data.
    BookUpdate {
        side: BookSide,
        price: Decimal,
        quantity: Decimal,
        is_delete: bool,
    },
    /// Trade data.
    Trade {
        price: Decimal,
        quantity: Decimal,
        aggressor_side: BookSide,
    },
    /// Quote (BBO) data.
    Quote {
        bid_price: Option<Decimal>,
        bid_size: Option<Decimal>,
        ask_price: Option<Decimal>,
        ask_size: Option<Decimal>,
    },
    /// Market status.
    MarketStatus {
        is_open: bool,
        reason: Option<String>,
    },
    /// Full snapshot.
    Snapshot {
        bids: Vec<(Decimal, Decimal)>,
        asks: Vec<(Decimal, Decimal)>,
    },
}

/// Order book side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BookSide {
    Bid,
    Ask,
}

/// Replay event sent to consumers.
#[derive(Debug, Clone)]
pub struct ReplayEvent {
    pub tick: HistoricalTick,
    pub books: HashMap<(Venue, String), SimulatedOrderBook>,
    pub replay_time_ms: u64,
    pub wall_time_ms: u64,
}

/// State of the replay engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayState {
    Idle,
    Running,
    Paused,
    Completed,
    Error,
}

/// Statistics for replay.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplayStats {
    pub ticks_processed: u64,
    pub ticks_skipped: u64,
    pub gaps_skipped: u64,
    pub current_time_ms: u64,
    pub elapsed_real_ms: u64,
    pub effective_speed: f64,
    pub events_per_second: f64,
}

/// Market replay engine for backtesting.
pub struct MarketReplayEngine {
    /// Replay configuration.
    config: ReplayConfig,
    /// Current state.
    state: Arc<RwLock<ReplayState>>,
    /// Event heap (min-heap by timestamp).
    event_heap: BinaryHeap<HistoricalTick>,
    /// Current order books per venue/market.
    books: Arc<RwLock<HashMap<(Venue, String), SimulatedOrderBook>>>,
    /// Replay statistics.
    stats: Arc<RwLock<ReplayStats>>,
    /// Current replay time.
    current_time_ms: u64,
}

impl MarketReplayEngine {
    /// Create a new replay engine.
    pub fn new(config: ReplayConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(ReplayState::Idle)),
            event_heap: BinaryHeap::new(),
            books: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(ReplayStats::default())),
            current_time_ms: 0,
        }
    }

    /// Load historical ticks into the replay engine.
    pub fn load_ticks(&mut self, ticks: Vec<HistoricalTick>) {
        for tick in ticks {
            // Filter by time range
            if tick.timestamp_ms < self.config.start_time_ms
                || tick.timestamp_ms > self.config.end_time_ms {
                continue;
            }

            // Filter by venue
            if !self.config.venues.contains(&tick.venue) {
                continue;
            }

            // Filter by market ID
            if !self.config.market_ids.is_empty()
                && !self.config.market_ids.contains(&tick.market_id) {
                continue;
            }

            self.event_heap.push(tick);
        }

        // Set current time to start time
        self.current_time_ms = self.config.start_time_ms;
    }

    /// Load ticks from a CSV file.
    pub fn load_from_csv(&mut self, path: &str) -> Result<usize, std::io::Error> {
        use std::io::{BufRead, BufReader};
        use std::fs::File;

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut count = 0;

        for line in reader.lines().skip(1) { // Skip header
            let line = line?;
            if let Some(tick) = Self::parse_csv_line(&line) {
                self.event_heap.push(tick);
                count += 1;
            }
        }

        self.current_time_ms = self.config.start_time_ms;
        Ok(count)
    }

    /// Parse a CSV line into a historical tick.
    fn parse_csv_line(line: &str) -> Option<HistoricalTick> {
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < 6 {
            return None;
        }

        let timestamp_ms = parts[0].parse().ok()?;
        let venue = match parts[1].to_lowercase().as_str() {
            "polymarket" => Venue::Polymarket,
            "kalshi" => Venue::Kalshi,
            "opinion" => Venue::Opinion,
            _ => return None,
        };
        let market_id = parts[2].to_string();
        let tick_type = match parts[3].to_lowercase().as_str() {
            "trade" => TickType::Trade,
            "quote" => TickType::Quote,
            "book" | "bookupdate" => TickType::BookUpdate,
            "snapshot" => TickType::Snapshot,
            "status" => TickType::MarketStatus,
            _ => return None,
        };

        let data = match tick_type {
            TickType::Trade => {
                let price = parts.get(4)?.parse().ok()?;
                let quantity = parts.get(5)?.parse().ok()?;
                let side = if parts.get(6).map(|s| s.to_lowercase()) == Some("buy".to_string()) {
                    BookSide::Bid
                } else {
                    BookSide::Ask
                };
                TickData::Trade { price, quantity, aggressor_side: side }
            }
            TickType::Quote => {
                TickData::Quote {
                    bid_price: parts.get(4).and_then(|s| s.parse().ok()),
                    bid_size: parts.get(5).and_then(|s| s.parse().ok()),
                    ask_price: parts.get(6).and_then(|s| s.parse().ok()),
                    ask_size: parts.get(7).and_then(|s| s.parse().ok()),
                }
            }
            TickType::BookUpdate => {
                let side = if parts.get(4)?.to_lowercase() == "bid" {
                    BookSide::Bid
                } else {
                    BookSide::Ask
                };
                let price = parts.get(5)?.parse().ok()?;
                let quantity = parts.get(6)?.parse().ok()?;
                let is_delete = quantity == Decimal::ZERO;
                TickData::BookUpdate { side, price, quantity, is_delete }
            }
            _ => return None,
        };

        Some(HistoricalTick {
            timestamp_ms,
            venue,
            market_id,
            tick_type,
            data,
        })
    }

    /// Run the replay and send events to the provided channel.
    pub async fn run(&mut self, event_tx: mpsc::Sender<ReplayEvent>) -> Result<ReplayStats, String> {
        *self.state.write() = ReplayState::Running;

        let start_wall_time = std::time::Instant::now();
        let mut last_tick_time = self.current_time_ms;
        let mut ticks_processed = 0u64;
        let mut ticks_skipped = 0u64;
        let mut gaps_skipped = 0u64;

        while let Some(tick) = self.event_heap.pop() {
            // Check if paused or stopped
            if *self.state.read() != ReplayState::Running {
                break;
            }

            // Handle time gaps
            let gap = tick.timestamp_ms.saturating_sub(last_tick_time);
            if gap > self.config.max_gap_ms {
                gaps_skipped += 1;
                // Skip the gap, don't wait
            } else if self.config.playback_speed > 0.0 {
                // Calculate how long to wait
                let wait_ms = (gap as f64 / self.config.playback_speed) as u64;
                if wait_ms > 0 && wait_ms < 1000 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(wait_ms)).await;
                }
            }

            last_tick_time = tick.timestamp_ms;
            self.current_time_ms = tick.timestamp_ms;

            // Apply tick to order book
            self.apply_tick(&tick);

            // Create replay event
            let event = ReplayEvent {
                tick: tick.clone(),
                books: self.books.read().clone(),
                replay_time_ms: self.current_time_ms,
                wall_time_ms: start_wall_time.elapsed().as_millis() as u64,
            };

            // Send event
            if event_tx.send(event).await.is_err() {
                // Receiver dropped
                break;
            }

            ticks_processed += 1;

            // Update stats periodically
            if ticks_processed % 1000 == 0 {
                let elapsed = start_wall_time.elapsed().as_millis() as f64;
                let mut stats = self.stats.write();
                stats.ticks_processed = ticks_processed;
                stats.ticks_skipped = ticks_skipped;
                stats.gaps_skipped = gaps_skipped;
                stats.current_time_ms = self.current_time_ms;
                stats.elapsed_real_ms = elapsed as u64;
                stats.events_per_second = if elapsed > 0.0 {
                    ticks_processed as f64 / (elapsed / 1000.0)
                } else {
                    0.0
                };
            }
        }

        *self.state.write() = ReplayState::Completed;

        let final_stats = self.stats.read().clone();
        Ok(final_stats)
    }

    /// Apply a tick to the order book state.
    fn apply_tick(&mut self, tick: &HistoricalTick) {
        let key = (tick.venue, tick.market_id.clone());

        match &tick.data {
            TickData::BookUpdate { side, price, quantity, is_delete } => {
                let mut books = self.books.write();
                let book = books.entry(key).or_insert_with(|| SimulatedOrderBook {
                    venue: tick.venue,
                    market_id: tick.market_id.clone(),
                    bids: Vec::new(),
                    asks: Vec::new(),
                    last_update_ms: tick.timestamp_ms,
                });

                let levels = match side {
                    BookSide::Bid => &mut book.bids,
                    BookSide::Ask => &mut book.asks,
                };

                if *is_delete {
                    levels.retain(|l| l.price != *price);
                } else {
                    if let Some(level) = levels.iter_mut().find(|l| l.price == *price) {
                        level.quantity = *quantity;
                    } else {
                        levels.push(SimulatedBookLevel {
                            price: *price,
                            quantity: *quantity,
                            order_count: 1,
                        });
                    }
                }

                // Sort: bids descending, asks ascending
                match side {
                    BookSide::Bid => levels.sort_by(|a, b| b.price.cmp(&a.price)),
                    BookSide::Ask => levels.sort_by(|a, b| a.price.cmp(&b.price)),
                }

                book.last_update_ms = tick.timestamp_ms;
            }

            TickData::Quote { bid_price, bid_size, ask_price, ask_size } => {
                let mut books = self.books.write();
                let book = books.entry(key).or_insert_with(|| SimulatedOrderBook {
                    venue: tick.venue,
                    market_id: tick.market_id.clone(),
                    bids: Vec::new(),
                    asks: Vec::new(),
                    last_update_ms: tick.timestamp_ms,
                });

                // Update BBO (top of book)
                if let (Some(price), Some(size)) = (bid_price, bid_size) {
                    if book.bids.is_empty() {
                        book.bids.push(SimulatedBookLevel {
                            price: *price,
                            quantity: *size,
                            order_count: 1,
                        });
                    } else {
                        book.bids[0].price = *price;
                        book.bids[0].quantity = *size;
                    }
                }

                if let (Some(price), Some(size)) = (ask_price, ask_size) {
                    if book.asks.is_empty() {
                        book.asks.push(SimulatedBookLevel {
                            price: *price,
                            quantity: *size,
                            order_count: 1,
                        });
                    } else {
                        book.asks[0].price = *price;
                        book.asks[0].quantity = *size;
                    }
                }

                book.last_update_ms = tick.timestamp_ms;
            }

            TickData::Snapshot { bids, asks } => {
                let mut books = self.books.write();
                let book = books.entry(key).or_insert_with(|| SimulatedOrderBook {
                    venue: tick.venue,
                    market_id: tick.market_id.clone(),
                    bids: Vec::new(),
                    asks: Vec::new(),
                    last_update_ms: tick.timestamp_ms,
                });

                book.bids = bids.iter().map(|(p, q)| SimulatedBookLevel {
                    price: *p,
                    quantity: *q,
                    order_count: 1,
                }).collect();

                book.asks = asks.iter().map(|(p, q)| SimulatedBookLevel {
                    price: *p,
                    quantity: *q,
                    order_count: 1,
                }).collect();

                book.bids.sort_by(|a, b| b.price.cmp(&a.price));
                book.asks.sort_by(|a, b| a.price.cmp(&b.price));
                book.last_update_ms = tick.timestamp_ms;
            }

            TickData::Trade { .. } => {
                // Trades don't directly update the book, but we track the time
                if let Some(book) = self.books.write().get_mut(&key) {
                    book.last_update_ms = tick.timestamp_ms;
                }
            }

            TickData::MarketStatus { .. } => {
                // Market status changes could clear the book
            }
        }
    }

    /// Pause the replay.
    pub fn pause(&self) {
        *self.state.write() = ReplayState::Paused;
    }

    /// Resume the replay.
    pub fn resume(&self) {
        *self.state.write() = ReplayState::Running;
    }

    /// Stop the replay.
    pub fn stop(&self) {
        *self.state.write() = ReplayState::Completed;
    }

    /// Get current state.
    pub fn state(&self) -> ReplayState {
        *self.state.read()
    }

    /// Get current statistics.
    pub fn stats(&self) -> ReplayStats {
        self.stats.read().clone()
    }

    /// Get current order books.
    pub fn books(&self) -> HashMap<(Venue, String), SimulatedOrderBook> {
        self.books.read().clone()
    }

    /// Get order book for a specific venue/market.
    pub fn get_book(&self, venue: Venue, market_id: &str) -> Option<SimulatedOrderBook> {
        self.books.read().get(&(venue, market_id.to_string())).cloned()
    }

    /// Get current replay time.
    pub fn current_time(&self) -> u64 {
        self.current_time_ms
    }

    /// Get remaining ticks.
    pub fn remaining_ticks(&self) -> usize {
        self.event_heap.len()
    }
}

/// Builder for creating historical tick data (for testing).
pub struct TickBuilder {
    ticks: Vec<HistoricalTick>,
    current_time: u64,
}

impl TickBuilder {
    /// Create a new tick builder.
    pub fn new(start_time: u64) -> Self {
        Self {
            ticks: Vec::new(),
            current_time: start_time,
        }
    }

    /// Advance time by milliseconds.
    pub fn advance(&mut self, ms: u64) -> &mut Self {
        self.current_time += ms;
        self
    }

    /// Add a quote tick.
    pub fn quote(
        &mut self,
        venue: Venue,
        market_id: &str,
        bid: Decimal,
        bid_size: Decimal,
        ask: Decimal,
        ask_size: Decimal,
    ) -> &mut Self {
        self.ticks.push(HistoricalTick {
            timestamp_ms: self.current_time,
            venue,
            market_id: market_id.to_string(),
            tick_type: TickType::Quote,
            data: TickData::Quote {
                bid_price: Some(bid),
                bid_size: Some(bid_size),
                ask_price: Some(ask),
                ask_size: Some(ask_size),
            },
        });
        self
    }

    /// Add a trade tick.
    pub fn trade(
        &mut self,
        venue: Venue,
        market_id: &str,
        price: Decimal,
        quantity: Decimal,
        side: BookSide,
    ) -> &mut Self {
        self.ticks.push(HistoricalTick {
            timestamp_ms: self.current_time,
            venue,
            market_id: market_id.to_string(),
            tick_type: TickType::Trade,
            data: TickData::Trade {
                price,
                quantity,
                aggressor_side: side,
            },
        });
        self
    }

    /// Add a book update.
    pub fn book_update(
        &mut self,
        venue: Venue,
        market_id: &str,
        side: BookSide,
        price: Decimal,
        quantity: Decimal,
    ) -> &mut Self {
        self.ticks.push(HistoricalTick {
            timestamp_ms: self.current_time,
            venue,
            market_id: market_id.to_string(),
            tick_type: TickType::BookUpdate,
            data: TickData::BookUpdate {
                side,
                price,
                quantity,
                is_delete: quantity == Decimal::ZERO,
            },
        });
        self
    }

    /// Build the tick vector.
    pub fn build(self) -> Vec<HistoricalTick> {
        self.ticks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tick_builder() {
        let ticks = TickBuilder::new(1000)
            .quote(Venue::Kalshi, "MKT1", Decimal::new(49, 2), Decimal::new(100, 0), Decimal::new(51, 2), Decimal::new(100, 0))
            .advance(100)
            .trade(Venue::Kalshi, "MKT1", Decimal::new(51, 2), Decimal::new(50, 0), BookSide::Bid)
            .build();

        assert_eq!(ticks.len(), 2);
        assert_eq!(ticks[0].timestamp_ms, 1000);
        assert_eq!(ticks[1].timestamp_ms, 1100);
    }

    #[test]
    fn test_replay_engine_creation() {
        let config = ReplayConfig::default();
        let engine = MarketReplayEngine::new(config);
        assert_eq!(engine.state(), ReplayState::Idle);
    }

    #[test]
    fn test_tick_loading() {
        let config = ReplayConfig {
            start_time_ms: 0,
            end_time_ms: 10000,
            ..Default::default()
        };
        let mut engine = MarketReplayEngine::new(config);

        let ticks = TickBuilder::new(1000)
            .quote(Venue::Kalshi, "MKT1", Decimal::new(49, 2), Decimal::new(100, 0), Decimal::new(51, 2), Decimal::new(100, 0))
            .advance(100)
            .quote(Venue::Polymarket, "MKT2", Decimal::new(48, 2), Decimal::new(200, 0), Decimal::new(52, 2), Decimal::new(200, 0))
            .build();

        engine.load_ticks(ticks);
        assert_eq!(engine.remaining_ticks(), 2);
    }
}
