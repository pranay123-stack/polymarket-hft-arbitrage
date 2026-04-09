//! Kalshi WebSocket Client for Real-Time Market Data
//!
//! Provides low-latency streaming of:
//! - Order book updates
//! - Trade executions
//! - Market status changes
//! - User order/fill notifications
//!
//! Critical for cross-platform arbitrage where latency matters.

use crate::core::{
    error::{Error, Result},
    types::{Price, Quantity, Side},
};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time::{interval, timeout, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use url::Url;

/// Kalshi WebSocket configuration
#[derive(Debug, Clone)]
pub struct KalshiWsConfig {
    pub ws_url: String,
    pub auth_token: String,
    pub reconnect_delay_ms: u64,
    pub max_reconnect_attempts: u32,
    pub ping_interval_secs: u64,
    pub pong_timeout_secs: u64,
}

impl Default for KalshiWsConfig {
    fn default() -> Self {
        Self {
            ws_url: "wss://trading-api.kalshi.com/trade-api/ws/v2".to_string(),
            auth_token: String::new(),
            reconnect_delay_ms: 1000,
            max_reconnect_attempts: 10,
            ping_interval_secs: 30,
            pong_timeout_secs: 10,
        }
    }
}

/// Kalshi WebSocket message types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KalshiWsMessage {
    /// Subscribe to channels
    Subscribe {
        channels: Vec<KalshiChannel>,
    },
    /// Unsubscribe from channels
    Unsubscribe {
        channels: Vec<KalshiChannel>,
    },
    /// Subscription confirmation
    Subscribed {
        channel: KalshiChannel,
    },
    /// Order book snapshot
    OrderbookSnapshot {
        market_ticker: String,
        yes: Vec<KalshiWsLevel>,
        no: Vec<KalshiWsLevel>,
        ts: i64,
    },
    /// Order book delta update
    OrderbookDelta {
        market_ticker: String,
        side: String,
        price: i64,
        delta: i64,
        ts: i64,
    },
    /// Trade execution
    Trade {
        market_ticker: String,
        trade_id: String,
        price: i64,
        count: i64,
        side: String,
        ts: i64,
    },
    /// Market status change
    MarketStatus {
        market_ticker: String,
        status: String,
        ts: i64,
    },
    /// User order update
    OrderUpdate {
        order_id: String,
        market_ticker: String,
        status: String,
        remaining_count: i64,
        avg_price: Option<i64>,
        ts: i64,
    },
    /// User fill notification
    Fill {
        trade_id: String,
        order_id: String,
        market_ticker: String,
        side: String,
        price: i64,
        count: i64,
        is_taker: bool,
        ts: i64,
    },
    /// Heartbeat
    Heartbeat {
        ts: i64,
    },
    /// Error message
    Error {
        code: String,
        message: String,
    },
}

/// Kalshi subscription channel
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "name", rename_all = "snake_case")]
pub enum KalshiChannel {
    /// Order book updates for a market
    Orderbook { market_ticker: String },
    /// Trade feed for a market
    Trades { market_ticker: String },
    /// Market status updates
    MarketStatus { market_ticker: String },
    /// User order updates (requires auth)
    Orders,
    /// User fill notifications (requires auth)
    Fills,
}

/// Order book level from WebSocket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiWsLevel {
    pub price: i64,
    pub count: i64,
}

/// Real-time order book maintained from WebSocket updates
#[derive(Debug, Clone)]
pub struct KalshiRealtimeOrderbook {
    pub market_ticker: String,
    pub yes_bids: HashMap<i64, i64>,  // price -> count
    pub yes_asks: HashMap<i64, i64>,
    pub no_bids: HashMap<i64, i64>,
    pub no_asks: HashMap<i64, i64>,
    pub last_update: DateTime<Utc>,
    pub sequence: u64,
}

impl KalshiRealtimeOrderbook {
    pub fn new(market_ticker: String) -> Self {
        Self {
            market_ticker,
            yes_bids: HashMap::new(),
            yes_asks: HashMap::new(),
            no_bids: HashMap::new(),
            no_asks: HashMap::new(),
            last_update: Utc::now(),
            sequence: 0,
        }
    }

    /// Apply a snapshot
    pub fn apply_snapshot(&mut self, yes_levels: &[KalshiWsLevel], no_levels: &[KalshiWsLevel]) {
        self.yes_bids.clear();
        self.yes_asks.clear();
        self.no_bids.clear();
        self.no_asks.clear();

        // In Kalshi, YES and NO are inverses
        // YES bids are at prices where people want to buy YES
        for level in yes_levels {
            if level.count > 0 {
                self.yes_bids.insert(level.price, level.count);
            }
        }

        for level in no_levels {
            if level.count > 0 {
                self.no_bids.insert(level.price, level.count);
            }
        }

        self.last_update = Utc::now();
        self.sequence += 1;
    }

    /// Apply a delta update
    pub fn apply_delta(&mut self, side: &str, price: i64, delta: i64) {
        let book = match side {
            "yes" => &mut self.yes_bids,
            "no" => &mut self.no_bids,
            _ => return,
        };

        let entry = book.entry(price).or_insert(0);
        *entry += delta;

        if *entry <= 0 {
            book.remove(&price);
        }

        self.last_update = Utc::now();
        self.sequence += 1;
    }

    /// Get best YES bid (highest price to buy YES)
    pub fn best_yes_bid(&self) -> Option<(i64, i64)> {
        self.yes_bids.iter()
            .max_by_key(|(price, _)| *price)
            .map(|(&p, &c)| (p, c))
    }

    /// Get best YES ask (lowest price to sell YES = 100 - best NO bid)
    pub fn best_yes_ask(&self) -> Option<(i64, i64)> {
        self.no_bids.iter()
            .max_by_key(|(price, _)| *price)
            .map(|(&no_price, &c)| (100 - no_price, c))
    }

    /// Get normalized prices (0.0-1.0)
    pub fn normalized_best_bid(&self) -> Option<Price> {
        self.best_yes_bid()
            .and_then(|(p, _)| Price::new(Decimal::from(p) / Decimal::from(100)))
    }

    pub fn normalized_best_ask(&self) -> Option<Price> {
        self.best_yes_ask()
            .and_then(|(p, _)| Price::new(Decimal::from(p) / Decimal::from(100)))
    }
}

/// Event emitted by Kalshi WebSocket
#[derive(Debug, Clone)]
pub enum KalshiWsEvent {
    Connected,
    Disconnected,
    Reconnecting { attempt: u32 },
    OrderbookUpdate { ticker: String, orderbook: KalshiRealtimeOrderbook },
    Trade { ticker: String, price: i64, count: i64, side: String },
    OrderUpdate { order_id: String, status: String, remaining: i64 },
    Fill { order_id: String, price: i64, count: i64 },
    Error { code: String, message: String },
}

/// Kalshi WebSocket client
pub struct KalshiWebSocket {
    config: KalshiWsConfig,
    /// Real-time order books by ticker
    orderbooks: Arc<RwLock<HashMap<String, KalshiRealtimeOrderbook>>>,
    /// Subscribed channels
    subscriptions: Arc<RwLock<Vec<KalshiChannel>>>,
    /// Event broadcast channel
    event_tx: broadcast::Sender<KalshiWsEvent>,
    /// Command channel for subscribe/unsubscribe
    cmd_tx: Option<mpsc::Sender<KalshiWsCommand>>,
    /// Connection state
    connected: Arc<RwLock<bool>>,
    /// Latency tracking (last ping/pong round trip)
    latency_ms: Arc<RwLock<u64>>,
}

/// Commands to WebSocket task
enum KalshiWsCommand {
    Subscribe(Vec<KalshiChannel>),
    Unsubscribe(Vec<KalshiChannel>),
    Disconnect,
}

impl KalshiWebSocket {
    pub fn new(config: KalshiWsConfig) -> Self {
        let (event_tx, _) = broadcast::channel(1000);

        Self {
            config,
            orderbooks: Arc::new(RwLock::new(HashMap::new())),
            subscriptions: Arc::new(RwLock::new(Vec::new())),
            event_tx,
            cmd_tx: None,
            connected: Arc::new(RwLock::new(false)),
            latency_ms: Arc::new(RwLock::new(0)),
        }
    }

    /// Get event receiver
    pub fn subscribe_events(&self) -> broadcast::Receiver<KalshiWsEvent> {
        self.event_tx.subscribe()
    }

    /// Get current latency
    pub async fn latency_ms(&self) -> u64 {
        *self.latency_ms.read().await
    }

    /// Check if connected
    pub async fn is_connected(&self) -> bool {
        *self.connected.read().await
    }

    /// Get order book for a ticker
    pub async fn get_orderbook(&self, ticker: &str) -> Option<KalshiRealtimeOrderbook> {
        self.orderbooks.read().await.get(ticker).cloned()
    }

    /// Subscribe to market data
    pub async fn subscribe_market(&self, ticker: &str) -> Result<()> {
        let channels = vec![
            KalshiChannel::Orderbook { market_ticker: ticker.to_string() },
            KalshiChannel::Trades { market_ticker: ticker.to_string() },
        ];

        if let Some(cmd_tx) = &self.cmd_tx {
            cmd_tx.send(KalshiWsCommand::Subscribe(channels.clone())).await
                .map_err(|_| Error::WebSocket("Failed to send subscribe command".to_string()))?;
        }

        // Initialize orderbook
        {
            let mut orderbooks = self.orderbooks.write().await;
            orderbooks.entry(ticker.to_string())
                .or_insert_with(|| KalshiRealtimeOrderbook::new(ticker.to_string()));
        }

        Ok(())
    }

    /// Subscribe to user updates (orders/fills)
    pub async fn subscribe_user(&self) -> Result<()> {
        let channels = vec![
            KalshiChannel::Orders,
            KalshiChannel::Fills,
        ];

        if let Some(cmd_tx) = &self.cmd_tx {
            cmd_tx.send(KalshiWsCommand::Subscribe(channels)).await
                .map_err(|_| Error::WebSocket("Failed to send subscribe command".to_string()))?;
        }

        Ok(())
    }

    /// Connect and start processing
    pub async fn connect(&mut self) -> Result<()> {
        let (cmd_tx, cmd_rx) = mpsc::channel(100);
        self.cmd_tx = Some(cmd_tx);

        let config = self.config.clone();
        let orderbooks = self.orderbooks.clone();
        let subscriptions = self.subscriptions.clone();
        let event_tx = self.event_tx.clone();
        let connected = self.connected.clone();
        let latency = self.latency_ms.clone();

        tokio::spawn(async move {
            Self::connection_loop(
                config,
                orderbooks,
                subscriptions,
                event_tx,
                cmd_rx,
                connected,
                latency,
            ).await;
        });

        // Wait for connection
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(10) {
            if *self.connected.read().await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Err(Error::WebSocket("Connection timeout".to_string()))
    }

    /// Main connection loop with reconnection
    async fn connection_loop(
        config: KalshiWsConfig,
        orderbooks: Arc<RwLock<HashMap<String, KalshiRealtimeOrderbook>>>,
        subscriptions: Arc<RwLock<Vec<KalshiChannel>>>,
        event_tx: broadcast::Sender<KalshiWsEvent>,
        mut cmd_rx: mpsc::Receiver<KalshiWsCommand>,
        connected: Arc<RwLock<bool>>,
        latency: Arc<RwLock<u64>>,
    ) {
        let mut reconnect_attempts = 0;

        loop {
            info!("Connecting to Kalshi WebSocket...");

            match Self::connect_and_run(
                &config,
                &orderbooks,
                &subscriptions,
                &event_tx,
                &mut cmd_rx,
                &connected,
                &latency,
            ).await {
                Ok(_) => {
                    reconnect_attempts = 0;
                }
                Err(e) => {
                    error!("Kalshi WebSocket error: {}", e);
                }
            }

            *connected.write().await = false;
            let _ = event_tx.send(KalshiWsEvent::Disconnected);

            reconnect_attempts += 1;
            if reconnect_attempts > config.max_reconnect_attempts {
                error!("Max reconnection attempts reached, stopping");
                break;
            }

            let _ = event_tx.send(KalshiWsEvent::Reconnecting { attempt: reconnect_attempts });

            let delay = config.reconnect_delay_ms * (2_u64.pow(reconnect_attempts.min(5)));
            warn!("Reconnecting in {}ms (attempt {})", delay, reconnect_attempts);
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
    }

    /// Single connection attempt
    async fn connect_and_run(
        config: &KalshiWsConfig,
        orderbooks: &Arc<RwLock<HashMap<String, KalshiRealtimeOrderbook>>>,
        subscriptions: &Arc<RwLock<Vec<KalshiChannel>>>,
        event_tx: &broadcast::Sender<KalshiWsEvent>,
        cmd_rx: &mut mpsc::Receiver<KalshiWsCommand>,
        connected: &Arc<RwLock<bool>>,
        latency: &Arc<RwLock<u64>>,
    ) -> Result<()> {
        let url = Url::parse(&config.ws_url)
            .map_err(|e| Error::WebSocket(format!("Invalid URL: {}", e)))?;

        let (ws_stream, _) = connect_async(url).await
            .map_err(|e| Error::WebSocket(format!("Connection failed: {}", e)))?;

        let (mut write, mut read) = ws_stream.split();

        // Authenticate
        let auth_msg = serde_json::json!({
            "type": "auth",
            "token": config.auth_token
        });
        write.send(Message::Text(auth_msg.to_string())).await
            .map_err(|e| Error::WebSocket(format!("Auth failed: {}", e)))?;

        *connected.write().await = true;
        let _ = event_tx.send(KalshiWsEvent::Connected);
        info!("Connected to Kalshi WebSocket");

        // Resubscribe to previous channels
        let subs = subscriptions.read().await.clone();
        if !subs.is_empty() {
            let sub_msg = KalshiWsMessage::Subscribe { channels: subs };
            let msg_str = serde_json::to_string(&sub_msg)
                .map_err(|e| Error::Internal(format!("Serialize error: {}", e)))?;
            write.send(Message::Text(msg_str)).await
                .map_err(|e| Error::WebSocket(format!("Subscribe failed: {}", e)))?;
        }

        let mut ping_interval = interval(Duration::from_secs(config.ping_interval_secs));
        let mut last_ping = std::time::Instant::now();

        loop {
            tokio::select! {
                // Incoming messages
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(e) = Self::handle_message(&text, orderbooks, event_tx).await {
                                warn!("Message handling error: {}", e);
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {
                            let rtt = last_ping.elapsed().as_millis() as u64;
                            *latency.write().await = rtt;
                            debug!("Kalshi pong received, latency: {}ms", rtt);
                        }
                        Some(Ok(Message::Close(_))) => {
                            info!("Kalshi WebSocket closed by server");
                            return Ok(());
                        }
                        Some(Err(e)) => {
                            return Err(Error::WebSocket(format!("Read error: {}", e)));
                        }
                        None => {
                            return Ok(());
                        }
                        _ => {}
                    }
                }

                // Commands from user
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(KalshiWsCommand::Subscribe(channels)) => {
                            let msg = KalshiWsMessage::Subscribe { channels: channels.clone() };
                            let msg_str = serde_json::to_string(&msg)
                                .map_err(|e| Error::Internal(format!("Serialize error: {}", e)))?;
                            write.send(Message::Text(msg_str)).await
                                .map_err(|e| Error::WebSocket(format!("Send error: {}", e)))?;

                            subscriptions.write().await.extend(channels);
                        }
                        Some(KalshiWsCommand::Unsubscribe(channels)) => {
                            let msg = KalshiWsMessage::Unsubscribe { channels: channels.clone() };
                            let msg_str = serde_json::to_string(&msg)
                                .map_err(|e| Error::Internal(format!("Serialize error: {}", e)))?;
                            write.send(Message::Text(msg_str)).await
                                .map_err(|e| Error::WebSocket(format!("Send error: {}", e)))?;

                            let mut subs = subscriptions.write().await;
                            subs.retain(|c| !channels.contains(c));
                        }
                        Some(KalshiWsCommand::Disconnect) => {
                            let _ = write.close().await;
                            return Ok(());
                        }
                        None => {
                            return Ok(());
                        }
                    }
                }

                // Ping/keepalive
                _ = ping_interval.tick() => {
                    last_ping = std::time::Instant::now();
                    if let Err(e) = write.send(Message::Ping(vec![])).await {
                        warn!("Ping failed: {}", e);
                    }
                }
            }
        }
    }

    /// Handle incoming WebSocket message
    async fn handle_message(
        text: &str,
        orderbooks: &Arc<RwLock<HashMap<String, KalshiRealtimeOrderbook>>>,
        event_tx: &broadcast::Sender<KalshiWsEvent>,
    ) -> Result<()> {
        let msg: KalshiWsMessage = serde_json::from_str(text)
            .map_err(|e| Error::Internal(format!("Parse error: {} - {}", e, text)))?;

        match msg {
            KalshiWsMessage::OrderbookSnapshot { market_ticker, yes, no, ts: _ } => {
                let mut books = orderbooks.write().await;
                let book = books.entry(market_ticker.clone())
                    .or_insert_with(|| KalshiRealtimeOrderbook::new(market_ticker.clone()));
                book.apply_snapshot(&yes, &no);

                let _ = event_tx.send(KalshiWsEvent::OrderbookUpdate {
                    ticker: market_ticker,
                    orderbook: book.clone(),
                });
            }

            KalshiWsMessage::OrderbookDelta { market_ticker, side, price, delta, ts: _ } => {
                let mut books = orderbooks.write().await;
                if let Some(book) = books.get_mut(&market_ticker) {
                    book.apply_delta(&side, price, delta);

                    let _ = event_tx.send(KalshiWsEvent::OrderbookUpdate {
                        ticker: market_ticker,
                        orderbook: book.clone(),
                    });
                }
            }

            KalshiWsMessage::Trade { market_ticker, price, count, side, .. } => {
                let _ = event_tx.send(KalshiWsEvent::Trade {
                    ticker: market_ticker,
                    price,
                    count,
                    side,
                });
            }

            KalshiWsMessage::OrderUpdate { order_id, status, remaining_count, .. } => {
                let _ = event_tx.send(KalshiWsEvent::OrderUpdate {
                    order_id,
                    status,
                    remaining: remaining_count,
                });
            }

            KalshiWsMessage::Fill { order_id, price, count, .. } => {
                let _ = event_tx.send(KalshiWsEvent::Fill {
                    order_id,
                    price,
                    count,
                });
            }

            KalshiWsMessage::Error { code, message } => {
                error!("Kalshi WS error: {} - {}", code, message);
                let _ = event_tx.send(KalshiWsEvent::Error { code, message });
            }

            _ => {}
        }

        Ok(())
    }

    /// Disconnect
    pub async fn disconnect(&self) {
        if let Some(cmd_tx) = &self.cmd_tx {
            let _ = cmd_tx.send(KalshiWsCommand::Disconnect).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orderbook_snapshot() {
        let mut book = KalshiRealtimeOrderbook::new("TEST".to_string());

        let yes_levels = vec![
            KalshiWsLevel { price: 55, count: 100 },
            KalshiWsLevel { price: 54, count: 200 },
        ];
        let no_levels = vec![
            KalshiWsLevel { price: 45, count: 150 },
            KalshiWsLevel { price: 44, count: 250 },
        ];

        book.apply_snapshot(&yes_levels, &no_levels);

        // Best YES bid should be 55
        assert_eq!(book.best_yes_bid(), Some((55, 100)));

        // Best YES ask = 100 - best NO bid = 100 - 45 = 55
        assert_eq!(book.best_yes_ask(), Some((55, 150)));
    }

    #[test]
    fn test_orderbook_delta() {
        let mut book = KalshiRealtimeOrderbook::new("TEST".to_string());

        book.apply_delta("yes", 55, 100);
        assert_eq!(book.yes_bids.get(&55), Some(&100));

        book.apply_delta("yes", 55, -30);
        assert_eq!(book.yes_bids.get(&55), Some(&70));

        book.apply_delta("yes", 55, -70);
        assert!(book.yes_bids.get(&55).is_none());  // Removed when <= 0
    }
}
