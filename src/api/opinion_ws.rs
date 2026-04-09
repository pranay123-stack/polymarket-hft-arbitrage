//! Opinion WebSocket Client for Real-Time Market Data
//!
//! Opinion uses a Betfair-style back/lay model with streaming price updates.
//! This provides real-time:
//! - Best available prices (back/lay)
//! - Available sizes at each price
//! - Trade notifications
//! - Market status changes

use crate::core::{
    error::{ArbitrageError, Result},
    types::{Price, Quantity},
};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time::{interval, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use url::Url;

type HmacSha256 = Hmac<Sha256>;

/// Opinion WebSocket configuration
#[derive(Debug, Clone)]
pub struct OpinionWsConfig {
    pub ws_url: String,
    pub api_key: String,
    pub api_secret: String,
    pub reconnect_delay_ms: u64,
    pub max_reconnect_attempts: u32,
    pub heartbeat_interval_secs: u64,
}

impl Default for OpinionWsConfig {
    fn default() -> Self {
        Self {
            ws_url: "wss://stream.opinion.exchange/v1/ws".to_string(),
            api_key: String::new(),
            api_secret: String::new(),
            reconnect_delay_ms: 1000,
            max_reconnect_attempts: 10,
            heartbeat_interval_secs: 25,
        }
    }
}

/// Opinion WebSocket message types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
pub enum OpinionWsMessage {
    /// Authentication request
    Authentication {
        #[serde(rename = "appKey")]
        app_key: String,
        session: String,
    },
    /// Authentication response
    Connection {
        #[serde(rename = "connectionId")]
        connection_id: String,
    },
    /// Subscribe to market
    MarketSubscription {
        id: i64,
        #[serde(rename = "marketFilter")]
        market_filter: OpinionMarketFilter,
        #[serde(rename = "marketDataFilter")]
        market_data_filter: OpinionMarketDataFilter,
    },
    /// Market change message
    Mcm {
        id: i64,
        #[serde(rename = "clk")]
        clock: Option<String>,
        pt: i64,  // Publish time
        mc: Option<Vec<OpinionMarketChange>>,
    },
    /// Order subscription
    OrderSubscription {
        id: i64,
    },
    /// Order change message
    Ocm {
        id: i64,
        #[serde(rename = "clk")]
        clock: Option<String>,
        pt: i64,
        oc: Option<Vec<OpinionOrderChange>>,
    },
    /// Heartbeat
    Heartbeat {
        id: i64,
    },
    /// Status message
    Status {
        id: i64,
        #[serde(rename = "statusCode")]
        status_code: String,
        #[serde(rename = "connectionClosed")]
        connection_closed: Option<bool>,
        #[serde(rename = "errorMessage")]
        error_message: Option<String>,
    },
}

/// Market filter for subscription
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionMarketFilter {
    #[serde(rename = "marketIds")]
    pub market_ids: Vec<String>,
}

/// Market data filter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionMarketDataFilter {
    #[serde(rename = "ladderLevels")]
    pub ladder_levels: Option<i32>,
    pub fields: Vec<String>,
}

impl Default for OpinionMarketDataFilter {
    fn default() -> Self {
        Self {
            ladder_levels: Some(10),
            fields: vec![
                "EX_BEST_OFFERS".to_string(),
                "EX_TRADED".to_string(),
                "EX_TRADED_VOL".to_string(),
            ],
        }
    }
}

/// Market change from stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionMarketChange {
    pub id: String,
    #[serde(rename = "marketDefinition")]
    pub market_definition: Option<OpinionMarketDefinition>,
    pub rc: Option<Vec<OpinionRunnerChange>>,  // Runner changes
    pub tv: Option<f64>,  // Total matched
}

/// Market definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionMarketDefinition {
    pub status: String,
    #[serde(rename = "inPlay")]
    pub in_play: bool,
    #[serde(rename = "bspReconciled")]
    pub bsp_reconciled: Option<bool>,
    pub complete: Option<bool>,
}

/// Runner (selection) change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionRunnerChange {
    pub id: i64,  // Selection ID
    pub atb: Option<Vec<Vec<f64>>>,  // Available to back [[price, size], ...]
    pub atl: Option<Vec<Vec<f64>>>,  // Available to lay [[price, size], ...]
    pub trd: Option<Vec<Vec<f64>>>,  // Traded [[price, size], ...]
    pub tv: Option<f64>,  // Total volume
    pub ltp: Option<f64>,  // Last traded price
    pub spn: Option<f64>,  // Starting price near
    pub spf: Option<f64>,  // Starting price far
}

/// Order change from stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionOrderChange {
    pub id: String,
    #[serde(rename = "orc")]
    pub order_runner_changes: Option<Vec<OpinionOrderRunnerChange>>,
}

/// Order runner change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionOrderRunnerChange {
    pub id: i64,  // Selection ID
    pub uo: Option<Vec<OpinionUnmatchedOrder>>,  // Unmatched orders
    pub mb: Option<Vec<Vec<f64>>>,  // Matched backs [[price, size], ...]
    pub ml: Option<Vec<Vec<f64>>>,  // Matched lays [[price, size], ...]
}

/// Unmatched order
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionUnmatchedOrder {
    pub id: String,
    pub p: f64,   // Price
    pub s: f64,   // Size
    pub sr: f64,  // Size remaining
    pub side: String,
    pub status: String,
    pub pt: String,  // Persistence type
    pub ot: String,  // Order type
    pub pd: i64,  // Placed date
    pub md: Option<i64>,  // Matched date
    pub sm: f64,  // Size matched
    pub avp: Option<f64>,  // Average price matched
}

/// Real-time price ladder for a selection
#[derive(Debug, Clone)]
pub struct OpinionPriceLadder {
    pub market_id: String,
    pub selection_id: i64,
    pub back_prices: Vec<(Decimal, Decimal)>,  // (price, size) sorted by price desc
    pub lay_prices: Vec<(Decimal, Decimal)>,   // (price, size) sorted by price asc
    pub last_traded_price: Option<Decimal>,
    pub total_matched: Decimal,
    pub last_update: DateTime<Utc>,
}

impl OpinionPriceLadder {
    pub fn new(market_id: String, selection_id: i64) -> Self {
        Self {
            market_id,
            selection_id,
            back_prices: Vec::new(),
            lay_prices: Vec::new(),
            last_traded_price: None,
            total_matched: Decimal::ZERO,
            last_update: Utc::now(),
        }
    }

    /// Apply runner change
    pub fn apply_change(&mut self, change: &OpinionRunnerChange) {
        // Update available to back
        if let Some(atb) = &change.atb {
            self.back_prices = atb.iter()
                .filter_map(|level| {
                    if level.len() >= 2 {
                        Some((
                            Decimal::try_from(level[0]).ok()?,
                            Decimal::try_from(level[1]).ok()?,
                        ))
                    } else {
                        None
                    }
                })
                .collect();
            self.back_prices.sort_by(|a, b| b.0.cmp(&a.0));  // Descending
        }

        // Update available to lay
        if let Some(atl) = &change.atl {
            self.lay_prices = atl.iter()
                .filter_map(|level| {
                    if level.len() >= 2 {
                        Some((
                            Decimal::try_from(level[0]).ok()?,
                            Decimal::try_from(level[1]).ok()?,
                        ))
                    } else {
                        None
                    }
                })
                .collect();
            self.lay_prices.sort_by(|a, b| a.0.cmp(&b.0));  // Ascending
        }

        // Update last traded price
        if let Some(ltp) = change.ltp {
            self.last_traded_price = Decimal::try_from(ltp).ok();
        }

        // Update total matched
        if let Some(tv) = change.tv {
            self.total_matched = Decimal::try_from(tv).unwrap_or(Decimal::ZERO);
        }

        self.last_update = Utc::now();
    }

    /// Get best back price (highest price you can bet on YES)
    pub fn best_back(&self) -> Option<(Decimal, Decimal)> {
        self.back_prices.first().cloned()
    }

    /// Get best lay price (lowest price you can bet against YES)
    pub fn best_lay(&self) -> Option<(Decimal, Decimal)> {
        self.lay_prices.first().cloned()
    }

    /// Convert back odds to probability
    pub fn back_implied_probability(&self) -> Option<Decimal> {
        self.best_back().map(|(price, _)| Decimal::ONE / price)
    }

    /// Convert lay odds to probability
    pub fn lay_implied_probability(&self) -> Option<Decimal> {
        self.best_lay().map(|(price, _)| Decimal::ONE / price)
    }

    /// Get spread (lay - back)
    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_back(), self.best_lay()) {
            (Some((back, _)), Some((lay, _))) => Some(lay - back),
            _ => None,
        }
    }
}

/// Event emitted by Opinion WebSocket
#[derive(Debug, Clone)]
pub enum OpinionWsEvent {
    Connected { connection_id: String },
    Disconnected,
    Reconnecting { attempt: u32 },
    MarketUpdate { market_id: String, selection_id: i64, ladder: OpinionPriceLadder },
    MarketStatusChange { market_id: String, status: String, in_play: bool },
    OrderUpdate { order_id: String, status: String, size_remaining: Decimal },
    MatchedBet { selection_id: i64, price: Decimal, size: Decimal, side: String },
    Error { code: String, message: String },
}

/// Opinion WebSocket client
pub struct OpinionWebSocket {
    config: OpinionWsConfig,
    /// Price ladders by market_id:selection_id
    ladders: Arc<RwLock<HashMap<String, OpinionPriceLadder>>>,
    /// Subscribed market IDs
    subscribed_markets: Arc<RwLock<Vec<String>>>,
    /// Event broadcast channel
    event_tx: broadcast::Sender<OpinionWsEvent>,
    /// Command channel
    cmd_tx: Option<mpsc::Sender<OpinionWsCommand>>,
    /// Connection state
    connected: Arc<RwLock<bool>>,
    /// Connection ID
    connection_id: Arc<RwLock<Option<String>>>,
    /// Latency tracking
    latency_ms: Arc<RwLock<u64>>,
}

enum OpinionWsCommand {
    SubscribeMarket(Vec<String>),
    SubscribeOrders,
    Disconnect,
}

impl OpinionWebSocket {
    pub fn new(config: OpinionWsConfig) -> Self {
        let (event_tx, _) = broadcast::channel(1000);

        Self {
            config,
            ladders: Arc::new(RwLock::new(HashMap::new())),
            subscribed_markets: Arc::new(RwLock::new(Vec::new())),
            event_tx,
            cmd_tx: None,
            connected: Arc::new(RwLock::new(false)),
            connection_id: Arc::new(RwLock::new(None)),
            latency_ms: Arc::new(RwLock::new(0)),
        }
    }

    /// Get event receiver
    pub fn subscribe_events(&self) -> broadcast::Receiver<OpinionWsEvent> {
        self.event_tx.subscribe()
    }

    /// Check if connected
    pub async fn is_connected(&self) -> bool {
        *self.connected.read().await
    }

    /// Get latency
    pub async fn latency_ms(&self) -> u64 {
        *self.latency_ms.read().await
    }

    /// Get price ladder
    pub async fn get_ladder(&self, market_id: &str, selection_id: i64) -> Option<OpinionPriceLadder> {
        let key = format!("{}:{}", market_id, selection_id);
        self.ladders.read().await.get(&key).cloned()
    }

    /// Subscribe to market
    pub async fn subscribe_market(&self, market_ids: Vec<String>) -> Result<()> {
        // Initialize ladders
        {
            let mut ladders = self.ladders.write().await;
            for mid in &market_ids {
                // Default selection ID 0, will be updated on first message
                let key = format!("{}:0", mid);
                ladders.entry(key.clone())
                    .or_insert_with(|| OpinionPriceLadder::new(mid.clone(), 0));
            }
        }

        if let Some(cmd_tx) = &self.cmd_tx {
            cmd_tx.send(OpinionWsCommand::SubscribeMarket(market_ids.clone())).await
                .map_err(|_| ArbitrageError::WebSocket("Failed to send subscribe command".to_string()))?;
        }

        self.subscribed_markets.write().await.extend(market_ids);
        Ok(())
    }

    /// Subscribe to order updates
    pub async fn subscribe_orders(&self) -> Result<()> {
        if let Some(cmd_tx) = &self.cmd_tx {
            cmd_tx.send(OpinionWsCommand::SubscribeOrders).await
                .map_err(|_| ArbitrageError::WebSocket("Failed to send subscribe command".to_string()))?;
        }
        Ok(())
    }

    /// Connect to Opinion WebSocket
    pub async fn connect(&mut self) -> Result<()> {
        let (cmd_tx, cmd_rx) = mpsc::channel(100);
        self.cmd_tx = Some(cmd_tx);

        let config = self.config.clone();
        let ladders = self.ladders.clone();
        let subscribed_markets = self.subscribed_markets.clone();
        let event_tx = self.event_tx.clone();
        let connected = self.connected.clone();
        let connection_id = self.connection_id.clone();
        let latency = self.latency_ms.clone();

        tokio::spawn(async move {
            Self::connection_loop(
                config,
                ladders,
                subscribed_markets,
                event_tx,
                cmd_rx,
                connected,
                connection_id,
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

        Err(ArbitrageError::WebSocket("Connection timeout".to_string()))
    }

    /// Connection loop with reconnection
    async fn connection_loop(
        config: OpinionWsConfig,
        ladders: Arc<RwLock<HashMap<String, OpinionPriceLadder>>>,
        subscribed_markets: Arc<RwLock<Vec<String>>>,
        event_tx: broadcast::Sender<OpinionWsEvent>,
        mut cmd_rx: mpsc::Receiver<OpinionWsCommand>,
        connected: Arc<RwLock<bool>>,
        connection_id: Arc<RwLock<Option<String>>>,
        latency: Arc<RwLock<u64>>,
    ) {
        let mut reconnect_attempts = 0;

        loop {
            info!("Connecting to Opinion WebSocket...");

            match Self::connect_and_run(
                &config,
                &ladders,
                &subscribed_markets,
                &event_tx,
                &mut cmd_rx,
                &connected,
                &connection_id,
                &latency,
            ).await {
                Ok(_) => {
                    reconnect_attempts = 0;
                }
                Err(e) => {
                    error!("Opinion WebSocket error: {}", e);
                }
            }

            *connected.write().await = false;
            let _ = event_tx.send(OpinionWsEvent::Disconnected);

            reconnect_attempts += 1;
            if reconnect_attempts > config.max_reconnect_attempts {
                error!("Max reconnection attempts reached, stopping");
                break;
            }

            let _ = event_tx.send(OpinionWsEvent::Reconnecting { attempt: reconnect_attempts });

            let delay = config.reconnect_delay_ms * (2_u64.pow(reconnect_attempts.min(5)));
            warn!("Reconnecting in {}ms (attempt {})", delay, reconnect_attempts);
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
    }

    /// Single connection
    async fn connect_and_run(
        config: &OpinionWsConfig,
        ladders: &Arc<RwLock<HashMap<String, OpinionPriceLadder>>>,
        subscribed_markets: &Arc<RwLock<Vec<String>>>,
        event_tx: &broadcast::Sender<OpinionWsEvent>,
        cmd_rx: &mut mpsc::Receiver<OpinionWsCommand>,
        connected: &Arc<RwLock<bool>>,
        connection_id: &Arc<RwLock<Option<String>>>,
        latency: &Arc<RwLock<u64>>,
    ) -> Result<()> {
        let url = Url::parse(&config.ws_url)
            .map_err(|e| ArbitrageError::WebSocket(format!("Invalid URL: {}", e)))?;

        let (ws_stream, _) = connect_async(url).await
            .map_err(|e| ArbitrageError::WebSocket(format!("Connection failed: {}", e)))?;

        let (mut write, mut read) = ws_stream.split();

        // Generate session token
        let timestamp = Utc::now().timestamp_millis().to_string();
        let message = format!("{}{}", timestamp, config.api_key);

        let mut mac = HmacSha256::new_from_slice(config.api_secret.as_bytes())
            .map_err(|_| ArbitrageError::WebSocket("HMAC init failed".to_string()))?;
        mac.update(message.as_bytes());
        let session = hex::encode(mac.finalize().into_bytes());

        // Authenticate
        let auth_msg = OpinionWsMessage::Authentication {
            app_key: config.api_key.clone(),
            session,
        };
        let auth_str = serde_json::to_string(&auth_msg)
            .map_err(|e| ArbitrageError::Internal(format!("Serialize error: {}", e)))?;
        write.send(Message::Text(auth_str)).await
            .map_err(|e| ArbitrageError::WebSocket(format!("Auth send failed: {}", e)))?;

        // Wait for connection response
        let mut msg_id = 1i64;
        let mut heartbeat_interval = interval(Duration::from_secs(config.heartbeat_interval_secs));
        let mut last_heartbeat = std::time::Instant::now();

        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(e) = Self::handle_message(
                                &text,
                                ladders,
                                event_tx,
                                connected,
                                connection_id,
                                latency,
                                &last_heartbeat,
                            ).await {
                                warn!("Message handling error: {}", e);
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            info!("Opinion WebSocket closed");
                            return Ok(());
                        }
                        Some(Err(e)) => {
                            return Err(ArbitrageError::WebSocket(format!("Read error: {}", e)));
                        }
                        None => {
                            return Ok(());
                        }
                        _ => {}
                    }
                }

                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(OpinionWsCommand::SubscribeMarket(market_ids)) => {
                            msg_id += 1;
                            let sub_msg = OpinionWsMessage::MarketSubscription {
                                id: msg_id,
                                market_filter: OpinionMarketFilter { market_ids },
                                market_data_filter: OpinionMarketDataFilter::default(),
                            };
                            let msg_str = serde_json::to_string(&sub_msg)
                                .map_err(|e| ArbitrageError::Internal(format!("Serialize error: {}", e)))?;
                            write.send(Message::Text(msg_str)).await
                                .map_err(|e| ArbitrageError::WebSocket(format!("Send error: {}", e)))?;
                        }
                        Some(OpinionWsCommand::SubscribeOrders) => {
                            msg_id += 1;
                            let sub_msg = OpinionWsMessage::OrderSubscription { id: msg_id };
                            let msg_str = serde_json::to_string(&sub_msg)
                                .map_err(|e| ArbitrageError::Internal(format!("Serialize error: {}", e)))?;
                            write.send(Message::Text(msg_str)).await
                                .map_err(|e| ArbitrageError::WebSocket(format!("Send error: {}", e)))?;
                        }
                        Some(OpinionWsCommand::Disconnect) => {
                            let _ = write.close().await;
                            return Ok(());
                        }
                        None => {
                            return Ok(());
                        }
                    }
                }

                _ = heartbeat_interval.tick() => {
                    msg_id += 1;
                    last_heartbeat = std::time::Instant::now();
                    let hb_msg = OpinionWsMessage::Heartbeat { id: msg_id };
                    let hb_str = serde_json::to_string(&hb_msg)
                        .map_err(|e| ArbitrageError::Internal(format!("Serialize error: {}", e)))?;
                    write.send(Message::Text(hb_str)).await
                        .map_err(|e| ArbitrageError::WebSocket(format!("Heartbeat failed: {}", e)))?;
                }
            }
        }
    }

    /// Handle incoming message
    async fn handle_message(
        text: &str,
        ladders: &Arc<RwLock<HashMap<String, OpinionPriceLadder>>>,
        event_tx: &broadcast::Sender<OpinionWsEvent>,
        connected: &Arc<RwLock<bool>>,
        connection_id: &Arc<RwLock<Option<String>>>,
        latency: &Arc<RwLock<u64>>,
        last_heartbeat: &std::time::Instant,
    ) -> Result<()> {
        // Parse the message - Opinion uses a specific format
        let msg: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| ArbitrageError::Internal(format!("Parse error: {}", e)))?;

        let op = msg.get("op").and_then(|v| v.as_str()).unwrap_or("");

        match op {
            "connection" => {
                let conn_id = msg.get("connectionId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();

                *connection_id.write().await = Some(conn_id.clone());
                *connected.write().await = true;
                info!("Opinion WebSocket connected: {}", conn_id);

                let _ = event_tx.send(OpinionWsEvent::Connected { connection_id: conn_id });
            }

            "mcm" => {
                // Market change message
                if let Some(mc_array) = msg.get("mc").and_then(|v| v.as_array()) {
                    for mc in mc_array {
                        let market_id = mc.get("id").and_then(|v| v.as_str()).unwrap_or("");

                        // Handle market definition changes
                        if let Some(def) = mc.get("marketDefinition") {
                            let status = def.get("status").and_then(|v| v.as_str()).unwrap_or("");
                            let in_play = def.get("inPlay").and_then(|v| v.as_bool()).unwrap_or(false);

                            let _ = event_tx.send(OpinionWsEvent::MarketStatusChange {
                                market_id: market_id.to_string(),
                                status: status.to_string(),
                                in_play,
                            });
                        }

                        // Handle runner changes
                        if let Some(rc_array) = mc.get("rc").and_then(|v| v.as_array()) {
                            for rc in rc_array {
                                let selection_id = rc.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
                                let key = format!("{}:{}", market_id, selection_id);

                                let mut ladders_lock = ladders.write().await;
                                let ladder = ladders_lock.entry(key)
                                    .or_insert_with(|| OpinionPriceLadder::new(market_id.to_string(), selection_id));

                                // Parse runner change
                                if let Ok(change) = serde_json::from_value::<OpinionRunnerChange>(rc.clone()) {
                                    ladder.apply_change(&change);

                                    let _ = event_tx.send(OpinionWsEvent::MarketUpdate {
                                        market_id: market_id.to_string(),
                                        selection_id,
                                        ladder: ladder.clone(),
                                    });
                                }
                            }
                        }
                    }
                }
            }

            "ocm" => {
                // Order change message
                if let Some(oc_array) = msg.get("oc").and_then(|v| v.as_array()) {
                    for oc in oc_array {
                        if let Some(orc_array) = oc.get("orc").and_then(|v| v.as_array()) {
                            for orc in orc_array {
                                let selection_id = orc.get("id").and_then(|v| v.as_i64()).unwrap_or(0);

                                // Unmatched orders
                                if let Some(uo_array) = orc.get("uo").and_then(|v| v.as_array()) {
                                    for uo in uo_array {
                                        let order_id = uo.get("id").and_then(|v| v.as_str()).unwrap_or("");
                                        let status = uo.get("status").and_then(|v| v.as_str()).unwrap_or("");
                                        let sr = uo.get("sr").and_then(|v| v.as_f64()).unwrap_or(0.0);

                                        let _ = event_tx.send(OpinionWsEvent::OrderUpdate {
                                            order_id: order_id.to_string(),
                                            status: status.to_string(),
                                            size_remaining: Decimal::try_from(sr).unwrap_or(Decimal::ZERO),
                                        });
                                    }
                                }

                                // Matched backs
                                if let Some(mb_array) = orc.get("mb").and_then(|v| v.as_array()) {
                                    for mb in mb_array {
                                        if let Some(arr) = mb.as_array() {
                                            if arr.len() >= 2 {
                                                let price = arr[0].as_f64().unwrap_or(0.0);
                                                let size = arr[1].as_f64().unwrap_or(0.0);
                                                let _ = event_tx.send(OpinionWsEvent::MatchedBet {
                                                    selection_id,
                                                    price: Decimal::try_from(price).unwrap_or(Decimal::ZERO),
                                                    size: Decimal::try_from(size).unwrap_or(Decimal::ZERO),
                                                    side: "back".to_string(),
                                                });
                                            }
                                        }
                                    }
                                }

                                // Matched lays
                                if let Some(ml_array) = orc.get("ml").and_then(|v| v.as_array()) {
                                    for ml in ml_array {
                                        if let Some(arr) = ml.as_array() {
                                            if arr.len() >= 2 {
                                                let price = arr[0].as_f64().unwrap_or(0.0);
                                                let size = arr[1].as_f64().unwrap_or(0.0);
                                                let _ = event_tx.send(OpinionWsEvent::MatchedBet {
                                                    selection_id,
                                                    price: Decimal::try_from(price).unwrap_or(Decimal::ZERO),
                                                    size: Decimal::try_from(size).unwrap_or(Decimal::ZERO),
                                                    side: "lay".to_string(),
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            "status" => {
                let status_code = msg.get("statusCode").and_then(|v| v.as_str()).unwrap_or("");
                let error_msg = msg.get("errorMessage").and_then(|v| v.as_str());

                if status_code != "SUCCESS" {
                    error!("Opinion status: {} - {:?}", status_code, error_msg);
                    let _ = event_tx.send(OpinionWsEvent::Error {
                        code: status_code.to_string(),
                        message: error_msg.unwrap_or("Unknown error").to_string(),
                    });
                }

                // Measure latency from heartbeat response
                let rtt = last_heartbeat.elapsed().as_millis() as u64;
                *latency.write().await = rtt;
            }

            _ => {
                debug!("Unknown Opinion message op: {}", op);
            }
        }

        Ok(())
    }

    /// Disconnect
    pub async fn disconnect(&self) {
        if let Some(cmd_tx) = &self.cmd_tx {
            let _ = cmd_tx.send(OpinionWsCommand::Disconnect).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_price_ladder() {
        let mut ladder = OpinionPriceLadder::new("market1".to_string(), 12345);

        let change = OpinionRunnerChange {
            id: 12345,
            atb: Some(vec![
                vec![2.0, 100.0],
                vec![1.95, 200.0],
            ]),
            atl: Some(vec![
                vec![2.02, 150.0],
                vec![2.05, 250.0],
            ]),
            trd: None,
            tv: Some(5000.0),
            ltp: Some(2.01),
            spn: None,
            spf: None,
        };

        ladder.apply_change(&change);

        // Best back should be highest price (2.0)
        let (back_price, back_size) = ladder.best_back().unwrap();
        assert_eq!(back_price, dec!(2.0));
        assert_eq!(back_size, dec!(100.0));

        // Best lay should be lowest price (2.02)
        let (lay_price, lay_size) = ladder.best_lay().unwrap();
        assert_eq!(lay_price, dec!(2.02));
        assert_eq!(lay_size, dec!(150.0));

        // Spread = 2.02 - 2.0 = 0.02
        assert_eq!(ladder.spread(), Some(dec!(0.02)));

        // Implied probability (back) = 1/2.0 = 0.5
        assert_eq!(ladder.back_implied_probability(), Some(dec!(0.5)));
    }
}
