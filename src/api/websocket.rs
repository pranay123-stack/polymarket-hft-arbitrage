//! WebSocket clients for real-time Polymarket data.
//!
//! Provides:
//! - Market WebSocket for orderbook updates
//! - User WebSocket for order/trade notifications
//! - Automatic reconnection with exponential backoff
//! - Message parsing and event emission

use crate::api::auth::Signer;
use crate::core::constants::*;
use crate::core::error::{Error, Result};
use crate::core::events::{Event, EventBus};
use crate::core::types::*;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, instrument, warn};

/// Market WebSocket for orderbook and price updates
pub struct MarketWebSocket {
    url: String,
    subscribed_tokens: Arc<RwLock<HashSet<String>>>,
    event_bus: EventBus,
    command_tx: Option<mpsc::Sender<WsCommand>>,
}

impl MarketWebSocket {
    /// Create a new market WebSocket client
    pub fn new(url: &str, event_bus: EventBus) -> Self {
        Self {
            url: url.to_string(),
            subscribed_tokens: Arc::new(RwLock::new(HashSet::new())),
            event_bus,
            command_tx: None,
        }
    }

    /// Start the WebSocket connection
    pub async fn start(&mut self) -> Result<()> {
        let (command_tx, command_rx) = mpsc::channel(100);
        self.command_tx = Some(command_tx);

        let url = self.url.clone();
        let subscribed_tokens = Arc::clone(&self.subscribed_tokens);
        let event_bus = self.event_bus.clone();

        tokio::spawn(async move {
            run_market_websocket(url, subscribed_tokens, event_bus, command_rx).await;
        });

        Ok(())
    }

    /// Subscribe to a token's orderbook updates
    pub async fn subscribe(&self, token_id: &str) -> Result<()> {
        {
            let mut tokens = self.subscribed_tokens.write().await;
            tokens.insert(token_id.to_string());
        }

        if let Some(ref tx) = self.command_tx {
            tx.send(WsCommand::Subscribe(token_id.to_string()))
                .await
                .map_err(|_| Error::ChannelSend)?;
        }

        Ok(())
    }

    /// Unsubscribe from a token
    pub async fn unsubscribe(&self, token_id: &str) -> Result<()> {
        {
            let mut tokens = self.subscribed_tokens.write().await;
            tokens.remove(token_id);
        }

        if let Some(ref tx) = self.command_tx {
            tx.send(WsCommand::Unsubscribe(token_id.to_string()))
                .await
                .map_err(|_| Error::ChannelSend)?;
        }

        Ok(())
    }

    /// Subscribe to multiple tokens
    pub async fn subscribe_many(&self, token_ids: &[&str]) -> Result<()> {
        for token_id in token_ids {
            self.subscribe(token_id).await?;
        }
        Ok(())
    }

    /// Stop the WebSocket connection
    pub async fn stop(&self) -> Result<()> {
        if let Some(ref tx) = self.command_tx {
            let _ = tx.send(WsCommand::Disconnect).await;
        }
        Ok(())
    }
}

/// User WebSocket for order and trade notifications
pub struct UserWebSocket {
    url: String,
    signer: Arc<Signer>,
    event_bus: EventBus,
    command_tx: Option<mpsc::Sender<WsCommand>>,
}

impl UserWebSocket {
    /// Create a new user WebSocket client
    pub fn new(url: &str, signer: Signer, event_bus: EventBus) -> Self {
        Self {
            url: url.to_string(),
            signer: Arc::new(signer),
            event_bus,
            command_tx: None,
        }
    }

    /// Start the WebSocket connection
    pub async fn start(&mut self) -> Result<()> {
        let (command_tx, command_rx) = mpsc::channel(100);
        self.command_tx = Some(command_tx);

        let url = self.url.clone();
        let signer = Arc::clone(&self.signer);
        let event_bus = self.event_bus.clone();

        tokio::spawn(async move {
            run_user_websocket(url, signer, event_bus, command_rx).await;
        });

        Ok(())
    }

    /// Stop the WebSocket connection
    pub async fn stop(&self) -> Result<()> {
        if let Some(ref tx) = self.command_tx {
            let _ = tx.send(WsCommand::Disconnect).await;
        }
        Ok(())
    }
}

/// Commands to control WebSocket connections
#[derive(Debug)]
enum WsCommand {
    Subscribe(String),
    Unsubscribe(String),
    Disconnect,
}

/// Run the market WebSocket connection loop
async fn run_market_websocket(
    url: String,
    subscribed_tokens: Arc<RwLock<HashSet<String>>>,
    event_bus: EventBus,
    mut command_rx: mpsc::Receiver<WsCommand>,
) {
    let mut reconnect_delay = Duration::from_millis(WS_RECONNECT_DELAY_MS);

    loop {
        info!("Connecting to market WebSocket: {}", url);

        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                info!("Market WebSocket connected");
                reconnect_delay = Duration::from_millis(WS_RECONNECT_DELAY_MS);

                let (mut write, mut read) = ws_stream.split();

                // Resubscribe to all tokens
                {
                    let tokens = subscribed_tokens.read().await;
                    for token_id in tokens.iter() {
                        let sub_msg = serde_json::json!({
                            "type": "subscribe",
                            "channel": "book",
                            "assets_id": token_id
                        });

                        if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
                            error!("Failed to send subscription: {}", e);
                        }
                    }
                }

                // Spawn ping task
                let ping_interval = interval(Duration::from_secs(WS_PING_INTERVAL_SECS));
                tokio::pin!(ping_interval);

                loop {
                    tokio::select! {
                        // Handle incoming messages
                        msg = read.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    handle_market_message(&text, &event_bus);
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    let _ = write.send(Message::Pong(data)).await;
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("Market WebSocket closed by server");
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!("Market WebSocket error: {}", e);
                                    break;
                                }
                                None => {
                                    warn!("Market WebSocket stream ended");
                                    break;
                                }
                                _ => {}
                            }
                        }

                        // Handle commands
                        cmd = command_rx.recv() => {
                            match cmd {
                                Some(WsCommand::Subscribe(token_id)) => {
                                    let sub_msg = serde_json::json!({
                                        "type": "subscribe",
                                        "channel": "book",
                                        "assets_id": token_id
                                    });
                                    let _ = write.send(Message::Text(sub_msg.to_string())).await;
                                }
                                Some(WsCommand::Unsubscribe(token_id)) => {
                                    let unsub_msg = serde_json::json!({
                                        "type": "unsubscribe",
                                        "channel": "book",
                                        "assets_id": token_id
                                    });
                                    let _ = write.send(Message::Text(unsub_msg.to_string())).await;
                                }
                                Some(WsCommand::Disconnect) | None => {
                                    info!("Disconnecting market WebSocket");
                                    let _ = write.send(Message::Close(None)).await;
                                    return;
                                }
                            }
                        }

                        // Send periodic pings
                        _ = ping_interval.tick() => {
                            if write.send(Message::Ping(vec![])).await.is_err() {
                                warn!("Failed to send ping");
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to connect to market WebSocket: {}", e);
            }
        }

        // Reconnect with exponential backoff
        warn!("Reconnecting in {:?}...", reconnect_delay);
        sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(Duration::from_millis(WS_MAX_RECONNECT_DELAY_MS));
    }
}

/// Run the user WebSocket connection loop
async fn run_user_websocket(
    url: String,
    signer: Arc<Signer>,
    event_bus: EventBus,
    mut command_rx: mpsc::Receiver<WsCommand>,
) {
    let mut reconnect_delay = Duration::from_millis(WS_RECONNECT_DELAY_MS);

    loop {
        info!("Connecting to user WebSocket: {}", url);

        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                info!("User WebSocket connected");
                reconnect_delay = Duration::from_millis(WS_RECONNECT_DELAY_MS);

                let (mut write, mut read) = ws_stream.split();

                // Authenticate
                let timestamp = Utc::now().timestamp_millis().to_string();
                let auth_msg = serde_json::json!({
                    "type": "auth",
                    "apiKey": signer.api_key(),
                    "timestamp": timestamp,
                    "signature": signer.sign_request("GET", "/ws/user", &timestamp, "").unwrap_or_default(),
                    "passphrase": signer.api_passphrase()
                });

                if let Err(e) = write.send(Message::Text(auth_msg.to_string())).await {
                    error!("Failed to send auth message: {}", e);
                    continue;
                }

                // Subscribe to user events
                let sub_msg = serde_json::json!({
                    "type": "subscribe",
                    "channel": "user",
                    "user": signer.address_string()
                });

                if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
                    error!("Failed to subscribe to user events: {}", e);
                    continue;
                }

                // Spawn ping task
                let ping_interval = interval(Duration::from_secs(WS_PING_INTERVAL_SECS));
                tokio::pin!(ping_interval);

                loop {
                    tokio::select! {
                        // Handle incoming messages
                        msg = read.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    handle_user_message(&text, &event_bus);
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    let _ = write.send(Message::Pong(data)).await;
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("User WebSocket closed by server");
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!("User WebSocket error: {}", e);
                                    break;
                                }
                                None => {
                                    warn!("User WebSocket stream ended");
                                    break;
                                }
                                _ => {}
                            }
                        }

                        // Handle commands
                        cmd = command_rx.recv() => {
                            match cmd {
                                Some(WsCommand::Disconnect) | None => {
                                    info!("Disconnecting user WebSocket");
                                    let _ = write.send(Message::Close(None)).await;
                                    return;
                                }
                                _ => {}
                            }
                        }

                        // Send periodic pings
                        _ = ping_interval.tick() => {
                            if write.send(Message::Ping(vec![])).await.is_err() {
                                warn!("Failed to send ping");
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to connect to user WebSocket: {}", e);
            }
        }

        // Reconnect with exponential backoff
        warn!("Reconnecting in {:?}...", reconnect_delay);
        sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(Duration::from_millis(WS_MAX_RECONNECT_DELAY_MS));
    }
}

/// Handle market WebSocket messages
fn handle_market_message(text: &str, event_bus: &EventBus) {
    debug!("Market WS message: {}", &text[..text.len().min(200)]);

    // Try to parse as orderbook update
    if let Ok(msg) = serde_json::from_str::<WsOrderbookMessage>(text) {
        if msg.event_type == "book" {
            if let Some(data) = msg.data {
                // Convert to internal orderbook type
                let mut orderbook = Orderbook::new(
                    data.asset_id.clone(),
                    Outcome::Yes, // Will be updated by caller
                );

                for bid in &data.bids {
                    if let (Ok(price), Ok(size)) = (
                        bid.price.parse::<Decimal>(),
                        bid.size.parse::<Decimal>(),
                    ) {
                        if let Some(p) = Price::new(price) {
                            orderbook.bids.levels.push(PriceLevel::new(
                                p,
                                Quantity(size),
                                1,
                            ));
                        }
                    }
                }

                for ask in &data.asks {
                    if let (Ok(price), Ok(size)) = (
                        ask.price.parse::<Decimal>(),
                        ask.size.parse::<Decimal>(),
                    ) {
                        if let Some(p) = Price::new(price) {
                            orderbook.asks.levels.push(PriceLevel::new(
                                p,
                                Quantity(size),
                                1,
                            ));
                        }
                    }
                }

                // Sort orderbook
                orderbook.bids.levels.sort_by(|a, b| b.price.cmp(&a.price));
                orderbook.asks.levels.sort_by(|a, b| a.price.cmp(&b.price));
                orderbook.timestamp = Utc::now();

                event_bus.publish(Event::orderbook_update(orderbook));
            }
        }
    }

    // Try to parse as price update
    if let Ok(msg) = serde_json::from_str::<WsPriceMessage>(text) {
        if msg.event_type == "price_change" {
            // Handle price change event
            debug!("Price change: {:?}", msg);
        }
    }
}

/// Handle user WebSocket messages
fn handle_user_message(text: &str, event_bus: &EventBus) {
    debug!("User WS message: {}", &text[..text.len().min(200)]);

    // Try to parse as order update
    if let Ok(msg) = serde_json::from_str::<WsUserMessage>(text) {
        match msg.event_type.as_str() {
            "order" => {
                if let Some(data) = msg.order_data {
                    debug!("Order update: {:?}", data);

                    // Parse order status
                    let status = match data.status.to_uppercase().as_str() {
                        "LIVE" => OrderStatus::Live,
                        "MATCHED" => OrderStatus::Matched,
                        "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
                        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
                        _ => return,
                    };

                    // Create order from data
                    let side = match data.side.to_uppercase().as_str() {
                        "BUY" => Side::Buy,
                        "SELL" => Side::Sell,
                        _ => return,
                    };

                    let price = data.price.parse::<Decimal>().ok()
                        .and_then(Price::new)
                        .unwrap_or(Price::ZERO);

                    let quantity = data.original_size.parse::<Decimal>()
                        .unwrap_or(Decimal::ZERO);

                    let order = Order {
                        id: data.id,
                        market_id: data.market.unwrap_or_default(),
                        token_id: data.asset_id,
                        side,
                        outcome: Outcome::Yes, // Will need to be determined
                        price,
                        quantity: Quantity(quantity),
                        order_type: OrderType::Gtc,
                        expiration: None,
                        created_at: Utc::now(),
                        client_order_id: None,
                    };

                    event_bus.publish(Event::order(order, status));
                }
            }
            "trade" => {
                if let Some(data) = msg.trade_data {
                    debug!("Trade executed: {:?}", data);

                    let side = match data.side.to_uppercase().as_str() {
                        "BUY" => Side::Buy,
                        "SELL" => Side::Sell,
                        _ => return,
                    };

                    let price = data.price.parse::<Decimal>().ok()
                        .and_then(Price::new)
                        .unwrap_or(Price::ZERO);

                    let quantity = data.size.parse::<Decimal>()
                        .unwrap_or(Decimal::ZERO);

                    let fee = data.fee.and_then(|f| f.parse::<Decimal>().ok())
                        .unwrap_or(Decimal::ZERO);

                    let trade = Trade {
                        id: data.id,
                        order_id: data.order_id.unwrap_or_default(),
                        market_id: data.market.unwrap_or_default(),
                        token_id: data.asset_id,
                        side,
                        outcome: Outcome::Yes,
                        price,
                        quantity: Quantity(quantity),
                        fee,
                        timestamp: Utc::now(),
                        tx_hash: data.transaction_hash,
                    };

                    event_bus.publish(Event::trade(trade));
                }
            }
            _ => {}
        }
    }
}

// ============================================================================
// WebSocket Message Types
// ============================================================================

#[derive(Debug, Deserialize)]
struct WsOrderbookMessage {
    #[serde(rename = "event_type")]
    event_type: String,
    data: Option<WsOrderbookData>,
}

#[derive(Debug, Deserialize)]
struct WsOrderbookData {
    asset_id: String,
    market: Option<String>,
    timestamp: Option<String>,
    hash: Option<String>,
    bids: Vec<WsLevel>,
    asks: Vec<WsLevel>,
}

#[derive(Debug, Deserialize)]
struct WsLevel {
    price: String,
    size: String,
}

#[derive(Debug, Deserialize)]
struct WsPriceMessage {
    #[serde(rename = "event_type")]
    event_type: String,
    asset_id: Option<String>,
    price: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WsUserMessage {
    #[serde(rename = "event_type")]
    event_type: String,
    #[serde(flatten)]
    order_data: Option<WsOrderData>,
    #[serde(flatten)]
    trade_data: Option<WsTradeData>,
}

#[derive(Debug, Deserialize)]
struct WsOrderData {
    id: String,
    market: Option<String>,
    asset_id: String,
    side: String,
    price: String,
    original_size: String,
    size_matched: Option<String>,
    status: String,
}

#[derive(Debug, Deserialize)]
struct WsTradeData {
    id: String,
    order_id: Option<String>,
    market: Option<String>,
    asset_id: String,
    side: String,
    price: String,
    size: String,
    fee: Option<String>,
    transaction_hash: Option<String>,
}

impl std::fmt::Debug for MarketWebSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MarketWebSocket")
            .field("url", &self.url)
            .field("connected", &self.command_tx.is_some())
            .finish()
    }
}

impl std::fmt::Debug for UserWebSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserWebSocket")
            .field("url", &self.url)
            .field("connected", &self.command_tx.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orderbook_message_parsing() {
        let msg = r#"{
            "event_type": "book",
            "data": {
                "asset_id": "12345",
                "bids": [
                    {"price": "0.45", "size": "100"},
                    {"price": "0.44", "size": "200"}
                ],
                "asks": [
                    {"price": "0.55", "size": "150"},
                    {"price": "0.56", "size": "250"}
                ]
            }
        }"#;

        let parsed: WsOrderbookMessage = serde_json::from_str(msg).unwrap();
        assert_eq!(parsed.event_type, "book");
        assert!(parsed.data.is_some());
    }
}
