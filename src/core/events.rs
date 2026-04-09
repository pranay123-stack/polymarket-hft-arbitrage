//! Event system for inter-component communication.
//!
//! Uses a high-performance broadcast channel for publishing events
//! that multiple subscribers can receive.

use crate::core::types::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast;
use uuid::Uuid;

/// Event bus for publishing and subscribing to system events
#[derive(Debug, Clone)]
pub struct EventBus {
    sender: broadcast::Sender<Arc<Event>>,
}

impl EventBus {
    /// Create a new event bus with the given channel capacity
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Publish an event to all subscribers
    pub fn publish(&self, event: Event) -> usize {
        // Returns number of receivers that received the event
        self.sender.send(Arc::new(event)).unwrap_or(0)
    }

    /// Subscribe to events
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Event>> {
        self.sender.subscribe()
    }

    /// Get the number of active subscribers
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(10000)
    }
}

/// System events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub payload: EventPayload,
}

impl Event {
    pub fn new(payload: EventPayload) -> Self {
        Self {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload,
        }
    }

    /// Create a market update event
    pub fn market_update(market: Market) -> Self {
        Self::new(EventPayload::MarketUpdate(MarketUpdateEvent { market }))
    }

    /// Create an orderbook update event
    pub fn orderbook_update(orderbook: Orderbook) -> Self {
        Self::new(EventPayload::OrderbookUpdate(OrderbookUpdateEvent { orderbook }))
    }

    /// Create an order event
    pub fn order(order: Order, status: OrderStatus) -> Self {
        Self::new(EventPayload::OrderUpdate(OrderUpdateEvent { order, status }))
    }

    /// Create a trade event
    pub fn trade(trade: Trade) -> Self {
        Self::new(EventPayload::TradeExecuted(TradeExecutedEvent { trade }))
    }

    /// Create an arbitrage opportunity event
    pub fn arbitrage_opportunity(opportunity: ArbitrageOpportunity) -> Self {
        Self::new(EventPayload::ArbitrageOpportunity(ArbitrageOpportunityEvent { opportunity }))
    }

    /// Create a position update event
    pub fn position_update(position: Position) -> Self {
        Self::new(EventPayload::PositionUpdate(PositionUpdateEvent { position }))
    }

    /// Create an alert event
    pub fn alert(level: AlertLevel, message: String, details: Option<String>) -> Self {
        Self::new(EventPayload::Alert(AlertEvent { level, message, details }))
    }

    /// Create a system status event
    pub fn system_status(status: SystemStatus) -> Self {
        Self::new(EventPayload::SystemStatus(SystemStatusEvent { status }))
    }
}

/// Event payload variants
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum EventPayload {
    // Market data events
    MarketUpdate(MarketUpdateEvent),
    OrderbookUpdate(OrderbookUpdateEvent),
    PriceUpdate(PriceUpdateEvent),

    // Order events
    OrderUpdate(OrderUpdateEvent),
    OrderFilled(OrderFilledEvent),
    OrderCancelled(OrderCancelledEvent),

    // Trade events
    TradeExecuted(TradeExecutedEvent),

    // Arbitrage events
    ArbitrageOpportunity(ArbitrageOpportunityEvent),
    ArbitrageExecuted(ArbitrageExecutedEvent),
    ArbitrageExpired(ArbitrageExpiredEvent),

    // Position events
    PositionUpdate(PositionUpdateEvent),
    PositionClosed(PositionClosedEvent),

    // Risk events
    RiskAlert(RiskAlertEvent),
    CircuitBreakerTriggered(CircuitBreakerEvent),

    // System events
    SystemStatus(SystemStatusEvent),
    ConnectionStatus(ConnectionStatusEvent),
    Alert(AlertEvent),

    // Blockchain events
    TokenTransfer(TokenTransferEvent),
    TransactionConfirmed(TransactionConfirmedEvent),
}

// Market data events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketUpdateEvent {
    pub market: Market,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookUpdateEvent {
    pub orderbook: Orderbook,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceUpdateEvent {
    pub market_id: String,
    pub token_id: TokenId,
    pub outcome: Outcome,
    pub bid: Option<Price>,
    pub ask: Option<Price>,
    pub mid: Option<Price>,
    pub timestamp: DateTime<Utc>,
}

// Order events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderUpdateEvent {
    pub order: Order,
    pub status: OrderStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderFilledEvent {
    pub order_id: OrderId,
    pub filled_quantity: Quantity,
    pub fill_price: Price,
    pub remaining_quantity: Quantity,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCancelledEvent {
    pub order_id: OrderId,
    pub reason: String,
    pub timestamp: DateTime<Utc>,
}

// Trade events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeExecutedEvent {
    pub trade: Trade,
}

// Arbitrage events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitrageOpportunityEvent {
    pub opportunity: ArbitrageOpportunity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitrageExecutedEvent {
    pub opportunity_id: Uuid,
    pub trades: Vec<Trade>,
    pub realized_profit: rust_decimal::Decimal,
    pub execution_time_ms: u64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitrageExpiredEvent {
    pub opportunity_id: Uuid,
    pub reason: String,
    pub timestamp: DateTime<Utc>,
}

// Position events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionUpdateEvent {
    pub position: Position,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionClosedEvent {
    pub position: Position,
    pub realized_pnl: rust_decimal::Decimal,
    pub close_reason: String,
    pub timestamp: DateTime<Utc>,
}

// Risk events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskAlertEvent {
    pub alert_type: RiskAlertType,
    pub message: String,
    pub current_value: String,
    pub threshold: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RiskAlertType {
    PositionSize,
    Exposure,
    DailyLoss,
    Drawdown,
    Volatility,
    Correlation,
    OrderRate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerEvent {
    pub reason: String,
    pub cooldown_until: DateTime<Utc>,
    pub timestamp: DateTime<Utc>,
}

// System events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatusEvent {
    pub status: SystemStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemStatus {
    Starting,
    Running,
    Paused,
    Stopping,
    Stopped,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionStatusEvent {
    pub connection_type: ConnectionType,
    pub status: ConnectionStatus,
    pub endpoint: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionType {
    ClobApi,
    ClobWebSocket,
    GammaApi,
    PolygonRpc,
    PolygonWebSocket,
    Database,
    Redis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
    Reconnecting,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    pub level: AlertLevel,
    pub message: String,
    pub details: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AlertLevel {
    Info,
    Warning,
    Error,
    Critical,
}

// Blockchain events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenTransferEvent {
    pub token_id: TokenId,
    pub from: String,
    pub to: String,
    pub amount: rust_decimal::Decimal,
    pub tx_hash: String,
    pub block_number: u64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionConfirmedEvent {
    pub tx_hash: String,
    pub block_number: u64,
    pub gas_used: u64,
    pub status: bool,
    pub timestamp: DateTime<Utc>,
}

/// Event handler trait for components that process events
#[async_trait::async_trait]
pub trait EventHandler: Send + Sync {
    /// Handle an event
    async fn handle(&self, event: &Event);

    /// Get the event types this handler is interested in
    fn event_types(&self) -> Vec<&'static str> {
        vec![] // Empty means all events
    }
}

/// Spawn an event processor that routes events to handlers
pub fn spawn_event_processor(
    event_bus: EventBus,
    handlers: Vec<Arc<dyn EventHandler>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut receiver = event_bus.subscribe();

        loop {
            match receiver.recv().await {
                Ok(event) => {
                    for handler in &handlers {
                        let event_types = handler.event_types();
                        let event_type = event.payload.event_type();

                        if event_types.is_empty() || event_types.contains(&event_type) {
                            handler.handle(&event).await;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("Event processor lagged by {} events", n);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::info!("Event bus closed, shutting down processor");
                    break;
                }
            }
        }
    })
}

impl EventPayload {
    /// Get the event type name for filtering
    pub fn event_type(&self) -> &'static str {
        match self {
            EventPayload::MarketUpdate(_) => "market_update",
            EventPayload::OrderbookUpdate(_) => "orderbook_update",
            EventPayload::PriceUpdate(_) => "price_update",
            EventPayload::OrderUpdate(_) => "order_update",
            EventPayload::OrderFilled(_) => "order_filled",
            EventPayload::OrderCancelled(_) => "order_cancelled",
            EventPayload::TradeExecuted(_) => "trade_executed",
            EventPayload::ArbitrageOpportunity(_) => "arbitrage_opportunity",
            EventPayload::ArbitrageExecuted(_) => "arbitrage_executed",
            EventPayload::ArbitrageExpired(_) => "arbitrage_expired",
            EventPayload::PositionUpdate(_) => "position_update",
            EventPayload::PositionClosed(_) => "position_closed",
            EventPayload::RiskAlert(_) => "risk_alert",
            EventPayload::CircuitBreakerTriggered(_) => "circuit_breaker_triggered",
            EventPayload::SystemStatus(_) => "system_status",
            EventPayload::ConnectionStatus(_) => "connection_status",
            EventPayload::Alert(_) => "alert",
            EventPayload::TokenTransfer(_) => "token_transfer",
            EventPayload::TransactionConfirmed(_) => "transaction_confirmed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_event_bus_pub_sub() {
        let bus = EventBus::new(100);
        let mut receiver = bus.subscribe();

        let event = Event::alert(AlertLevel::Info, "Test".to_string(), None);
        let sent = bus.publish(event.clone());

        assert_eq!(sent, 1);

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.id, event.id);
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let bus = EventBus::new(100);
        let mut receiver1 = bus.subscribe();
        let mut receiver2 = bus.subscribe();

        let event = Event::alert(AlertLevel::Warning, "Test".to_string(), None);
        let sent = bus.publish(event);

        assert_eq!(sent, 2);

        let r1 = receiver1.recv().await.unwrap();
        let r2 = receiver2.recv().await.unwrap();

        assert_eq!(r1.id, r2.id);
    }
}
