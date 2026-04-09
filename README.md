# Cross-Platform Prediction Market Arbitrage Bot

A production-grade, high-frequency trading bot for cross-platform arbitrage across **Polymarket**, **Kalshi**, and **Opinion** prediction markets. Built in Rust for maximum performance and reliability.

## Key Differentiators

This bot specifically addresses the challenges of **cross-platform execution** where fills are **NOT atomic**:

- **Leg Sequencing**: Executes on the MORE liquid venue first for higher fill probability
- **Non-Atomic Fill Handling**: Robust handling when neither leg can be guaranteed
- **Hedge Slippage Recovery**: Automatic unwind when hedge leg moves or partially fills
- **Multi-Venue State Coordination**: Persistent state recovery across venues
- **Latency-Aware Execution**: Accounts for venue latency asymmetry

## Features

### Arbitrage Strategies

#### Single-Venue (Polymarket)
- **Binary Market Mispricing**: Exploits when YES + NO prices ≠ $1.00
- **Multi-Outcome Arbitrage**: Detects mispricing across 3+ outcome markets
- **Cross-Market Arbitrage**: Finds inconsistencies between related markets
- **Temporal Arbitrage**: Specialized for 5-minute BTC/ETH markets

#### Cross-Platform (Polymarket ↔ Kalshi/Opinion)
- **Cross-Venue Price Discovery**: Detects price discrepancies across exchanges
- **Fee-Adjusted Opportunity Scoring**: Accounts for different fee structures
- **Liquidity-Weighted Execution**: Prioritizes venues with better liquidity
- **Correlation-Based Market Matching**: Auto-discovers equivalent markets

### Execution Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CROSS-PLATFORM EXECUTION FLOW                        │
└─────────────────────────────────────────────────────────────────────────────┘

        ┌──────────────┐         ┌──────────────┐         ┌──────────────┐
        │  POLYMARKET  │         │    KALSHI    │         │   OPINION    │
        │    (CLOB)    │         │  (Exchange)  │         │  (Back/Lay)  │
        └──────┬───────┘         └──────┬───────┘         └──────┬───────┘
               │                        │                        │
               └────────────────────────┼────────────────────────┘
                                        │
                            ┌───────────▼───────────┐
                            │   PRICE AGGREGATOR    │
                            │  (Real-time Prices)   │
                            └───────────┬───────────┘
                                        │
                            ┌───────────▼───────────┐
                            │ OPPORTUNITY DETECTOR  │
                            │ - Fee calculation     │
                            │ - Liquidity scoring   │
                            │ - Correlation check   │
                            └───────────┬───────────┘
                                        │
                            ┌───────────▼───────────┐
                            │   LEG SEQUENCER       │
                            │                       │
                            │ 1. Analyze liquidity  │
                            │ 2. Select first leg   │
                            │    (MORE liquid)      │
                            │ 3. Execute first leg  │
                            │ 4. Wait for fill      │
                            │ 5. Execute hedge leg  │
                            │    (aggressive price) │
                            └───────────┬───────────┘
                                        │
               ┌────────────────────────┼────────────────────────┐
               │                        │                        │
      ┌────────▼────────┐     ┌────────▼────────┐     ┌────────▼────────┐
      │   FULL FILL     │     │  PARTIAL FILL   │     │  HEDGE FAILED   │
      │   = SUCCESS     │     │  = RETRY/ABORT  │     │  = UNWIND       │
      └─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Execution Scenarios

#### Scenario 1: Both Legs Fill Successfully
```
1. Detect opportunity: Buy YES on Polymarket @ $0.45, Sell YES on Kalshi @ $0.50
2. Execute first leg (more liquid): Buy 100 YES on Polymarket
3. First leg fills @ $0.45
4. Execute hedge leg: Sell 100 YES on Kalshi (aggressive @ $0.49)
5. Hedge fills @ $0.49
6. Profit: $4.00 (before fees)
```

#### Scenario 2: Hedge Partially Fills
```
1. First leg fills: 100 YES on Polymarket @ $0.45
2. Hedge order placed on Kalshi
3. Only 60/100 fills before price moves
4. Decision:
   - If 60% > min_threshold (50%): Accept partial hedge, reduce exposure
   - Retry with more aggressive pricing
   - If still fails: Unwind first leg position
```

#### Scenario 3: Hedge Price Moves Beyond Threshold
```
1. First leg fills: 100 YES on Polymarket @ $0.45
2. Kalshi YES bid moves from $0.50 → $0.42 (market moved against us)
3. Slippage (8 cents) > max_threshold (3 cents)
4. ABORT: Unwind first leg position on Polymarket
5. Loss limited to spread + fees
```

### Risk Management

#### Per-Execution Safeguards
- **Max Hedge Slippage**: 3% (configurable)
- **Min First Leg Fill**: 50% (configurable)
- **Hedge Timeout**: 5 seconds before retry/abort
- **Max Hedge Retries**: 3 attempts with increasing aggression
- **Abort Price Threshold**: 5% move triggers immediate unwind

#### Portfolio-Level Controls
- Position size limits per venue
- Total cross-venue exposure limits
- Daily loss limits
- Drawdown protection
- Circuit breaker with automatic cooldown
- Kill switch for emergencies

### State Recovery

The system maintains persistent state to recover from:
- Process crashes during execution
- Network disconnections
- Exchange API failures

```rust
enum ExecutionState {
    Pending,
    FirstLegSubmitted { venue, order_id, submitted_at },
    FirstLegPartialFill { filled_qty, remaining_qty, avg_price },
    FirstLegFilled { filled_qty, avg_price, filled_at },
    HedgeLegSubmitted { first_leg, hedge_venue, hedge_order_id },
    HedgeLegPartialFill { hedge_filled_qty, hedge_remaining_qty },
    Completed { first_leg, hedge_leg, realized_profit },
    Aborted { reason, first_leg, hedge_leg },
    Unwinding { first_leg, partial_hedge, reason },
    UnwindCompleted { net_pnl },
    Failed { error, requires_manual_intervention },
}
```

## Quick Start

### Prerequisites
- Rust 1.76+
- Docker & Docker Compose (for full deployment)
- PostgreSQL 16+ (for state persistence)
- Redis 7+ (for caching)

### Installation

```bash
# Clone the repository
git clone https://github.com/pranay123-stack/polymarket-hft-arbitrage.git
cd polymarket-hft-arbitrage

# Build the project
cargo build --release

# Or use Docker
docker-compose build
```

### Configuration

1. Copy the environment template:
```bash
cp .env.example .env
```

2. Set your credentials:
```bash
# Polymarket (required)
export POLYBOT_PRIVATE_KEY="your_ethereum_private_key"

# Kalshi (required for cross-platform)
export KALSHI_EMAIL="your_kalshi_email"
export KALSHI_PASSWORD="your_kalshi_password"

# Opinion (optional)
export OPINION_API_KEY="your_opinion_api_key"
export OPINION_API_SECRET="your_opinion_api_secret"
```

3. Customize `config/default.toml`:
```toml
[strategy]
cross_platform = true

[strategy.cross_platform_config]
min_profit_usd = "1.0"
min_profit_pct = "0.005"
max_position_size = "1000"
max_hedge_slippage = "0.03"
min_first_leg_fill_pct = "0.5"
first_leg_timeout_ms = 3000
hedge_leg_timeout_ms = 5000
max_hedge_retries = 3
hedge_price_aggression = "0.01"
use_ioc_for_hedge = true
enable_state_recovery = true
kalshi_enabled = true
opinion_enabled = true
```

### Running

**Paper Trading Mode (Recommended First)**
```bash
./target/release/polymarket-hft run --dry-run
```

**Scan for Cross-Platform Opportunities**
```bash
./target/release/polymarket-hft scan --cross-platform --count 10
```

**Live Trading**
```bash
./target/release/polymarket-hft run --mode live
```

## Project Structure

```
polymarket-hft-arbitrage/
├── src/
│   ├── api/
│   │   ├── clob.rs           # Polymarket CLOB client
│   │   ├── gamma.rs          # Polymarket market data
│   │   ├── kalshi.rs         # Kalshi API client
│   │   ├── opinion.rs        # Opinion API client
│   │   ├── websocket.rs      # Real-time feeds
│   │   └── auth.rs           # Multi-venue authentication
│   ├── execution/
│   │   ├── cross_platform.rs # Cross-platform executor (KEY FILE)
│   │   ├── state_coordinator.rs # Multi-venue state recovery
│   │   ├── atomic.rs         # Single-venue atomic execution
│   │   └── router.rs         # Smart order routing
│   ├── strategies/
│   │   ├── cross_platform.rs # Cross-venue opportunity detection
│   │   ├── binary.rs         # Binary market arbitrage
│   │   ├── multi_outcome.rs  # Multi-outcome arbitrage
│   │   └── temporal.rs       # 5-minute market arbitrage
│   ├── risk/
│   │   ├── manager.rs        # Risk manager
│   │   ├── circuit_breaker.rs# Circuit breaker
│   │   └── limits.rs         # Position/exposure limits
│   └── monitoring/
│       ├── metrics.rs        # Prometheus metrics
│       └── health.rs         # Health checks
├── config/
│   └── default.toml          # Configuration template
├── docker-compose.yml        # Full deployment stack
└── README.md
```

## Configuration Reference

### Cross-Platform Execution Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `first_leg_timeout_ms` | 3000 | Max wait for first leg fill |
| `hedge_leg_timeout_ms` | 5000 | Max wait for hedge leg fill |
| `max_hedge_slippage` | 3% | Max slippage before abort |
| `min_first_leg_fill_pct` | 50% | Min fill before hedging |
| `hedge_price_aggression` | 1 cent | Added to hedge price |
| `max_hedge_retries` | 3 | Retry attempts for hedge |
| `abort_price_threshold` | 5% | Price move triggers abort |

### Venue Fee Structures

| Venue | Fee Type | Default Rate |
|-------|----------|--------------|
| Polymarket | Maker/Taker | 2% |
| Kalshi | Included in spread | 1% |
| Opinion | Commission on winnings | 2% |

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `POLYBOT_PRIVATE_KEY` | Yes | Ethereum private key (Polymarket) |
| `KALSHI_EMAIL` | Yes* | Kalshi account email |
| `KALSHI_PASSWORD` | Yes* | Kalshi account password |
| `OPINION_API_KEY` | No | Opinion API key |
| `OPINION_API_SECRET` | No | Opinion API secret |
| `POLYBOT_DB_PASSWORD` | No | PostgreSQL password |

*Required if `kalshi_enabled = true`

## API Documentation

### Kalshi Integration
- REST API: `https://trading-api.kalshi.com/trade-api/v2`
- Authentication: Session-based (email/password login)
- Price format: Cents (0-100)
- Rate limits: 10 requests/second

### Opinion Integration
- REST API: Configurable base URL
- Authentication: HMAC-SHA256 signed requests
- Price format: Decimal odds (e.g., 1.50 = 66% implied)
- Order types: Back (buy YES) / Lay (sell YES)

### Polymarket Integration
- CLOB API: `https://clob.polymarket.com`
- WebSocket: `wss://ws-subscriptions-clob.polymarket.com`
- Authentication: EIP-712 typed data signing
- Price format: Decimal probability (0-1)

## Monitoring

### Prometheus Metrics
```
GET http://localhost:9090/metrics

# Key metrics
arbitrage_opportunities_detected_total
cross_platform_executions_total
cross_platform_executions_successful
cross_platform_executions_failed
hedge_leg_retries_total
unwind_operations_total
venue_latency_ms{venue="polymarket|kalshi|opinion"}
position_exposure_usd{venue="..."}
```

### Health Check
```
GET http://localhost:8081/health
{
  "status": "healthy",
  "venues": {
    "polymarket": {"connected": true, "latency_ms": 45},
    "kalshi": {"connected": true, "latency_ms": 78},
    "opinion": {"connected": true, "latency_ms": 112}
  },
  "pending_recoveries": 0,
  "circuit_breaker": "closed"
}
```

## Security Considerations

1. **Never commit credentials** - Use environment variables
2. **Use paper trading first** - Validate strategy before live
3. **Set conservative limits** - Start with small positions
4. **Monitor continuously** - Watch for anomalies
5. **Have a kill switch plan** - Know how to stop quickly
6. **Secure API credentials** - Use separate accounts per environment
7. **Network security** - Run on isolated/secured infrastructure

## Backtesting

```bash
cargo run --release --bin backtest -- \
  --start-date 2024-01-01 \
  --end-date 2024-03-01 \
  --strategy cross-platform \
  --venues polymarket,kalshi
```

## Performance Tuning

### Rust Compilation
```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

### System Tuning
```bash
ulimit -n 65535  # Increase file descriptors
sudo swapoff -a  # Disable swap
sudo cpupower frequency-set -g performance  # CPU governor
```

### Latency Optimization
- Co-locate servers near exchange infrastructure
- Use dedicated network connections
- Minimize serialization overhead
- Pre-compute execution plans

## License

MIT License - See [LICENSE](LICENSE) for details.

## Disclaimer

**This software is for educational purposes only. Trading involves substantial risk of loss. Cross-platform arbitrage carries additional risks including execution risk, counterparty risk, and regulatory risk. Past performance does not guarantee future results. Use at your own risk.**

## Support

- Issues: [GitHub Issues](https://github.com/pranay123-stack/polymarket-hft-arbitrage/issues)
- Email: pranay@hft.dev
