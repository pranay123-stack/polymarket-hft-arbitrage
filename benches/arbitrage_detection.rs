//! Arbitrage detection performance benchmarks.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

/// Simulated market state for benchmarking.
#[derive(Clone)]
struct MarketState {
    token_id: String,
    yes_bid: Decimal,
    yes_ask: Decimal,
    no_bid: Decimal,
    no_ask: Decimal,
    liquidity: Decimal,
}

impl MarketState {
    fn new(i: usize) -> Self {
        let base = dec!(0.40) + Decimal::from(i % 20) * dec!(0.01);
        Self {
            token_id: format!("token_{}", i),
            yes_bid: base - dec!(0.02),
            yes_ask: base,
            no_bid: dec!(1.0) - base - dec!(0.02),
            no_ask: dec!(1.0) - base + dec!(0.02),
            liquidity: dec!(10000.0) + Decimal::from(i % 100) * dec!(100.0),
        }
    }
}

/// Benchmark binary arbitrage detection.
fn bench_binary_arbitrage(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_arbitrage");

    for num_markets in [10, 50, 100, 500].iter() {
        let markets: Vec<MarketState> = (0..*num_markets)
            .map(|i| MarketState::new(i))
            .collect();

        group.throughput(Throughput::Elements(*num_markets as u64));

        group.bench_with_input(
            BenchmarkId::new("check_all", num_markets),
            &markets,
            |b, markets| {
                b.iter(|| {
                    let mut opportunities = Vec::new();
                    for market in markets.iter() {
                        // Check if yes_ask + no_ask < 1.0 (buy both sides)
                        let buy_cost = market.yes_ask + market.no_ask;
                        if buy_cost < dec!(1.0) {
                            let profit = dec!(1.0) - buy_cost;
                            opportunities.push((market.token_id.clone(), profit));
                        }

                        // Check if yes_bid + no_bid > 1.0 (sell both sides)
                        let sell_value = market.yes_bid + market.no_bid;
                        if sell_value > dec!(1.0) {
                            let profit = sell_value - dec!(1.0);
                            opportunities.push((market.token_id.clone(), profit));
                        }
                    }
                    black_box(opportunities)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark cross-venue arbitrage detection.
fn bench_cross_venue_arbitrage(c: &mut Criterion) {
    let mut group = c.benchmark_group("cross_venue_arbitrage");

    // Simulate markets across 3 venues
    struct VenueMarket {
        venue: &'static str,
        bid: Decimal,
        ask: Decimal,
        liquidity: Decimal,
    }

    for num_markets in [10, 50, 100].iter() {
        let venues = ["polymarket", "kalshi", "opinion"];
        let mut markets: HashMap<String, Vec<VenueMarket>> = HashMap::new();

        for i in 0..*num_markets {
            let market_id = format!("market_{}", i);
            let base = dec!(0.45) + Decimal::from(i % 10) * dec!(0.01);

            let venue_markets: Vec<VenueMarket> = venues
                .iter()
                .enumerate()
                .map(|(v, &venue)| {
                    let offset = Decimal::from(v as i32) * dec!(0.01);
                    VenueMarket {
                        venue,
                        bid: base - dec!(0.02) + offset,
                        ask: base + dec!(0.02) - offset,
                        liquidity: dec!(5000.0),
                    }
                })
                .collect();

            markets.insert(market_id, venue_markets);
        }

        group.throughput(Throughput::Elements(*num_markets as u64));

        group.bench_with_input(
            BenchmarkId::new("cross_venue_scan", num_markets),
            &markets,
            |b, markets| {
                b.iter(|| {
                    let mut opportunities = Vec::new();

                    for (market_id, venue_markets) in markets.iter() {
                        // Find best bid and best ask across venues
                        let mut best_bid = (Decimal::ZERO, "");
                        let mut best_ask = (Decimal::MAX, "");

                        for vm in venue_markets.iter() {
                            if vm.bid > best_bid.0 {
                                best_bid = (vm.bid, vm.venue);
                            }
                            if vm.ask < best_ask.0 {
                                best_ask = (vm.ask, vm.venue);
                            }
                        }

                        // Cross-venue arb: buy at best ask, sell at best bid
                        if best_bid.1 != best_ask.1 && best_bid.0 > best_ask.0 {
                            let spread = best_bid.0 - best_ask.0;
                            opportunities.push((market_id.clone(), spread, best_ask.1, best_bid.1));
                        }
                    }

                    black_box(opportunities)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark profit calculation with fees.
fn bench_profit_calculation(c: &mut Criterion) {
    let mut group = c.benchmark_group("profit_calculation");

    let fee_rate = dec!(0.001); // 0.1% fee
    let min_profit = dec!(0.005); // 0.5% minimum profit

    for num_calculations in [100, 1000, 10000].iter() {
        let trades: Vec<(Decimal, Decimal, Decimal)> = (0..*num_calculations)
            .map(|i| {
                let buy_price = dec!(0.45) + Decimal::from(i % 10) * dec!(0.001);
                let sell_price = buy_price + dec!(0.02) + Decimal::from(i % 5) * dec!(0.002);
                let quantity = dec!(100.0) + Decimal::from(i % 50) * dec!(10.0);
                (buy_price, sell_price, quantity)
            })
            .collect();

        group.throughput(Throughput::Elements(*num_calculations as u64));

        group.bench_with_input(
            BenchmarkId::new("net_profit", num_calculations),
            &trades,
            |b, trades| {
                b.iter(|| {
                    let mut profitable_count = 0u64;
                    let mut total_profit = Decimal::ZERO;

                    for (buy_price, sell_price, quantity) in trades.iter() {
                        let buy_cost = buy_price * quantity;
                        let sell_revenue = sell_price * quantity;

                        // Calculate fees
                        let buy_fee = buy_cost * fee_rate;
                        let sell_fee = sell_revenue * fee_rate;

                        // Net profit
                        let gross_profit = sell_revenue - buy_cost;
                        let net_profit = gross_profit - buy_fee - sell_fee;

                        // Check profitability
                        let profit_pct = net_profit / buy_cost;
                        if profit_pct > min_profit {
                            profitable_count += 1;
                            total_profit += net_profit;
                        }
                    }

                    black_box((profitable_count, total_profit))
                });
            },
        );
    }

    group.finish();
}

/// Benchmark opportunity ranking.
fn bench_opportunity_ranking(c: &mut Criterion) {
    let mut group = c.benchmark_group("opportunity_ranking");

    #[derive(Clone)]
    struct Opportunity {
        market_id: String,
        expected_profit: Decimal,
        liquidity: Decimal,
        latency_score: Decimal,
        risk_score: Decimal,
    }

    for num_opportunities in [10, 50, 100, 500].iter() {
        let opportunities: Vec<Opportunity> = (0..*num_opportunities)
            .map(|i| Opportunity {
                market_id: format!("opp_{}", i),
                expected_profit: dec!(10.0) + Decimal::from(i % 100),
                liquidity: dec!(1000.0) + Decimal::from(i % 50) * dec!(100.0),
                latency_score: dec!(0.8) + Decimal::from(i % 20) * dec!(0.01),
                risk_score: dec!(0.1) + Decimal::from(i % 30) * dec!(0.01),
            })
            .collect();

        group.throughput(Throughput::Elements(*num_opportunities as u64));

        group.bench_with_input(
            BenchmarkId::new("rank_by_score", num_opportunities),
            &opportunities,
            |b, opportunities| {
                b.iter(|| {
                    let mut scored: Vec<(Decimal, &Opportunity)> = opportunities
                        .iter()
                        .map(|opp| {
                            // Composite score: profit * liquidity_factor * latency_factor / risk
                            let liquidity_factor = (opp.liquidity / dec!(10000.0)).min(dec!(1.0));
                            let score = opp.expected_profit
                                * liquidity_factor
                                * opp.latency_score
                                / (opp.risk_score + dec!(0.1));
                            (score, opp)
                        })
                        .collect();

                    scored.sort_by(|a, b| b.0.cmp(&a.0));

                    // Take top 5
                    let top: Vec<_> = scored.into_iter().take(5).collect();
                    black_box(top)
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_binary_arbitrage,
    bench_cross_venue_arbitrage,
    bench_profit_calculation,
    bench_opportunity_ranking,
);

criterion_main!(benches);
