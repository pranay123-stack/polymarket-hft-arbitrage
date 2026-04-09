//! Orderbook performance benchmarks.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Benchmark orderbook update operations.
fn bench_orderbook_updates(c: &mut Criterion) {
    let mut group = c.benchmark_group("orderbook_updates");

    // Test different orderbook depths
    for depth in [100, 500, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(*depth as u64));

        group.bench_with_input(
            BenchmarkId::new("insert_levels", depth),
            depth,
            |b, &depth| {
                b.iter(|| {
                    let mut levels: Vec<(Decimal, Decimal)> = Vec::with_capacity(depth);
                    for i in 0..depth {
                        levels.push((
                            dec!(0.50) + Decimal::from(i) * dec!(0.001),
                            dec!(100.0) + Decimal::from(i % 10) * dec!(10.0),
                        ));
                    }
                    black_box(levels)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("sort_levels", depth),
            depth,
            |b, &depth| {
                let mut levels: Vec<(Decimal, Decimal)> = (0..depth)
                    .map(|i| {
                        (
                            dec!(0.50) + Decimal::from(i) * dec!(0.001),
                            dec!(100.0),
                        )
                    })
                    .collect();

                b.iter(|| {
                    let mut sorted = levels.clone();
                    sorted.sort_by(|a, b| a.0.cmp(&b.0));
                    black_box(sorted)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark spread calculation.
fn bench_spread_calculation(c: &mut Criterion) {
    let mut group = c.benchmark_group("spread_calculation");

    let bid = dec!(0.45);
    let ask = dec!(0.55);

    group.bench_function("absolute_spread", |b| {
        b.iter(|| {
            let spread = black_box(ask) - black_box(bid);
            black_box(spread)
        });
    });

    group.bench_function("relative_spread", |b| {
        b.iter(|| {
            let mid = (black_box(bid) + black_box(ask)) / dec!(2);
            let spread = (black_box(ask) - black_box(bid)) / mid;
            black_box(spread)
        });
    });

    group.bench_function("mid_price", |b| {
        b.iter(|| {
            let mid = (black_box(bid) + black_box(ask)) / dec!(2);
            black_box(mid)
        });
    });

    group.finish();
}

/// Benchmark price level matching.
fn bench_price_matching(c: &mut Criterion) {
    let mut group = c.benchmark_group("price_matching");

    let levels: Vec<(Decimal, Decimal)> = (0..1000)
        .map(|i| {
            (
                dec!(0.001) * Decimal::from(i),
                dec!(100.0),
            )
        })
        .collect();

    group.bench_function("binary_search", |b| {
        let target = dec!(0.500);
        b.iter(|| {
            let result = levels.binary_search_by(|probe| probe.0.cmp(&target));
            black_box(result)
        });
    });

    group.bench_function("linear_search", |b| {
        let target = dec!(0.500);
        b.iter(|| {
            let result = levels.iter().position(|&(p, _)| p == target);
            black_box(result)
        });
    });

    group.finish();
}

/// Benchmark VWAP calculation.
fn bench_vwap_calculation(c: &mut Criterion) {
    let mut group = c.benchmark_group("vwap_calculation");

    for size in [10, 50, 100, 500].iter() {
        let levels: Vec<(Decimal, Decimal)> = (0..*size)
            .map(|i| {
                (
                    dec!(0.50) + Decimal::from(i) * dec!(0.001),
                    dec!(100.0) + Decimal::from(i % 5) * dec!(50.0),
                )
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("vwap", size),
            &levels,
            |b, levels| {
                let target_qty = dec!(1000.0);
                b.iter(|| {
                    let mut remaining = target_qty;
                    let mut total_cost = Decimal::ZERO;
                    let mut total_qty = Decimal::ZERO;

                    for (price, qty) in levels.iter() {
                        if remaining <= Decimal::ZERO {
                            break;
                        }
                        let fill_qty = remaining.min(*qty);
                        total_cost += price * fill_qty;
                        total_qty += fill_qty;
                        remaining -= fill_qty;
                    }

                    let vwap = if total_qty > Decimal::ZERO {
                        total_cost / total_qty
                    } else {
                        Decimal::ZERO
                    };
                    black_box(vwap)
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_orderbook_updates,
    bench_spread_calculation,
    bench_price_matching,
    bench_vwap_calculation,
);

criterion_main!(benches);
