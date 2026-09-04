//! Event-bus publish fan-out.
//!
//! ```text
//! cargo bench -p benches --bench events
//! ```
//!
//! Every write publishes an update, and the publish walks every subscriber. So
//! the cost that matters is not one publish but one publish against a
//! subscriber count, and that curve is what decides whether a node with many
//! open subscriptions pays for them on the write path. Nothing measured it.
//!
//! Also measured: a publish nobody is listening to, which is the common case
//! on a server with no open subscriptions and should be close to free.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use events::{Bus, ChannelBus, EventName, Message};

const SUBSCRIBERS: [usize; 5] = [0, 1, 8, 64, 512];

fn publish_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("events_publish");
    for count in SUBSCRIBERS {
        let bus = ChannelBus::new();
        // Held for the whole measurement: a dropped subscription is a dead
        // subscriber the bus sweeps, which would measure the sweep instead of
        // the fan-out.
        let subscriptions: Vec<_> = (0..count)
            .map(|_| bus.subscribe(&[EventName::Merge]))
            .collect();
        assert_eq!(
            bus.subscriber_count(),
            count,
            "every subscriber must be live"
        );

        group.throughput(Throughput::Elements(count.max(1) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &bus, |b, bus| {
            b.iter(|| bus.publish(black_box(Message::merge())))
        });
        drop(subscriptions);
    }
    group.finish();
}

/// A subscriber that never drains fills its channel. What a publish costs once
/// that has happened is the difference between a slow consumer degrading
/// gracefully and one stalling every writer behind it.
fn publish_to_a_full_subscriber(c: &mut Criterion) {
    let bus = ChannelBus::new();
    let _subscription = bus.subscribe(&[EventName::Merge]);
    for _ in 0..100_000 {
        bus.publish(Message::merge());
    }
    let mut group = c.benchmark_group("events_publish_backlogged");
    group.bench_function("1_subscriber_not_draining", |b| {
        b.iter(|| bus.publish(black_box(Message::merge())))
    });
    group.finish();
}

criterion_group!(benches, publish_fanout, publish_to_a_full_subscriber);
criterion_main!(benches);
