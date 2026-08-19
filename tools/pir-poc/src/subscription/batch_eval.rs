//! Exact batch evaluators used by the live-subscription optimization spike.
//!
//! The Compact DPF construction remains the existing two-party fss-rs point
//! function.  These kernels only change server-local ordering, allocation, and
//! parallel scheduling.  They do not alter keys, outputs, or the privacy proof.

use std::{collections::HashMap, mem::size_of, time::Instant};

use anyhow::{bail, Context, Result};
use fss_rs::group::byte::ByteGroup;
use rayon::{prelude::*, ThreadPool, ThreadPoolBuilder};

use super::{
    encode_bucket, CompactServerKey, CompactSubscriptionServer, InnerKey, NotificationShare,
    SubscriptionId, OUTPUT_BYTES,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactBatchKernel {
    /// Compatibility baseline: call the existing HashMap-backed event API and
    /// reorder its allocated notification objects into canonical order.
    ExistingEventMajor,
    /// Pre-sort subscription references once, pre-encode the event batch, and
    /// write packed event-major output shares.
    PreprocessedEventMajor,
    /// Keep one key hot while walking every pre-encoded event.
    SubscriptionMajor,
    /// Work on bounded subscription/event tiles without changing output order.
    CacheBlocked {
        subscription_block: usize,
        event_block: usize,
    },
    /// Split event-major output rows over a persistent bounded Rayon pool.
    ParallelEventShards,
}

impl CompactBatchKernel {
    pub fn name(self) -> &'static str {
        match self {
            Self::ExistingEventMajor => "existing-hashmap-event-major",
            Self::PreprocessedEventMajor => "preprocessed-event-major",
            Self::SubscriptionMajor => "subscription-major",
            Self::CacheBlocked { .. } => "cache-blocked",
            Self::ParallelEventShards => "parallel-event-shards",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactBatchMetrics {
    pub subscriptions: usize,
    pub events: usize,
    pub point_evaluations: usize,
    pub dpf_tree_level_expansions: usize,
    pub encoded_event_inputs: usize,
    pub key_order_preprocessing_bytes: usize,
    pub output_share_bytes: usize,
    pub table_ordering_passes: usize,
    pub parallel_shards: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactBatchEvaluation {
    /// Stable canonical subscription order shared by both server replicas.
    pub subscription_ids: Vec<SubscriptionId>,
    /// Event-major packed 16-byte shares.  Entry `(event, subscription)` is at
    /// `event * subscription_ids.len() + subscription`.
    pub values: Vec<[u8; OUTPUT_BYTES]>,
    pub metrics: CompactBatchMetrics,
}

pub struct CompactBatchEvaluator<'a> {
    server: &'a CompactSubscriptionServer,
    ordered: Vec<(SubscriptionId, &'a CompactServerKey)>,
    pool: ThreadPool,
    preprocessing_elapsed: std::time::Duration,
}

impl<'a> CompactBatchEvaluator<'a> {
    pub fn new(server: &'a CompactSubscriptionServer, parallel_shards: usize) -> Result<Self> {
        if parallel_shards == 0 {
            bail!("Compact DPF batch evaluator needs at least one parallel shard");
        }
        let started = Instant::now();
        let mut ordered = server
            .subscriptions
            .iter()
            .map(|(id, key)| (*id, key))
            .collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|(id, _)| id.0);
        let pool = ThreadPoolBuilder::new()
            .num_threads(parallel_shards)
            .thread_name(|index| format!("pir-live-dpf-{index}"))
            .build()
            .context("build Compact DPF batch worker pool")?;
        let preprocessing_elapsed = started.elapsed();
        Ok(Self {
            server,
            ordered,
            pool,
            preprocessing_elapsed,
        })
    }

    pub fn preprocessing_elapsed(&self) -> std::time::Duration {
        self.preprocessing_elapsed
    }

    pub fn evaluate(
        &self,
        event_buckets: &[usize],
        kernel: CompactBatchKernel,
    ) -> Result<CompactBatchEvaluation> {
        if event_buckets.is_empty() {
            bail!("Compact DPF event batch must not be empty");
        }
        for &bucket in event_buckets {
            if bucket >= self.server.bucket_count {
                bail!("event bucket is outside the subscription domain");
            }
        }
        if let CompactBatchKernel::CacheBlocked {
            subscription_block,
            event_block,
        } = kernel
        {
            if subscription_block == 0 || event_block == 0 {
                bail!("Compact DPF cache blocks must be non-zero");
            }
        }

        let subscription_count = self.ordered.len();
        let output_count = subscription_count
            .checked_mul(event_buckets.len())
            .context("Compact DPF batch output count overflow")?;
        let encoded_events = event_buckets
            .iter()
            .map(|&bucket| encode_bucket(bucket, self.server.depth))
            .collect::<Vec<_>>();
        let values = match kernel {
            CompactBatchKernel::ExistingEventMajor => {
                self.evaluate_existing(event_buckets, output_count)?
            }
            CompactBatchKernel::PreprocessedEventMajor => {
                self.evaluate_preprocessed_event_major(&encoded_events, output_count)
            }
            CompactBatchKernel::SubscriptionMajor => {
                self.evaluate_subscription_major(&encoded_events, output_count)
            }
            CompactBatchKernel::CacheBlocked {
                subscription_block,
                event_block,
            } => self.evaluate_cache_blocked(
                &encoded_events,
                output_count,
                subscription_block,
                event_block,
            ),
            CompactBatchKernel::ParallelEventShards => {
                self.evaluate_parallel_events(&encoded_events, output_count)
            }
        };
        let point_evaluations = output_count;
        let dpf_tree_level_expansions = point_evaluations
            .checked_mul(self.server.depth)
            .context("Compact DPF level expansion count overflow")?;
        let output_share_bytes = output_count
            .checked_mul(OUTPUT_BYTES)
            .context("Compact DPF output byte count overflow")?;
        let table_ordering_passes = match kernel {
            CompactBatchKernel::ExistingEventMajor
            | CompactBatchKernel::PreprocessedEventMajor
            | CompactBatchKernel::ParallelEventShards => event_buckets.len(),
            CompactBatchKernel::SubscriptionMajor => subscription_count,
            CompactBatchKernel::CacheBlocked {
                subscription_block,
                event_block: _,
            } => subscription_count.div_ceil(subscription_block),
        };
        Ok(CompactBatchEvaluation {
            subscription_ids: self.ordered.iter().map(|(id, _)| *id).collect(),
            values,
            metrics: CompactBatchMetrics {
                subscriptions: subscription_count,
                events: event_buckets.len(),
                point_evaluations,
                dpf_tree_level_expansions,
                encoded_event_inputs: event_buckets.len(),
                key_order_preprocessing_bytes: subscription_count
                    * (size_of::<SubscriptionId>() + size_of::<&InnerKey>()),
                output_share_bytes,
                table_ordering_passes,
                parallel_shards: if kernel == CompactBatchKernel::ParallelEventShards {
                    self.pool.current_num_threads()
                } else {
                    1
                },
            },
        })
    }

    fn evaluate_existing(&self, events: &[usize], output_count: usize) -> Result<Vec<[u8; 16]>> {
        let positions = self
            .ordered
            .iter()
            .enumerate()
            .map(|(index, (id, _))| (*id, index))
            .collect::<HashMap<_, _>>();
        let mut output = vec![[0u8; OUTPUT_BYTES]; output_count];
        for (event_index, &bucket) in events.iter().enumerate() {
            for share in self.server.evaluate_event(bucket)? {
                let position = positions
                    .get(&share.subscription_id)
                    .copied()
                    .context("existing evaluator returned an unknown subscription")?;
                output[event_index * self.ordered.len() + position] = share.value;
            }
        }
        Ok(output)
    }

    fn evaluate_preprocessed_event_major(
        &self,
        encoded_events: &[[u8; 4]],
        output_count: usize,
    ) -> Vec<[u8; 16]> {
        let mut output = vec![[0u8; OUTPUT_BYTES]; output_count];
        for (event_index, event) in encoded_events.iter().enumerate() {
            let row = &mut output
                [event_index * self.ordered.len()..(event_index + 1) * self.ordered.len()];
            for ((_, key), value) in self.ordered.iter().zip(row) {
                *value = self.eval_one(key, event);
            }
        }
        output
    }

    fn evaluate_subscription_major(
        &self,
        encoded_events: &[[u8; 4]],
        output_count: usize,
    ) -> Vec<[u8; 16]> {
        let mut output = vec![[0u8; OUTPUT_BYTES]; output_count];
        let subscriptions = self.ordered.len();
        for (subscription_index, (_, key)) in self.ordered.iter().enumerate() {
            for (event_index, event) in encoded_events.iter().enumerate() {
                output[event_index * subscriptions + subscription_index] =
                    self.eval_one(key, event);
            }
        }
        output
    }

    fn evaluate_cache_blocked(
        &self,
        encoded_events: &[[u8; 4]],
        output_count: usize,
        subscription_block: usize,
        event_block: usize,
    ) -> Vec<[u8; 16]> {
        let mut output = vec![[0u8; OUTPUT_BYTES]; output_count];
        let subscriptions = self.ordered.len();
        for first_subscription in (0..subscriptions).step_by(subscription_block) {
            let subscription_end = (first_subscription + subscription_block).min(subscriptions);
            for first_event in (0..encoded_events.len()).step_by(event_block) {
                let event_end = (first_event + event_block).min(encoded_events.len());
                for (local_event, event) in
                    encoded_events[first_event..event_end].iter().enumerate()
                {
                    let event_index = first_event + local_event;
                    for subscription_index in first_subscription..subscription_end {
                        output[event_index * subscriptions + subscription_index] =
                            self.eval_one(self.ordered[subscription_index].1, event);
                    }
                }
            }
        }
        output
    }

    fn evaluate_parallel_events(
        &self,
        encoded_events: &[[u8; 4]],
        output_count: usize,
    ) -> Vec<[u8; 16]> {
        let subscriptions = self.ordered.len();
        let mut output = vec![[0u8; OUTPUT_BYTES]; output_count];
        self.pool.install(|| {
            output
                .par_chunks_mut(subscriptions)
                .zip(encoded_events.par_iter())
                .for_each(|(row, event)| {
                    for ((_, key), value) in self.ordered.iter().zip(row) {
                        *value = self.eval_one(key, event);
                    }
                });
        });
        output
    }

    #[inline]
    fn eval_one(&self, key: &CompactServerKey, event: &[u8; 4]) -> [u8; OUTPUT_BYTES] {
        let mut output = ByteGroup([0; OUTPUT_BYTES]);
        self.server
            .engine
            .eval_point(self.server.party, &key.inner, event, &mut output);
        output.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecoyBatchKernel {
    EventMajor,
    DeduplicatedEvents,
    ParallelEventShards,
}

impl DecoyBatchKernel {
    pub fn name(self) -> &'static str {
        match self {
            Self::EventMajor => "event-major-index-lookups",
            Self::DeduplicatedEvents => "deduplicated-event-lookups",
            Self::ParallelEventShards => "parallel-event-shards",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexedDecoyBatchServer {
    by_bucket: HashMap<usize, Vec<SubscriptionId>>,
    memberships: usize,
}

impl IndexedDecoyBatchServer {
    pub fn register(&mut self, id: SubscriptionId, candidates: &[usize]) -> Result<()> {
        if candidates.is_empty() {
            bail!("indexed decoy registration needs at least one candidate");
        }
        for &bucket in candidates {
            self.by_bucket.entry(bucket).or_default().push(id);
            self.memberships += 1;
        }
        Ok(())
    }

    pub fn memberships(&self) -> usize {
        self.memberships
    }

    pub fn distinct_buckets(&self) -> usize {
        self.by_bucket.len()
    }

    pub fn estimated_state_bytes(&self) -> usize {
        self.by_bucket.capacity() * (size_of::<usize>() + size_of::<Vec<SubscriptionId>>() + 1)
            + self
                .by_bucket
                .values()
                .map(|members| members.capacity() * size_of::<SubscriptionId>())
                .sum::<usize>()
    }

    pub fn evaluate(
        &self,
        events: &[usize],
        kernel: DecoyBatchKernel,
    ) -> Result<IndexedDecoyBatchEvaluation> {
        if events.is_empty() {
            bail!("indexed decoy event batch must not be empty");
        }
        let (notifications, lookups, parallel_shards) = match kernel {
            DecoyBatchKernel::EventMajor => (
                events
                    .iter()
                    .map(|event| self.by_bucket.get(event).cloned().unwrap_or_default())
                    .collect(),
                events.len(),
                1,
            ),
            DecoyBatchKernel::DeduplicatedEvents => {
                let mut cached = HashMap::<usize, Vec<SubscriptionId>>::new();
                for &event in events {
                    cached
                        .entry(event)
                        .or_insert_with(|| self.by_bucket.get(&event).cloned().unwrap_or_default());
                }
                let lookups = cached.len();
                let output: Vec<Vec<SubscriptionId>> = events
                    .iter()
                    .map(|event| cached.get(event).cloned().expect("cached every event"))
                    .collect();
                (output, lookups, 1)
            }
            DecoyBatchKernel::ParallelEventShards => {
                let pool = ThreadPoolBuilder::new()
                    .num_threads(4)
                    .thread_name(|index| format!("pir-live-decoy-{index}"))
                    .build()
                    .context("build bounded indexed-decoy batch pool")?;
                let output: Vec<Vec<SubscriptionId>> = pool.install(|| {
                    events
                        .par_iter()
                        .map(|event| self.by_bucket.get(event).cloned().unwrap_or_default())
                        .collect()
                });
                (output, events.len(), 4)
            }
        };
        let matching_notifications = notifications.iter().map(Vec::len).sum();
        Ok(IndexedDecoyBatchEvaluation {
            notifications,
            index_lookups: lookups,
            matching_notifications,
            parallel_shards,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedDecoyBatchEvaluation {
    pub notifications: Vec<Vec<SubscriptionId>>,
    pub index_lookups: usize,
    pub matching_notifications: usize,
    pub parallel_shards: usize,
}

pub fn shares_for_event_subscription(
    left: &CompactBatchEvaluation,
    right: &CompactBatchEvaluation,
    event_index: usize,
    subscription_index: usize,
) -> Result<[NotificationShare; 2]> {
    if left.subscription_ids != right.subscription_ids {
        bail!("Compact DPF replicas returned different subscription orderings");
    }
    let subscriptions = left.subscription_ids.len();
    if subscription_index >= subscriptions {
        bail!("subscription index is outside the batch");
    }
    let index = event_index
        .checked_mul(subscriptions)
        .and_then(|value| value.checked_add(subscription_index))
        .context("Compact DPF result index overflow")?;
    let left_value = *left.values.get(index).context("left result is too short")?;
    let right_value = *right
        .values
        .get(index)
        .context("right result is too short")?;
    Ok([
        NotificationShare {
            subscription_id: left.subscription_ids[subscription_index],
            party: false,
            value: left_value,
        },
        NotificationShare {
            subscription_id: right.subscription_ids[subscription_index],
            party: true,
            value: right_value,
        },
    ])
}
