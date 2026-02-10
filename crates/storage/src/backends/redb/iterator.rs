use async_trait::async_trait;

use crate::corekv::{Error, IterOptions, Iterator, KvPair, Result};

/// Merging iterator that combines pre-materialized snapshot and pending changes.
///
/// Snapshot and pending items matching the query are materialized into Vecs at
/// iterator creation. The merge itself happens on-demand during iteration via
/// `next_merged()`. For large result sets, memory usage scales with the number
/// of matching keys in the queried range.
pub(crate) struct MergingIterator {
    /// Items from snapshot (sorted ascending)
    snapshot_items: Vec<(Vec<u8>, Vec<u8>)>,
    /// Current position in snapshot
    snapshot_pos: usize,

    /// Pending changes (sorted ascending, None = deletion)
    pending_items: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    /// Current position in pending
    pending_pos: usize,

    /// Whether iteration is reversed
    reverse: bool,
    /// Whether to return only keys
    keys_only: bool,
    /// Whether the iterator is closed
    closed: bool,
}

impl MergingIterator {
    pub(crate) fn new(
        mut snapshot_items: Vec<(Vec<u8>, Vec<u8>)>,
        mut pending_items: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        opts: IterOptions,
    ) -> Self {
        let reverse = opts.reverse();
        if reverse {
            snapshot_items.reverse();
            pending_items.reverse();
        }

        Self {
            snapshot_items,
            snapshot_pos: 0,
            pending_items,
            pending_pos: 0,
            reverse,
            keys_only: opts.keys_only(),
            closed: false,
        }
    }

    /// Get the next merged key-value pair, handling overrides and deletions.
    fn next_merged(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        loop {
            let snap_key = self.snapshot_items.get(self.snapshot_pos).map(|(k, _)| k);
            let pend_key = self.pending_items.get(self.pending_pos).map(|(k, _)| k);

            match (snap_key, pend_key) {
                (None, None) => return None,

                (Some(_), None) => {
                    let (key, value) = self.snapshot_items[self.snapshot_pos].clone();
                    self.snapshot_pos += 1;
                    return Some((key, value));
                }

                (None, Some(_)) => {
                    let (key, value_opt) = self.pending_items[self.pending_pos].clone();
                    self.pending_pos += 1;
                    match value_opt {
                        Some(value) => return Some((key, value)),
                        None => continue, // Deletion of non-existent key, skip
                    }
                }

                (Some(sk), Some(pk)) => {
                    let cmp = if self.reverse {
                        pk.cmp(sk) // Reversed: larger keys come first
                    } else {
                        sk.cmp(pk)
                    };

                    match cmp {
                        std::cmp::Ordering::Less => {
                            // Snapshot key comes first (no pending override)
                            let (key, value) = self.snapshot_items[self.snapshot_pos].clone();
                            self.snapshot_pos += 1;
                            return Some((key, value));
                        }
                        std::cmp::Ordering::Greater => {
                            // Pending key comes first (new key not in snapshot)
                            let (key, value_opt) = self.pending_items[self.pending_pos].clone();
                            self.pending_pos += 1;
                            match value_opt {
                                Some(value) => return Some((key, value)),
                                None => continue, // Deletion of non-existent key
                            }
                        }
                        std::cmp::Ordering::Equal => {
                            // Same key: pending overrides snapshot
                            let (key, value_opt) = self.pending_items[self.pending_pos].clone();
                            self.snapshot_pos += 1; // Skip snapshot version
                            self.pending_pos += 1;
                            match value_opt {
                                Some(value) => return Some((key, value)),
                                None => continue, // Deletion
                            }
                        }
                    }
                }
            }
        }
    }

    /// Binary search for seek position in a sorted Vec.
    fn binary_search_position<T>(items: &[(Vec<u8>, T)], key: &[u8], reverse: bool) -> usize {
        if reverse {
            // Reversed: items are [k4, k3, k2, k1], find first <= key
            items.partition_point(|(k, _)| k.as_slice() > key)
        } else {
            // Forward: items are [k1, k2, k3, k4], find first >= key
            items.partition_point(|(k, _)| k.as_slice() < key)
        }
    }
}

#[async_trait]
impl Iterator for MergingIterator {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        match self.next_merged() {
            Some((key, value)) => {
                if self.keys_only {
                    Ok(Some(KvPair::key_only(key)))
                } else {
                    Ok(Some(KvPair::new(key, value)))
                }
            }
            None => Ok(None),
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }

    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        // Seek both iterators to the target position
        self.snapshot_pos = Self::binary_search_position(&self.snapshot_items, key, self.reverse);
        self.pending_pos = Self::binary_search_position(&self.pending_items, key, self.reverse);

        // Check if there's actual visible data at or after the seek position.
        // This accounts for pending deletions that might mask snapshot data.
        // We do this by peeking at next_merged() without advancing the iterator.
        let saved_snapshot_pos = self.snapshot_pos;
        let saved_pending_pos = self.pending_pos;
        let has_data = self.next_merged().is_some();
        self.snapshot_pos = saved_snapshot_pos;
        self.pending_pos = saved_pending_pos;

        Ok(has_data)
    }

    async fn reset(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        self.snapshot_pos = 0;
        self.pending_pos = 0;
        Ok(())
    }

    fn is_valid(&self) -> bool {
        !self.closed
    }
}
