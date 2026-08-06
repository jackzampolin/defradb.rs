use async_trait::async_trait;

use crate::chunked::ChunkedSnapshot;
use crate::corekv::{Error, IterOptions, Iterator, KvPair, Result};

/// Merging iterator that combines a bounded-window snapshot read with pending changes.
///
/// The snapshot side is read from the transaction's `BTreeMap` snapshot in
/// bounded windows via `ChunkedSnapshot`, so a query that stops early (e.g.
/// `LIMIT`) does not pay for cloning the whole snapshot. Pending changes are
/// bounded by the transaction's own writes, not by collection size, so they
/// stay materialized into a Vec at iterator creation. The merge itself
/// happens on-demand during iteration via `next_merged()`.
pub(crate) struct MergingIterator {
    /// Bounded-window reader over the snapshot (sorted ascending for forward
    /// scans, descending for reverse scans — see `MemoryTxn::iterator`).
    snapshot: ChunkedSnapshot,

    /// Pending changes (sorted ascending, None = deletion)
    pending_items: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    /// Current position in pending
    pending_pos: usize,

    /// A pair already resolved by `seek`, returned before merging resumes.
    peeked: Option<(Vec<u8>, Vec<u8>)>,

    /// Whether iteration is reversed
    reverse: bool,
    /// Whether to return only keys
    keys_only: bool,
    /// Whether the iterator is closed
    closed: bool,
}

impl MergingIterator {
    /// `snapshot` must already be in the iteration order implied by
    /// `opts.reverse()`; only `pending_items` is reversed here.
    pub(crate) fn new(
        snapshot: ChunkedSnapshot,
        mut pending_items: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        opts: IterOptions,
    ) -> Self {
        let reverse = opts.reverse();
        if reverse {
            pending_items.reverse();
        }

        Self {
            snapshot,
            pending_items,
            pending_pos: 0,
            peeked: None,
            reverse,
            keys_only: opts.keys_only(),
            closed: false,
        }
    }

    /// Get the next merged key-value pair, handling overrides and deletions.
    async fn next_merged(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            let snap_key = self.snapshot.peek().await?.map(|(k, _)| k.clone());
            let pend_key = self
                .pending_items
                .get(self.pending_pos)
                .map(|(k, _)| k.clone());

            match (snap_key, pend_key) {
                (None, None) => return Ok(None),

                (Some(_), None) => {
                    return self.snapshot.next().await;
                }

                (None, Some(_)) => {
                    let (key, value_opt) = self.pending_items[self.pending_pos].clone();
                    self.pending_pos += 1;
                    match value_opt {
                        Some(value) => return Ok(Some((key, value))),
                        None => continue, // Deletion of non-existent key, skip
                    }
                }

                (Some(sk), Some(pk)) => {
                    let cmp = if self.reverse {
                        pk.cmp(&sk) // Reversed: larger keys come first
                    } else {
                        sk.cmp(&pk)
                    };

                    match cmp {
                        std::cmp::Ordering::Less => {
                            // Snapshot key comes first (no pending override)
                            return self.snapshot.next().await;
                        }
                        std::cmp::Ordering::Greater => {
                            // Pending key comes first (new key not in snapshot)
                            let (key, value_opt) = self.pending_items[self.pending_pos].clone();
                            self.pending_pos += 1;
                            match value_opt {
                                Some(value) => return Ok(Some((key, value))),
                                None => continue, // Deletion of non-existent key
                            }
                        }
                        std::cmp::Ordering::Equal => {
                            // Same key: pending overrides snapshot
                            let (key, value_opt) = self.pending_items[self.pending_pos].clone();
                            self.snapshot.next().await?; // Skip snapshot version
                            self.pending_pos += 1;
                            match value_opt {
                                Some(value) => return Ok(Some((key, value))),
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

impl crate::corekv::private::Sealed for MergingIterator {}

#[async_trait]
impl Iterator for MergingIterator {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        let next = match self.peeked.take() {
            Some(pair) => Some(pair),
            None => self.next_merged().await?,
        };

        match next {
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

    /// Reposition so the next `next()` yields the first pair at or after
    /// `key` (at or before it, for reverse scans).
    ///
    /// A target still within the currently loaded window is a free,
    /// in-memory reposition (see `ChunkedSnapshot::seek_within_window`). A
    /// target outside it — including any seek backward past pairs already
    /// discarded — re-reads from the start of the range and walks forward
    /// discarding until `key` is reached. That re-read is the price paid for
    /// the snapshot side never holding more than `DEFAULT_CHUNK_SIZE` pairs
    /// in memory at once; it is not free, and repeated far-seeks pay it
    /// every time.
    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        if !self.snapshot.seek_within_window(key, self.reverse) {
            // The snapshot side only reads forward, so seeking (even
            // backwards) restarts it and discards pairs on the wrong side of
            // `key`.
            self.snapshot.reset();
            while let Some((k, _)) = self.snapshot.peek().await? {
                let before_target = if self.reverse {
                    k.as_slice() > key
                } else {
                    k.as_slice() < key
                };
                if before_target {
                    self.snapshot.next().await?;
                } else {
                    break;
                }
            }
        }

        self.pending_pos = Self::binary_search_position(&self.pending_items, key, self.reverse);

        // Resolve and buffer the seek result: the merge check below consumes
        // whichever side wins, so it must not be lost.
        self.peeked = self.next_merged().await?;
        Ok(self.peeked.is_some())
    }

    async fn reset(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        self.snapshot.reset();
        self.pending_pos = 0;
        self.peeked = None;
        Ok(())
    }

    fn is_valid(&self) -> bool {
        !self.closed
    }
}
