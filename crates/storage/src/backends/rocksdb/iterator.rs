use async_trait::async_trait;

use crate::corekv::{Error, IterOptions, Iterator, KvPair, Result};

/// Merging iterator that combines pre-materialized snapshot and pending changes.
///
/// Same pattern as fjall and redb — snapshot and pending items are materialized
/// into Vecs at creation time, merged on-demand during iteration.
pub(crate) struct RocksDbMergingIterator {
    snapshot_items: Vec<(Vec<u8>, Vec<u8>)>,
    snapshot_pos: usize,
    pending_items: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pending_pos: usize,
    reverse: bool,
    keys_only: bool,
    closed: bool,
}

impl RocksDbMergingIterator {
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
                        None => continue,
                    }
                }

                (Some(sk), Some(pk)) => {
                    let cmp = if self.reverse { pk.cmp(sk) } else { sk.cmp(pk) };

                    match cmp {
                        std::cmp::Ordering::Less => {
                            let (key, value) = self.snapshot_items[self.snapshot_pos].clone();
                            self.snapshot_pos += 1;
                            return Some((key, value));
                        }
                        std::cmp::Ordering::Greater => {
                            let (key, value_opt) = self.pending_items[self.pending_pos].clone();
                            self.pending_pos += 1;
                            match value_opt {
                                Some(value) => return Some((key, value)),
                                None => continue,
                            }
                        }
                        std::cmp::Ordering::Equal => {
                            let (key, value_opt) = self.pending_items[self.pending_pos].clone();
                            self.snapshot_pos += 1;
                            self.pending_pos += 1;
                            match value_opt {
                                Some(value) => return Some((key, value)),
                                None => continue,
                            }
                        }
                    }
                }
            }
        }
    }

    fn binary_search_position<T>(items: &[(Vec<u8>, T)], key: &[u8], reverse: bool) -> usize {
        if reverse {
            items.partition_point(|(k, _)| k.as_slice() > key)
        } else {
            items.partition_point(|(k, _)| k.as_slice() < key)
        }
    }
}

impl crate::corekv::private::Sealed for RocksDbMergingIterator {}

#[async_trait]
impl Iterator for RocksDbMergingIterator {
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

        self.snapshot_pos = Self::binary_search_position(&self.snapshot_items, key, self.reverse);
        self.pending_pos = Self::binary_search_position(&self.pending_items, key, self.reverse);

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
