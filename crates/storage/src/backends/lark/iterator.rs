use async_trait::async_trait;
use std::cmp::Ordering;
use std::ops::Bound;

use crate::corekv::{Error, IterOptions, Iterator, KvPair, Result};

/// Merging iterator that combines a streaming Lark snapshot cursor and pending changes.
pub(crate) struct MergingIterator {
    snapshot_iter: lark_kv::OwnedSnapshotIter,
    snapshot_item: Option<(Vec<u8>, Vec<u8>)>,
    pending_items: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pending_pos: usize,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    prefix: Option<Vec<u8>>,
    reverse: bool,
    keys_only: bool,
    closed: bool,
}

impl MergingIterator {
    pub(crate) fn new(
        snapshot_iter: lark_kv::OwnedSnapshotIter,
        mut pending_items: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        opts: IterOptions,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> Result<Self> {
        let reverse = opts.reverse();
        if reverse {
            pending_items.reverse();
        }

        let mut iter = Self {
            snapshot_iter,
            snapshot_item: None,
            pending_items,
            pending_pos: 0,
            start_bound,
            end_bound,
            prefix: opts.prefix().map(Vec::from),
            reverse,
            keys_only: opts.keys_only(),
            closed: false,
        };
        iter.reset_positions()?;
        Ok(iter)
    }

    fn next_merged(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            let snap_key = self.snapshot_item.as_ref().map(|(k, _)| k);
            let pend_key = self.pending_items.get(self.pending_pos).map(|(k, _)| k);

            match (snap_key, pend_key) {
                (None, None) => return Ok(None),

                (Some(_), None) => {
                    let item = self.snapshot_item.take();
                    self.advance_snapshot()?;
                    return Ok(item);
                }

                (None, Some(_)) => {
                    let (key, value_opt) = self.pending_items[self.pending_pos].clone();
                    self.pending_pos += 1;
                    match value_opt {
                        Some(value) => return Ok(Some((key, value))),
                        None => continue,
                    }
                }

                (Some(sk), Some(pk)) => {
                    let cmp = if self.reverse { pk.cmp(sk) } else { sk.cmp(pk) };

                    match cmp {
                        Ordering::Less => {
                            let item = self.snapshot_item.take();
                            self.advance_snapshot()?;
                            return Ok(item);
                        }
                        Ordering::Greater => {
                            let (key, value_opt) = self.pending_items[self.pending_pos].clone();
                            self.pending_pos += 1;
                            match value_opt {
                                Some(value) => return Ok(Some((key, value))),
                                None => continue,
                            }
                        }
                        Ordering::Equal => {
                            let (key, value_opt) = self.pending_items[self.pending_pos].clone();
                            self.pending_pos += 1;
                            self.snapshot_item = None;
                            self.advance_snapshot()?;
                            match value_opt {
                                Some(value) => return Ok(Some((key, value))),
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

    fn reset_positions(&mut self) -> Result<()> {
        self.pending_pos = 0;
        if self.reverse {
            match &self.end_bound {
                Bound::Included(end) | Bound::Excluded(end) => {
                    self.snapshot_iter.seek_for_prev(end)
                }
                Bound::Unbounded => self.snapshot_iter.seek_to_last(),
            }
        } else {
            match &self.start_bound {
                Bound::Included(start) | Bound::Excluded(start) => self.snapshot_iter.seek(start),
                Bound::Unbounded => self.snapshot_iter.seek_to_first(),
            }
        }
        self.refresh_snapshot_item()
    }

    fn seek_positions(&mut self, key: &[u8]) -> Result<()> {
        let snapshot_target = self.snapshot_seek_target(key);
        if self.reverse {
            self.snapshot_iter.seek_for_prev(&snapshot_target);
        } else {
            self.snapshot_iter.seek(&snapshot_target);
        }
        self.refresh_snapshot_item()?;
        self.pending_pos = Self::binary_search_position(&self.pending_items, key, self.reverse);
        Ok(())
    }

    fn snapshot_seek_target(&self, key: &[u8]) -> Vec<u8> {
        if self.reverse {
            match &self.end_bound {
                Bound::Included(end) | Bound::Excluded(end) if key >= end.as_slice() => end.clone(),
                _ => key.to_vec(),
            }
        } else {
            match &self.start_bound {
                Bound::Included(start) | Bound::Excluded(start) if key < start.as_slice() => {
                    start.clone()
                }
                _ => key.to_vec(),
            }
        }
    }

    fn advance_snapshot(&mut self) -> Result<()> {
        if self.reverse {
            self.snapshot_iter.prev();
        } else {
            self.snapshot_iter.next();
        }
        self.refresh_snapshot_item()
    }

    fn refresh_snapshot_item(&mut self) -> Result<()> {
        self.snapshot_item = None;
        loop {
            self.snapshot_iter
                .status()
                .map_err(|e| Error::Backend(format!("lark iterator error: {}", e)))?;
            if !self.snapshot_iter.valid() {
                return Ok(());
            }

            let key = self
                .snapshot_iter
                .key()
                .ok_or_else(|| Error::Backend("lark iterator returned no key".into()))?
                .to_vec();

            if self.in_bounds(&key) {
                let value = if self.keys_only {
                    Vec::new()
                } else {
                    self.snapshot_iter
                        .value()
                        .ok_or_else(|| Error::Backend("lark iterator returned no value".into()))?
                        .to_vec()
                };
                self.snapshot_item = Some((key, value));
                return Ok(());
            }

            if self.reverse {
                if self.below_start(&key) {
                    return Ok(());
                }
                self.snapshot_iter.prev();
            } else {
                if self.at_or_after_end(&key) {
                    return Ok(());
                }
                self.snapshot_iter.next();
            }
        }
    }

    fn in_bounds(&self, key: &[u8]) -> bool {
        if self
            .prefix
            .as_deref()
            .is_some_and(|prefix| !key.starts_with(prefix))
        {
            return false;
        }
        !self.below_start(key) && !self.at_or_after_end(key)
    }

    fn below_start(&self, key: &[u8]) -> bool {
        match &self.start_bound {
            Bound::Included(start) => key < start.as_slice(),
            Bound::Excluded(start) => key <= start.as_slice(),
            Bound::Unbounded => false,
        }
    }

    fn at_or_after_end(&self, key: &[u8]) -> bool {
        match &self.end_bound {
            Bound::Included(end) => key > end.as_slice(),
            Bound::Excluded(end) => key >= end.as_slice(),
            Bound::Unbounded => false,
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

        match self.next_merged()? {
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

        self.seek_positions(key)?;
        let has_data = self.next_merged()?.is_some();
        self.seek_positions(key)?;

        Ok(has_data)
    }

    async fn reset(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        self.reset_positions()
    }

    fn is_valid(&self) -> bool {
        !self.closed
    }
}
