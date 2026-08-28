//! Iteration over a regolith transaction or snapshot.
//!
//! Nothing here ever holds the range. A read-only scan is a cursor that
//! holds one entry, and a scan over a writing transaction holds one
//! bounded page, the same shape as regolith's own `scan_page`. Values
//! arrive as [`regolith::DbSlice`] and are handed on as [`Bytes`] without
//! a copy.
//!
//! # Why a writing transaction is paged
//!
//! regolith merges a transaction's buffered writes over the snapshot
//! beneath it, and the stream that does so borrows the transaction. A
//! `Box<dyn Iterator>` is `'static`, so that stream cannot be kept
//! between calls. It is rebuilt instead, resuming from the last key
//! returned, draining a page each time. The transaction is held through
//! an `Arc`, so the borrow lives and dies inside one call.
//!
//! Resuming is sound because both sides are anchored: the snapshot side
//! is pinned at the transaction's read sequence and the buffered side is
//! the transaction's own writes, so a rebuild cannot pick up a write that
//! committed in between.

use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use super::handle::Handle;
use crate::corekv::{Error, IterOptions, Iterator, KvPair, Result};

/// Bytes a page may hold before it stops filling.
///
/// Bounding by bytes, not entries: an entry count says nothing about
/// memory when values vary from a few bytes to megabytes, and this runs
/// on devices where the difference is the whole budget.
const PAGE_BYTES: usize = 256 * 1024;

/// A ceiling on entries per page as well, so a range of tiny values does
/// not turn one page into a very long list.
const PAGE_ENTRIES: usize = 1024;

/// The source being drained.
enum Source {
    /// Already owned and `'static`, so it streams end to end with no
    /// rebuild, holding only the current entry.
    Forward(regolith::ScanStream),
    Reverse(regolith::Entries<regolith::OwnedSnapshotIter>),
    /// Rebuilt per page from the transaction the `Arc` keeps alive.
    Paged {
        /// Where the next page starts. `None` once the range ran out.
        resume: Option<Option<Vec<u8>>>,
        page: VecDeque<KvPair>,
    },
}

/// A streaming scan over one key range.
pub(crate) struct RegolithIterator {
    source: Source,
    handle: Arc<Handle>,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    /// Only keys under this prefix are returned. The range already
    /// excludes the rest, except for a prefix with no expressible
    /// successor, which is a prefix of all `0xff` bytes.
    prefix: Option<Vec<u8>>,
    reverse: bool,
    keys_only: bool,
    closed: bool,
    /// An entry `seek` pulled off to answer whether one exists, handed
    /// back by the next `next` rather than skipped.
    peeked: Option<KvPair>,
}

impl RegolithIterator {
    pub(crate) fn open(handle: Arc<Handle>, opts: &IterOptions) -> Result<Self> {
        if opts.reverse() && handle.is_writable() {
            return Err(Error::Other(
                "reverse iteration over a transaction with pending writes is not supported: \
                 open a read-only transaction to scan backwards"
                    .to_string(),
            ));
        }
        let (start, end) = range_bounds(opts);
        let mut iter = Self {
            source: Source::Paged {
                resume: None,
                page: VecDeque::new(),
            },
            handle,
            start: start.clone(),
            end,
            prefix: opts.prefix().map(<[u8]>::to_vec),
            reverse: opts.reverse(),
            keys_only: opts.keys_only(),
            closed: false,
            peeked: None,
        };
        iter.rebuild(start);
        Ok(iter)
    }

    /// Point the scan at `from` and start again from there.
    fn rebuild(&mut self, from: Option<Vec<u8>>) {
        let end = self.end.as_deref();
        self.source = match self.handle.as_ref() {
            Handle::ReadOnly(snapshot) if self.reverse => {
                let mut cursor = snapshot.owned_iter();
                match end {
                    // `end` is exclusive, so a backward walk starts below it.
                    Some(end) => {
                        cursor.seek_for_prev(end);
                        if cursor.valid() && cursor.key() == Some(end) {
                            cursor.prev();
                        }
                    }
                    None => cursor.seek_to_last(),
                }
                Source::Reverse(cursor.entries_rev())
            }
            Handle::ReadOnly(snapshot) => {
                Source::Forward(snapshot.scan_stream(from.as_deref(), end))
            }
            Handle::Writable(_) => Source::Paged {
                resume: Some(from),
                page: VecDeque::new(),
            },
        };
    }

    /// Fill the page buffer from the transaction, resuming where the last
    /// page stopped.
    fn refill(&mut self) {
        let (from, mut filled, mut last) = match &mut self.source {
            Source::Paged { resume, .. } => match resume.take() {
                Some(from) => (from, VecDeque::new(), None),
                None => return,
            },
            _ => return,
        };
        let Handle::Writable(txn) = self.handle.as_ref() else {
            return;
        };
        let mut page_bytes = 0usize;

        // The borrow of the transaction opens and closes inside this
        // call, so the iterator outlives no borrow.
        for (key, value) in txn.scan_stream(from.as_deref(), self.end.as_deref()) {
            // The resume key was already returned by the previous page.
            if Some(&key) == from.as_ref() {
                continue;
            }
            last = Some(key.clone());
            page_bytes += key.len() + value.len();
            let value = if self.keys_only {
                Bytes::new()
            } else {
                // `DbSlice` holds a refcount on bytes the database already
                // has, so adopting it copies nothing.
                Bytes::from_owner(value)
            };
            filled.push_back(KvPair { key, value });
            if page_bytes >= PAGE_BYTES || filled.len() >= PAGE_ENTRIES {
                break;
            }
        }

        // A page that stopped on a bound has more behind it; one that ran
        // out did not.
        let full = page_bytes >= PAGE_BYTES || filled.len() >= PAGE_ENTRIES;
        if let Source::Paged { resume, page } = &mut self.source {
            *page = filled;
            // A short page means the range ran out, so there is nothing
            // left to resume from.
            *resume = if full { last.map(Some) } else { None };
        }
    }

    fn step(&mut self) -> Option<KvPair> {
        let (key, value) = match &mut self.source {
            Source::Forward(stream) => stream.next()?,
            Source::Reverse(entries) => entries.next()?,
            Source::Paged { page, .. } => {
                if page.is_empty() {
                    self.refill();
                }
                return match &mut self.source {
                    Source::Paged { page, .. } => page.pop_front(),
                    _ => None,
                };
            }
        };
        let value = if self.keys_only {
            Bytes::new()
        } else {
            Bytes::from_owner(value)
        };
        Some(KvPair { key, value })
    }

    /// The next entry inside the prefix, or `None` at the end.
    fn next_in_range(&mut self) -> Option<KvPair> {
        while let Some(pair) = self.step() {
            // A reverse walk runs down to `start`, which is inclusive, so
            // it stops at the first key below it.
            if self.reverse {
                if let Some(start) = &self.start {
                    if pair.key.as_slice() < start.as_slice() {
                        return None;
                    }
                }
            }
            if let Some(prefix) = &self.prefix {
                if !pair.key.starts_with(prefix) {
                    continue;
                }
            }
            return Some(pair);
        }
        None
    }
}

/// Lower and upper bound for the scan, folding a prefix into the range so
/// the engine skips what it can instead of the iterator filtering it.
fn range_bounds(opts: &IterOptions) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let mut start = opts.start().map(<[u8]>::to_vec);
    let mut end = opts.end().map(<[u8]>::to_vec);
    if let Some(prefix) = opts.prefix() {
        if start.as_deref().is_none_or(|s| s < prefix) {
            start = Some(prefix.to_vec());
        }
        // The successor of a prefix is it with the last non-`0xff` byte
        // raised. An all-`0xff` prefix has none, and the scan then runs to
        // the end of the keyspace with the prefix check doing the work.
        if let Some(successor) = prefix_successor(prefix) {
            if end.as_deref().is_none_or(|e| e > successor.as_slice()) {
                end = Some(successor);
            }
        }
    }
    (start, end)
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    while let Some(last) = successor.pop() {
        if last != 0xff {
            successor.push(last + 1);
            return Some(successor);
        }
    }
    None
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Iterator for RegolithIterator {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        if self.closed {
            return Ok(None);
        }
        if let Some(peeked) = self.peeked.take() {
            return Ok(Some(peeked));
        }
        Ok(self.next_in_range())
    }

    async fn close(&mut self) -> Result<()> {
        self.closed = true;
        self.peeked = None;
        Ok(())
    }

    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        if self.closed {
            return Err(Error::Other("seek on a closed iterator".to_string()));
        }
        if self.reverse {
            return Err(Error::Other(
                "seek during reverse iteration is not supported".to_string(),
            ));
        }
        self.rebuild(Some(key.to_vec()));
        // Pull the entry the seek landed on so the caller learns whether
        // one exists; `next` hands back that same entry.
        self.peeked = self.next_in_range();
        Ok(self.peeked.is_some())
    }

    async fn reset(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::Other("reset on a closed iterator".to_string()));
        }
        self.peeked = None;
        self.rebuild(self.start.clone());
        Ok(())
    }

    fn is_valid(&self) -> bool {
        !self.closed
    }
}

impl crate::corekv::private::Sealed for RegolithIterator {}
