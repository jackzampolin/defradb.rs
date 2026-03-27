use async_trait::async_trait;
use std::collections::BTreeMap;

use crate::corekv::{Error, IterOptions, Iterator, KvPair, Result};

/// Iterator over in-memory key-value pairs.
pub(crate) struct MemoryIterator {
    /// Sorted vector of key-value pairs
    data: Vec<(Vec<u8>, Vec<u8>)>,

    /// Current position in the iterator
    position: usize,

    /// Whether the iterator is closed
    closed: bool,

    /// Whether this is a keys-only iterator
    keys_only: bool,

    /// Whether this iterator is in reverse mode
    reverse: bool,
}

impl MemoryIterator {
    pub(crate) fn new(data: BTreeMap<Vec<u8>, Vec<u8>>, opts: IterOptions) -> Result<Self> {
        // Apply filters and convert to Vec
        let mut filtered: Vec<_> = data
            .into_iter()
            .filter(|(k, _)| {
                // Apply prefix filter
                if let Some(prefix) = opts.prefix() {
                    if !k.starts_with(prefix) {
                        return false;
                    }
                }

                // Apply start filter
                if let Some(start) = opts.start() {
                    if k.as_slice() < start {
                        return false;
                    }
                }

                // Apply end filter
                if let Some(end) = opts.end() {
                    if k.as_slice() >= end {
                        return false;
                    }
                }

                true
            })
            .collect();

        // Apply reverse ordering
        let reverse = opts.reverse();
        if reverse {
            filtered.reverse();
        }

        Ok(Self {
            data: filtered,
            position: 0,
            closed: false,
            keys_only: opts.keys_only(),
            reverse,
        })
    }
}

impl crate::corekv::private::Sealed for MemoryIterator {}

#[async_trait]
impl Iterator for MemoryIterator {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        if self.position >= self.data.len() {
            return Ok(None);
        }

        let (key, value) = &self.data[self.position];
        self.position += 1;

        if self.keys_only {
            Ok(Some(KvPair::key_only(key.clone())))
        } else {
            Ok(Some(KvPair::new(key.clone(), value.clone())))
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

        // Find the position to seek to based on iteration direction.
        // For forward iteration: find first key >= seek_key
        // For reverse iteration: find first key <= seek_key
        //
        // The data is stored in iteration order (reversed for reverse mode),
        // so in reverse mode we're looking for k <= key in descending order.
        let pos = if self.reverse {
            // Reverse mode: data is [k4, k3, k2, k1] (descending)
            // seek(k2) should find the first key <= k2, which is k2 at position 2
            self.data.iter().position(|(k, _)| k.as_slice() <= key)
        } else {
            // Forward mode: data is [k1, k2, k3, k4] (ascending)
            // seek(k2) should find the first key >= k2, which is k2 at position 1
            self.data.iter().position(|(k, _)| k.as_slice() >= key)
        };

        match pos {
            Some(p) => {
                self.position = p;
                Ok(true)
            }
            None => {
                // No matching key found, position at end
                self.position = self.data.len();
                Ok(false)
            }
        }
    }

    async fn reset(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        self.position = 0;
        Ok(())
    }

    fn is_valid(&self) -> bool {
        !self.closed
    }
}
