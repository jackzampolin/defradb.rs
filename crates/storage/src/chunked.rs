//! Bounded-window snapshot reader shared by the materializing backends.

use defra_core::thread_bounds::{MaybeBoxFuture, MaybeSend};

use crate::corekv::Result;

/// Default number of key-value pairs read per chunk.
///
/// Bounds peak memory per scan without making refills frequent enough to
/// matter. Revisit against measurements rather than treating as tuned.
pub(crate) const DEFAULT_CHUNK_SIZE: usize = 256;

/// Refill closure, type-erased so `ChunkedSnapshot` itself stays non-generic.
///
/// `Send` is required directly (rather than via the `MaybeSend` marker trait)
/// because trait objects only accept auto traits as additional bounds. The
/// whole module is `not(wasm32)`, so no wasm variant is needed.
type ReadFn = Box<
    dyn FnMut(Option<Vec<u8>>) -> MaybeBoxFuture<'static, Result<Vec<(Vec<u8>, Vec<u8>)>>> + Send,
>;

/// Reads a snapshot range in bounded windows.
///
/// Holds no borrow of the underlying store: each window is read and returned
/// by the supplied closure within one call, so nothing self-referential is
/// stored between calls. When the window empties, the next one is requested
/// starting strictly after the last key already yielded.
pub(crate) struct ChunkedSnapshot {
    /// Current window of pairs, awaiting consumption.
    window: Vec<(Vec<u8>, Vec<u8>)>,
    /// Position of the next pair to yield within `window`.
    pos: usize,
    /// Key of the last pair yielded, used to resume a refill after it.
    last_key: Option<Vec<u8>>,
    /// Maximum pairs requested per refill; validated against `read`'s output.
    chunk_size: usize,
    /// Set once a refill has returned no pairs at all.
    exhausted: bool,
    /// Refill closure: called with `None`, then with the last key yielded.
    ///
    /// `None` when the whole range is already in `window` (see
    /// `from_window`): there is nothing left to read from.
    read: Option<ReadFn>,
}

impl ChunkedSnapshot {
    /// `read` is called with `None` for the first window, then with the last
    /// key yielded. It must return at most `chunk_size` pairs, ascending,
    /// strictly after the supplied key.
    pub(crate) fn new<F, Fut>(chunk_size: usize, mut read: F) -> Self
    where
        F: FnMut(Option<Vec<u8>>) -> Fut + MaybeSend + 'static,
        Fut: std::future::Future<Output = Result<Vec<(Vec<u8>, Vec<u8>)>>> + MaybeSend + 'static,
    {
        Self {
            window: Vec::new(),
            pos: 0,
            last_key: None,
            chunk_size,
            exhausted: false,
            read: Some(Box::new(move |after| Box::pin(read(after)))),
        }
    }

    /// Wrap a window that already holds the entire range.
    ///
    /// The eager paths (reverse scans) materialize their whole result before
    /// building a reader. Taking ownership of that Vec keeps exactly one copy
    /// alive, and lets `reset` rewind in place instead of re-reading.
    pub(crate) fn from_window(window: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self {
            chunk_size: window.len().max(1),
            window,
            pos: 0,
            last_key: None,
            exhausted: true,
            read: None,
        }
    }

    /// Refill the window if consumed and the range is not known to be exhausted.
    async fn refill(&mut self) -> Result<()> {
        if self.pos < self.window.len() || self.exhausted {
            return Ok(());
        }

        let Some(read) = self.read.as_mut() else {
            return Ok(());
        };
        let next_window = read(self.last_key.clone()).await?;
        debug_assert!(
            next_window.len() <= self.chunk_size,
            "read closure returned more pairs than chunk_size"
        );
        // Only an empty refill proves the range is exhausted: a short refill
        // could still be followed by more matching keys behind a filter.
        self.exhausted = next_window.is_empty();
        self.window = next_window;
        self.pos = 0;
        Ok(())
    }

    /// Return the next pair, refilling the window from the underlying store as needed.
    pub(crate) async fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        self.refill().await?;
        match self.window.get(self.pos) {
            Some(pair) => {
                let pair = pair.clone();
                self.last_key = Some(pair.0.clone());
                self.pos += 1;
                Ok(Some(pair))
            }
            None => Ok(None),
        }
    }

    /// Return the next pair without consuming it, refilling the window as needed.
    ///
    /// Lets a caller merge this reader against another sorted source (as
    /// `MergingIterator` does) without losing an item to a discarded peek.
    pub(crate) async fn peek(&mut self) -> Result<Option<&(Vec<u8>, Vec<u8>)>> {
        self.refill().await?;
        Ok(self.window.get(self.pos))
    }

    /// Discard buffered state so the next `next`/`peek` call re-reads from the
    /// start of the range via the same closure.
    ///
    /// The closure itself is untouched (still holds its table/range handle),
    /// so this is cheap: it does not reopen anything until actually polled.
    ///
    /// A `from_window` reader has no closure to re-read through, and its
    /// window is already the whole range, so it rewinds in place.
    pub(crate) fn reset(&mut self) {
        self.pos = 0;
        self.last_key = None;
        if self.read.is_none() {
            return;
        }
        self.window.clear();
        self.exhausted = false;
    }

    /// If `target` lies within the currently loaded window, reposition to it
    /// in place — no I/O, no discarded window — and return `true`.
    ///
    /// Returns `false` (leaving all state untouched) when `target` falls
    /// outside the window, ahead or behind; the caller must fall back to
    /// `reset()` plus walking forward in that case.
    pub(crate) fn seek_within_window(&mut self, target: &[u8], reverse: bool) -> bool {
        let (Some((front, _)), Some((back, _))) = (self.window.first(), self.window.last()) else {
            return false;
        };
        let in_window = if reverse {
            target <= front.as_slice() && target >= back.as_slice()
        } else {
            target >= front.as_slice() && target <= back.as_slice()
        };
        if !in_window {
            return false;
        }

        self.pos = if reverse {
            self.window.partition_point(|(k, _)| k.as_slice() > target)
        } else {
            self.window.partition_point(|(k, _)| k.as_slice() < target)
        };
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn pair(n: u8) -> (Vec<u8>, Vec<u8>) {
        (vec![n], vec![n, n])
    }

    /// Refills only when the window empties, and only as often as needed.
    #[tokio::test]
    async fn yields_all_pairs_across_chunk_boundaries() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let all: Vec<_> = (0u8..10).map(pair).collect();

        let mut s = ChunkedSnapshot::new(4, move |after: Option<Vec<u8>>| {
            c.fetch_add(1, Ordering::SeqCst);
            let all = all.clone();
            async move {
                let start = match after {
                    None => 0,
                    Some(k) => all.iter().position(|(pk, _)| *pk > k).unwrap_or(all.len()),
                };
                Ok(all[start..(start + 4).min(all.len())].to_vec())
            }
        });

        let mut seen = Vec::new();
        while let Some(p) = s.next().await.unwrap() {
            seen.push(p);
        }

        assert_eq!(seen.len(), 10);
        assert_eq!(seen, (0u8..10).map(pair).collect::<Vec<_>>());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "2 full chunks, 1 short, 1 empty read to terminate"
        );
    }

    /// A consumer that stops early must not trigger further reads.
    #[tokio::test]
    async fn stops_reading_when_consumer_stops() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let all: Vec<_> = (0u8..100).map(pair).collect();

        let mut s = ChunkedSnapshot::new(4, move |_after| {
            c.fetch_add(1, Ordering::SeqCst);
            let all = all.clone();
            async move { Ok(all[..4].to_vec()) }
        });

        for _ in 0..3 {
            assert!(s.next().await.unwrap().is_some());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1, "one chunk covers 3 pulls");
    }

    /// Exact multiples must terminate, not loop forever on an empty refill.
    #[tokio::test]
    async fn exact_multiple_of_chunk_size_terminates() {
        let all: Vec<_> = (0u8..8).map(pair).collect();
        let mut s = ChunkedSnapshot::new(4, move |after: Option<Vec<u8>>| {
            let all = all.clone();
            async move {
                let start = match after {
                    None => 0,
                    Some(k) => all.iter().position(|(pk, _)| *pk > k).unwrap_or(all.len()),
                };
                Ok(all[start..(start + 4).min(all.len())].to_vec())
            }
        });

        let mut n = 0;
        while s.next().await.unwrap().is_some() {
            n += 1;
            assert!(n <= 8, "did not terminate");
        }
        assert_eq!(n, 8);
    }

    #[tokio::test]
    async fn empty_range_yields_none() {
        let mut s = ChunkedSnapshot::new(4, |_| async { Ok(Vec::new()) });
        assert!(s.next().await.unwrap().is_none());
    }

    /// Seeking to a key inside the loaded window, forward or backward, must
    /// reposition without a single extra call to `read`.
    #[tokio::test]
    async fn seek_within_window_repositions_without_reading() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let all: Vec<_> = (0u8..10).map(pair).collect();
        let mut s = ChunkedSnapshot::new(10, move |after: Option<Vec<u8>>| {
            c.fetch_add(1, Ordering::SeqCst);
            let all = all.clone();
            async move {
                let start = match after {
                    None => 0,
                    Some(k) => all.iter().position(|(pk, _)| *pk > k).unwrap_or(all.len()),
                };
                Ok(all[start..].to_vec())
            }
        });

        // Load the (only) window.
        assert_eq!(s.next().await.unwrap(), Some(pair(0)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Forward seek within the loaded window: no extra read.
        assert!(s.seek_within_window(&[5], false));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "in-window seek must not read"
        );
        assert_eq!(s.next().await.unwrap(), Some(pair(5)));

        // Backward seek within the loaded window: no extra read either.
        assert!(s.seek_within_window(&[1], false));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(s.next().await.unwrap(), Some(pair(1)));

        // Target outside the window: fast path declines and leaves position untouched.
        assert!(!s.seek_within_window(&[200], false));
        assert_eq!(
            s.next().await.unwrap(),
            Some(pair(2)),
            "declined seek must not move pos"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// The fast path must respect descending order for reverse scans, since
    /// the reverse (eager) path wraps its already-reversed Vec the same way.
    #[tokio::test]
    async fn seek_within_window_handles_reverse_order() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let all: Vec<_> = (0u8..10).rev().map(pair).collect();
        let mut s = ChunkedSnapshot::new(10, move |after: Option<Vec<u8>>| {
            c.fetch_add(1, Ordering::SeqCst);
            let batch = if after.is_none() {
                all.clone()
            } else {
                Vec::new()
            };
            async move { Ok(batch) }
        });

        assert_eq!(s.next().await.unwrap(), Some(pair(9)), "largest key first");
        assert!(s.seek_within_window(&[3], true));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "in-window seek must not read"
        );
        assert_eq!(s.next().await.unwrap(), Some(pair(3)));
    }

    /// A materialized window must survive `reset` — the reverse paths seek
    /// and reset through it, and it has no closure to re-read from.
    #[tokio::test]
    async fn from_window_rewinds_in_place() {
        let all: Vec<_> = (0u8..5).map(pair).collect();
        let mut s = ChunkedSnapshot::from_window(all.clone());

        let mut seen = Vec::new();
        while let Some(p) = s.next().await.unwrap() {
            seen.push(p);
        }
        assert_eq!(seen, all);

        s.reset();
        assert_eq!(s.next().await.unwrap(), Some(pair(0)), "reset must rewind");

        // And the in-window seek fast path still applies to it.
        assert!(s.seek_within_window(&[3], false));
        assert_eq!(s.next().await.unwrap(), Some(pair(3)));
    }

    #[tokio::test]
    async fn seek_within_window_on_empty_window_declines() {
        let mut s = ChunkedSnapshot::new(4, |_| async { Ok(Vec::new()) });
        assert!(!s.seek_within_window(&[1], false));
    }
}
