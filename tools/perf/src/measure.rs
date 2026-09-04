//! Sampling and process accounting shared by the benchmark suite.
//!
//! Everything here is deliberately exact rather than sampled. A peak read from
//! the kernel is the peak; a peak polled from a background thread is the
//! largest value a poll happened to catch, and reporting that as "peak" is a
//! silent cap.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use crate::emit::Row;

/// Peak resident set size of this process since it started, in bytes.
///
/// `ru_maxrss` is kibibytes on Linux and bytes on the Apple platforms, which
/// is a real and often-missed ABI difference. Returns `None` where the call
/// is unavailable, so a caller reports a gap rather than a zero.
pub fn peak_rss_bytes() -> Option<u64> {
    #[cfg(unix)]
    {
        // SAFETY: `getrusage` writes into the caller-provided struct and reads
        // nothing else; a zeroed `rusage` is a valid destination.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
            return None;
        }
        let raw = usage.ru_maxrss as u64;
        #[cfg(target_vendor = "apple")]
        return Some(raw);
        #[cfg(not(target_vendor = "apple"))]
        return Some(raw * 1024);
    }
    #[cfg(not(unix))]
    None
}

pub fn median(samples: &mut [f64]) -> f64 {
    assert!(!samples.is_empty(), "median of an empty sample");
    samples.sort_by(f64::total_cmp);
    let mid = samples.len() / 2;
    if samples.len().is_multiple_of(2) {
        (samples[mid - 1] + samples[mid]) / 2.0
    } else {
        samples[mid]
    }
}

pub fn min_max(samples: &[f64]) -> (f64, f64) {
    assert!(!samples.is_empty(), "min_max of an empty sample");
    samples
        .iter()
        .fold((samples[0], samples[0]), |(lo, hi), &x| {
            (lo.min(x), hi.max(x))
        })
}

/// Run `f` `reps` times and fold the samples into one row carrying its range.
///
/// The range is what lets a comparison refuse to call an overlap a change, so
/// a repeated measurement should always report through here.
pub fn repeat<F: FnMut() -> f64>(name: impl Into<String>, reps: usize, mut f: F) -> Row {
    assert!(reps > 0, "a measurement needs at least one repetition");
    let mut samples: Vec<f64> = (0..reps).map(|_| f()).collect();
    let (lo, hi) = min_max(&samples);
    Row::new(name, median(&mut samples)).range(lo, hi)
}

/// Operations per second from a closure that performs `ops` operations.
pub fn ops_per_s<F: FnMut()>(ops: u64, mut f: F) -> f64 {
    let start = Instant::now();
    f();
    let secs = start.elapsed().as_secs_f64();
    if secs <= 0.0 {
        return f64::NAN;
    }
    ops as f64 / secs
}

/// A global allocator that counts allocations, for the per-operation
/// allocation budgets.
///
/// One `#[global_allocator]` exists per binary, so this is installed by the
/// bench target that measures allocations and by nothing else.
pub struct CountingAllocator;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK_LIVE: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every method forwards to the system allocator with the same layout
// it was given, so the allocation contract is the system allocator's. The
// counters are plain atomics and touch no allocator state.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
        PEAK_LIVE.fetch_max(live, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        if new_size > layout.size() {
            BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            let live = LIVE.fetch_add(new_size - layout.size(), Ordering::Relaxed) + new_size
                - layout.size();
            PEAK_LIVE.fetch_max(live, Ordering::Relaxed);
        } else {
            LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// What the counters have seen since [`reset`].
#[derive(Clone, Copy, Debug, Default)]
pub struct Allocs {
    pub count: u64,
    pub bytes: u64,
    pub peak_live_bytes: usize,
}

pub fn reset() {
    ALLOCS.store(0, Ordering::SeqCst);
    BYTES.store(0, Ordering::SeqCst);
    PEAK_LIVE.store(LIVE.load(Ordering::SeqCst), Ordering::SeqCst);
}

pub fn taken() -> Allocs {
    Allocs {
        count: ALLOCS.load(Ordering::SeqCst),
        bytes: BYTES.load(Ordering::SeqCst),
        peak_live_bytes: PEAK_LIVE
            .load(Ordering::SeqCst)
            .saturating_sub(LIVE.load(Ordering::SeqCst)),
    }
}

/// Allocations attributable to one operation, averaged over `ops` of them.
pub fn per_op<F: FnMut()>(ops: u64, mut f: F) -> f64 {
    assert!(
        ops > 0,
        "allocations per operation needs at least one operation"
    );
    reset();
    f();
    taken().count as f64 / ops as f64
}
