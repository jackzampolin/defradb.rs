pub mod config;
mod errors;
mod iterator;
mod store;
mod transaction;

#[cfg(test)]
mod tests;

use std::ops::Bound;

use crate::corekv::IterOptions;

pub use config::FjallStoreOptions;
pub use store::FjallStore;

/// Compute the start and end bounds for a range query from IterOptions.
fn compute_range_bounds(opts: &IterOptions) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let start_bound = match (opts.prefix(), opts.start()) {
        (Some(prefix), Some(start)) => {
            if prefix > start {
                Bound::Included(prefix.to_vec())
            } else {
                Bound::Included(start.to_vec())
            }
        }
        (Some(prefix), None) => Bound::Included(prefix.to_vec()),
        (None, Some(start)) => Bound::Included(start.to_vec()),
        (None, None) => Bound::Unbounded,
    };

    let end_bound = match (opts.prefix(), opts.end()) {
        (Some(prefix), Some(end)) => {
            let prefix_end = prefix_to_end_bound(prefix);
            if let Some(pe) = prefix_end {
                if pe.as_slice() < end {
                    Bound::Excluded(pe)
                } else {
                    Bound::Excluded(end.to_vec())
                }
            } else {
                Bound::Excluded(end.to_vec())
            }
        }
        (Some(prefix), None) => match prefix_to_end_bound(prefix) {
            Some(end) => Bound::Excluded(end),
            None => Bound::Unbounded,
        },
        (None, Some(end)) => Bound::Excluded(end.to_vec()),
        (None, None) => Bound::Unbounded,
    };

    (start_bound, end_bound)
}

/// Compute the exclusive end bound for a prefix.
///
/// Given a prefix like "foo", returns "fop" (the first key that doesn't match the prefix).
/// Returns None if the prefix is empty or all 0xFF bytes.
fn prefix_to_end_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    if prefix.is_empty() {
        return None;
    }

    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xFF {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}
