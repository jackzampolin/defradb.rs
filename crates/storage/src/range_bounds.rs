use std::ops::Bound;

use crate::corekv::IterOptions;

/// Compute the start and end bounds for a range query from `IterOptions`.
///
/// Returns `None` when the bounds cannot contain any key. Callers must yield an
/// empty scan in that case: `BTreeMap::range` panics on an inverted range, and
/// every backend merges pending writes through one.
#[allow(clippy::type_complexity)]
pub(crate) fn compute_range_bounds(opts: &IterOptions) -> Option<(Bound<Vec<u8>>, Bound<Vec<u8>>)> {
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

    (!is_empty_range(&start_bound, &end_bound)).then_some((start_bound, end_bound))
}

/// Whether no key can fall between these bounds.
fn is_empty_range(start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    match (start, end) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(s), Bound::Included(e)) => s > e,
        (Bound::Included(s), Bound::Excluded(e))
        | (Bound::Excluded(s), Bound::Included(e))
        | (Bound::Excluded(s), Bound::Excluded(e)) => s >= e,
    }
}

/// Compute the exclusive end bound for a prefix.
///
/// Given a prefix like "foo", returns "fop" (the first key that doesn't match the prefix).
/// Returns None if the prefix is empty or all 0xFF bytes (meaning iteration should go to the end).
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_above_end_has_no_matching_range() {
        let opts = IterOptions::new()
            .with_start(b"z".to_vec())
            .with_end(b"a".to_vec());
        assert!(compute_range_bounds(&opts).is_none());
    }

    #[test]
    fn start_beyond_prefix_end_has_no_matching_range() {
        let opts = IterOptions::new()
            .with_prefix(b"foo".to_vec())
            .with_start(b"z".to_vec());
        assert!(compute_range_bounds(&opts).is_none());
    }

    #[test]
    fn equal_start_and_end_has_no_matching_range() {
        let opts = IterOptions::new()
            .with_start(b"k".to_vec())
            .with_end(b"k".to_vec());
        assert!(compute_range_bounds(&opts).is_none());
    }

    #[test]
    fn ordinary_range_is_returned_unchanged() {
        let opts = IterOptions::new()
            .with_start(b"a".to_vec())
            .with_end(b"z".to_vec());
        let (start, end) = compute_range_bounds(&opts).unwrap();
        assert_eq!(start, Bound::Included(b"a".to_vec()));
        assert_eq!(end, Bound::Excluded(b"z".to_vec()));
    }

    #[test]
    fn unbounded_scan_is_returned_unchanged() {
        let (start, end) = compute_range_bounds(&IterOptions::new()).unwrap();
        assert_eq!(start, Bound::Unbounded);
        assert_eq!(end, Bound::Unbounded);
    }
}
