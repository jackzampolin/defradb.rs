//! Public-DocID tie-break for equal index keys (#1602).

use std::collections::HashSet;

use query::planner::index_selection::IndexScanType;

/// Append one InScan value's short IDs as a group, skipping IDs already seen.
pub(crate) fn extend_equal_key_group(
    all: &mut Vec<u64>,
    group_lens: &mut Vec<usize>,
    seen: &mut HashSet<u64>,
    short_ids: impl IntoIterator<Item = u64>,
) {
    let start = all.len();
    for id in short_ids {
        if seen.insert(id) {
            all.push(id);
        }
    }
    group_lens.push(all.len() - start);
}

/// Sort one equal-key group by public DocID.
pub(crate) fn sort_equal_key_group(doc_ids: &mut [String]) {
    doc_ids.sort();
}

/// Sort consecutive equal-key groups in `doc_ids`.
///
/// `group_lens[i]` is the length of group i. Lengths that run past the
/// slice are clamped.
pub(crate) fn sort_equal_key_groups(doc_ids: &mut [String], group_lens: &[usize]) {
    let mut start: usize = 0;
    for &len in group_lens {
        let end = start.saturating_add(len).min(doc_ids.len());
        if start < end {
            sort_equal_key_group(&mut doc_ids[start..end]);
        }
        start = end;
    }
}

fn apply_offset_limit(doc_ids: &mut Vec<String>, offset: u64, limit: Option<u64>) {
    let start = (offset as usize).min(doc_ids.len());
    if start == 0 && limit.is_none() {
        return;
    }
    let end = match limit {
        Some(lim) => start.saturating_add(lim as usize).min(doc_ids.len()),
        None => doc_ids.len(),
    };
    *doc_ids = doc_ids[start..end].to_vec();
}

/// Sort ExactMatch results by public DocID, then apply offset/limit.
/// Sort each InScan value's group the same way, keeping `_in` list order
/// between groups.
///
/// Index keys suffix node-local short IDs, so equal field values scan in
/// persist order and diverge across replicas. Public DocIDs are
/// content-addressed and identical everywhere, the same identity unique-index
/// merge already uses.
pub(crate) fn apply_equal_key_doc_id_tie_break(
    scan_type: &IndexScanType,
    doc_ids: &mut Vec<String>,
    offset: u64,
    limit: Option<u64>,
    group_lens: &[usize],
) {
    match scan_type {
        IndexScanType::ExactMatch { .. } => {
            sort_equal_key_group(doc_ids);
            apply_offset_limit(doc_ids, offset, limit);
        }
        IndexScanType::InScan { .. } => {
            sort_equal_key_groups(doc_ids, group_lens);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use query::planner::index_selection::IndexScanType;
    use storage::index::Bound;

    fn exact() -> IndexScanType {
        IndexScanType::ExactMatch { values: vec![] }
    }

    fn in_scan() -> IndexScanType {
        IndexScanType::InScan {
            values: vec![],
            suffix_values: vec![],
        }
    }

    fn range() -> IndexScanType {
        IndexScanType::RangeScan {
            prefix_values: vec![],
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
            reverse: false,
        }
    }

    #[test]
    fn exact_match_sorts_by_doc_id() {
        let mut ids = vec!["b".into(), "a".into()];
        apply_equal_key_doc_id_tie_break(&exact(), &mut ids, 0, None, &[]);
        assert_eq!(ids, ["a", "b"]);
    }

    #[test]
    fn exact_match_limit_one_picks_min_doc_id() {
        let mut ids = vec!["b".into(), "a".into(), "c".into()];
        apply_equal_key_doc_id_tie_break(&exact(), &mut ids, 0, Some(1), &[]);
        assert_eq!(ids, ["a"]);
    }

    #[test]
    fn exact_match_offset_past_end_is_empty() {
        let mut ids = vec!["b".into(), "a".into()];
        apply_equal_key_doc_id_tie_break(&exact(), &mut ids, 5, None, &[]);
        assert!(ids.is_empty());
    }

    #[test]
    fn exact_match_empty_is_empty() {
        let mut ids: Vec<String> = vec![];
        apply_equal_key_doc_id_tie_break(&exact(), &mut ids, 0, Some(1), &[]);
        assert!(ids.is_empty());
    }

    #[test]
    fn non_exact_non_in_is_noop() {
        let mut ids = vec!["b".into(), "a".into()];
        apply_equal_key_doc_id_tie_break(&range(), &mut ids, 0, Some(1), &[]);
        assert_eq!(ids, ["b", "a"]);
    }

    #[test]
    fn in_scan_sorts_each_group_and_keeps_group_order() {
        let mut ids = vec!["b".into(), "a".into(), "d".into(), "c".into()];
        apply_equal_key_doc_id_tie_break(&in_scan(), &mut ids, 0, None, &[2, 2]);
        assert_eq!(ids, ["a", "b", "c", "d"]);
    }
}
