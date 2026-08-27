//! Public-DocID tie-break for equal index keys (#1602).

use query::planner::index_selection::IndexScanType;

/// Sort ExactMatch results by public DocID, then apply offset/limit.
///
/// Index keys suffix node-local short IDs, so equal field values scan in
/// persist order and diverge across replicas. Public DocIDs are
/// content-addressed and identical everywhere, the same identity unique-index
/// merge already uses. Other scan types keep index-key order (field values
/// differ); their equal-key runs still follow short IDs.
pub(crate) fn apply_equal_key_doc_id_tie_break(
    scan_type: &IndexScanType,
    doc_ids: &mut Vec<String>,
    offset: u64,
    limit: Option<u64>,
) {
    if !matches!(scan_type, IndexScanType::ExactMatch { .. }) {
        return;
    }
    doc_ids.sort();
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
