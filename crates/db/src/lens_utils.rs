//! Shared lens and migration utilities.
//!
//! This module contains common functions used by both LensedDocFetcher
//! and LensedAutoCommitFetcher for document migration operations.

use std::collections::HashMap;

use document::Document;
use lens::{
    build_targeted_history, CollectionHistoryLink, LensDoc, TargetedHistoryLink, DOC_ID_FIELD,
};
use schema::CollectionVersion;
use tracing::{debug, info, warn};

/// Convert a Document to a LensDoc for lens transformation.
///
/// Uses Document's to_map which handles all field conversions properly.
pub fn doc_to_lens_doc(doc: &Document) -> Option<LensDoc> {
    let map = doc.to_map().ok()?;
    let mut lens_doc = LensDoc::new();
    for (key, value) in map {
        lens_doc.insert(key, value);
    }
    Some(lens_doc)
}

/// Convert a LensDoc back to a Document after transformation.
///
/// Preserves the original document's ID while replacing all other fields.
pub fn lens_doc_to_doc(lens_doc: LensDoc, original_doc: &Document) -> Document {
    let mut doc = Document::new();

    // Preserve original ID
    if let Some(id) = original_doc.id() {
        doc.set_id(id.clone());
    }

    // Copy fields from lens doc (skip _docID as it's set via set_id)
    for (field_name, value) in lens_doc {
        if field_name != DOC_ID_FIELD {
            doc.set(&field_name, value);
        }
    }

    doc
}

/// Check if a document needs migration to the target version.
///
/// Returns false if no migrations are registered or if the document
/// is already at the target version.
pub fn doc_needs_migration(doc: &Document, target_version_id: &str, has_migrations: bool) -> bool {
    if !has_migrations {
        return false;
    }

    let doc_version = doc.schema_version_id();
    let needs = doc.needs_migration(target_version_id);

    debug!(
        doc_id = ?doc.id(),
        doc_version = ?doc_version,
        target_version = %target_version_id,
        has_migrations = has_migrations,
        needs_migration = needs,
        "Checking if document needs migration"
    );

    needs
}

/// Check if any version in a list of collection versions has migrations registered.
///
/// A collection has migrations if any version in its history has a transform
/// configured in the previous_version field. This matches Go's behavior of
/// checking the full history, not just the current version.
pub fn versions_have_migrations(versions: &[CollectionVersion]) -> bool {
    for version in versions {
        if let Some(ref prev) = version.previous_version {
            if prev.transform.is_some() {
                return true;
            }
        }
    }
    false
}

/// Build collection history from a list of versions.
///
/// This takes all known versions and builds a directed graph showing
/// the migration path to the target version.
///
/// # Arguments
/// * `versions` - All versions of the collection loaded from systemstore
/// * `target_version_id` - The version to build the history toward
///
/// # Returns
/// A hashmap of version_id -> TargetedHistoryLink, or None if history cannot be built.
pub fn build_collection_history(
    versions: &[CollectionVersion],
    target_version_id: &str,
) -> Option<HashMap<String, TargetedHistoryLink>> {
    info!(
        version_count = versions.len(),
        target_version_id = %target_version_id,
        "Building collection history"
    );

    if versions.is_empty() {
        warn!("No versions provided, cannot build history");
        return None;
    }

    let mut full_history: HashMap<String, CollectionHistoryLink> = HashMap::new();

    // Add each version to the history
    for version in versions {
        let mut link = CollectionHistoryLink::new(&version.version_id, &version.collection_id);

        if let Some(ref prev) = version.previous_version {
            info!(
                version_id = %version.version_id,
                previous_source_collection_id = %prev.source_collection_id,
                transform = ?prev.transform,
                "Version has previous_version"
            );
            link = link.with_previous(&prev.source_collection_id);
            if let Some(ref transform_id) = prev.transform {
                link = link.with_transform(transform_id);
            }
        } else {
            debug!(
                version_id = %version.version_id,
                "Version has no previous_version (root version)"
            );
        }

        full_history.insert(version.version_id.clone(), link);
    }

    info!(
        full_history_size = full_history.len(),
        "Built initial history graph"
    );

    // Build `next` links by reverse-indexing `previous` links.
    // Each version's `previous` points to its parent; the parent's `next` should point back.
    let reverse_links: Vec<(String, String)> = full_history
        .values()
        .flat_map(|link| {
            link.previous
                .iter()
                .map(|prev_id| (prev_id.clone(), link.version_id.clone()))
                .collect::<Vec<_>>()
        })
        .collect();

    info!(
        reverse_links_count = reverse_links.len(),
        "Building reverse links"
    );

    for (parent_id, child_id) in &reverse_links {
        debug!(
            parent_id = %parent_id,
            child_id = %child_id,
            "Adding next link"
        );
        if let Some(parent_link) = full_history.get_mut(parent_id) {
            if !parent_link.next.contains(child_id) {
                parent_link.next.push(child_id.clone());
            }
        } else {
            warn!(
                parent_id = %parent_id,
                "Parent version not found in history when adding next link"
            );
        }
    }

    // Log the final full history before targeting
    for (vid, link) in &full_history {
        info!(
            version_id = %vid,
            transform = ?link.transform,
            previous = ?link.previous,
            next = ?link.next,
            "Full history link"
        );
    }

    let result = build_targeted_history(&full_history, target_version_id);

    if result.is_none() {
        warn!(
            target_version_id = %target_version_id,
            "build_targeted_history returned None"
        );
    } else {
        info!(
            target_version_id = %target_version_id,
            targeted_history_size = result.as_ref().map_or(0, |h| h.len()),
            "Successfully built targeted history"
        );
    }

    result
}
