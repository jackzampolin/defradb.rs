//! Helper functions for migration placeholder creation and document materialization.

use datastore::NamespaceView;
use document::Document;
use lens::{LensDoc, DOC_ID_FIELD};
use schema::{CollectionVersion, FieldKind, ORPHAN_COLLECTION_ID};

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::index_manager::IndexManager;

/// Create an orphan placeholder collection version.
///
/// Used when a migration references a version that doesn't exist yet.
pub(super) fn create_orphan_placeholder(
    version_id: &str,
    name: &str,
    collection_id: &str,
) -> CollectionVersion {
    let mut placeholder = CollectionVersion {
        version_id: version_id.to_string(),
        collection_id: if collection_id.is_empty() {
            ORPHAN_COLLECTION_ID.to_string()
        } else {
            collection_id.to_string()
        },
        name: name.to_string(),
        is_materialized: true,
        is_placeholder: true,
        ..CollectionVersion::new("", "", "", Vec::new())
    };
    placeholder.is_active = false;
    placeholder
}

/// Create a placeholder with source collection info.
pub(super) fn create_placeholder_with_source(
    version_id: &str,
    source_name: &str,
    source_collection_id: &str,
) -> CollectionVersion {
    let mut placeholder = CollectionVersion {
        name: source_name.to_string(),
        version_id: version_id.to_string(),
        collection_id: source_collection_id.to_string(),
        is_materialized: true,
        is_placeholder: true,
        ..CollectionVersion::new("", "", "", Vec::new())
    };
    placeholder.is_active = false;
    placeholder
}

/// Convert a JSON value to a native NormalValue based on the field's schema type.
///
/// When documents are migrated through lens transforms, they come back as JSON values.
/// This function converts them to the appropriate native type (Int, Float, Time, etc.)
/// based on the field's declared type in the schema.
///
/// Scalar and scalar-array coercion delegates to the shared document converters, producing the
/// same native representation as mutation writes. Reindexed entries are therefore byte-identical
/// to freshly-written ones (notably DateTime → `Time`, and arrays → typed array variants rather
/// than a JSON blob). Values that cannot be coerced fall back to raw JSON, preserving prior
/// best-effort behavior.
pub fn json_to_native_value(
    value: &serde_json::Value,
    field_name: &str,
    schema: &CollectionVersion,
) -> document::NormalValue {
    if value.is_null() {
        return document::NormalValue::Null;
    }

    let field_kind = schema
        .fields
        .iter()
        .find(|f| f.name == field_name)
        .map(|f| &f.kind);

    if let Some(field_kind) = field_kind {
        match field_kind {
            FieldKind::Scalar(scalar) => {
                if let Some(nv) = document::encoding::json_to_normal_value_for_kind(value, scalar) {
                    return nv;
                }
            }
            FieldKind::ScalarArray(array) => {
                if let Some(nv) =
                    document::encoding::json_to_normal_value_for_array_kind(value, array)
                {
                    return nv;
                }
            }
            _ => {}
        }
    }

    document::NormalValue::Json(value.clone())
}

/// Convert a transformed lens document to the active collection's storage representation.
///
/// Lens output is JSON-shaped, while document storage and indexes use schema-aware native
/// values. Unknown output fields are ignored, matching Go's lensed fetcher.
pub fn lens_doc_to_document(
    mut lens_doc: LensDoc,
    original_doc: &Document,
    collection: &Collection,
) -> Document {
    let mut doc = Document::new();

    if let Some(id) = original_doc.id() {
        doc.set_id(id.clone());
    }

    for field in &collection.schema().fields {
        if field.name == DOC_ID_FIELD {
            continue;
        }
        if let Some(value) = lens_doc.remove(&field.name) {
            doc.set(
                &field.name,
                json_to_native_value(&value, &field.name, collection.schema()),
            );
        } else if original_doc.get(&field.name).is_some() {
            // Go's updateDataStore treats a field removed by a lens as an
            // explicit nil assignment. Rust omits nils from CBOR storage, but
            // retaining Null in the returned in-memory document preserves the
            // same clear semantics for the query that performed the migration.
            doc.set(&field.name, document::NormalValue::Null);
        }
    }

    doc.set_schema_version_id(collection.version_id());
    doc
}

/// Persist a lensed document directly to the datastore without creating CRDT commits.
///
/// Rust stores document fields in one CBOR blob, so this is the current-layout equivalent of
/// Go's per-field `updateDataStore`: replace the blob and update the real version key in the same
/// transaction.
pub(crate) async fn cache_migrated_document(
    datastore: &NamespaceView,
    systemstore: &NamespaceView,
    collection: &Collection,
    doc: &Document,
) -> Result<bool> {
    let Some(doc_id) = doc.id() else {
        return Ok(false);
    };
    let Some(doc_short_id) = collection.resolve_doc_short_id(systemstore, doc_id).await? else {
        return Ok(false);
    };

    let data = doc.to_cbor()?;
    datastore
        .set(&collection.doc_key(doc_short_id), &data)
        .await
        .map_err(Error::Storage)?;
    collection.store_version(datastore, doc_short_id).await?;

    Ok(true)
}

/// Persist a lazily migrated document and update its secondary indexes in the
/// same transaction.
///
/// Unlike a user mutation, migration write-back deliberately creates no CRDT
/// blocks or commits. It still has to remove index entries derived from the old
/// stored blob and add entries derived from the migrated representation.
pub(crate) async fn cache_migrated_document_with_indexes(
    datastore: &NamespaceView,
    systemstore: &NamespaceView,
    collection: &Collection,
    doc: &Document,
) -> Result<bool> {
    let Some(doc_id) = doc.id() else {
        return Ok(false);
    };
    let Some(doc_short_id) = collection.resolve_doc_short_id(systemstore, doc_id).await? else {
        return Ok(false);
    };

    let key = collection.doc_key(doc_short_id);
    let Some(old_data) = datastore.get(&key).await.map_err(Error::Storage)? else {
        return Ok(false);
    };
    let mut old_doc = Document::from_cbor(&old_data)?;
    old_doc.set_id(doc_id.clone());

    datastore
        .set(&key, &doc.to_cbor()?)
        .await
        .map_err(Error::Storage)?;
    collection.store_version(datastore, doc_short_id).await?;

    // Deleted documents have already been removed from secondary indexes.
    // Materializing their retained blob must not add those entries back.
    if !collection.is_deleted(datastore, doc_short_id).await? {
        let index_manager = IndexManager::from_indexes(
            collection.resolved_root_id(),
            collection.schema(),
            collection.write_indexes(),
        )?;
        index_manager
            .on_document_update(datastore, &old_doc, doc, doc_short_id, collection.schema())
            .await?;
    }

    Ok(true)
}

/// Advance only a document's stored schema version.
pub(crate) async fn cache_document_version(
    datastore: &NamespaceView,
    systemstore: &NamespaceView,
    collection: &Collection,
    doc: &Document,
) -> Result<bool> {
    let Some(doc_id) = doc.id() else {
        return Ok(false);
    };
    let Some(doc_short_id) = collection.resolve_doc_short_id(systemstore, doc_id).await? else {
        return Ok(false);
    };

    collection.store_version(datastore, doc_short_id).await?;
    Ok(true)
}
