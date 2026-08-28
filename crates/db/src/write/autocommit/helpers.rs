use bytes::Bytes;
use super::*;

use crate::block::builder::BlockResult;
use crate::collection::Collection;
use crdt::traits::{Context, ValueReader};
use crdt::{Counter, CounterDelta, NumericKind};
use datastore::NamespaceView;
use defra_core::types::DocId as CrdtDocId;
use document::NormalValue;
use schema::{FieldKind, ScalarKind};

pub(crate) async fn write_branchable_collection_block(
    collection_name: &str,
    collection: &Collection,
    blockstore: &NamespaceView,
    headstore: &NamespaceView,
    doc_cid: Cid,
    signing_config: Option<&defra_core::signing::SigningConfig>,
) -> query::error::Result<Option<(Cid, Bytes)>> {
    if !collection.schema().is_branchable {
        return Ok(None);
    }

    write_collection_block(
        blockstore,
        headstore,
        collection.resolved_root_id(),
        collection.version_id(),
        doc_cid,
        signing_config,
    )
    .await
    .map(Some)
    .map_err(|error| {
        query::error::QueryError::execution(format!(
            "failed to write collection block for branchable mutation on collection {}: {}",
            collection_name, error
        ))
    })
}

/// Persist a local UPDATE: apply CRDT field deltas to the authoritative store
/// (the counter RMW, #1021) and then write the document + maintain indexes. These
/// are bundled so no mutator can write a document blob without first advancing the
/// CRDT accumulation store — the single-store invariant is enforced by
/// construction, not by each mutator remembering to call the counter helper.
pub(crate) async fn write_local_update(
    datastore: &NamespaceView,
    collection: &Collection,
    doc: &mut Document,
    doc_short_id: u64,
    index_manager: &IndexManager,
) -> query::error::Result<()> {
    apply_local_counter_deltas(datastore, collection, doc, doc_short_id).await?;
    collection
        .update_with_indexes(datastore, doc, doc_short_id, index_manager)
        .await
        .map_err(|e| match e {
            crate::error::Error::DocumentNotFound(id) => {
                query::error::QueryError::document_not_found(id)
            }
            other => crate::error::index_write_query_error("update", other),
        })
}

/// Persist a local CREATE: write the document + maintain indexes, then seed the
/// CRDT accumulation store for counter fields (#1021). Bundled for the same
/// by-construction reason as `write_local_update`.
pub(crate) async fn write_local_create(
    datastore: &NamespaceView,
    collection: &Collection,
    doc: &Document,
    doc_short_id: u64,
    index_manager: &IndexManager,
) -> query::error::Result<()> {
    collection
        .create_with_indexes(datastore, doc, doc_short_id, index_manager)
        .await
        .map_err(|e| crate::error::index_write_query_error("create", e))?;
    init_counter_stores_on_create(datastore, collection, doc).await
}

/// Persist a local UPDATE WITHOUT the counter read-modify-write (#1044
/// interactive path). The doc blob + indexes are written with the provisional
/// value; the counter RMW (and the blob correction) is deferred to the
/// commit-time finalize. Used only by the interactive `DbDocMutator`; the
/// auto-commit/batch paths keep `write_local_update` (RMW bundled in).
pub(crate) async fn write_local_update_deferred(
    datastore: &NamespaceView,
    collection: &Collection,
    doc: &Document,
    doc_short_id: u64,
    index_manager: &IndexManager,
) -> query::error::Result<()> {
    collection
        .update_with_indexes(datastore, doc, doc_short_id, index_manager)
        .await
        .map_err(|e| match e {
            crate::error::Error::DocumentNotFound(id) => {
                query::error::QueryError::document_not_found(id)
            }
            other => crate::error::index_write_query_error("update", other),
        })
}

/// Persist a local CREATE WITHOUT seeding the counter store (#1044 interactive
/// path). The blob is written; the counter store is seeded at the commit-time
/// finalize. Used only by the interactive `DbDocMutator`.
pub(crate) async fn write_local_create_deferred(
    datastore: &NamespaceView,
    collection: &Collection,
    doc: &Document,
    doc_short_id: u64,
    index_manager: &IndexManager,
) -> query::error::Result<()> {
    collection
        .create_with_indexes(datastore, doc, doc_short_id, index_manager)
        .await
        .map_err(|e| crate::error::index_write_query_error("create", e))?;
    Ok(())
}

/// Register the identity of a freshly created document (Go `save()` isAdd):
/// duplicate-check the derived DocID against the mapping, persist the
/// short-ID <-> DocID mapping, and record block ownership for the genesis
/// composite, field, and encryption blocks. Returns the parsed DocID.
pub(crate) async fn register_created_doc(
    systemstore: &NamespaceView,
    datastore: &NamespaceView,
    collection: &Collection,
    doc_short_id: u64,
    block_result: &BlockResult,
) -> query::error::Result<DocID> {
    let doc_id_str = &block_result.doc_id;

    let existing = crate::docid::map::get_doc_ref(systemstore, doc_id_str)
        .await
        .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
    if let Some(doc_ref) = existing {
        let is_deleted = collection
            .is_deleted(datastore, doc_ref.doc_short_id)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
        if is_deleted {
            return Err(query::error::QueryError::execution(format!(
                "Document with ID {} has been deleted",
                doc_id_str
            )));
        }
        return Err(query::error::QueryError::execution(format!(
            "Document with ID {} already exists",
            doc_id_str
        )));
    }

    crate::docid::map::set_doc_id_mapping(
        systemstore,
        collection.resolved_root_id(),
        doc_short_id,
        doc_id_str,
    )
    .await
    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

    register_block_doc_id_mappings(systemstore, block_result, doc_id_str).await?;

    DocID::from_string(doc_id_str)
        .map_err(|e| query::error::QueryError::execution(format!("invalid derived DocID: {}", e)))
}

/// Record block ownership (`/d/b/{cid}/{docID}`) for every block produced by
/// a mutation: the composite, each field block, and each encryption block.
pub(crate) async fn register_block_doc_id_mappings(
    systemstore: &NamespaceView,
    block_result: &BlockResult,
    doc_id: &str,
) -> query::error::Result<()> {
    let mut cids = Vec::with_capacity(1 + block_result.field_cids.len());
    cids.push(block_result.cid);
    cids.extend(block_result.field_cids.iter().copied());
    cids.extend(block_result.encryption_cids.iter().copied());
    for cid in cids {
        crate::docid::map::set_block_doc_id_mapping(systemstore, &cid.to_string(), doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
    }
    Ok(())
}

/// Apply a SINGLE recorded counter delta (or create-seed) to the authoritative
/// accumulation store at the commit-time finalize (#1044). For updates this is
/// the delta-driven equivalent of `apply_local_counter_deltas` for one field;
/// for creates it seeds the store like `init_counter_stores_on_create`. Returns
/// the post-operation store value so the caller can mirror it into the blob.
///
/// The per-doc guard protecting this RMW is held by the caller (the finalize
/// driver) and released only after the durable commit, preserving the #1021
/// invariant that a concurrent merge never observes a partial RMW.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_pending_counter_op(
    datastore: &NamespaceView,
    collection: &Collection,
    schema_version_id: &str,
    doc_id: &str,
    field: &str,
    base: Option<&NormalValue>,
    delta: &NormalValue,
    is_create: bool,
) -> query::error::Result<Option<NormalValue>> {
    let Some(field_desc) = collection.schema().fields.iter().find(|f| f.name == field) else {
        return Ok(None);
    };
    if !field_desc.crdt_type.is_counter() {
        return Ok(None);
    }
    let Some(kind) = numeric_kind_from_field_kind(&field_desc.kind) else {
        return Ok(None);
    };
    let allow_decrement = field_desc.crdt_type.allows_decrement();

    let doc_id_bytes = doc_id.as_bytes().to_vec();
    let counter = Counter::new(
        schema_version_id.to_string(),
        &doc_id_bytes,
        field.to_string(),
        allow_decrement,
        kind,
    )
    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

    let mut rw = datastore.clone();

    if is_create {
        // Seed the store directly to the created (absolute) value, init-if-absent.
        match (kind, delta) {
            (NumericKind::Int64, NormalValue::Int(v)) => counter
                .reconcile_int64(&mut rw, *v)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            (NumericKind::Float64, NormalValue::Float64(v)) => counter
                .reconcile_float64(&mut rw, *v)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            (NumericKind::Float32, NormalValue::Float32(v)) => counter
                .reconcile_float32(&mut rw, *v)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            (NumericKind::Float32, NormalValue::Float64(v)) => counter
                .reconcile_float32(&mut rw, *v as f32)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            (NumericKind::Float32, NormalValue::Int(v)) => counter
                .reconcile_float32(&mut rw, *v as f32)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            (NumericKind::Float64, NormalValue::Int(v)) => counter
                .reconcile_float64(&mut rw, *v as f64)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            _ => {}
        }
        // On create the blob already equals the seeded value — no correction.
        return Ok(None);
    }

    // UPDATE: init-if-absent from the PRE-WRITE committed value captured at record
    // time (`base`), then apply the recorded delta. We deliberately do NOT re-read
    // the committed doc here: the deferred update already overwrote the blob with
    // the query-plan's provisional (base+delta) value, so re-reading it as the
    // reconcile base would migrate a present PCounter store UPWARD to base+delta
    // and then add the delta again — double-counting (#1044). Using the captured
    // pre-update `base` exactly replicates the inline `apply_local_counter_deltas`.
    match (kind, base) {
        (NumericKind::Int64, Some(NormalValue::Int(v))) => counter
            .reconcile_int64(&mut rw, *v)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
        (NumericKind::Float64, Some(NormalValue::Float64(v))) => counter
            .reconcile_float64(&mut rw, *v)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
        (NumericKind::Float32, Some(NormalValue::Float32(v))) => counter
            .reconcile_float32(&mut rw, *v)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
        (NumericKind::Float32, Some(NormalValue::Float64(v))) => counter
            .reconcile_float32(&mut rw, *v as f32)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
        (NumericKind::Float32, Some(NormalValue::Int(v))) => counter
            .reconcile_float32(&mut rw, *v as f32)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
        (NumericKind::Float64, Some(NormalValue::Int(v))) => counter
            .reconcile_float64(&mut rw, *v as f64)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
        (NumericKind::Int64, None) => counter
            .reconcile_int64(&mut rw, 0)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
        (NumericKind::Float64, None) => counter
            .reconcile_float64(&mut rw, 0.0)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
        (NumericKind::Float32, None) => counter
            .reconcile_float32(&mut rw, 0.0)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
        _ => {}
    }

    let counter_delta =
        build_local_counter_delta(&doc_id_bytes, field, schema_version_id, kind, delta)?;
    let ctx = Context {
        doc_id: CrdtDocId::new(doc_id)
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
        schema_version: schema_version_id.to_string(),
        is_create: false,
    };
    crdt::traits::ReplicatedData::merge(&counter, &mut rw, &ctx, &counter_delta)
        .await
        .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

    let bytes = ValueReader::value(&counter, &rw)
        .await
        .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
    Ok(decode_counter_value(&bytes, kind))
}

/// Apply local counter-field increments to the CRDT accumulation store (the
/// single source of truth) via a fresh read-modify-write, then mirror the
/// resulting value back into the document blob.
///
/// Without this, a local counter update only advanced the materialized blob; the
/// accumulation store (`value_key`) was advanced solely by merges, and each merge
/// reset the store from the (possibly stale) blob — silently dropping increments
/// under concurrency (#1021). By RMWing the *delta* into the authoritative store
/// here (never writing the query-plan's pre-computed absolute value), local
/// writes and merges share one delta-based code path, matching Go DefraDB. The
/// approach is delta-based (no value-magnitude comparison) so it is correct for
/// PNCounter decrements too. Used by the update path; create seeding goes through
/// `init_counter_stores_on_create`.
async fn apply_local_counter_deltas(
    datastore: &NamespaceView,
    collection: &Collection,
    doc: &mut Document,
    doc_short_id: u64,
) -> query::error::Result<()> {
    let schema_version_id = collection.version_id().to_string();

    // Collect the counter fields that carry a local increment, with their kind.
    let mut counter_fields: Vec<(String, NumericKind, bool, NormalValue)> = Vec::new();
    for field in &collection.schema().fields {
        if !field.crdt_type.is_counter() {
            continue;
        }
        let Some(delta) = doc.get_counter_delta(&field.name) else {
            continue;
        };
        let Some(kind) = numeric_kind_from_field_kind(&field.kind) else {
            continue;
        };
        counter_fields.push((
            field.name.clone(),
            kind,
            field.crdt_type.allows_decrement(),
            delta.clone(),
        ));
    }

    if counter_fields.is_empty() {
        return Ok(());
    }

    let doc_id = doc
        .id()
        .cloned()
        .ok_or_else(|| query::error::QueryError::execution("counter update requires a doc ID"))?;
    let doc_id_str = doc_id.to_string();
    let doc_id_bytes = doc_id_str.as_bytes().to_vec();

    // Freshly read committed doc (inside this write txn) to seed the store the
    // first time it is touched (init-if-absent). A first update of a doc with no
    // committed value seeds 0.
    let committed = collection
        .get_with_datastore(datastore, doc_short_id, &doc_id)
        .await
        .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

    // The CRDT operates on a mutable `ReaderWriter`; `NamespaceView` is one, but
    // it is shared by `&` here. Take an owned clone to obtain a mutable handle —
    // the clone writes through to the same underlying transaction.
    let mut rw = datastore.clone();

    for (field_name, kind, allow_decrement, delta) in counter_fields {
        let counter = Counter::new(
            schema_version_id.clone(),
            &doc_id_bytes,
            field_name.clone(),
            allow_decrement,
            kind,
        )
        .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        // Init-if-absent: seed the accumulation store from the committed blob the
        // first time only. Never overwrites a present (authoritative) store.
        let committed_value = committed.as_ref().and_then(|d| d.get(&field_name));
        match (kind, committed_value) {
            (NumericKind::Int64, Some(NormalValue::Int(v))) => {
                counter
                    .reconcile_int64(&mut rw, *v)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Float64, Some(NormalValue::Float64(v))) => {
                counter
                    .reconcile_float64(&mut rw, *v)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Float32, Some(NormalValue::Float32(v))) => {
                counter
                    .reconcile_float32(&mut rw, *v)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Float32, Some(NormalValue::Float64(v))) => {
                counter
                    .reconcile_float32(&mut rw, *v as f32)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Float32, Some(NormalValue::Int(v))) => {
                counter
                    .reconcile_float32(&mut rw, *v as f32)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Float64, Some(NormalValue::Int(v))) => {
                counter
                    .reconcile_float64(&mut rw, *v as f64)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Int64, None) => {
                counter
                    .reconcile_int64(&mut rw, 0)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Float64, None) => {
                counter
                    .reconcile_float64(&mut rw, 0.0)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Float32, None) => {
                counter
                    .reconcile_float32(&mut rw, 0.0)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            _ => {}
        }

        let counter_delta = build_local_counter_delta(
            &doc_id_bytes,
            &field_name,
            &schema_version_id,
            kind,
            &delta,
        )?;

        let ctx = Context {
            doc_id: CrdtDocId::new(&doc_id_str)
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            schema_version: schema_version_id.clone(),
            is_create: false,
        };

        crdt::traits::ReplicatedData::merge(&counter, &mut rw, &ctx, &counter_delta)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        // Mirror the resulting authoritative store value into the blob, overriding
        // the absolute value the query-plan layer pre-computed from a possibly
        // stale read.
        let bytes = ValueReader::value(&counter, &rw)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
        if let Some(value) = decode_counter_value(&bytes, kind) {
            doc.set(field_name.clone(), value);
        }
    }

    Ok(())
}

/// Initialize the CRDT accumulation store for counter fields on document
/// creation so the store is authoritative from creation (matching the
/// single-store invariant). The created value is absolute (no delta recorded on
/// create), so the store is seeded directly to it via init-if-absent.
async fn init_counter_stores_on_create(
    datastore: &NamespaceView,
    collection: &Collection,
    doc: &Document,
) -> query::error::Result<()> {
    let schema_version_id = collection.version_id().to_string();

    let mut counter_fields: Vec<(String, NumericKind, bool, NormalValue)> = Vec::new();
    for field in &collection.schema().fields {
        if !field.crdt_type.is_counter() {
            continue;
        }
        let Some(value) = doc.get(&field.name) else {
            continue;
        };
        let Some(kind) = numeric_kind_from_field_kind(&field.kind) else {
            continue;
        };
        counter_fields.push((
            field.name.clone(),
            kind,
            field.crdt_type.allows_decrement(),
            value.clone(),
        ));
    }

    if counter_fields.is_empty() {
        return Ok(());
    }

    let doc_id = doc
        .id()
        .cloned()
        .ok_or_else(|| query::error::QueryError::execution("counter create requires a doc ID"))?;
    let doc_id_bytes = doc_id.to_string().into_bytes();

    let mut rw = datastore.clone();
    for (field_name, kind, allow_decrement, value) in counter_fields {
        let counter = Counter::new(
            schema_version_id.clone(),
            &doc_id_bytes,
            field_name.clone(),
            allow_decrement,
            kind,
        )
        .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        match (kind, &value) {
            (NumericKind::Int64, NormalValue::Int(v)) => counter
                .reconcile_int64(&mut rw, *v)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            (NumericKind::Float64, NormalValue::Float64(v)) => counter
                .reconcile_float64(&mut rw, *v)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            (NumericKind::Float32, NormalValue::Float32(v)) => counter
                .reconcile_float32(&mut rw, *v)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            (NumericKind::Float32, NormalValue::Float64(v)) => counter
                .reconcile_float32(&mut rw, *v as f32)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            (NumericKind::Float32, NormalValue::Int(v)) => counter
                .reconcile_float32(&mut rw, *v as f32)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            (NumericKind::Float64, NormalValue::Int(v)) => counter
                .reconcile_float64(&mut rw, *v as f64)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            _ => {}
        }
    }

    Ok(())
}

fn numeric_kind_from_field_kind(kind: &FieldKind) -> Option<NumericKind> {
    match kind.as_scalar().map(ScalarKind::base_kind) {
        Some(ScalarKind::Int) => Some(NumericKind::Int64),
        Some(ScalarKind::Float32) => Some(NumericKind::Float32),
        Some(ScalarKind::Float64) => Some(NumericKind::Float64),
        _ => None,
    }
}

fn build_local_counter_delta(
    doc_id_bytes: &[u8],
    field_name: &str,
    schema_version_id: &str,
    kind: NumericKind,
    delta: &NormalValue,
) -> query::error::Result<CounterDelta> {
    // priority/nonce are irrelevant to the accumulated value (counter merge is
    // unconditional commutative addition); use stable placeholders.
    match kind {
        NumericKind::Int64 => {
            let inc = match delta {
                NormalValue::Int(v) => *v,
                other => {
                    return Err(query::error::QueryError::execution(format!(
                        "counter Int64 field got non-int delta: {other:?}"
                    )))
                }
            };
            CounterDelta::new_int64(
                doc_id_bytes.to_vec(),
                field_name.to_string(),
                1,
                0,
                schema_version_id.to_string(),
                inc,
            )
            .map_err(|e| query::error::QueryError::execution(e.to_string()))
        }
        NumericKind::Float64 => {
            let inc = match delta {
                NormalValue::Float64(v) => *v,
                NormalValue::Float32(v) => *v as f64,
                NormalValue::Int(v) => *v as f64,
                other => {
                    return Err(query::error::QueryError::execution(format!(
                        "counter Float64 field got non-numeric delta: {other:?}"
                    )))
                }
            };
            CounterDelta::new_float64(
                doc_id_bytes.to_vec(),
                field_name.to_string(),
                1,
                0,
                schema_version_id.to_string(),
                inc,
            )
            .map_err(|e| query::error::QueryError::execution(e.to_string()))
        }
        NumericKind::Float32 => {
            let inc = match delta {
                NormalValue::Float32(v) => *v,
                NormalValue::Float64(v) => *v as f32,
                NormalValue::Int(v) => *v as f32,
                other => {
                    return Err(query::error::QueryError::execution(format!(
                        "counter Float32 field got non-numeric delta: {other:?}"
                    )))
                }
            };
            CounterDelta::new_float32(
                doc_id_bytes.to_vec(),
                field_name.to_string(),
                1,
                0,
                schema_version_id.to_string(),
                inc,
            )
            .map_err(|e| query::error::QueryError::execution(e.to_string()))
        }
        other => Err(query::error::QueryError::execution(format!(
            "unsupported counter NumericKind {other:?}"
        ))),
    }
}

fn decode_counter_value(bytes: &[u8], kind: NumericKind) -> Option<NormalValue> {
    match kind {
        NumericKind::Int64 if bytes.len() == 8 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(NormalValue::Int(i64::from_be_bytes(arr)))
        }
        NumericKind::Float64 if bytes.len() == 8 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(NormalValue::Float64(f64::from_be_bytes(arr)))
        }
        NumericKind::Float32 if bytes.len() == 4 => {
            let arr: [u8; 4] = bytes.try_into().ok()?;
            Some(NormalValue::Float32(f32::from_be_bytes(arr)))
        }
        _ => None,
    }
}

pub(crate) fn ensure_collection_is_active<S: Store>(
    db: &DB<S>,
    collection_name: &str,
    collection: &Collection,
) -> query::error::Result<()> {
    let is_active = db
        .find_collection_by_id(collection.collection_id())
        .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
        .is_some();

    if is_active {
        Ok(())
    } else {
        Err(query::error::QueryError::collection_not_found(
            collection_name,
        ))
    }
}

impl<S: Store + 'static> AutoCommitMutator<S> {
    /// Get collection from DB cache or return a not-found error.
    pub(super) fn get_collection_or_err(
        &self,
        collection_name: &str,
    ) -> query::error::Result<Collection> {
        self.db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))
    }

    /// Emit update events for subscriptions, carrying the actual block bytes
    /// so downstream consumers can traverse the DAG without an extra fetch.
    ///
    /// For branchable collections, emits a second event keyed by collection_id
    /// using the collection block's own cid/bytes (Go publishes the collection
    /// block separately at internal/db/collection.go:789).
    pub(super) fn emit_update_events(
        &self,
        collection: &Collection,
        doc_id_str: &str,
        doc_cid: Cid,
        doc_block: Bytes,
        collection_block: Option<(Cid, Bytes)>,
    ) {
        if let Some(bus) = self.db.event_bus() {
            let update = Update::new(
                doc_id_str.to_string(),
                doc_cid,
                collection.collection_id().to_string(),
                doc_block,
                false, // is_retry
                false, // is_relay (local mutation)
            );
            bus.publish(Message::update(update));

            if let Some((col_cid, col_block)) = collection_block {
                let col_update = Update::new_with_subject_doc_id(
                    String::new(), // empty doc_id → keyed by collection_id
                    doc_id_str.to_string(),
                    col_cid,
                    collection.collection_id().to_string(),
                    col_block,
                    false,
                    false,
                );
                bus.publish(Message::update(col_update));
            }
        }
    }
}
