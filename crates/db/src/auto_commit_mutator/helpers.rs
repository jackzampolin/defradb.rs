use super::*;

use crate::collection::Collection;
use crdt::traits::{Context, ValueReader};
use crdt::{Counter, CounterDelta, NumericKind};
use datastore::NamespaceView;
use defra_core::types::DocId as CrdtDocId;
use document::NormalValue;
use schema::{FieldKind, ScalarKind};

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
/// PNCounter decrements too.
///
/// `is_create` skips the committed-doc lookup: on create there is no prior
/// committed value, so the seed is 0 and the delta is the created value.
pub(super) async fn apply_local_counter_deltas(
    datastore: &NamespaceView,
    collection: &Collection,
    doc: &mut Document,
    is_create: bool,
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
    // first time it is touched. On create there is no committed doc → seed 0.
    let committed = if is_create {
        None
    } else {
        collection
            .get_with_datastore(datastore, &doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?
    };

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
pub(super) async fn init_counter_stores_on_create(
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
            (NumericKind::Float64, NormalValue::Float32(v)) => counter
                .reconcile_float64(&mut rw, *v as f64)
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
    match kind {
        FieldKind::Scalar(ScalarKind::Int) => Some(NumericKind::Int64),
        FieldKind::Scalar(ScalarKind::Float64) | FieldKind::Scalar(ScalarKind::Float32) => {
            Some(NumericKind::Float64)
        }
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
        _ => None,
    }
}

pub(super) fn ensure_collection_is_active<S: Store>(
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
        doc_block: Vec<u8>,
        collection_block: Option<(Cid, Vec<u8>)>,
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
