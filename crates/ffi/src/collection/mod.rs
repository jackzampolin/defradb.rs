//! Collection management operations for FFI.
//!
//! This module exposes collection lifecycle and management functions
//! that match Go's collection management behavior.

mod migration;
mod read;
mod view;
mod write;

pub use migration::{delete_collection_versions, set_migration, set_migration_in_txn};
pub use read::{
    find_collection_by_id, get_collection_by_name, get_collection_by_version_id, has_collection,
};
pub use view::{add_view, refresh_views};
pub use write::{
    delete_collection, patch_collection, set_active_collection_version, truncate_collection,
};

/// Convert a Rust `Select` into a Go-compatible `request.Select` JSON object.
///
/// Go's `request.Select` uses PascalCase keys and `immutable.Option[T]` which
/// serializes as `null` when empty or the bare value when present.
fn select_to_go_json(select: &query::Select) -> serde_json::Value {
    let fields: Vec<serde_json::Value> = select
        .fields
        .iter()
        .map(|f| match f {
            query::mapper::Requestable::Field(field) => {
                let mut m = serde_json::Map::new();
                m.insert("Name".into(), serde_json::Value::String(field.name.clone()));
                m.insert(
                    "Alias".into(),
                    field
                        .alias
                        .as_ref()
                        .map(|a| serde_json::Value::String(a.clone()))
                        .unwrap_or(serde_json::Value::Null),
                );
                serde_json::Value::Object(m)
            }
            query::mapper::Requestable::Select(sub) => select_to_go_json(sub),
            query::mapper::Requestable::Similarity(_) => {
                // Similarity fields are not used in view query serialization
                serde_json::Value::Null
            }
            query::mapper::Requestable::Aggregate(agg) => {
                let mut m = serde_json::Map::new();
                m.insert(
                    "Name".into(),
                    serde_json::Value::String(agg.aggregate_type.as_str().to_string()),
                );
                m.insert(
                    "Alias".into(),
                    agg.alias
                        .as_ref()
                        .map(|a| serde_json::Value::String(a.clone()))
                        .unwrap_or(serde_json::Value::Null),
                );
                let targets: Vec<serde_json::Value> = agg
                    .targets
                    .iter()
                    .map(|t| {
                        let mut tm = serde_json::Map::new();
                        tm.insert(
                            "HostName".into(),
                            serde_json::Value::String(t.host_name.clone()),
                        );
                        tm.insert(
                            "ChildName".into(),
                            t.field_name
                                .as_ref()
                                .map(|n| serde_json::Value::String(n.clone()))
                                .unwrap_or(serde_json::Value::Null),
                        );
                        tm.insert("Filter".into(), serde_json::Value::Null);
                        tm.insert("Limit".into(), serde_json::Value::Null);
                        tm.insert("Offset".into(), serde_json::Value::Null);
                        tm.insert("OrderBy".into(), serde_json::Value::Null);
                        serde_json::Value::Object(tm)
                    })
                    .collect();
                m.insert("Targets".into(), serde_json::Value::Array(targets));
                serde_json::Value::Object(m)
            }
        })
        .collect();

    let mut m = serde_json::Map::new();
    m.insert(
        "Name".into(),
        serde_json::Value::String(select.collection_name.clone()),
    );
    m.insert(
        "Alias".into(),
        select
            .field
            .alias
            .as_ref()
            .map(|a| serde_json::Value::String(a.clone()))
            .unwrap_or(serde_json::Value::Null),
    );
    m.insert("Fields".into(), serde_json::Value::Array(fields));
    m.insert(
        "Limit".into(),
        select
            .limit
            .as_ref()
            .and_then(|l| l.limit)
            .map(|v| serde_json::Value::Number(v.into()))
            .unwrap_or(serde_json::Value::Null),
    );
    m.insert(
        "Offset".into(),
        select
            .limit
            .as_ref()
            .map(|l| {
                if l.offset > 0 {
                    serde_json::Value::Number(l.offset.into())
                } else {
                    serde_json::Value::Null
                }
            })
            .unwrap_or(serde_json::Value::Null),
    );
    m.insert("OrderBy".into(), serde_json::Value::Null);
    m.insert(
        "Filter".into(),
        select
            .filter
            .as_ref()
            .map(|f| {
                let conditions: serde_json::Map<String, serde_json::Value> = f
                    .conditions()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let mut filter_obj = serde_json::Map::new();
                filter_obj.insert("Conditions".into(), serde_json::Value::Object(conditions));
                serde_json::Value::Object(filter_obj)
            })
            .unwrap_or(serde_json::Value::Null),
    );
    m.insert("DocIDs".into(), serde_json::Value::Null);
    m.insert("CID".into(), serde_json::Value::Null);
    m.insert("GroupBy".into(), serde_json::Value::Null);
    m.insert(
        "ShowDeleted".into(),
        serde_json::Value::Bool(select.show_deleted),
    );
    m.insert(
        "IsEncrypted".into(),
        serde_json::Value::Bool(select.is_encrypted),
    );

    serde_json::Value::Object(m)
}
