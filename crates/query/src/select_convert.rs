/// Convert a Rust `Select` into a Go-compatible `request.Select` JSON object.
///
/// Go's `request.Select` uses PascalCase keys and `immutable.Option[T]` which
/// serializes as `null` when empty or the bare value when present.
pub fn select_to_go_json(select: &crate::Select) -> serde_json::Value {
    let fields: Vec<serde_json::Value> = select
        .fields
        .iter()
        .map(|f| match f {
            crate::mapper::Requestable::Field(field) => {
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
            crate::mapper::Requestable::Select(sub) => select_to_go_json(sub),
            crate::mapper::Requestable::Similarity(_) => serde_json::Value::Null,
            crate::mapper::Requestable::FullTextSearch(_) => serde_json::Value::Null,
            crate::mapper::Requestable::Aggregate(agg) => {
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
    m.insert("CIDs".into(), serde_json::Value::Null);
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
