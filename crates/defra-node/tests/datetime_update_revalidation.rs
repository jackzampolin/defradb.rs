//! Reproduction for the DateTime update-revalidation bug.
//!
//! Document storage is schema-blind for DateTime: a `Time` is written to CBOR
//! as an untagged text string and read back as `NormalValue::String`. The
//! index path was taught to re-coerce String->Time (hotfix 5739ecd8), but the
//! document *validation* path (`Collection::validate_document`) was not — so
//! updating ANY field on a document that already holds a DateTime value fails
//! re-validation with "incompatible type: expected Scalar(DateTime), got
//! String(...)". This is exactly what observability-mcp's InferenceBackend
//! reconcile hit every 30s.
//!
//! These tests prove (a) the bug reproduces at the embedded-node layer, and
//! (b) whether @index changes the behavior.

use anyhow::{Context, Result};
use defra_node::EmbeddedNode;

const SDL_NOINDEX: &str = r#"
type Probe {
    name: String
    last_probe: DateTime
}
"#;

const SDL_INDEXED: &str = r#"
type ProbeIdx {
    name: String
    last_probe: DateTime @index
}
"#;

/// Mirror obs-mcp exactly: create via `create_<Type>`, then resolve the docID
/// with a filter query (obs-mcp never parses the create response).
async fn create_get_id(node: &EmbeddedNode, coll: &str) -> Result<String> {
    let m = format!(r#"mutation {{ create_{coll}(input: {{ name: "initial" }}) {{ _docID }} }}"#);
    let r = node.execute(&m).await;
    assert!(!r.has_errors(), "create_{coll} failed: {:?}", r.errors);

    let q = format!(r#"query {{ {coll}(filter: {{ name: {{ _eq: "initial" }} }}) {{ _docID }} }}"#);
    let qr = node.execute(&q).await;
    assert!(!qr.has_errors(), "lookup {coll} failed: {:?}", qr.errors);
    let id = qr
        .data
        .as_ref()
        .and_then(|d| d.get(coll))
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|v| v.get("_docID"))
        .and_then(|v| v.as_str())
        .context("no _docID after create+lookup")?
        .to_string();
    Ok(id)
}

/// Set a DateTime field, then update a *different* field. The second update is
/// where obs-mcp fails. EXPECTED TO FAIL on unfixed defradb.rs.
#[tokio::test(flavor = "current_thread")]
async fn datetime_update_revalidation_noindex() -> Result<()> {
    let node = EmbeddedNode::builder().build().await?;
    node.add_schema(SDL_NOINDEX).await?;
    let id = create_get_id(&node, "Probe").await?;

    let set_dt = format!(
        r#"mutation {{ update_Probe(docID: "{id}", input: {{ last_probe: "2026-06-03T15:00:00Z" }}) {{ _docID }} }}"#
    );
    let r1 = node.execute(&set_dt).await;
    assert!(
        !r1.has_errors(),
        "setting last_probe failed: {:?}",
        r1.errors
    );

    // Update an UNRELATED field — re-validates the stored DateTime.
    let upd_other = format!(
        r#"mutation {{ update_Probe(docID: "{id}", input: {{ name: "changed" }}) {{ _docID }} }}"#
    );
    let r2 = node.execute(&upd_other).await;
    assert!(
        !r2.has_errors(),
        "BUG REPRODUCED: updating an unrelated field re-validates the stored \
         DateTime (read back as String) and fails: {:?}",
        r2.errors
    );
    Ok(())
}

/// Same scenario but the DateTime field is indexed (coding-store's
/// CodingSession.created_at pattern). Confirms whether @index is the difference.
#[tokio::test(flavor = "current_thread")]
async fn datetime_update_revalidation_indexed() -> Result<()> {
    let node = EmbeddedNode::builder().build().await?;
    node.add_schema(SDL_INDEXED).await?;
    let id = create_get_id(&node, "ProbeIdx").await?;

    let set_dt = format!(
        r#"mutation {{ update_ProbeIdx(docID: "{id}", input: {{ last_probe: "2026-06-03T15:00:00Z" }}) {{ _docID }} }}"#
    );
    let r1 = node.execute(&set_dt).await;
    assert!(
        !r1.has_errors(),
        "setting last_probe failed: {:?}",
        r1.errors
    );

    let upd_other = format!(
        r#"mutation {{ update_ProbeIdx(docID: "{id}", input: {{ name: "changed" }}) {{ _docID }} }}"#
    );
    let r2 = node.execute(&upd_other).await;
    assert!(
        !r2.has_errors(),
        "indexed DateTime field also fails update-revalidation: {:?}",
        r2.errors
    );
    Ok(())
}
