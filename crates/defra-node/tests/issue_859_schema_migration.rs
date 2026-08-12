//! Tests for issue #859: collection patching, schema migration, and
//! introspection exposed through [`EmbeddedNode`].

use anyhow::{Context, Result};
use defra_node::EmbeddedNode;
#[cfg(feature = "wasmtime-runtime")]
use defra_node::{LensConfig, LensModule};

const USER_SDL: &str = r#"
type User {
    name: String
    age: Int
}
"#;

// Minimal valid WebAssembly module — just the 4-byte `\0asm` magic plus the
// 4-byte version (`1`). Wasmtime accepts this as an empty module, which is
// all we need to prove [`EmbeddedNode::set_migration`] wires through to the
// underlying lens store.
#[cfg(feature = "wasmtime-runtime")]
const EMPTY_WASM: &[u8] = &[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

#[tokio::test(flavor = "current_thread")]
async fn list_collections_reports_added_schema() -> Result<()> {
    let node = EmbeddedNode::builder().build().await?;

    assert!(node.list_collections()?.is_empty());

    node.add_schema(USER_SDL).await?;

    let names = node.list_collections()?;
    assert_eq!(names, vec!["User".to_string()]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn get_collection_returns_active_schema() -> Result<()> {
    let node = EmbeddedNode::builder().build().await?;
    node.add_schema(USER_SDL).await?;

    let collection = node
        .get_collection("User")?
        .context("User collection should exist")?;
    assert_eq!(collection.name, "User");
    assert!(collection.is_active);
    assert!(collection.fields.iter().any(|f| f.name == "name"));
    assert!(collection.fields.iter().any(|f| f.name == "age"));

    assert!(node.get_collection("NotAType")?.is_none());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn patch_collection_adds_field_and_bumps_version() -> Result<()> {
    let node = EmbeddedNode::builder().build().await?;
    node.add_schema(USER_SDL).await?;

    let original = node
        .get_collection("User")?
        .context("User should exist after add_schema")?;

    let patch = r#"[
        {"op": "add", "path": "/User/Fields/-", "value": {"Name": "email", "Kind": "String"}}
    ]"#;
    let patched = node.patch_collection("User", patch).await?;

    assert_ne!(patched.version_id, original.version_id);
    assert!(patched.fields.iter().any(|f| f.name == "email"));

    let current = node
        .get_collection("User")?
        .context("User should still exist after patch")?;
    assert_eq!(current.version_id, patched.version_id);
    assert!(current.fields.iter().any(|f| f.name == "email"));

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn patch_collection_chained_evolutions_produce_distinct_versions() -> Result<()> {
    let node = EmbeddedNode::builder().build().await?;
    node.add_schema(USER_SDL).await?;

    let v0 = node
        .get_collection("User")?
        .context("User should exist")?
        .version_id;

    let v1 = node
        .patch_collection(
            "User",
            r#"[{"op":"add","path":"/User/Fields/-","value":{"Name":"email","Kind":"String"}}]"#,
        )
        .await?
        .version_id;
    let v2 = node
        .patch_collection(
            "User",
            r#"[{"op":"add","path":"/User/Fields/-","value":{"Name":"phone","Kind":"String"}}]"#,
        )
        .await?
        .version_id;

    assert_ne!(v0, v1);
    assert_ne!(v1, v2);
    assert_ne!(v0, v2);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn get_collection_by_version_id_finds_prior_and_new() -> Result<()> {
    let node = EmbeddedNode::builder().build().await?;
    node.add_schema(USER_SDL).await?;

    let original_version = node
        .get_collection("User")?
        .context("User should exist")?
        .version_id;

    let patch = r#"[
        {"op": "add", "path": "/User/Fields/-", "value": {"Name": "email", "Kind": "String"}}
    ]"#;
    let patched = node.patch_collection("User", patch).await?;

    let by_new = node
        .get_collection_by_version_id(&patched.version_id)
        .await?
        .context("new version should be retrievable")?;
    assert_eq!(by_new.version_id, patched.version_id);

    let by_old = node
        .get_collection_by_version_id(&original_version)
        .await?
        .context("old version should still be retrievable")?;
    assert_eq!(by_old.version_id, original_version);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn get_all_collection_versions_returns_history() -> Result<()> {
    let node = EmbeddedNode::builder().build().await?;
    node.add_schema(USER_SDL).await?;

    let initial = node.get_all_collection_versions().await?;
    assert_eq!(initial.len(), 1);

    let patch = r#"[
        {"op": "add", "path": "/User/Fields/-", "value": {"Name": "email", "Kind": "String"}}
    ]"#;
    node.patch_collection("User", patch).await?;

    let after_patch = node.get_all_collection_versions().await?;
    assert!(
        after_patch.len() >= 2,
        "expected at least two versions after patching, got {}",
        after_patch.len()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn set_active_collection_version_round_trips_to_original() -> Result<()> {
    let node = EmbeddedNode::builder().build().await?;
    node.add_schema(USER_SDL).await?;

    let original_version = node
        .get_collection("User")?
        .context("User should exist")?
        .version_id;

    let patch = r#"[
        {"op": "add", "path": "/User/Fields/-", "value": {"Name": "email", "Kind": "String"}}
    ]"#;
    let patched = node.patch_collection("User", patch).await?;
    assert_ne!(patched.version_id, original_version);
    assert_eq!(
        node.get_collection("User")?
            .context("User should exist")?
            .version_id,
        patched.version_id,
    );

    node.set_active_collection_version(&original_version)
        .await?;
    assert_eq!(
        node.get_collection("User")?
            .context("User should exist")?
            .version_id,
        original_version,
    );

    Ok(())
}

#[cfg(feature = "wasmtime-runtime")]
#[tokio::test(flavor = "current_thread")]
async fn set_migration_registers_lens_between_versions() -> Result<()> {
    let node = EmbeddedNode::builder().build().await?;
    node.add_schema(USER_SDL).await?;

    let original_version = node
        .get_collection("User")?
        .context("User should exist")?
        .version_id;

    let patch = r#"[
        {"op": "add", "path": "/User/Fields/-", "value": {"Name": "email", "Kind": "String"}}
    ]"#;
    let patched = node.patch_collection("User", patch).await?;

    let module = LensModule::from_bytes(EMPTY_WASM.to_vec());
    let config = LensConfig::new(&original_version, &patched.version_id, module);

    let transform_id = node.set_migration(config).await?;
    assert!(
        !transform_id.0.is_empty(),
        "transform id should be a non-empty CID"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn materialize_collection_is_exposed_on_embedded_node() -> Result<()> {
    let node = EmbeddedNode::builder().build().await?;
    node.add_schema(USER_SDL).await?;

    let patch =
        r#"[{"op":"add","path":"/User/Fields/-","value":{"Name":"email","Kind":"String"}}]"#;
    node.patch_collection("User", patch).await?;

    assert_eq!(node.materialize_collection("User").await?, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn introspection_supports_idempotent_schema_ensure() -> Result<()> {
    // Demonstrates the idempotent bootstrap pattern the issue calls out:
    // the application checks whether a collection exists before falling
    // back to add_schema. Running twice must not error.
    let node = EmbeddedNode::builder().build().await?;

    ensure_user_schema(&node).await?;
    ensure_user_schema(&node).await?;

    assert_eq!(node.list_collections()?, vec!["User".to_string()]);
    Ok(())
}

async fn ensure_user_schema(node: &EmbeddedNode) -> Result<()> {
    if node.get_collection("User")?.is_none() {
        node.add_schema(USER_SDL).await?;
    }
    Ok(())
}
