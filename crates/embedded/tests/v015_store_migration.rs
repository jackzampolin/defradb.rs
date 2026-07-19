use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use cid::Cid;
use embedded::{EmbeddedNode, EmbeddedStore, NodeBuilder};
use serde_json::{json, Value};

const OLD_TASK_1_ID: &str = "bae-7513a70e-b156-5803-b492-017180bd20b8";
const LAST_LEGACY_TASK_ID: &str = "bae-aba20f79-3f40-5f9e-a2e3-849786094ac9";
const TASK_COLLECTION_ID: &str = "bafyreia5e7ttg5pzmgkokb7fafom5nqb5tajlc443p5fmbzsb4646lw334";
const LAST_LEGACY_COMPOSITE_HEAD: &str =
    "bafyreidz66r3ozihn5gum475v5s7evufcjy4fewvywv6n24eh3x5vncwae";
const MIGRATION_MARKER: &[u8] = b"/migration/doc-short-id/v1";

fn materialize_fixture(directory: &Path) -> Result<PathBuf> {
    let encoded: String = include_str!("fixtures/v015_populated.redb.zst.b64")
        .split_whitespace()
        .collect();
    let compressed = BASE64_STANDARD
        .decode(encoded)
        .context("decode v0.15 fixture base64")?;
    let bytes =
        zstd::stream::decode_all(compressed.as_slice()).context("decompress v0.15 Redb fixture")?;
    let path = directory.join("v015.redb");
    fs::write(&path, bytes).context("write disposable v0.15 Redb fixture")?;
    Ok(path)
}

async fn execute_ok(node: &EmbeddedNode<EmbeddedStore>, query: &str) -> Result<Value> {
    let response = node.execute(query).await;
    ensure!(
        response.errors.is_empty(),
        "query failed: {:?}",
        response.errors
    );
    response.data.context("successful query returned no data")
}

#[tokio::test]
async fn migrates_populated_v015_store_and_reopens_idempotently() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = materialize_fixture(directory.path())?;

    let node = NodeBuilder::default().data_path(&path).build().await?;
    let documents = execute_ok(
        &node,
        r#"query { Task(order: {task_id: ASC}) { _docID task_id status note } }"#,
    )
    .await?;
    assert_eq!(
        documents,
        json!({
            "Task": [
                {
                    "_docID": "bae-cfe4febb-cee3-541d-afe7-86d76da576b5",
                    "task_id": "task-1",
                    "status": "running",
                    "note": "updated"
                },
                {
                    "_docID": "bae-7f630507-5c0f-511d-a448-df2927da12e0",
                    "task_id": "task-2",
                    "status": "complete",
                    "note": "second"
                },
                {
                    "_docID": "bae-6f2358f8-d095-55a1-b572-35fe6864ac4a",
                    "task_id": "task-3",
                    "status": "failed",
                    "note": "third"
                }
            ]
        })
    );

    let indexed = execute_ok(
        &node,
        r#"query { Task(filter: {task_id: {_eq: "task-1"}}) { _docID task_id status note } }"#,
    )
    .await?;
    assert_eq!(indexed["Task"].as_array().map(Vec::len), Some(1));
    assert_eq!(indexed["Task"][0]["note"], "updated");

    let commits = execute_ok(
        &node,
        &format!(
            r#"query {{ _commits(docID: "{OLD_TASK_1_ID}") {{ cid docID height fieldName }} }}"#
        ),
    )
    .await?;
    assert_eq!(commits["_commits"].as_array().map(Vec::len), Some(7));
    ensure!(
        commits["_commits"]
            .as_array()
            .into_iter()
            .flatten()
            .all(|commit| commit["docID"] == "bae-cfe4febb-cee3-541d-afe7-86d76da576b5"),
        "old DocID alias did not resolve to the canonical commit history"
    );

    let duplicate = node
        .execute(
            r#"mutation { create_Task(input: {task_id: "task-1", status: "duplicate", note: "must fail"}) { _docID } }"#,
        )
        .await;
    ensure!(
        duplicate.has_errors(),
        "unique-index duplicate was accepted"
    );
    ensure!(
        duplicate
            .errors
            .iter()
            .any(|error| error.message.to_ascii_lowercase().contains("unique")),
        "duplicate failed for an unexpected reason: {:?}",
        duplicate.errors
    );

    let created = execute_ok(
        &node,
        r#"mutation { create_Task(input: {task_id: "task-4", status: "pending", note: "post-migration"}) { task_id status note } }"#,
    )
    .await?;
    assert_eq!(
        created["add_Task"][0]["task_id"], "task-4",
        "unexpected create response: {created}"
    );

    let updated = execute_ok(
        &node,
        r#"mutation { update_Task(filter: {task_id: {_eq: "task-2"}}, input: {status: "verified"}) { task_id status } }"#,
    )
    .await?;
    assert_eq!(updated["update_Task"][0]["status"], "verified");
    node.shutdown().await;
    drop(node);

    let reopened = NodeBuilder::default().data_path(&path).build().await?;
    let after_reopen = execute_ok(
        &reopened,
        r#"query { Task(order: {task_id: ASC}) { task_id status note } }"#,
    )
    .await?;
    assert_eq!(after_reopen["Task"].as_array().map(Vec::len), Some(4));
    assert_eq!(after_reopen["Task"][1]["task_id"], "task-2");
    assert_eq!(after_reopen["Task"][1]["status"], "verified");
    assert_eq!(after_reopen["Task"][3]["task_id"], "task-4");
    reopened.shutdown().await;

    Ok(())
}

#[tokio::test]
async fn malformed_v015_store_rolls_back_the_entire_migration() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = materialize_fixture(directory.path())?;

    let store = Arc::new(storage::RedbStore::open(
        path.to_str().context("fixture path contains non-UTF-8")?,
    )?);
    {
        let database = db::DB::from_arc(Arc::clone(&store))?;
        let txn = database.new_txn(false).await?;
        let cid = LAST_LEGACY_COMPOSITE_HEAD.parse::<Cid>()?;
        txn.blockstore()?.delete(&cid.to_bytes()).await?;
        txn.commit().await?;
    }

    let error = match db::DB::open_from_arc(Arc::clone(&store)).await {
        Ok(database) => {
            database.close().await?;
            bail!("migration unexpectedly accepted a missing legacy block")
        }
        Err(error) => error,
    };
    ensure!(
        format!("{error:#}").contains("references missing block"),
        "migration failed for an unexpected reason: {error:#}"
    );

    let database = db::DB::from_arc(store)?;
    let txn = database.new_txn(true).await?;
    {
        let systemstore = txn.systemstore()?;
        ensure!(
            !systemstore.has(MIGRATION_MARKER).await?,
            "failed migration published its completion marker"
        );
        ensure!(
            !systemstore.has(b"/d/s/\x01").await?,
            "failed migration leaked a short-ID mapping"
        );
        ensure!(
            !systemstore.has(b"/seq/doc").await?,
            "failed migration advanced the short-ID sequence"
        );
        let legacy_key = format!("/d/{TASK_COLLECTION_ID}/{LAST_LEGACY_TASK_ID}");
        ensure!(
            txn.datastore()?.has(legacy_key.as_bytes()).await?,
            "failed migration deleted a legacy document"
        );
    }
    txn.discard()?;
    database.close().await?;

    Ok(())
}
