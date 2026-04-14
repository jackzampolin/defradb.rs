use std::io;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use defra_node::EmbeddedNode;
use tracing::Level;

const USER_SDL: &str = r#"
type User {
    name: String
    age: Int
}
"#;

#[derive(Clone, Default)]
struct SharedLogBuffer {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl SharedLogBuffer {
    fn contents(&self) -> String {
        String::from_utf8(self.inner.lock().unwrap().clone()).expect("log buffer should be utf-8")
    }
}

struct SharedLogWriter {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for SharedLogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn warning_capture_subscriber(
    buffer: &SharedLogBuffer,
) -> impl tracing::Subscriber + Send + Sync + 'static {
    tracing_subscriber::fmt()
        .with_max_level(Level::WARN)
        .with_writer({
            let buffer = buffer.clone();
            move || SharedLogWriter {
                inner: buffer.inner.clone(),
            }
        })
        .without_time()
        .with_ansi(false)
        .finish()
}

#[tokio::test(flavor = "current_thread")]
async fn embedded_upsert_does_not_warn_when_discarding_get_for_update_txn() -> Result<()> {
    let log_buffer = SharedLogBuffer::default();
    let subscriber = warning_capture_subscriber(&log_buffer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let node = EmbeddedNode::builder().build().await?;
    node.add_schema(USER_SDL).await?;

    let inserted = node
        .execute(
            r#"mutation {
                add_User(input: {name: "Alice", age: 30}) {
                    _docID
                }
            }"#,
        )
        .await;
    ensure_success(&inserted, "insert user")?;

    let upserted = node
        .execute(
            r#"mutation {
                upsert_User(
                    filter: {name: {_eq: "Alice"}}
                    add: {name: "Alice", age: 31}
                    update: {age: 31}
                ) {
                    _docID
                    age
                }
            }"#,
        )
        .await;
    ensure_success(&upserted, "upsert existing user")?;

    let age = upserted
        .data
        .as_ref()
        .and_then(|data| data.get("upsert_User"))
        .and_then(|rows| rows.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("age"))
        .and_then(|age| age.as_i64())
        .context("upsert result missing age")?;
    assert_eq!(age, 31);

    let logs = log_buffer.contents();
    assert!(
        !logs.contains("Failed to discard read-only transaction after get_for_update"),
        "unexpected get_for_update discard warning: {logs}"
    );

    Ok(())
}

fn ensure_success(response: &query::QueryResponse, context: &str) -> Result<()> {
    if response.errors.is_empty() {
        Ok(())
    } else {
        anyhow::bail!("{context} failed: {:?}", response.errors);
    }
}
