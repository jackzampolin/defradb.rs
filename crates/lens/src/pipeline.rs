//! Lens migration pipeline.
//!
//! Matches Go's internal/lens/lens.go Lens type and behavior.
//!
//! Two modes:
//! - With `wasmtime-runtime`: spawns a background task via tokio for processing
//! - Without: processes documents inline (suitable for wasm32/browser)

#[cfg(not(feature = "wasmtime-runtime"))]
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::StreamExt;

use crate::store::TransformStore;
use crate::{Error, LensDoc, Result, TargetedHistoryLink, TransformId};

/// Input to the lens pipeline: a document with its schema version.
#[derive(Debug, Clone)]
pub struct LensInput {
    /// The schema version ID of the document.
    pub schema_version_id: String,
    /// The document to transform.
    pub doc: LensDoc,
}

impl LensInput {
    /// Create a new lens input.
    pub fn new(schema_version_id: impl Into<String>, doc: LensDoc) -> Self {
        Self {
            schema_version_id: schema_version_id.into(),
            doc,
        }
    }
}

/// Lens migrates documents to a target schema version.
///
/// Documents may be of various schema versions and may need migration across multiple
/// versions. The pipeline is constructed lazily as new source versions are discovered.
///
/// Matches Go's Lens interface.
pub struct Lens {
    #[cfg_attr(feature = "wasmtime-runtime", allow(dead_code))]
    store: Arc<dyn TransformStore>,
    target_version_id: String,
    #[cfg_attr(feature = "wasmtime-runtime", allow(dead_code))]
    collection_history: HashMap<String, TargetedHistoryLink>,
    #[cfg(feature = "wasmtime-runtime")]
    input_tx: futures::channel::mpsc::UnboundedSender<LensInput>,
    #[cfg(feature = "wasmtime-runtime")]
    output_rx: futures::channel::mpsc::UnboundedReceiver<Result<LensDoc>>,
    #[cfg(not(feature = "wasmtime-runtime"))]
    pending: VecDeque<LensInput>,
}

impl Lens {
    /// Create a new lens pipeline.
    ///
    /// # Arguments
    /// * `store` - The transform store for executing WASM transforms
    /// * `target_version_id` - The target schema version to migrate documents to
    /// * `collection_history` - The targeted history for version traversal
    pub fn new(
        store: Arc<dyn TransformStore>,
        target_version_id: impl Into<String>,
        collection_history: HashMap<String, TargetedHistoryLink>,
    ) -> Self {
        let target_version_id = target_version_id.into();

        #[cfg(feature = "wasmtime-runtime")]
        {
            use futures::channel::mpsc;

            let (input_tx, input_rx) = mpsc::unbounded();
            let (output_tx, output_rx) = mpsc::unbounded();

            let pipeline = PipelineProcessor {
                store: store.clone(),
                target_version_id: target_version_id.clone(),
                collection_history: collection_history.clone(),
                input_rx,
                output_tx,
            };

            tokio::spawn(pipeline.run());

            Self {
                store,
                target_version_id,
                collection_history,
                input_tx,
                output_rx,
            }
        }

        #[cfg(not(feature = "wasmtime-runtime"))]
        {
            Self {
                store,
                target_version_id,
                collection_history,
                pending: VecDeque::new(),
            }
        }
    }

    /// Put a document into the pipeline for transformation.
    pub async fn put(&mut self, schema_version_id: &str, doc: LensDoc) -> Result<()> {
        #[cfg(feature = "wasmtime-runtime")]
        {
            use futures::SinkExt;
            self.input_tx
                .send(LensInput::new(schema_version_id, doc))
                .await
                .map_err(|e| Error::Pipeline(format!("failed to send input: {}", e)))
        }

        #[cfg(not(feature = "wasmtime-runtime"))]
        {
            self.pending
                .push_back(LensInput::new(schema_version_id, doc));
            Ok(())
        }
    }

    /// Get the next transformed document from the pipeline.
    pub async fn next(&mut self) -> Option<Result<LensDoc>> {
        #[cfg(feature = "wasmtime-runtime")]
        {
            self.output_rx.next().await
        }

        #[cfg(not(feature = "wasmtime-runtime"))]
        {
            let input = self.pending.pop_front()?;
            Some(
                transform_to_target(
                    &self.store,
                    &self.target_version_id,
                    &self.collection_history,
                    input,
                )
                .await,
            )
        }
    }

    /// Check if a migration is needed for the given schema version.
    pub fn needs_migration(&self, schema_version_id: &str) -> bool {
        schema_version_id != self.target_version_id
            && self.collection_history.contains_key(schema_version_id)
    }

    /// Get the target schema version ID.
    pub fn target_version_id(&self) -> &str {
        &self.target_version_id
    }
}

/// Internal pipeline processor that handles document transformation (spawned mode).
#[cfg(feature = "wasmtime-runtime")]
struct PipelineProcessor {
    store: Arc<dyn TransformStore>,
    target_version_id: String,
    collection_history: HashMap<String, TargetedHistoryLink>,
    input_rx: futures::channel::mpsc::UnboundedReceiver<LensInput>,
    output_tx: futures::channel::mpsc::UnboundedSender<Result<LensDoc>>,
}

#[cfg(feature = "wasmtime-runtime")]
impl PipelineProcessor {
    async fn run(mut self) {
        while let Some(input) = self.input_rx.next().await {
            let result = transform_to_target(
                &self.store,
                &self.target_version_id,
                &self.collection_history,
                input,
            )
            .await;
            if self.output_tx.unbounded_send(result).is_err() {
                break;
            }
        }
    }
}

/// Transform a document to the target schema version.
///
/// Shared between spawned and inline modes.
async fn transform_to_target(
    store: &Arc<dyn TransformStore>,
    target_version_id: &str,
    collection_history: &HashMap<String, TargetedHistoryLink>,
    input: LensInput,
) -> Result<LensDoc> {
    if input.schema_version_id == target_version_id {
        return Ok(input.doc);
    }

    let _history_link = collection_history
        .get(&input.schema_version_id)
        .ok_or_else(|| Error::SchemaVersionNotFound(input.schema_version_id.clone()))?;

    let mut current_doc = input.doc;
    let mut current_version = input.schema_version_id.clone();

    let mut visited = HashSet::new();
    visited.insert(current_version.clone());

    loop {
        if current_version == target_version_id {
            return Ok(current_doc);
        }

        let current_link = collection_history
            .get(&current_version)
            .ok_or_else(|| Error::SchemaVersionNotFound(current_version.clone()))?;

        // Try forward (next) first, then backward (previous).
        // If the forward direction was already visited, fall through to backward.
        let can_go_next = current_link
            .next
            .as_ref()
            .is_some_and(|v| !visited.contains(v));
        let can_go_prev = current_link
            .previous
            .as_ref()
            .is_some_and(|v| !visited.contains(v));

        if can_go_next {
            let next_version = current_link.next.as_ref().unwrap();

            let next_link = collection_history
                .get(next_version)
                .ok_or_else(|| Error::SchemaVersionNotFound(next_version.clone()))?;

            if let Some(ref transform_id) = next_link.transform {
                current_doc =
                    apply_transform(store, &TransformId::new(transform_id), current_doc, false)
                        .await?;
            }

            current_version = next_version.clone();
            visited.insert(current_version.clone());
        } else if can_go_prev {
            let prev_version = current_link.previous.as_ref().unwrap();

            if let Some(ref transform_id) = current_link.transform {
                current_doc =
                    apply_transform(store, &TransformId::new(transform_id), current_doc, true)
                        .await?;
            }

            current_version = prev_version.clone();
            visited.insert(current_version.clone());
        } else {
            return Err(Error::Pipeline(format!(
                "no migration path from {} to {}",
                input.schema_version_id, target_version_id
            )));
        }
    }
}

async fn apply_transform(
    store: &Arc<dyn TransformStore>,
    transform_id: &TransformId,
    doc: LensDoc,
    inverse: bool,
) -> Result<LensDoc> {
    let input_stream = Box::pin(futures::stream::once(async move { doc }));

    let mut output_stream = if inverse {
        store.inverse(transform_id, input_stream)?
    } else {
        store.transform(transform_id, input_stream)?
    };

    output_stream
        .next()
        .await
        .ok_or_else(|| Error::Pipeline("transform produced no output".to_string()))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryTransformStore;
    use crate::{LensConfig, LensModule};
    use serde_json::json;

    #[tokio::test]
    async fn test_lens_passthrough_at_target() {
        let store = Arc::new(MemoryTransformStore::new());
        let history = HashMap::new();
        let mut lens = Lens::new(store, "v1", history);

        let mut doc = LensDoc::new();
        doc.insert("name".to_string(), json!("Alice"));

        lens.put("v1", doc.clone()).await.unwrap();

        let result = lens.next().await.unwrap().unwrap();
        assert_eq!(result.get("name").unwrap(), &json!("Alice"));
    }

    #[tokio::test]
    async fn test_lens_needs_migration() {
        let store = Arc::new(MemoryTransformStore::new());
        let mut history = HashMap::new();

        history.insert(
            "v1".to_string(),
            TargetedHistoryLink::new("v1", "col_1").with_next("v2"),
        );
        history.insert(
            "v2".to_string(),
            TargetedHistoryLink::new("v2", "col_1")
                .with_transform(Some("transform_1".to_string()))
                .with_previous("v1"),
        );

        let lens = Lens::new(store, "v2", history);

        assert!(lens.needs_migration("v1"));
        assert!(!lens.needs_migration("v2"));
        assert!(!lens.needs_migration("v_unknown"));
    }

    #[tokio::test]
    async fn test_lens_transform_with_registered_migration() {
        let store = Arc::new(MemoryTransformStore::new());

        // Register a transform
        let config = LensConfig::new("v1", "v2", LensModule::from_path("/path/to/transform.wasm"));
        let transform_id = store.add(config).await.unwrap();

        let mut history = HashMap::new();
        history.insert(
            "v1".to_string(),
            TargetedHistoryLink::new("v1", "col_1").with_next("v2"),
        );
        history.insert(
            "v2".to_string(),
            TargetedHistoryLink::new("v2", "col_1")
                .with_transform(Some(transform_id.to_string()))
                .with_previous("v1"),
        );

        let mut lens = Lens::new(store, "v2", history);

        let mut doc = LensDoc::new();
        doc.insert("name".to_string(), json!("Alice"));

        lens.put("v1", doc).await.unwrap();

        // MemoryTransformStore passes through unchanged
        let result = lens.next().await.unwrap().unwrap();
        assert_eq!(result.get("name").unwrap(), &json!("Alice"));
    }
}
