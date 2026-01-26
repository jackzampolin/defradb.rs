//! Lens migration pipeline.
//!
//! Matches Go's internal/lens/lens.go Lens type and behavior.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};

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
    #[allow(dead_code)]
    store: Arc<dyn TransformStore>,
    target_version_id: String,
    collection_history: HashMap<String, TargetedHistoryLink>,
    input_tx: mpsc::UnboundedSender<LensInput>,
    output_rx: mpsc::UnboundedReceiver<Result<LensDoc>>,
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
        let (input_tx, input_rx) = mpsc::unbounded();
        let (output_tx, output_rx) = mpsc::unbounded();

        let target_version_id = target_version_id.into();

        // Spawn the pipeline processor
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

    /// Put a document into the pipeline for transformation.
    pub async fn put(&mut self, schema_version_id: &str, doc: LensDoc) -> Result<()> {
        self.input_tx
            .send(LensInput::new(schema_version_id, doc))
            .await
            .map_err(|e| Error::Pipeline(format!("failed to send input: {}", e)))
    }

    /// Get the next transformed document from the pipeline.
    pub async fn next(&mut self) -> Option<Result<LensDoc>> {
        self.output_rx.next().await
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

/// Internal pipeline processor that handles document transformation.
struct PipelineProcessor {
    store: Arc<dyn TransformStore>,
    target_version_id: String,
    collection_history: HashMap<String, TargetedHistoryLink>,
    input_rx: mpsc::UnboundedReceiver<LensInput>,
    output_tx: mpsc::UnboundedSender<Result<LensDoc>>,
}

impl PipelineProcessor {
    async fn run(mut self) {
        while let Some(input) = self.input_rx.next().await {
            let result = self.transform_to_target(input).await;
            if self.output_tx.unbounded_send(result).is_err() {
                // Output channel closed, stop processing
                break;
            }
        }
    }

    async fn transform_to_target(&self, input: LensInput) -> Result<LensDoc> {
        // If already at target version, pass through unchanged
        if input.schema_version_id == self.target_version_id {
            return Ok(input.doc);
        }

        // Find the history link for this version
        let _history_link = self
            .collection_history
            .get(&input.schema_version_id)
            .ok_or_else(|| Error::SchemaVersionNotFound(input.schema_version_id.clone()))?;

        // Determine the migration path (forward or backward)
        let mut current_doc = input.doc;
        let mut current_version = input.schema_version_id.clone();

        // Track visited versions to detect cycles
        let mut visited = HashSet::new();
        visited.insert(current_version.clone());

        loop {
            if current_version == self.target_version_id {
                return Ok(current_doc);
            }

            let current_link = self
                .collection_history
                .get(&current_version)
                .ok_or_else(|| Error::SchemaVersionNotFound(current_version.clone()))?;

            // Try to move forward first
            if let Some(ref next_version) = current_link.next {
                // Check for cycle before moving forward
                if visited.contains(next_version) {
                    return Err(Error::Pipeline(format!(
                        "cycle detected in migration path at version {}",
                        next_version
                    )));
                }

                let next_link = self
                    .collection_history
                    .get(next_version)
                    .ok_or_else(|| Error::SchemaVersionNotFound(next_version.clone()))?;

                if let Some(ref transform_id) = next_link.transform {
                    // Apply forward transform
                    current_doc = self
                        .apply_transform(&TransformId::new(transform_id), current_doc, false)
                        .await?;
                }

                current_version = next_version.clone();
                visited.insert(current_version.clone());
            } else if let Some(ref prev_version) = current_link.previous {
                // Check for cycle before moving backward
                if visited.contains(prev_version) {
                    return Err(Error::Pipeline(format!(
                        "cycle detected in migration path at version {}",
                        prev_version
                    )));
                }

                // Move backward (inverse transform)
                if let Some(ref transform_id) = current_link.transform {
                    // Apply inverse transform
                    current_doc = self
                        .apply_transform(&TransformId::new(transform_id), current_doc, true)
                        .await?;
                }

                current_version = prev_version.clone();
                visited.insert(current_version.clone());
            } else {
                // No path to target
                return Err(Error::Pipeline(format!(
                    "no migration path from {} to {}",
                    input.schema_version_id, self.target_version_id
                )));
            }
        }
    }

    async fn apply_transform(
        &self,
        transform_id: &TransformId,
        doc: LensDoc,
        inverse: bool,
    ) -> Result<LensDoc> {
        // Create a single-item stream
        let input_stream = Box::pin(futures::stream::once(async move { doc }));

        // Apply the transform
        let mut output_stream = if inverse {
            self.store.inverse(transform_id, input_stream)?
        } else {
            self.store.transform(transform_id, input_stream)?
        };

        // Get the transformed document
        output_stream
            .next()
            .await
            .ok_or_else(|| Error::Pipeline("transform produced no output".to_string()))?
    }
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
