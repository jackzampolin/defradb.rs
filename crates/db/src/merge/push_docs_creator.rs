use std::fmt;

use crate::Collection;
use acp::DocumentACP;

#[derive(Debug)]
pub enum PushCreatorError {
    AcpUnavailable {
        collection: String,
        collection_id: String,
        doc_id: String,
    },
    LookupFailed {
        collection: String,
        collection_id: String,
        doc_id: String,
        errors: Vec<String>,
    },
    OwnerMissing {
        collection: String,
        collection_id: String,
        doc_id: String,
    },
}

impl fmt::Display for PushCreatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AcpUnavailable {
                collection,
                collection_id,
                doc_id,
            } => write!(
                f,
                "ACP is unavailable for protected replay document {collection}/{collection_id}/{doc_id}"
            ),
            Self::LookupFailed {
                collection,
                collection_id,
                doc_id,
                errors,
            } => write!(
                f,
                "failed to resolve ACP owner for replay document {collection}/{collection_id}/{doc_id}: {}",
                errors.join("; ")
            ),
            Self::OwnerMissing {
                collection,
                collection_id,
                doc_id,
            } => write!(
                f,
                "ACP owner is missing for protected replay document {collection}/{collection_id}/{doc_id}"
            ),
        }
    }
}

pub async fn resolve_push_creator(
    document_acp: Option<&dyn DocumentACP>,
    collection: &Collection,
    doc_id: &str,
    fallback_creator: &str,
) -> Result<String, PushCreatorError> {
    let Some(policy) = &collection.schema().policy else {
        return Ok(fallback_creator.to_string());
    };

    let mut resource_names = vec![policy.resource_name.clone()];
    for candidate in [
        collection.name().to_string(),
        collection.name().to_lowercase(),
        format!("{}s", collection.name().to_lowercase()),
    ] {
        if !resource_names.iter().any(|existing| existing == &candidate) {
            resource_names.push(candidate);
        }
    }

    let Some(acp) = document_acp else {
        return Err(PushCreatorError::AcpUnavailable {
            collection: collection.name().to_string(),
            collection_id: collection.collection_id().to_string(),
            doc_id: doc_id.to_string(),
        });
    };

    let mut lookup_errors = Vec::new();
    for resource_name in &resource_names {
        match acp.get_doc_owner(&policy.id, resource_name, doc_id).await {
            Ok(Some(owner)) => {
                if resource_name != &policy.resource_name {
                    tracing::info!(
                        collection = %collection.name(),
                        collection_id = %collection.collection_id(),
                        resource_name = %resource_name,
                        doc_id = %doc_id,
                        "Resolved ACP owner for replicator push using fallback resource name"
                    );
                }
                return Ok(owner.to_string());
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(
                    collection = %collection.name(),
                    collection_id = %collection.collection_id(),
                    resource_name = %resource_name,
                    doc_id = %doc_id,
                    error = %error,
                    "Failed to resolve ACP owner for replicator push"
                );
                lookup_errors.push(format!("{resource_name}: {error}"));
            }
        }
    }

    if lookup_errors.is_empty() {
        Err(PushCreatorError::OwnerMissing {
            collection: collection.name().to_string(),
            collection_id: collection.collection_id().to_string(),
            doc_id: doc_id.to_string(),
        })
    } else {
        Err(PushCreatorError::LookupFailed {
            collection: collection.name().to_string(),
            collection_id: collection.collection_id().to_string(),
            doc_id: doc_id.to_string(),
            errors: lookup_errors,
        })
    }
}
