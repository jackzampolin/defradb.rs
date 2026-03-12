use acp::DocumentACP;

use crate::Collection;

pub(crate) async fn resolve_push_creator(
    document_acp: Option<&dyn DocumentACP>,
    collection: &Collection,
    doc_id: &str,
    fallback_creator: &str,
) -> String {
    let Some(policy) = &collection.schema().policy else {
        return fallback_creator.to_string();
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

    if let Some(acp) = document_acp {
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
                    return owner.to_string();
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
                }
            }
        }
    }

    tracing::warn!(
        collection = %collection.name(),
        collection_id = %collection.collection_id(),
        resource_names = ?resource_names,
        doc_id = %doc_id,
        fallback_creator = %fallback_creator,
        "Falling back to peer identity for replicator replay creator"
    );
    fallback_creator.to_string()
}
