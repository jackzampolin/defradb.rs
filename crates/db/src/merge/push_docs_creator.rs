use std::fmt;

use crate::Collection;
use acp::DocumentACP;

#[derive(Debug)]
pub(crate) enum PushCreatorError {
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

pub(crate) async fn resolve_push_creator(
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

#[cfg(test)]
mod tests {
    use super::*;
    use acp::{DocumentPermission, Identity, LocalDocumentACP, MemoryAcpStore};
    use identity::Did;
    use schema::{CollectionVersion, PolicyDescription};
    use std::sync::Arc;

    #[tokio::test]
    async fn resolve_push_creator_uses_owner_for_protected_document() {
        let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
        let owner = Did::new("did:key:z6Mkowner").unwrap();
        acp.register_doc_object(&owner, "policy-1", "Users", "doc1")
            .await
            .unwrap();

        let creator =
            resolve_push_creator(Some(&acp), &protected_collection(), "doc1", "local-peer")
                .await
                .unwrap();

        assert_eq!(creator, owner.to_string());
    }

    #[tokio::test]
    async fn resolve_push_creator_falls_back_only_without_collection_policy() {
        let collection = Collection::new(CollectionVersion::new("Users", "v1", "col1", vec![]));

        let creator = resolve_push_creator(None, &collection, "doc1", "local-peer")
            .await
            .unwrap();

        assert_eq!(creator, "local-peer");
    }

    #[tokio::test]
    async fn resolve_push_creator_errors_when_owner_is_missing() {
        let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));

        let error = resolve_push_creator(Some(&acp), &protected_collection(), "doc1", "local-peer")
            .await
            .unwrap_err();

        assert!(matches!(error, PushCreatorError::OwnerMissing { .. }));
    }

    #[tokio::test]
    async fn resolve_push_creator_errors_when_acp_lookup_fails() {
        let error = resolve_push_creator(
            Some(&FailingAcp),
            &protected_collection(),
            "doc1",
            "local-peer",
        )
        .await
        .unwrap_err();

        assert!(matches!(error, PushCreatorError::LookupFailed { .. }));
    }

    fn protected_collection() -> Collection {
        Collection::new(
            CollectionVersion::new("Users", "v1", "col1", vec![])
                .with_policy(PolicyDescription::new("policy-1", "Users")),
        )
    }

    struct FailingAcp;

    #[async_trait::async_trait]
    impl DocumentACP for FailingAcp {
        async fn register_doc_object(
            &self,
            _identity: &Did,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> acp::Result<()> {
            Err(acp::Error::Storage("boom".to_string()))
        }

        async fn is_doc_registered(
            &self,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> acp::Result<bool> {
            Err(acp::Error::Storage("boom".to_string()))
        }

        async fn get_doc_owner(
            &self,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> acp::Result<Option<Did>> {
            Err(acp::Error::Storage("boom".to_string()))
        }

        async fn check_doc_access(
            &self,
            _identity: &Identity,
            _permission: DocumentPermission,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> acp::Result<bool> {
            Err(acp::Error::Storage("boom".to_string()))
        }

        async fn add_actor_relationship(
            &self,
            _requestor: &Did,
            _target: &Did,
            _policy_id: &str,
            _collection_id: &str,
            _doc_id: &str,
            _relation: &str,
            _managing_relations: &[String],
        ) -> acp::Result<bool> {
            Err(acp::Error::Storage("boom".to_string()))
        }

        async fn delete_actor_relationship(
            &self,
            _requestor: &Did,
            _target: &Did,
            _policy_id: &str,
            _collection_id: &str,
            _doc_id: &str,
            _relation: &str,
            _managing_relations: &[String],
        ) -> acp::Result<bool> {
            Err(acp::Error::Storage("boom".to_string()))
        }

        async fn unregister_doc_object(
            &self,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> acp::Result<()> {
            Err(acp::Error::Storage("boom".to_string()))
        }
    }
}
