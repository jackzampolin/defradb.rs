use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::Persistence;

pub(crate) async fn create_document_acp<S>(
    store: Arc<S>,
    persistence: Persistence,
    config: &crate::DocumentAcpConfig,
) -> Result<(
    Arc<dyn acp::DocumentACP>,
    Option<Arc<dyn acp::ZanzibarStore>>,
    Option<Arc<sourcehub::SourceHubDocumentACP>>,
)>
where
    S: storage::corekv::Store + 'static,
{
    match config {
        crate::DocumentAcpConfig::SourceHub(sourcehub_config) => {
            let tuning = sourcehub::AcpTuning::default();
            let provider = Arc::new(
                sourcehub::CosmosProvider::new(
                    sourcehub_config.grpc_address.clone(),
                    sourcehub_config.comet_rpc_address.clone(),
                    &sourcehub_config.signer_key,
                    &sourcehub_config.chain_id,
                    &tuning,
                )
                .map_err(|error| anyhow!("failed to create SourceHub provider: {error}"))?,
            );
            let sh_acp = Arc::new(sourcehub::SourceHubDocumentACP::new(
                provider,
                tuning.cache_ttl,
            ));
            Ok((sh_acp.clone(), None, Some(sh_acp)))
        }
        crate::DocumentAcpConfig::Local => match persistence {
            Persistence::Persistent => {
                let zanzibar_store = Arc::new(acp::PersistentZanzibarStore::from_store(store));
                let document_acp = Arc::new(acp::ZanzibarDocumentACP::new(zanzibar_store.clone()));
                Ok((document_acp, Some(zanzibar_store), None))
            }
            Persistence::Memory => {
                let zanzibar_store = Arc::new(acp::MemoryZanzibarStore::new());
                let document_acp = Arc::new(acp::ZanzibarDocumentACP::new(zanzibar_store.clone()));
                Ok((document_acp, Some(zanzibar_store), None))
            }
        },
    }
}

pub(crate) async fn create_nac_manager<S>(
    store: Arc<S>,
    persistence: Persistence,
) -> Result<Arc<dyn db::NacManagerApi>>
where
    S: storage::corekv::Store + 'static,
{
    match persistence {
        Persistence::Persistent => {
            let nac_store = Arc::new(acp::PersistentZanzibarStore::from_store(store));
            let nac_config = db::NacConfig::new().with_dev_mode();
            let manager = Arc::new(db::NacManager::new(nac_store, nac_config));
            manager.initialize(None).await.map_err(|error| {
                anyhow!("failed to initialize NAC from persistent store: {error}")
            })?;
            Ok(manager)
        }
        Persistence::Memory => {
            let nac_store = Arc::new(acp::MemoryZanzibarStore::new());
            let nac_config = db::NacConfig::new().with_dev_mode();
            Ok(Arc::new(db::NacManager::new(nac_store, nac_config)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::create_document_acp;
    use crate::{DocumentAcpConfig, Persistence};
    use acp::{DocumentPermission, Identity, StorePolicyOptions};
    use identity::Did;
    use std::sync::Arc;

    fn did(value: &str) -> Did {
        Did::new(value).unwrap()
    }

    #[tokio::test]
    async fn local_document_acp_enforces_stored_custom_policy() {
        let store = Arc::new(storage::MemoryStore::new());
        let (document_acp, local_store, sourcehub_acp) =
            create_document_acp(store, Persistence::Memory, &DocumentAcpConfig::Local)
                .await
                .unwrap();

        assert!(sourcehub_acp.is_none());
        let local_store = local_store.expect("local ACP should expose a Zanzibar store");

        let parsed = acp::policy_yaml::parse_policy_yaml(
            r#"
name: Viewer Policy
resources:
  - name: users
    relations:
      - name: viewer
      - name: editor
      - name: remover
    permissions:
      - name: read
        expr: viewer
      - name: update
        expr: editor
      - name: delete
        expr: remover
"#,
        )
        .unwrap();
        let policy = acp::policy_yaml::build_policy(&parsed, 1).unwrap();
        let options = StorePolicyOptions::new()
            .with_validation()
            .with_dpi_enforcement();
        local_store
            .store_policy_with_options(&policy, &options)
            .await
            .unwrap();

        let owner = did("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");
        let viewer = did("did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR");

        document_acp
            .register_doc_object(&owner, &policy.id, "users", "doc1")
            .await
            .unwrap();
        document_acp
            .add_actor_relationship(&owner, &viewer, &policy.id, "users", "doc1", "viewer", &[])
            .await
            .unwrap();

        assert!(document_acp
            .check_doc_access(
                &Identity::Authenticated(viewer.clone()),
                DocumentPermission::Read,
                &policy.id,
                "users",
                "doc1",
            )
            .await
            .unwrap());
        assert!(!document_acp
            .check_doc_access(
                &Identity::Authenticated(viewer),
                DocumentPermission::Update,
                &policy.id,
                "users",
                "doc1",
            )
            .await
            .unwrap());
    }
}
