//! Application state and builder.

use std::sync::Arc;

use query::executor::QueryExecutor;
use query::rest::RestOperations;
use query::QueryLimits;

use super::{
    AcpOperations, BackupOperations, BlockOperations, BrowserSyncOperations,
    CollectionManagementOperations, CollectionVersionOperations, DocumentAcpOperations,
    DumpOperations, EncryptedIndexOperations, IndexOperations, LensOperations, ManageRequester,
    NodeAcpOperations, P2POperations, SchemaOperations, TransactionOperations, ViewOperations,
};

struct CollectionVersionsFromManagement(Arc<dyn CollectionManagementOperations>);

#[async_trait::async_trait]
impl CollectionVersionOperations for CollectionVersionsFromManagement {
    async fn get_all_collections(&self) -> Result<Vec<schema::CollectionVersion>, String> {
        self.0.get_all_collections().await
    }
}

/// Application state shared across handlers.
#[derive(Clone)]
pub struct AppState {
    pub executor: Arc<dyn QueryExecutor>,
    pub rest: Option<Arc<dyn RestOperations>>,
    pub p2p: Option<Arc<dyn P2POperations>>,
    pub manage: Option<Arc<dyn ManageRequester>>,
    pub acp: Option<Arc<dyn AcpOperations>>,
    pub index: Option<Arc<dyn IndexOperations>>,
    pub encrypted_index: Option<Arc<dyn EncryptedIndexOperations>>,
    pub backup: Option<Arc<dyn BackupOperations>>,
    pub block: Option<Arc<dyn BlockOperations>>,
    pub browser_sync: Option<Arc<dyn BrowserSyncOperations>>,
    pub schema: Option<Arc<dyn SchemaOperations>>,
    pub lens: Option<Arc<dyn LensOperations>>,
    pub nac: Option<Arc<dyn NodeAcpOperations>>,
    pub collection_versions: Option<Arc<dyn CollectionVersionOperations>>,
    pub collection_mgmt: Option<Arc<dyn CollectionManagementOperations>>,
    pub doc_acp: Option<Arc<dyn DocumentAcpOperations>>,
    pub view: Option<Arc<dyn ViewOperations>>,
    pub dump: Option<Arc<dyn DumpOperations>>,
    pub txn_ops: Option<Arc<dyn TransactionOperations>>,
    pub event_bus: Option<Arc<dyn events::Bus>>,
    pub node_identity_did: Option<String>,
    pub signing_enabled: bool,
    pub dev_mode: bool,
    pub max_txn_retries: u32,
    pub query_limits: QueryLimits,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("executor", &"<QueryExecutor>")
            .field("rest", &self.rest.as_ref().map(|_| "<RestOperations>"))
            .field("p2p", &self.p2p.as_ref().map(|_| "<P2POperations>"))
            .field("manage", &self.manage.as_ref().map(|_| "<ManageRequester>"))
            .field("acp", &self.acp.as_ref().map(|_| "<AcpOperations>"))
            .field("index", &self.index.as_ref().map(|_| "<IndexOperations>"))
            .field(
                "encrypted_index",
                &self
                    .encrypted_index
                    .as_ref()
                    .map(|_| "<EncryptedIndexOperations>"),
            )
            .field(
                "backup",
                &self.backup.as_ref().map(|_| "<BackupOperations>"),
            )
            .field("block", &self.block.as_ref().map(|_| "<BlockOperations>"))
            .field(
                "browser_sync",
                &self
                    .browser_sync
                    .as_ref()
                    .map(|_| "<BrowserSyncOperations>"),
            )
            .field(
                "schema",
                &self.schema.as_ref().map(|_| "<SchemaOperations>"),
            )
            .field("lens", &self.lens.as_ref().map(|_| "<LensOperations>"))
            .field("nac", &self.nac.as_ref().map(|_| "<NodeAcpOperations>"))
            .field(
                "collection_versions",
                &self
                    .collection_versions
                    .as_ref()
                    .map(|_| "<CollectionVersionOperations>"),
            )
            .field(
                "collection_mgmt",
                &self
                    .collection_mgmt
                    .as_ref()
                    .map(|_| "<CollectionManagementOperations>"),
            )
            .field(
                "doc_acp",
                &self.doc_acp.as_ref().map(|_| "<DocumentAcpOperations>"),
            )
            .field("view", &self.view.as_ref().map(|_| "<ViewOperations>"))
            .field("dump", &self.dump.as_ref().map(|_| "<DumpOperations>"))
            .field(
                "txn_ops",
                &self.txn_ops.as_ref().map(|_| "<TransactionOperations>"),
            )
            .field("event_bus", &self.event_bus.as_ref().map(|_| "<EventBus>"))
            .field("node_identity_did", &self.node_identity_did)
            .field("dev_mode", &self.dev_mode)
            .field("max_txn_retries", &self.max_txn_retries)
            .field("query_limits", &self.query_limits)
            .finish()
    }
}

impl AppState {
    /// Get P2P operations or return ServiceUnavailable error.
    pub fn require_p2p(&self) -> Result<&Arc<dyn P2POperations>, crate::error::HttpError> {
        self.p2p.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "P2P networking is not enabled. Start the server with P2P enabled to use this feature.".into()
            )
        })
    }

    /// Get the management requester or return ServiceUnavailable error.
    pub fn require_manage(&self) -> Result<&Arc<dyn ManageRequester>, crate::error::HttpError> {
        self.manage.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "P2P management is not enabled. Start the server with P2P enabled to use this feature.".into()
            )
        })
    }

    /// Get ACP operations or return ServiceUnavailable error.
    pub fn require_acp(&self) -> Result<&Arc<dyn AcpOperations>, crate::error::HttpError> {
        self.acp.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "ACP (Access Control Policy) is not enabled. Start the server with ACP enabled to use this feature.".into()
            )
        })
    }

    /// Get index operations or return ServiceUnavailable error.
    pub fn require_index(&self) -> Result<&Arc<dyn IndexOperations>, crate::error::HttpError> {
        self.index.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "Index operations are not enabled. Start the server with indexing enabled to use this feature.".into()
            )
        })
    }

    /// Get encrypted index operations or return ServiceUnavailable error.
    pub fn require_encrypted_index(
        &self,
    ) -> Result<&Arc<dyn EncryptedIndexOperations>, crate::error::HttpError> {
        self.encrypted_index.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "Encrypted index operations are not enabled.".into(),
            )
        })
    }

    /// Get block operations or return ServiceUnavailable error.
    pub fn require_block(&self) -> Result<&Arc<dyn BlockOperations>, crate::error::HttpError> {
        self.block.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable("Block operations are not enabled.".into())
        })
    }

    /// Get browser sync operations or return ServiceUnavailable error.
    pub fn require_browser_sync(
        &self,
    ) -> Result<&Arc<dyn BrowserSyncOperations>, crate::error::HttpError> {
        self.browser_sync.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "browser synchronization is not enabled".into(),
            )
        })
    }

    /// Get backup operations or return ServiceUnavailable error.
    pub fn require_backup(&self) -> Result<&Arc<dyn BackupOperations>, crate::error::HttpError> {
        self.backup.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "Backup operations are not enabled. Start the server with backup enabled to use this feature.".into()
            )
        })
    }

    /// Get schema operations or return ServiceUnavailable error.
    pub fn require_schema(&self) -> Result<&Arc<dyn SchemaOperations>, crate::error::HttpError> {
        self.schema.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "Schema operations are not enabled. Start the server with schema enabled to use this feature.".into()
            )
        })
    }

    /// Get lens operations or return ServiceUnavailable error.
    pub fn require_lens(&self) -> Result<&Arc<dyn LensOperations>, crate::error::HttpError> {
        self.lens.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "Lens operations are not enabled. Start the server with lens enabled to use this feature.".into()
            )
        })
    }

    /// Get collection management operations or return ServiceUnavailable error.
    pub fn require_collection_mgmt(
        &self,
    ) -> Result<&Arc<dyn CollectionManagementOperations>, crate::error::HttpError> {
        self.collection_mgmt.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "Collection management operations are not enabled.".into(),
            )
        })
    }

    /// Get read-only collection-version operations or return ServiceUnavailable.
    pub fn require_collection_versions(
        &self,
    ) -> Result<&Arc<dyn CollectionVersionOperations>, crate::error::HttpError> {
        self.collection_versions.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "Collection version observation is not enabled.".into(),
            )
        })
    }

    /// Get document ACP operations or return ServiceUnavailable error.
    pub fn require_doc_acp(
        &self,
    ) -> Result<&Arc<dyn DocumentAcpOperations>, crate::error::HttpError> {
        self.doc_acp.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "Document ACP operations are not enabled. Start the server with ACP enabled to use this feature.".into(),
            )
        })
    }

    /// Get dump operations or return ServiceUnavailable error.
    pub fn require_dump(&self) -> Result<&Arc<dyn DumpOperations>, crate::error::HttpError> {
        self.dump.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable("Dump operations are not enabled.".into())
        })
    }

    /// Get view operations or return ServiceUnavailable error.
    pub fn require_view(&self) -> Result<&Arc<dyn ViewOperations>, crate::error::HttpError> {
        self.view.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable("View operations are not enabled.".into())
        })
    }

    /// Get transaction operations or return ServiceUnavailable error.
    pub fn require_txn_ops(
        &self,
    ) -> Result<&Arc<dyn TransactionOperations>, crate::error::HttpError> {
        self.txn_ops.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "Transaction operations are not enabled.".into(),
            )
        })
    }

    /// Get NAC operations or return ServiceUnavailable error.
    pub fn require_nac(&self) -> Result<&Arc<dyn NodeAcpOperations>, crate::error::HttpError> {
        self.nac.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "NAC (Node Access Control) is not enabled. Start the server with --node-acp-enable to use this feature.".into()
            )
        })
    }
}

/// Builder for constructing AppState with optional components.
pub struct AppStateBuilder {
    executor: Arc<dyn QueryExecutor>,
    rest: Option<Arc<dyn RestOperations>>,
    p2p: Option<Arc<dyn P2POperations>>,
    manage: Option<Arc<dyn ManageRequester>>,
    acp: Option<Arc<dyn AcpOperations>>,
    index: Option<Arc<dyn IndexOperations>>,
    encrypted_index: Option<Arc<dyn EncryptedIndexOperations>>,
    backup: Option<Arc<dyn BackupOperations>>,
    block: Option<Arc<dyn BlockOperations>>,
    browser_sync: Option<Arc<dyn BrowserSyncOperations>>,
    schema: Option<Arc<dyn SchemaOperations>>,
    lens: Option<Arc<dyn LensOperations>>,
    nac: Option<Arc<dyn NodeAcpOperations>>,
    collection_versions: Option<Arc<dyn CollectionVersionOperations>>,
    collection_mgmt: Option<Arc<dyn CollectionManagementOperations>>,
    doc_acp: Option<Arc<dyn DocumentAcpOperations>>,
    view: Option<Arc<dyn ViewOperations>>,
    dump: Option<Arc<dyn DumpOperations>>,
    txn_ops: Option<Arc<dyn TransactionOperations>>,
    event_bus: Option<Arc<dyn events::Bus>>,
    node_identity_did: Option<String>,
    signing_enabled: bool,
    dev_mode: bool,
    max_txn_retries: u32,
    query_limits: QueryLimits,
}

impl AppStateBuilder {
    /// Create a new builder with the required executor.
    pub fn new(executor: Arc<dyn QueryExecutor>) -> Self {
        Self {
            executor,
            rest: None,
            p2p: None,
            manage: None,
            acp: None,
            index: None,
            encrypted_index: None,
            backup: None,
            block: None,
            browser_sync: None,
            schema: None,
            lens: None,
            nac: None,
            collection_versions: None,
            collection_mgmt: None,
            doc_acp: None,
            view: None,
            dump: None,
            txn_ops: None,
            event_bus: None,
            node_identity_did: None,
            signing_enabled: false,
            dev_mode: false,
            max_txn_retries: db::DEFAULT_MAX_TXN_RETRIES,
            query_limits: QueryLimits::default(),
        }
    }

    /// Set REST operations.
    pub fn with_rest(mut self, rest: Arc<dyn RestOperations>) -> Self {
        self.rest = Some(rest);
        self
    }

    /// Set P2P operations.
    pub fn with_p2p(mut self, p2p: Arc<dyn P2POperations>) -> Self {
        self.p2p = Some(p2p);
        self
    }

    /// Set the remote management requester.
    pub fn with_manage(mut self, manage: Arc<dyn ManageRequester>) -> Self {
        self.manage = Some(manage);
        self
    }

    /// Set ACP operations.
    pub fn with_acp(mut self, acp: Arc<dyn AcpOperations>) -> Self {
        self.acp = Some(acp);
        self
    }

    /// Set index operations.
    pub fn with_index(mut self, index: Arc<dyn IndexOperations>) -> Self {
        self.index = Some(index);
        self
    }

    /// Set encrypted index operations.
    pub fn with_encrypted_index(
        mut self,
        encrypted_index: Arc<dyn EncryptedIndexOperations>,
    ) -> Self {
        self.encrypted_index = Some(encrypted_index);
        self
    }

    /// Set backup operations.
    pub fn with_backup(mut self, backup: Arc<dyn BackupOperations>) -> Self {
        self.backup = Some(backup);
        self
    }

    /// Set block operations.
    pub fn with_block(mut self, block: Arc<dyn BlockOperations>) -> Self {
        self.block = Some(block);
        self
    }

    /// Set browser sync operations.
    pub fn with_browser_sync(mut self, browser_sync: Arc<dyn BrowserSyncOperations>) -> Self {
        self.browser_sync = Some(browser_sync);
        self
    }

    /// Set schema operations.
    pub fn with_schema(mut self, schema: Arc<dyn SchemaOperations>) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set lens operations.
    pub fn with_lens(mut self, lens: Arc<dyn LensOperations>) -> Self {
        self.lens = Some(lens);
        self
    }

    /// Set NAC (Node Access Control) operations.
    pub fn with_nac(mut self, nac: Arc<dyn NodeAcpOperations>) -> Self {
        self.nac = Some(nac);
        self
    }

    /// Set collection management operations.
    pub fn with_collection_mgmt(
        mut self,
        collection_mgmt: Arc<dyn CollectionManagementOperations>,
    ) -> Self {
        self.collection_versions = Some(Arc::new(CollectionVersionsFromManagement(Arc::clone(
            &collection_mgmt,
        ))));
        self.collection_mgmt = Some(collection_mgmt);
        self
    }

    /// Set read-only collection-version operations.
    pub fn with_collection_versions(
        mut self,
        collection_versions: Arc<dyn CollectionVersionOperations>,
    ) -> Self {
        self.collection_versions = Some(collection_versions);
        self
    }

    /// Set document ACP operations.
    pub fn with_doc_acp(mut self, doc_acp: Arc<dyn DocumentAcpOperations>) -> Self {
        self.doc_acp = Some(doc_acp);
        self
    }

    /// Set view operations.
    pub fn with_view(mut self, view: Arc<dyn ViewOperations>) -> Self {
        self.view = Some(view);
        self
    }

    /// Set dump operations.
    pub fn with_dump(mut self, dump: Arc<dyn DumpOperations>) -> Self {
        self.dump = Some(dump);
        self
    }

    /// Set transaction operations.
    pub fn with_txn_ops(mut self, txn_ops: Arc<dyn TransactionOperations>) -> Self {
        self.txn_ops = Some(txn_ops);
        self
    }

    /// Set event bus for subscriptions.
    pub fn with_event_bus(mut self, bus: Arc<dyn events::Bus>) -> Self {
        self.event_bus = Some(bus);
        self
    }

    /// Set the node identity DID for signing config fallback.
    pub fn with_node_identity_did(mut self, did: String) -> Self {
        self.node_identity_did = Some(did);
        self
    }

    /// Enable block signing for anonymous requests.
    pub fn with_signing_enabled(mut self, enabled: bool) -> Self {
        self.signing_enabled = enabled;
        self
    }

    /// Enable development mode (allows purge and other dev-only operations).
    pub fn with_dev_mode(mut self, dev_mode: bool) -> Self {
        self.dev_mode = dev_mode;
        self
    }

    /// Set the maximum number of auto-commit transaction conflict retries.
    pub fn with_max_txn_retries(mut self, retries: u32) -> Self {
        self.max_txn_retries = retries;
        self
    }

    /// Set GraphQL parsing and filter evaluation limits.
    pub fn with_query_limits(mut self, limits: QueryLimits) -> Self {
        self.query_limits = limits;
        self
    }

    /// Build the AppState.
    pub fn build(self) -> AppState {
        AppState {
            executor: self.executor,
            rest: self.rest,
            p2p: self.p2p,
            manage: self.manage,
            acp: self.acp,
            index: self.index,
            encrypted_index: self.encrypted_index,
            backup: self.backup,
            block: self.block,
            browser_sync: self.browser_sync,
            schema: self.schema,
            lens: self.lens,
            nac: self.nac,
            collection_versions: self.collection_versions,
            collection_mgmt: self.collection_mgmt,
            doc_acp: self.doc_acp,
            view: self.view,
            dump: self.dump,
            txn_ops: self.txn_ops,
            event_bus: self.event_bus,
            node_identity_did: self.node_identity_did,
            signing_enabled: self.signing_enabled,
            dev_mode: self.dev_mode,
            max_txn_retries: self.max_txn_retries,
            query_limits: self.query_limits,
        }
    }
}
