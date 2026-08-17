//! Router configuration and route definitions.

mod routes;
mod state;
mod traits;

pub(crate) use routes::create_router_with_state_and_sync_body_limit;
pub use routes::{create_router, create_router_with_rest, create_router_with_state};
pub use state::{AppState, AppStateBuilder};
pub use traits::{
    AcpLightClientStatus, AcpOperations, BackupOperations, BlockOperations, BrowserSyncError,
    BrowserSyncOperations, BrowserSyncRequest, BrowserSyncResponse, BrowserSyncResult,
    CollectionManagementOperations, CollectionObservationOperations, DocumentAcpOperations,
    DumpOperations, EncryptedIndexInfo, EncryptedIndexOperations, ExplicitReplayCapabilityInput,
    ImportResult, IndexFieldInfo, IndexInfo, IndexOperations, LensOperations, ManageRequester,
    NacStatus, NacStatusInfo, NodeAcpOperations, NodePermission, P2PError, P2POperations,
    P2PResult, P2pDocumentInfo, P2pDocumentRequest, PolicyInfo, RemoteManageDocRef, RemoteManageOp,
    RemoteManageQueryOp, RemoteManageQueryResult, ReplicationFilter, ReplicationFilters,
    ReplicatorInfo, SchemaOperations, SyncBranchableRequest, SyncDocumentsRequest,
    SyncVersionsRequest, TransactionOperations, ViewOperations, MANAGE_UNAUTHORIZED,
};
