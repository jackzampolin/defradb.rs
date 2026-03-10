//! Router configuration and route definitions.

mod routes;
mod state;
mod traits;

pub use routes::{create_router, create_router_with_rest, create_router_with_state};
pub use state::{AppState, AppStateBuilder};
pub use traits::{
    AcpLightClientStatus, AcpOperations, BackupOperations, BlockOperations,
    CollectionManagementOperations, DocumentAcpOperations, DumpOperations, EncryptedIndexInfo,
    EncryptedIndexOperations, ImportResult, IndexFieldInfo, IndexInfo, IndexOperations,
    LensOperations, NacStatus, NacStatusInfo, NodeAcpOperations, NodePermission, P2POperations,
    P2pDocumentInfo, P2pDocumentRequest, PolicyInfo, ReplicatorInfo, SchemaOperations,
    SyncBranchableRequest, SyncDocumentsRequest, SyncVersionsRequest, TransactionOperations,
    ViewOperations,
};
