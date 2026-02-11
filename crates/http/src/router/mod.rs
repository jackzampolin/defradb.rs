//! Router configuration and route definitions.

mod routes;
mod state;
mod traits;

pub use routes::{create_router, create_router_with_rest, create_router_with_state};
pub use state::{AppState, AppStateBuilder};
pub use traits::{
    AcpOperations, BackupOperations, CollectionManagementOperations, DocumentAcpOperations,
    ImportResult, IndexFieldInfo, IndexInfo, IndexOperations, LensOperations, NacStatus,
    NacStatusInfo, NodeAcpOperations, NodePermission, P2POperations, P2pDocumentInfo,
    P2pDocumentRequest, PolicyInfo, ReplicatorInfo, SchemaOperations,
};
