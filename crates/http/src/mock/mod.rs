//! Mock executors for testing HTTP routes.

mod acp;
mod backup;
mod collections;
mod doc_acp;
mod index;
mod lens;
mod nac;
mod p2p;
mod query;
mod rest;

pub use acp::{FailingMockAcpOperations, MockAcpOperations};
pub use backup::{FailingMockBackupOperations, MockBackupOperations};
pub use collections::MockCollectionManagementOperations;
pub use doc_acp::MockDocumentAcpOperations;
pub use index::{FailingMockIndexOperations, MockIndexOperations};
pub use lens::MockLensOperations;
pub use nac::{FailingMockNodeAcpOperations, MockNodeAcpOperations};
pub use p2p::{FailingMockP2POperations, MockP2POperations};
pub use query::{FailingMockExecutor, MockQueryExecutor};
pub use rest::{FailingMockRestOperations, MockRestOperations};
