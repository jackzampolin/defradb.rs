//! Mock executors for testing HTTP routes.

mod acp;
mod backup;
mod block;
mod collections;
mod doc_acp;
mod encrypted_index;
mod index;
mod lens;
mod nac;
mod p2p;
mod query;
mod rest;
mod txn_ops;

pub use acp::{FailingMockAcpOperations, MockAcpOperations};
pub use backup::{FailingMockBackupOperations, MockBackupOperations};
pub use block::MockBlockOperations;
pub use collections::MockCollectionManagementOperations;
pub use doc_acp::MockDocumentAcpOperations;
pub use encrypted_index::MockEncryptedIndexOperations;
pub use index::{FailingMockIndexOperations, MockIndexOperations};
pub use lens::MockLensOperations;
pub use nac::{FailingMockNodeAcpOperations, MockNodeAcpOperations};
pub use p2p::{FailingMockP2POperations, MockP2POperations};
pub use query::{FailingMockExecutor, MockQueryExecutor};
pub use rest::{FailingMockRestOperations, MockRestOperations};
pub use txn_ops::MockTransactionOperations;
