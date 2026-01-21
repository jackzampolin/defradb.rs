//! Access Control Policy (ACP) for DefraDB
//!
//! This crate provides document-level access control following Go DefraDB's pattern:
//! 1. If ACP not configured -> allow all
//! 2. Check if peer is replicator -> allow (fast path)
//! 3. Verify peer's identity token
//! 4. Check document-level ACP permissions -> allow/deny
//!
//! # Architecture
//!
//! - `DocumentACP` trait: Core interface for document access control
//! - `LocalDocumentACP`: Local implementation using in-memory storage
//! - `DocumentPermission`: Read/Update/Delete permissions
//! - `RelationTuple`: Subject-relation-object tuples for permission storage
//!
//! # Public vs Registered Documents
//!
//! - Document created WITHOUT identity -> public (unregistered) -> anyone can access
//! - Document created WITH identity -> registered -> creator is owner -> ACP enforced
//!
//! # DPI Rules (DefraDB Policy Interface)
//!
//! - Every resource MUST have `owner` relation
//! - Every permission expression MUST start with `owner`
//! - Only union (`+`) operations allowed: `owner + reader`

mod dac;
mod error;
mod identity;
mod local;
pub mod nac;
mod permission;
mod persistent;
mod relation;
mod store;
pub mod zanzibar;

pub use dac::DocumentACP;
pub use error::{Error, Result};
pub use identity::Identity;
pub use local::{LocalDocumentACP, MemoryAcpStore};
pub use permission::DocumentPermission;
pub use persistent::PersistentAcpStore;
pub use relation::{
    RelationTuple, DELETER_RELATION, OWNER_RELATION, READER_RELATION, UPDATER_RELATION,
};
pub use store::AcpStore;

// Re-export key zanzibar types
pub use zanzibar::{
    EvaluationStep, EvaluationTrace, MemoryZanzibarStore, PermissionCheckRequest, PermissionEngine,
    PermissionExplanation, PersistentZanzibarStore, Policy, Relation, RelationExpression,
    Relationship, Resource, StepResult, StorePolicyOptions, Subject, SubjectRestriction,
    ZanzibarDocumentACP, ZanzibarStore,
};

// Re-export NAC types
pub use nac::{
    create_node_policy, validate_node_policy, NacStatus, NodeACP, NodeAcpOperations,
    NodePermission, ADMIN_RELATION as NAC_ADMIN_RELATION, NODE_OBJECT_ID, NODE_POLICY_ID,
    NODE_RESOURCE_NAME, OWNER_RELATION as NAC_OWNER_RELATION,
};
