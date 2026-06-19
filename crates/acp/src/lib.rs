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
//! - Every permission expression MUST include `owner` access
//! - Difference (`-`) and tuple-to-userset (`->`) are allowed
//! - Intersection (`&`) is not allowed in DPI-enforced policies

mod auth_error;
mod dac;
pub mod error;
mod identity;
mod local;
pub mod nac;
mod permission;
mod persistent;
pub mod policy_yaml;
mod relation;
mod store;
mod target_subject;
mod validation;
pub mod zanzibar;

pub use auth_error::normalize_auth_error;
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
pub use target_subject::parse_target_subject;
pub use validation::{validate_resource_interface, REQUIRED_DOCUMENT_PERMISSIONS};

// Re-export key zanzibar engine types from the standalone zanzibar crate
pub use zanzibar::PersistentZanzibarStore;
pub use zanzibar::ZanzibarDocumentACP;

pub use ::zanzibar::{
    EvaluationStep, EvaluationTrace, MemoryZanzibarStore, PermissionCheckRequest, PermissionEngine,
    PermissionExplanation, Policy, Relation, RelationExpression, Relationship, Resource,
    StepResult, StorePolicyOptions, Subject, SubjectRestriction, ZanzibarStore,
};

// Re-export NAC types
pub use nac::{
    create_node_policy, is_valid_nac_relation, validate_node_policy, NacStatus, NodeACP,
    NodeAcpOperations, NodePermission, ADMIN_RELATION as NAC_ADMIN_RELATION, NODE_OBJECT_ID,
    NODE_POLICY_ID, NODE_RESOURCE_NAME, OWNER_RELATION as NAC_OWNER_RELATION, VALID_NAC_RELATIONS,
};
