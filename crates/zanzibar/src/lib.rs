//! Standalone Zanzibar permission engine with pluggable KV backend.
//!
//! Implements the Google Zanzibar permission model:
//! - Policy-based resource/relation definitions
//! - Userset rewrite rules (computed usersets, tuple-to-userset)
//! - Set operations (union, intersection, difference)
//! - Goal-tree search with cycle detection
//!
//! Storage is pluggable via the `ZanzibarStore` trait. Consumers implement
//! the trait against their own KV store (e.g., redb, QMDB, rocksdb).

pub mod did;
pub mod engine;
pub mod error;
pub mod expression;
pub mod lookup;
pub mod store;
pub mod thread_bounds;
pub mod types;

pub use did::Did;
pub use engine::{
    EvaluationStep, EvaluationTrace, PermissionCheckRequest, PermissionEngine,
    PermissionExplanation, StepResult,
};
pub use expression::RelationExpression;
pub use lookup::PolicyLookupTable;
pub use store::{MemoryZanzibarStore, StorePolicyOptions, ZanzibarStore};
pub use types::{ObjectRef, Policy, Relation, Relationship, Resource, Subject, SubjectRestriction};
