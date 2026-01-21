//! Zanzibar permission model implementation.
//!
//! This module implements the full Zanzibar permission model with:
//! - Policy-based resource/relation definitions
//! - Userset rewrite rules (computed usersets, tuple-to-userset)
//! - Set operations (union, intersection, difference)
//! - Goal-tree search with cycle detection

mod acp;
mod engine;
mod expression;
mod lookup;
mod store;
mod types;

pub use acp::ZanzibarDocumentACP;
pub use engine::PermissionEngine;
pub use expression::RelationExpression;
pub use lookup::PolicyLookupTable;
pub use store::{MemoryZanzibarStore, PersistentZanzibarStore, ZanzibarStore};
pub use types::{Policy, Relation, Relationship, Resource, Subject};
