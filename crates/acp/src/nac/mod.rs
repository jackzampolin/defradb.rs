//! Node Access Control (NAC) module.
//!
//! NAC provides node-level access control using the Zanzibar permission model.
//! Unlike Document Access Control (DAC) which operates at the document level,
//! NAC controls access to node-level operations like:
//!
//! - Enabling/disabling DAC
//! - Managing replicators
//! - Schema modifications
//! - Index management
//! - P2P operations
//!
//! # Default Behavior
//!
//! NAC is disabled by default. When disabled, all node operations are allowed
//! without authentication. To enable NAC, start the node with `--node-acp-enable`.
//!
//! # Architecture
//!
//! NAC uses a local Zanzibar store (separate from DAC) with a built-in policy
//! containing 33 permission relations. The policy has:
//!
//! - `owner` relation: the node identity that enabled NAC
//! - `admin` relation: identities with admin access
//! - 33 permission relations: one for each `NodePermission`
//!
//! All permissions have expression `owner + admin`, meaning either the owner
//! or any admin can perform any operation.

mod node_acp;
mod permission;
mod policy;

pub use node_acp::{NacStatus, NodeACP, NodeAcpOperations, NODE_OBJECT_ID};
pub use permission::NodePermission;
pub use policy::{
    create_node_policy, validate_node_policy, ADMIN_RELATION, NODE_POLICY_ID, NODE_RESOURCE_NAME,
    OWNER_RELATION,
};
