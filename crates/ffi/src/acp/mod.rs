//! Access Control Policy (ACP) operations for FFI.
//!
//! This module exposes ACP management functions for both:
//! - NAC (Node Access Control) - node-level permissions
//! - DAC (Document Access Control) - document-level permissions

mod dac;
mod identity;
mod nac;

pub use dac::{
    add_dac_actor_relationship, add_dac_policy, delete_dac_actor_relationship, get_dac_policy,
    list_dac_policies,
};
pub use identity::{create_identity, get_node_identity, RegisterIdentity};
pub use nac::{
    add_nac_actor_relationship, delete_nac_actor_relationship, disable_nac, enable_nac,
    get_nac_status, re_enable_nac,
};
