//! libipld integration for full IPLD data model support.
//!
//! This module provides conversions between DefraDB block types and the
//! libipld `Ipld` data model, enabling link traversal and IPLD-native operations.
//!
//! # Submodules
//!
//! - [`cid_convert`]: CID conversion between crate versions
//! - [`to_ipld`]: Conversions from DefraDB types to IPLD
//! - [`from_ipld`]: Conversions from IPLD to DefraDB types
//! - [`traversal`]: Link traversal helpers and visitor pattern

mod cid_convert;
mod from_ipld;
mod to_ipld;
mod traversal;

pub use traversal::{collect_block_links, extract_links, walk_ipld, IpldVisitor};

#[cfg(test)]
mod tests;
