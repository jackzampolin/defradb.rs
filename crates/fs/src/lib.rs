/// FUSE filesystem overlay for DefraDB.
///
/// Maps collections to directories and documents to JSON files,
/// enabling AI agents to interact with DefraDB using standard
/// filesystem operations (ls, cat, grep, jq).
///
/// # Layout
///
/// ```text
/// <mountpoint>/
/// ├── _schema.graphql              # virtual: all types (read-only)
/// ├── _collections.json            # virtual: collection list + doc counts (read-only)
/// ├── <collection_name>/           # directory per collection
/// │   ├── _schema.graphql          # virtual: collection SDL (read-only)
/// │   ├── _view.json               # virtual: all docs as JSON array (read-only, greppable)
/// │   ├── alice.json               # document (via _name field)
/// │   ├── <bae-docid>.json         # document (raw docID)
/// │   └── ...
/// └── ...
/// ```
///
/// # Agent Workflow
///
/// The primary search surface is `_view.json` per collection — a materialized
/// view of all documents as a JSON array. Agents should `grep` or `jq` this
/// file rather than iterating individual doc files.
#[cfg_attr(not(feature = "fuse"), allow(dead_code))]
pub(crate) mod cache;
#[cfg_attr(not(feature = "fuse"), allow(dead_code))]
pub(crate) mod errno;
mod error;
#[cfg_attr(not(feature = "fuse"), allow(dead_code))]
pub(crate) mod inode;
#[cfg_attr(not(feature = "fuse"), allow(dead_code))]
pub(crate) mod virtual_files;

#[cfg(feature = "fuse")]
mod mount;
#[cfg(feature = "fuse")]
mod ops;

pub use error::Error;
#[cfg(feature = "fuse")]
pub use mount::{mount, MountHandle};
