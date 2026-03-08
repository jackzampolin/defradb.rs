/// FUSE filesystem overlay for DefraDB.
///
/// Maps collections to directories and documents to JSON files,
/// enabling AI agents to interact with DefraDB using standard
/// filesystem operations (ls, cat, echo, rm).
///
/// # Layout
///
/// ```text
/// <mountpoint>/
/// ├── <collection_name>/           # directory per collection
/// │   ├── _schema.graphql          # virtual: collection SDL (read-only)
/// │   ├── _view.json               # virtual: all docs as JSON array (read-only)
/// │   ├── alice.json               # document (via _name field)
/// │   ├── <bae-docid>.json         # document (raw docID)
/// │   └── ...
/// └── ...
/// ```
mod errno;
mod error;
mod inode;
mod mount;
mod ops;
mod virtual_files;

pub use error::Error;
pub use mount::{mount, MountHandle};
