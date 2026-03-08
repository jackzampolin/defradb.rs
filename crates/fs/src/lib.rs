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
/// │   ├── <bae-docid>.json         # file per document
/// │   └── ...
/// └── ...
/// ```
mod errno;
mod error;
mod inode;
mod mount;
mod ops;

pub use error::Error;
pub use mount::{mount, MountHandle, MountOptions};
