/// Inode mapping between filesystem inodes and DefraDB entities.
///
/// Inode layout:
/// - 1: root directory (mountpoint)
/// - 2..N: collection directories (allocated on first readdir of root)
/// - N+1..: document files (allocated on first readdir/lookup of collection)
use std::collections::HashMap;

/// The root directory inode (standard FUSE convention).
pub const ROOT_INO: u64 = 1;

/// Reserved field name for filesystem display names.
/// If a document has this field, its value is used as the filename
/// instead of the raw docID.
pub const NAME_FIELD: &str = "_name";

/// Represents what a filesystem inode maps to in DefraDB.
#[derive(Debug, Clone)]
pub enum InodeTarget {
    /// Root directory containing all collections.
    Root,
    /// A collection directory.
    Collection { name: String },
    /// A document JSON file within a collection.
    Document {
        collection: String,
        doc_id: String,
        /// Filesystem display name (from _name field, or doc_id if unset).
        display_name: String,
    },
}

/// Bidirectional mapping between inodes and DefraDB entities.
pub struct InodeTable {
    next_ino: u64,
    by_ino: HashMap<u64, InodeTarget>,
    collection_inos: HashMap<String, u64>,
    /// Forward: (collection, doc_id) -> inode
    doc_inos: HashMap<(String, String), u64>,
    /// Reverse: (collection, display_name) -> doc_id
    name_to_doc_id: HashMap<(String, String), String>,
}

impl InodeTable {
    pub fn new() -> Self {
        let mut by_ino = HashMap::new();
        by_ino.insert(ROOT_INO, InodeTarget::Root);

        Self {
            next_ino: 2,
            by_ino,
            collection_inos: HashMap::new(),
            doc_inos: HashMap::new(),
            name_to_doc_id: HashMap::new(),
        }
    }

    /// Get the target for an inode.
    pub fn get(&self, ino: u64) -> Option<&InodeTarget> {
        self.by_ino.get(&ino)
    }

    /// Allocate or return existing inode for a collection.
    pub fn collection_ino(&mut self, name: &str) -> u64 {
        if let Some(&ino) = self.collection_inos.get(name) {
            return ino;
        }
        let ino = self.next_ino;
        self.next_ino += 1;
        self.by_ino.insert(
            ino,
            InodeTarget::Collection {
                name: name.to_string(),
            },
        );
        self.collection_inos.insert(name.to_string(), ino);
        ino
    }

    /// Allocate or return existing inode for a document.
    /// `display_name` is either the `_name` field value or the doc_id.
    pub fn doc_ino(&mut self, collection: &str, doc_id: &str, display_name: &str) -> u64 {
        let key = (collection.to_string(), doc_id.to_string());
        if let Some(&ino) = self.doc_inos.get(&key) {
            return ino;
        }
        let ino = self.next_ino;
        self.next_ino += 1;
        self.by_ino.insert(
            ino,
            InodeTarget::Document {
                collection: collection.to_string(),
                doc_id: doc_id.to_string(),
                display_name: display_name.to_string(),
            },
        );
        self.doc_inos.insert(key, ino);
        self.name_to_doc_id.insert(
            (collection.to_string(), display_name.to_string()),
            doc_id.to_string(),
        );
        ino
    }

    /// Resolve a display name (filename without .json) to a doc_id.
    pub fn resolve_name(&self, collection: &str, display_name: &str) -> Option<&str> {
        let key = (collection.to_string(), display_name.to_string());
        self.name_to_doc_id.get(&key).map(|s| s.as_str())
    }

    /// Remove a document inode (for unlink/delete).
    pub fn remove_doc(&mut self, collection: &str, doc_id: &str) {
        let key = (collection.to_string(), doc_id.to_string());
        if let Some(ino) = self.doc_inos.remove(&key) {
            if let Some(InodeTarget::Document { display_name, .. }) = self.by_ino.remove(&ino) {
                self.name_to_doc_id
                    .remove(&(collection.to_string(), display_name));
            }
        }
    }

    /// Clear all document inodes for a collection (forces re-scan on next readdir).
    pub fn invalidate_collection(&mut self, collection: &str) {
        let doc_keys: Vec<(String, String)> = self
            .doc_inos
            .keys()
            .filter(|(c, _)| c == collection)
            .cloned()
            .collect();

        for key in doc_keys {
            if let Some(ino) = self.doc_inos.remove(&key) {
                if let Some(InodeTarget::Document { display_name, .. }) = self.by_ino.remove(&ino) {
                    self.name_to_doc_id
                        .remove(&(collection.to_string(), display_name));
                }
            }
        }
    }
}
