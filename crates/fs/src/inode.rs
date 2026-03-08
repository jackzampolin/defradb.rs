/// Inode mapping between filesystem inodes and DefraDB entities.
///
/// Inode layout:
/// - 1: root directory (mountpoint)
/// - 2..N: collection directories (allocated on first readdir of root)
/// - N+1..: document files (allocated on first readdir/lookup of collection)
use std::collections::HashMap;

/// The root directory inode (standard FUSE convention).
pub const ROOT_INO: u64 = 1;

/// Represents what a filesystem inode maps to in DefraDB.
#[derive(Debug, Clone)]
pub enum InodeTarget {
    /// Root directory containing all collections.
    Root,
    /// A collection directory.
    Collection { name: String },
    /// A document JSON file within a collection.
    Document { collection: String, doc_id: String },
}

/// Bidirectional mapping between inodes and DefraDB entities.
pub struct InodeTable {
    next_ino: u64,
    by_ino: HashMap<u64, InodeTarget>,
    /// Reverse lookup: collection name -> inode
    collection_inos: HashMap<String, u64>,
    /// Reverse lookup: (collection, doc_id) -> inode
    doc_inos: HashMap<(String, String), u64>,
    /// Track which collections have had their documents loaded.
    populated_collections: std::collections::HashSet<String>,
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
            populated_collections: std::collections::HashSet::new(),
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
    pub fn doc_ino(&mut self, collection: &str, doc_id: &str) -> u64 {
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
            },
        );
        self.doc_inos.insert(key, ino);
        ino
    }

    /// Remove a document inode (for unlink/delete).
    pub fn remove_doc(&mut self, collection: &str, doc_id: &str) {
        let key = (collection.to_string(), doc_id.to_string());
        if let Some(ino) = self.doc_inos.remove(&key) {
            self.by_ino.remove(&ino);
        }
    }

    /// Invalidate a collection's population status (after write).
    pub fn invalidate(&mut self, collection: &str) {
        self.populated_collections.remove(collection);
    }
}
