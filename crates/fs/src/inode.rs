/// Inode mapping between filesystem inodes and DefraDB entities.
///
/// Inode layout:
/// - 1: root directory (mountpoint)
/// - 2..N: root virtual files, collection directories
/// - N+1..: collection virtual files, document files
use std::collections::HashMap;

pub const ROOT_INO: u64 = 1;

/// Reserved field name for filesystem display names.
pub const NAME_FIELD: &str = "_name";

/// Represents what a filesystem inode maps to in DefraDB.
#[derive(Debug, Clone)]
pub enum InodeTarget {
    Root,
    Collection {
        name: String,
    },
    Document {
        collection: String,
        doc_id: String,
        display_name: String,
    },
    VirtualFile {
        collection: String,
        filename: String,
    },
    RootVirtualFile {
        filename: String,
    },
}

/// Bidirectional mapping between inodes and DefraDB entities.
pub struct InodeTable {
    next_ino: u64,
    by_ino: HashMap<u64, InodeTarget>,
    collection_inos: HashMap<String, u64>,
    doc_inos: HashMap<(String, String), u64>,
    name_to_doc_id: HashMap<(String, String), String>,
    virtual_inos: HashMap<(String, String), u64>,
    root_virtual_inos: HashMap<String, u64>,
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
            virtual_inos: HashMap::new(),
            root_virtual_inos: HashMap::new(),
        }
    }

    pub fn get(&self, ino: u64) -> Option<&InodeTarget> {
        self.by_ino.get(&ino)
    }

    pub fn collection_ino(&mut self, name: &str) -> u64 {
        if let Some(&ino) = self.collection_inos.get(name) {
            return ino;
        }
        let ino = self.alloc();
        self.by_ino.insert(
            ino,
            InodeTarget::Collection {
                name: name.to_string(),
            },
        );
        self.collection_inos.insert(name.to_string(), ino);
        ino
    }

    pub fn doc_ino(&mut self, collection: &str, doc_id: &str, display_name: &str) -> u64 {
        let key = (collection.to_string(), doc_id.to_string());
        if let Some(&ino) = self.doc_inos.get(&key) {
            return ino;
        }
        let ino = self.alloc();
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

    pub fn virtual_ino(&mut self, collection: &str, filename: &str) -> u64 {
        let key = (collection.to_string(), filename.to_string());
        if let Some(&ino) = self.virtual_inos.get(&key) {
            return ino;
        }
        let ino = self.alloc();
        self.by_ino.insert(
            ino,
            InodeTarget::VirtualFile {
                collection: collection.to_string(),
                filename: filename.to_string(),
            },
        );
        self.virtual_inos.insert(key, ino);
        ino
    }

    pub fn root_virtual_ino(&mut self, filename: &str) -> u64 {
        if let Some(&ino) = self.root_virtual_inos.get(filename) {
            return ino;
        }
        let ino = self.alloc();
        self.by_ino.insert(
            ino,
            InodeTarget::RootVirtualFile {
                filename: filename.to_string(),
            },
        );
        self.root_virtual_inos.insert(filename.to_string(), ino);
        ino
    }

    pub fn resolve_name(&self, collection: &str, display_name: &str) -> Option<&str> {
        let key = (collection.to_string(), display_name.to_string());
        self.name_to_doc_id.get(&key).map(|s| s.as_str())
    }

    pub fn remove_doc(&mut self, collection: &str, doc_id: &str) {
        let key = (collection.to_string(), doc_id.to_string());
        if let Some(ino) = self.doc_inos.remove(&key) {
            if let Some(InodeTarget::Document { display_name, .. }) = self.by_ino.remove(&ino) {
                self.name_to_doc_id
                    .remove(&(collection.to_string(), display_name));
            }
        }
    }

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

    fn alloc(&mut self) -> u64 {
        let ino = self.next_ino;
        self.next_ino += 1;
        ino
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_inode_1() {
        let table = InodeTable::new();
        assert!(matches!(table.get(ROOT_INO), Some(InodeTarget::Root)));
    }

    #[test]
    fn collection_ino_is_stable() {
        let mut table = InodeTable::new();
        let ino1 = table.collection_ino("Users");
        let ino2 = table.collection_ino("Users");
        assert_eq!(ino1, ino2);
        assert!(ino1 > ROOT_INO);
    }

    #[test]
    fn different_collections_get_different_inodes() {
        let mut table = InodeTable::new();
        let ino1 = table.collection_ino("Users");
        let ino2 = table.collection_ino("Posts");
        assert_ne!(ino1, ino2);
    }

    #[test]
    fn doc_ino_with_name_resolution() {
        let mut table = InodeTable::new();
        let ino = table.doc_ino("Users", "bae-abc123", "alice");
        assert!(ino > ROOT_INO);
        assert_eq!(table.resolve_name("Users", "alice"), Some("bae-abc123"));
    }

    #[test]
    fn remove_doc_cleans_up_name_mapping() {
        let mut table = InodeTable::new();
        table.doc_ino("Users", "bae-abc123", "alice");
        table.remove_doc("Users", "bae-abc123");
        assert!(table.resolve_name("Users", "alice").is_none());
    }

    #[test]
    fn invalidate_collection_clears_all_docs() {
        let mut table = InodeTable::new();
        table.doc_ino("Users", "bae-1", "alice");
        table.doc_ino("Users", "bae-2", "bob");
        table.doc_ino("Posts", "bae-3", "post1");

        table.invalidate_collection("Users");

        assert!(table.resolve_name("Users", "alice").is_none());
        assert!(table.resolve_name("Users", "bob").is_none());
        // Other collections untouched
        assert_eq!(table.resolve_name("Posts", "post1"), Some("bae-3"));
    }

    #[test]
    fn root_virtual_ino_is_stable() {
        let mut table = InodeTable::new();
        let ino1 = table.root_virtual_ino("_schema.graphql");
        let ino2 = table.root_virtual_ino("_schema.graphql");
        assert_eq!(ino1, ino2);
        assert!(matches!(
            table.get(ino1),
            Some(InodeTarget::RootVirtualFile { .. })
        ));
    }
}
