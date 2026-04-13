//! Document permission types.
//!
//! Matches Go's acp/types/types.go

use serde::{Deserialize, Serialize};

/// Document resource permissions (matches Go acp/types/types.go)
///
/// These are the permission types that can be checked against a document.
/// Having Update or Delete permission implies Read access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[non_exhaustive]
pub enum DocumentPermission {
    /// Can read document
    Read = 0,
    /// Can update document (implies read)
    Update = 1,
    /// Can delete document (implies read)
    Delete = 2,
}

impl DocumentPermission {
    /// Returns the string representation used in policy definitions
    pub fn as_str(&self) -> &'static str {
        match self {
            DocumentPermission::Read => "read",
            DocumentPermission::Update => "update",
            DocumentPermission::Delete => "delete",
        }
    }

    /// Returns the canonical relation name for this permission in
    /// document ACP policies (`reader` / `updater` / `deleter`). This
    /// is the noun form used in the `relations:` block, vs `as_str()`
    /// which returns the verb form used in permission expressions.
    pub fn relation_name(&self) -> &'static str {
        match self {
            DocumentPermission::Read => "reader",
            DocumentPermission::Update => "updater",
            DocumentPermission::Delete => "deleter",
        }
    }

    /// Check if this permission implies read access.
    ///
    /// All three document permissions (Read, Update, Delete) imply read
    /// access — matches Go's `ImplyDocumentReadPerm` in
    /// `acp/types/types.go`. Use `implies_read_permissions()` to iterate
    /// over the full set.
    pub fn implies_read(&self) -> bool {
        matches!(
            self,
            DocumentPermission::Read | DocumentPermission::Update | DocumentPermission::Delete
        )
    }

    /// Returns the set of permissions that imply read access.
    ///
    /// Matches Go's `ImplyDocumentReadPerm = []DocumentResourcePermission{Read, Update, Delete}`
    /// in `acp/types/types.go`. When checking read access, backends iterate
    /// this list and grant read if the caller has any of them.
    pub const fn implies_read_permissions() -> &'static [DocumentPermission] {
        &[
            DocumentPermission::Read,
            DocumentPermission::Update,
            DocumentPermission::Delete,
        ]
    }
}

impl std::fmt::Display for DocumentPermission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permission_repr() {
        assert_eq!(DocumentPermission::Read as u8, 0);
        assert_eq!(DocumentPermission::Update as u8, 1);
        assert_eq!(DocumentPermission::Delete as u8, 2);
    }

    #[test]
    fn test_permission_as_str() {
        assert_eq!(DocumentPermission::Read.as_str(), "read");
        assert_eq!(DocumentPermission::Update.as_str(), "update");
        assert_eq!(DocumentPermission::Delete.as_str(), "delete");
    }

    #[test]
    fn test_permission_display() {
        assert_eq!(format!("{}", DocumentPermission::Read), "read");
        assert_eq!(format!("{}", DocumentPermission::Update), "update");
        assert_eq!(format!("{}", DocumentPermission::Delete), "delete");
    }

    #[test]
    fn test_all_permissions_imply_read() {
        assert!(DocumentPermission::Read.implies_read());
        assert!(DocumentPermission::Update.implies_read());
        assert!(DocumentPermission::Delete.implies_read());
    }

    #[test]
    fn test_implies_read_permissions_matches_go() {
        let perms = DocumentPermission::implies_read_permissions();
        assert_eq!(perms.len(), 3);
        assert_eq!(perms[0], DocumentPermission::Read);
        assert_eq!(perms[1], DocumentPermission::Update);
        assert_eq!(perms[2], DocumentPermission::Delete);
        for p in perms {
            assert!(p.implies_read());
        }
    }

    #[test]
    fn test_relation_name() {
        assert_eq!(DocumentPermission::Read.relation_name(), "reader");
        assert_eq!(DocumentPermission::Update.relation_name(), "updater");
        assert_eq!(DocumentPermission::Delete.relation_name(), "deleter");
    }

    #[test]
    fn test_permission_serde() {
        let perm = DocumentPermission::Update;
        let json = serde_json::to_string(&perm).unwrap();
        let parsed: DocumentPermission = serde_json::from_str(&json).unwrap();
        assert_eq!(perm, parsed);
    }
}
