use crate::corekv::Key;

const DATA_PREFIX: &[u8] = b"/data/";
const PRIORITY_SUFFIX: &[u8] = b"/priority";
const NONCES_SUFFIX: &[u8] = b"/nonces/";

/// CRDTValueKey: Stores a CRDT field value.
///
/// Structure: /data/[SchemaVersionID]/[DocID bytes]/[FieldName]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CRDTValueKey {
    /// Schema version identifier.
    pub schema_version_id: String,
    /// Raw document identifier bytes.
    pub doc_id: Vec<u8>,
    /// Field name.
    pub field_name: String,
}

impl CRDTValueKey {
    /// Create a new CRDTValueKey.
    pub fn new(
        schema_version_id: impl Into<String>,
        doc_id: impl Into<Vec<u8>>,
        field_name: impl Into<String>,
    ) -> Self {
        Self {
            schema_version_id: schema_version_id.into(),
            doc_id: doc_id.into(),
            field_name: field_name.into(),
        }
    }

    /// Create the corresponding priority key.
    pub fn priority_key(&self) -> CRDTPriorityKey {
        CRDTPriorityKey::new(
            self.schema_version_id.clone(),
            self.doc_id.clone(),
            self.field_name.clone(),
        )
    }

    /// Create a nonce key prefix for this field.
    pub fn nonce_prefix(&self) -> CRDTNoncePrefix {
        CRDTNoncePrefix::new(
            self.schema_version_id.clone(),
            self.doc_id.clone(),
            self.field_name.clone(),
        )
    }
}

impl Key for CRDTValueKey {
    fn bytes(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(
            DATA_PREFIX.len()
                + self.schema_version_id.len()
                + 1
                + self.doc_id.len()
                + 1
                + self.field_name.len(),
        );
        key.extend_from_slice(DATA_PREFIX);
        key.extend_from_slice(self.schema_version_id.as_bytes());
        key.push(b'/');
        key.extend_from_slice(&self.doc_id);
        key.push(b'/');
        key.extend_from_slice(self.field_name.as_bytes());
        key
    }

    fn to_string(&self) -> String {
        format!(
            "/data/{}/{}/{}",
            self.schema_version_id,
            String::from_utf8_lossy(&self.doc_id),
            self.field_name
        )
    }
}

/// CRDTPriorityKey: Stores CRDT field priority metadata.
///
/// Structure: /data/[SchemaVersionID]/[DocID bytes]/[FieldName]/priority
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CRDTPriorityKey {
    /// Value key this priority metadata is associated with.
    pub value_key: CRDTValueKey,
}

impl CRDTPriorityKey {
    /// Create a new CRDTPriorityKey.
    pub fn new(
        schema_version_id: impl Into<String>,
        doc_id: impl Into<Vec<u8>>,
        field_name: impl Into<String>,
    ) -> Self {
        Self {
            value_key: CRDTValueKey::new(schema_version_id, doc_id, field_name),
        }
    }
}

impl Key for CRDTPriorityKey {
    fn bytes(&self) -> Vec<u8> {
        let mut key = self.value_key.bytes();
        key.extend_from_slice(PRIORITY_SUFFIX);
        key
    }

    fn to_string(&self) -> String {
        format!("{}/priority", self.value_key.to_string())
    }
}

/// CRDTNoncePrefix: Prefix for CRDT nonce-tracking keys.
///
/// Structure: /data/[SchemaVersionID]/[DocID bytes]/[FieldName]/nonces/
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CRDTNoncePrefix {
    /// Value key this nonce prefix is associated with.
    pub value_key: CRDTValueKey,
}

impl CRDTNoncePrefix {
    /// Create a new CRDTNoncePrefix.
    pub fn new(
        schema_version_id: impl Into<String>,
        doc_id: impl Into<Vec<u8>>,
        field_name: impl Into<String>,
    ) -> Self {
        Self {
            value_key: CRDTValueKey::new(schema_version_id, doc_id, field_name),
        }
    }

    /// Create a nonce key by appending the nonce bytes.
    pub fn nonce_key(&self, nonce: i64) -> CRDTNonceKey {
        CRDTNonceKey {
            prefix: self.clone(),
            nonce,
        }
    }
}

impl Key for CRDTNoncePrefix {
    fn bytes(&self) -> Vec<u8> {
        let mut key = self.value_key.bytes();
        key.extend_from_slice(NONCES_SUFFIX);
        key
    }

    fn to_string(&self) -> String {
        format!("{}/nonces/", self.value_key.to_string())
    }
}

/// CRDTNonceKey: Tracks an applied counter nonce.
///
/// Structure: /data/[SchemaVersionID]/[DocID bytes]/[FieldName]/nonces/[NonceBE]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CRDTNonceKey {
    /// Prefix for the nonce-tracking keys.
    pub prefix: CRDTNoncePrefix,
    /// Nonce encoded as big-endian bytes.
    pub nonce: i64,
}

impl Key for CRDTNonceKey {
    fn bytes(&self) -> Vec<u8> {
        let mut key = self.prefix.bytes();
        key.extend_from_slice(&self.nonce.to_be_bytes());
        key
    }

    fn to_string(&self) -> String {
        format!("{:?}", self.bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crdt_value_key_bytes_match_legacy_encoding() {
        let key = CRDTValueKey::new("schema", b"doc-123".to_vec(), "field");

        assert_eq!(key.bytes(), b"/data/schema/doc-123/field");
        assert_eq!(key.to_string(), "/data/schema/doc-123/field");
    }

    #[test]
    fn test_crdt_priority_key_bytes_match_legacy_encoding() {
        let key = CRDTPriorityKey::new("schema", b"doc-123".to_vec(), "field");

        assert_eq!(key.bytes(), b"/data/schema/doc-123/field/priority");
        assert_eq!(key.to_string(), "/data/schema/doc-123/field/priority");
    }

    #[test]
    fn test_crdt_nonce_prefix_and_key_bytes_match_legacy_encoding() {
        let prefix = CRDTNoncePrefix::new("schema", b"doc-123".to_vec(), "field");
        let key = prefix.nonce_key(42);

        let mut expected = b"/data/schema/doc-123/field/nonces/".to_vec();
        expected.extend_from_slice(&42i64.to_be_bytes());

        assert_eq!(prefix.bytes(), b"/data/schema/doc-123/field/nonces/");
        assert_eq!(key.bytes(), expected);
    }

    #[test]
    fn test_crdt_value_key_builds_related_keys() {
        let value_key = CRDTValueKey::new("schema", b"doc-123".to_vec(), "field");

        assert_eq!(
            value_key.priority_key().bytes(),
            b"/data/schema/doc-123/field/priority"
        );
        assert_eq!(
            value_key.nonce_prefix().bytes(),
            b"/data/schema/doc-123/field/nonces/"
        );
    }
}
