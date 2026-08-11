use super::doc_id_index::{decode_doc_short_id_prefix, encode_doc_short_id};
use crate::corekv::Key;
use cid::Cid;
use std::str::FromStr;

const PRIORITY_HEX_WIDTH: usize = 16;

/// HeadstoreDocKey: Links documents to their current block head CID
///
/// Structure: /d/[DocShortID uvarint]/[FieldID]/[CID]
/// Example: /d/\x01/fieldname/bafyreih7c4pdkyvosses56rmyomakxvicn4cehjrw3w3mmk57iagt6f4sq
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadstoreDocKey {
    /// Document short ID
    pub doc_short_id: u64,
    /// Field identifier (can be 'C' for collection)
    pub field_id: String,
    /// IPFS Content Identifier
    pub cid: Cid,
}

impl HeadstoreDocKey {
    /// Create a new HeadstoreDocKey
    pub fn new(doc_short_id: u64, field_id: impl Into<String>, cid: Cid) -> Self {
        Self {
            doc_short_id,
            field_id: field_id.into(),
            cid,
        }
    }

    /// Create a prefix for all heads in a document
    pub fn document_prefix(doc_short_id: u64) -> Vec<u8> {
        let mut buf = b"/d/".to_vec();
        buf.extend_from_slice(&encode_doc_short_id(doc_short_id));
        buf.push(b'/');
        buf
    }

    /// Create a prefix for all heads of a specific field in a document
    pub fn field_prefix(doc_short_id: u64, field_id: impl Into<String>) -> Vec<u8> {
        let field_id = field_id.into();
        let mut buf = Self::document_prefix(doc_short_id);
        buf.extend_from_slice(field_id.as_bytes());
        buf.push(b'/');
        buf
    }

    /// Parse a serialized headstore document key from raw bytes.
    ///
    /// Decodes the binary `doc_short_id` uvarint without a lossy UTF-8 conversion,
    /// so a short ID whose encoding is `0x2F` (`/`) cannot create a false separator.
    /// Modelled on [`super::systemstore::ActionStatusKey::parse`]: fail-closed,
    /// no trailing garbage.
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        let rest = bytes.strip_prefix(b"/d/")?;
        let (rest, doc_short_id) = decode_doc_short_id_prefix(rest).ok()?;
        let rest = rest.strip_prefix(b"/")?;
        // SAFETY: `Key::bytes` always writes `field_id` and the CID as UTF-8
        // (ASCII field names + base32 CID). After the binary short-id segment,
        // the remainder is only those text fields.
        let text = unsafe { std::str::from_utf8_unchecked(rest) };
        let (field_id, cid_str) = text.rsplit_once('/')?;
        if field_id.is_empty() || cid_str.is_empty() {
            return None;
        }
        let cid = Cid::from_str(cid_str).ok()?;
        Some(Self {
            doc_short_id,
            field_id: field_id.to_string(),
            cid,
        })
    }
}

impl Key for HeadstoreDocKey {
    fn bytes(&self) -> Vec<u8> {
        let mut buf = Self::field_prefix(self.doc_short_id, &self.field_id);
        buf.extend_from_slice(self.cid.to_string().as_bytes());
        buf
    }

    fn to_string(&self) -> String {
        format!("/d/{}/{}/{}", self.doc_short_id, self.field_id, self.cid)
    }
}

/// HeadstorePriorityKey: Secondary index for document commit height lookups
///
/// Structure: /p/[DocShortID uvarint]/[PriorityHex16]/[CID bytes]
/// Example: /p/\x01/0000000000000042/<cid-bytes>
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadstorePriorityKey {
    /// Document short ID
    pub doc_short_id: u64,
    /// Commit priority encoded in fixed-width hexadecimal
    pub priority: u64,
    /// IPFS Content Identifier
    pub cid: Cid,
}

impl HeadstorePriorityKey {
    /// Create a new HeadstorePriorityKey.
    pub fn new(doc_short_id: u64, priority: u64, cid: Cid) -> Self {
        Self {
            doc_short_id,
            priority,
            cid,
        }
    }

    /// Create a prefix for all priority-indexed commits in a document.
    pub fn document_prefix(doc_short_id: u64) -> Vec<u8> {
        let mut buf = b"/p/".to_vec();
        buf.extend_from_slice(&encode_doc_short_id(doc_short_id));
        buf.push(b'/');
        buf
    }

    /// Create a prefix for a specific priority in a document.
    pub fn priority_prefix(doc_short_id: u64, priority: u64) -> Vec<u8> {
        let mut buf = Self::document_prefix(doc_short_id);
        buf.extend_from_slice(
            format!("{:0width$x}/", priority, width = PRIORITY_HEX_WIDTH).as_bytes(),
        );
        buf
    }

    /// Return the byte offset of the raw CID suffix within a serialized key.
    pub fn cid_offset(doc_short_id: u64) -> usize {
        Self::document_prefix(doc_short_id).len() + PRIORITY_HEX_WIDTH + 1
    }
}

impl Key for HeadstorePriorityKey {
    fn bytes(&self) -> Vec<u8> {
        let mut bytes = Self::priority_prefix(self.doc_short_id, self.priority);
        bytes.extend_from_slice(&self.cid.to_bytes());
        bytes
    }

    fn to_string(&self) -> String {
        format!(
            "/p/{}/{:0width$x}/{}",
            self.doc_short_id,
            self.priority,
            self.cid,
            width = PRIORITY_HEX_WIDTH
        )
    }
}

/// HeadstoreColKey: Stores current collection head CID
///
/// Structure: /c/[CollectionShortID]/[CID]
/// Example: /c/1/bafyreih7c4pdkyvosses56rmyomakxvicn4cehjrw3w3mmk57iagt6f4sq
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadstoreColKey {
    /// Collection short ID (decimal format)
    pub collection_id: u32,
    /// IPFS Content Identifier
    pub cid: Cid,
}

impl HeadstoreColKey {
    /// Create a new HeadstoreColKey
    pub fn new(collection_id: u32, cid: Cid) -> Self {
        Self { collection_id, cid }
    }

    /// Create a prefix for all heads in a collection
    pub fn collection_prefix(collection_id: u32) -> Vec<u8> {
        format!("/c/{}/", collection_id).into_bytes()
    }
}

impl Key for HeadstoreColKey {
    fn bytes(&self) -> Vec<u8> {
        format!("/c/{}/{}", self.collection_id, self.cid).into_bytes()
    }

    fn to_string(&self) -> String {
        format!("/c/{}/{}", self.collection_id, self.cid)
    }
}

/// HeadstoreFieldDefinition: Maps field definitions to their block head
///
/// Structure: /f/[CollectionName]/[FieldName]/[CID]
/// Example: /f/users/email/bafyreih7c4pdkyvosses56rmyomakxvicn4cehjrw3w3mmk57iagt6f4sq
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadstoreFieldDefinition {
    /// Collection name
    pub collection_name: String,
    /// Field name
    pub field_name: String,
    /// IPFS Content Identifier
    pub cid: Cid,
}

impl HeadstoreFieldDefinition {
    /// Create a new HeadstoreFieldDefinition
    pub fn new(
        collection_name: impl Into<String>,
        field_name: impl Into<String>,
        cid: Cid,
    ) -> Self {
        Self {
            collection_name: collection_name.into(),
            field_name: field_name.into(),
            cid,
        }
    }

    /// Create a prefix for all field definitions
    pub fn field_definition_prefix() -> Vec<u8> {
        b"/f/".to_vec()
    }

    /// Create a prefix for a specific collection's field definitions
    pub fn collection_prefix(collection_name: impl Into<String>) -> Vec<u8> {
        let collection_name = collection_name.into();
        format!("/f/{}/", collection_name).into_bytes()
    }

    /// Create a prefix for a specific field in a collection
    pub fn field_prefix(
        collection_name: impl Into<String>,
        field_name: impl Into<String>,
    ) -> Vec<u8> {
        let collection_name = collection_name.into();
        let field_name = field_name.into();
        format!("/f/{}/{}/", collection_name, field_name).into_bytes()
    }
}

impl Key for HeadstoreFieldDefinition {
    fn bytes(&self) -> Vec<u8> {
        format!(
            "/f/{}/{}/{}",
            self.collection_name, self.field_name, self.cid
        )
        .into_bytes()
    }

    fn to_string(&self) -> String {
        format!(
            "/f/{}/{}/{}",
            self.collection_name, self.field_name, self.cid
        )
    }
}

/// HeadstoreCollectionDefinition: Maps collection definitions to their block head
///
/// Structure: /g/[CollectionName]/[CID]
/// Example: /g/users/bafyreih7c4pdkyvosses56rmyomakxvicn4cehjrw3w3mmk57iagt6f4sq
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadstoreCollectionDefinition {
    /// Collection name
    pub collection_name: String,
    /// IPFS Content Identifier
    pub cid: Cid,
}

impl HeadstoreCollectionDefinition {
    /// Create a new HeadstoreCollectionDefinition
    pub fn new(collection_name: impl Into<String>, cid: Cid) -> Self {
        Self {
            collection_name: collection_name.into(),
            cid,
        }
    }

    /// Create a prefix for all collection definitions
    pub fn collection_definition_prefix() -> Vec<u8> {
        b"/g/".to_vec()
    }

    /// Create a prefix for a specific collection
    pub fn collection_prefix(collection_name: impl Into<String>) -> Vec<u8> {
        let collection_name = collection_name.into();
        format!("/g/{}/", collection_name).into_bytes()
    }
}

impl Key for HeadstoreCollectionDefinition {
    fn bytes(&self) -> Vec<u8> {
        format!("/g/{}/{}", self.collection_name, self.cid).into_bytes()
    }

    fn to_string(&self) -> String {
        format!("/g/{}/{}", self.collection_name, self.cid)
    }
}

/// HeadstoreCollectionSetDefinition: Maps collection set definitions to their block head
///
/// Structure: /s/[FirstCollectionID]/[CID]
/// Example: /s/col_a/bafyreih7c4pdkyvosses56rmyomakxvicn4cehjrw3w3mmk57iagt6f4sq
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadstoreCollectionSetDefinition {
    /// ID of lexicographically smallest collection in set
    pub first_collection_id: String,
    /// IPFS Content Identifier
    pub cid: Cid,
}

impl HeadstoreCollectionSetDefinition {
    /// Create a new HeadstoreCollectionSetDefinition
    pub fn new(first_collection_id: impl Into<String>, cid: Cid) -> Self {
        Self {
            first_collection_id: first_collection_id.into(),
            cid,
        }
    }

    /// Create a prefix for all collection set definitions
    pub fn set_definition_prefix() -> Vec<u8> {
        b"/s/".to_vec()
    }

    /// Create a prefix for a specific collection set
    pub fn set_prefix(first_collection_id: impl Into<String>) -> Vec<u8> {
        let first_collection_id = first_collection_id.into();
        format!("/s/{}/", first_collection_id).into_bytes()
    }
}

impl Key for HeadstoreCollectionSetDefinition {
    fn bytes(&self) -> Vec<u8> {
        format!("/s/{}/{}", self.first_collection_id, self.cid).into_bytes()
    }

    fn to_string(&self) -> String {
        format!("/s/{}/{}", self.first_collection_id, self.cid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    // Test CID (V1, dag-pb, sha2-256)
    fn test_cid() -> Cid {
        Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
    }

    #[test]
    fn test_headstore_doc_key() {
        let cid = test_cid();
        let key = HeadstoreDocKey::new(42, "fieldname", cid);

        let string = key.to_string();
        assert_eq!(string, format!("/d/42/fieldname/{}", cid));

        let bytes = key.bytes();
        let mut expected = b"/d/".to_vec();
        expected.push(0x2a);
        expected.extend_from_slice(b"/fieldname/");
        expected.extend_from_slice(cid.to_string().as_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_headstore_doc_key_parse_roundtrip() {
        let cid = test_cid();
        for doc_short_id in [1u64, 46, 47, 48, 239, 240, 2287, 2288] {
            let key = HeadstoreDocKey::new(doc_short_id, "fieldname", cid);
            assert_eq!(HeadstoreDocKey::parse(&key.bytes()), Some(key));
        }
        let composite = HeadstoreDocKey::new(47, "C", cid);
        assert_eq!(HeadstoreDocKey::parse(&composite.bytes()), Some(composite));
    }

    #[test]
    fn test_headstore_doc_key_parse_rejects_malformed() {
        assert_eq!(HeadstoreDocKey::parse(b""), None);
        assert_eq!(HeadstoreDocKey::parse(b"/c/1/notacid"), None);
        assert_eq!(HeadstoreDocKey::parse(b"/d/"), None);
        // prefix only — no field or cid
        assert_eq!(
            HeadstoreDocKey::parse(&HeadstoreDocKey::document_prefix(1)),
            None
        );
        // trailing garbage after a valid key
        let mut bad = HeadstoreDocKey::new(1, "C", test_cid()).bytes();
        bad.extend_from_slice(b"/extra");
        assert_eq!(HeadstoreDocKey::parse(&bad), None);
    }

    /// Documents the encoding hazard: uvarint 47 is raw `0x2F` (`/`), so a naive
    /// forward `split('/')` after lossy UTF-8 sees a different segment count than
    /// neighbouring short IDs. Typed [`HeadstoreDocKey::parse`] is immune.
    #[test]
    fn test_headstore_doc_key_short_id_47_false_slash_separator() {
        let cid = test_cid();
        let segment_count = |doc_short_id: u64| -> usize {
            let bytes = HeadstoreDocKey::new(doc_short_id, "C", cid).bytes();
            String::from_utf8_lossy(&bytes).split('/').count()
        };
        let count_46 = segment_count(46);
        let count_47 = segment_count(47);
        let count_48 = segment_count(48);
        assert_eq!(count_46, count_48);
        assert_ne!(
            count_47, count_46,
            "doc_short_id=47 must insert a false '/' under naive split"
        );
        // And the typed decoder still recovers 47 correctly.
        let key = HeadstoreDocKey::new(47, "C", cid);
        let parsed = HeadstoreDocKey::parse(&key.bytes()).expect("parse 47");
        assert_eq!(parsed.doc_short_id, 47);
        assert_eq!(parsed.field_id, "C");
        assert_eq!(parsed.cid, cid);
    }

    #[test]
    fn test_headstore_col_key() {
        let cid = test_cid();
        let key = HeadstoreColKey::new(1, cid);

        let string = key.to_string();
        assert_eq!(string, format!("/c/1/{}", cid));

        let bytes = key.bytes();
        assert_eq!(bytes, string.as_bytes());
    }

    #[test]
    fn test_headstore_priority_key() {
        let cid = test_cid();
        let key = HeadstorePriorityKey::new(42, 66, cid);

        let string = key.to_string();
        assert_eq!(string, format!("/p/42/0000000000000042/{}", cid));

        let bytes = key.bytes();
        let mut expected_prefix = b"/p/".to_vec();
        expected_prefix.push(0x2a);
        expected_prefix.extend_from_slice(b"/0000000000000042/");
        assert_eq!(
            &bytes[..HeadstorePriorityKey::cid_offset(42)],
            expected_prefix
        );
        assert_eq!(
            &bytes[HeadstorePriorityKey::cid_offset(42)..],
            cid.to_bytes()
        );
    }

    #[test]
    fn test_headstore_field_definition() {
        let cid = test_cid();
        let key = HeadstoreFieldDefinition::new("users", "email", cid);

        let string = key.to_string();
        assert_eq!(string, format!("/f/users/email/{}", cid));

        let bytes = key.bytes();
        assert_eq!(bytes, string.as_bytes());
    }

    #[test]
    fn test_headstore_collection_definition() {
        let cid = test_cid();
        let key = HeadstoreCollectionDefinition::new("users", cid);

        let string = key.to_string();
        assert_eq!(string, format!("/g/users/{}", cid));

        let bytes = key.bytes();
        assert_eq!(bytes, string.as_bytes());
    }

    #[test]
    fn test_headstore_collection_set_definition() {
        let cid = test_cid();
        let key = HeadstoreCollectionSetDefinition::new("col_a", cid);

        let string = key.to_string();
        assert_eq!(string, format!("/s/col_a/{}", cid));

        let bytes = key.bytes();
        assert_eq!(bytes, string.as_bytes());
    }

    #[test]
    fn test_headstore_prefixes() {
        let prefix = HeadstoreDocKey::document_prefix(42);
        assert_eq!(prefix, [b"/d/".as_slice(), &[0x2a], b"/"].concat());

        let prefix = HeadstoreDocKey::field_prefix(42, "field");
        assert_eq!(prefix, [b"/d/".as_slice(), &[0x2a], b"/field/"].concat());

        let prefix = HeadstorePriorityKey::document_prefix(42);
        assert_eq!(prefix, [b"/p/".as_slice(), &[0x2a], b"/"].concat());

        let prefix = HeadstorePriorityKey::priority_prefix(42, 66);
        assert_eq!(
            prefix,
            [b"/p/".as_slice(), &[0x2a], b"/0000000000000042/"].concat()
        );

        let prefix = HeadstoreColKey::collection_prefix(1);
        assert_eq!(prefix, b"/c/1/");

        let prefix = HeadstoreFieldDefinition::field_definition_prefix();
        assert_eq!(prefix, b"/f/");

        let prefix = HeadstoreCollectionDefinition::collection_definition_prefix();
        assert_eq!(prefix, b"/g/");

        let prefix = HeadstoreCollectionSetDefinition::set_definition_prefix();
        assert_eq!(prefix, b"/s/");
    }
}
