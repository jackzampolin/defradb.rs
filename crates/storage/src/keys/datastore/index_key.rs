use super::super::utils::{encode_uvarint_ascending, SEPARATOR};
use crate::corekv::Key;

/// An indexed field with value and sort direction.
///
/// Matches Go's keys.IndexedField
#[derive(Debug, Clone)]
pub struct IndexedField {
    /// The field value
    pub value: document::NormalValue,
    /// Whether this field is indexed in descending order
    pub descending: bool,
}

impl IndexedField {
    /// Create a new indexed field
    pub fn new(value: document::NormalValue, descending: bool) -> Self {
        Self { value, descending }
    }

    /// Create an ascending indexed field
    pub fn ascending(value: document::NormalValue) -> Self {
        Self {
            value,
            descending: false,
        }
    }

    /// Create a descending indexed field
    pub fn descending(value: document::NormalValue) -> Self {
        Self {
            value,
            descending: true,
        }
    }
}

impl PartialEq for IndexedField {
    fn eq(&self, other: &Self) -> bool {
        self.descending == other.descending && self.value == other.value
    }
}

/// IndexDataStoreKey: Stores indexed field values for secondary indexes
///
/// Structure: /[CollectionShortID]/[IndexID]/[EncodedFieldValue1][EncodedFieldValue2]...(/[DocShortID])
/// Example: /1/2/<encoded value A><encoded value B>
///
/// The trailing doc short ID makes equal index values unique and lets
/// index scans resolve the matching document without storing the full
/// DocID. It is 0 (omitted) for unique-index keys, whose values carry
/// the doc identity instead.
///
/// Note: Field values are encoded using order-preserving encoding that
/// maintains sort order when compared as byte sequences.
#[derive(Debug, Clone)]
pub struct IndexDataStoreKey {
    /// Collection short ID (varint-encoded in key bytes)
    pub collection_short_id: u32,
    /// Index ID (varint-encoded in key bytes)
    pub index_id: u32,
    /// Indexed fields with values and sort direction
    pub fields: Vec<IndexedField>,
    /// Document short ID suffix (varint-encoded in key bytes; 0 = omitted)
    pub doc_short_id: u64,
}

impl PartialEq for IndexDataStoreKey {
    fn eq(&self, other: &Self) -> bool {
        self.collection_short_id == other.collection_short_id
            && self.index_id == other.index_id
            && self.fields == other.fields
            && self.doc_short_id == other.doc_short_id
    }
}

impl IndexDataStoreKey {
    /// Create a new IndexDataStoreKey without a doc short ID suffix.
    pub fn new(collection_short_id: u32, index_id: u32, fields: Vec<IndexedField>) -> Self {
        Self {
            collection_short_id,
            index_id,
            fields,
            doc_short_id: 0,
        }
    }

    /// Create a new IndexDataStoreKey with a doc short ID suffix.
    pub fn with_doc_short_id(
        collection_short_id: u32,
        index_id: u32,
        fields: Vec<IndexedField>,
        doc_short_id: u64,
    ) -> Self {
        Self {
            collection_short_id,
            index_id,
            fields,
            doc_short_id,
        }
    }

    /// Create a prefix for all entries in an index
    pub fn index_prefix(collection_short_id: u32, index_id: u32) -> Vec<u8> {
        let mut buf = vec![SEPARATOR];
        buf = encode_uvarint_ascending(buf, collection_short_id as u64);
        buf.push(SEPARATOR);
        buf = encode_uvarint_ascending(buf, index_id as u64);
        buf.push(SEPARATOR);
        buf
    }

    /// Create a prefix for all entries in a collection's indexes
    pub fn collection_prefix(collection_short_id: u32) -> Vec<u8> {
        let mut buf = vec![SEPARATOR];
        buf = encode_uvarint_ascending(buf, collection_short_id as u64);
        buf.push(SEPARATOR);
        buf
    }

    /// Convert the key to bytes, returning an error if encoding fails.
    ///
    /// Use this method when you need to handle encoding errors (e.g., unsupported
    /// field types or timestamp overflow).
    pub fn try_bytes(&self) -> crate::corekv::Result<Vec<u8>> {
        let mut buf = vec![SEPARATOR];
        buf = encode_uvarint_ascending(buf, self.collection_short_id as u64);
        buf.push(SEPARATOR);
        buf = encode_uvarint_ascending(buf, self.index_id as u64);
        buf.push(SEPARATOR);

        for field in &self.fields {
            buf = crate::field_value::encode_field_value(buf, &field.value, field.descending)?;
        }

        if self.doc_short_id != 0 {
            buf.push(SEPARATOR);
            buf.extend_from_slice(&super::super::doc_id_index::encode_doc_short_id(
                self.doc_short_id,
            ));
        }

        Ok(buf)
    }
}

impl Key for IndexDataStoreKey {
    fn bytes(&self) -> Vec<u8> {
        // Note: Prefer try_bytes() for proper error handling. This implementation
        // panics on encoding errors (e.g., unsupported field types, timestamp overflow).
        match self.try_bytes() {
            Ok(bytes) => bytes,
            Err(e) => {
                panic!(
                    "IndexDataStoreKey encoding failed for collection={}, index={}: {}. \
                     Use try_bytes() for proper error handling.",
                    self.collection_short_id, self.index_id, e
                )
            }
        }
    }

    fn to_string(&self) -> String {
        let values_str = self
            .fields
            .iter()
            .map(|f| {
                format!(
                    "{:?}({})",
                    f.value,
                    if f.descending { "desc" } else { "asc" }
                )
            })
            .collect::<Vec<_>>()
            .join("/");
        format!(
            "/{}/{}/{}",
            self.collection_short_id, self.index_id, values_str
        )
    }
}
