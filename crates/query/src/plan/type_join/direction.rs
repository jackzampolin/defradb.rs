//! Join direction types for type joins

/// Represents the direction of a join operation.
///
/// Join direction is determined by which side holds the foreign key (FK):
/// - **Primary**: The parent side holds the FK. Lookup is done by extracting the FK
///   value from the parent document and finding the child with matching `_docID`.
/// - **Inverted**: The child side holds the FK. Lookup is done by scanning children
///   to find those whose FK matches the parent's `_docID`.
#[derive(Clone, Debug)]
pub enum JoinDirection {
    /// Primary join: parent has FK field at the given index.
    /// Lookup: child._docID == parent.FK_field
    Primary {
        /// Index of the FK field in the parent document (e.g., `author_id`)
        parent_fk_index: usize,
    },
    /// Inverted join: child has FK field, parent does not.
    /// Lookup: child.FK_field == parent._docID
    Inverted,
    /// Inverted index join: child scanned first with index, parent looked up
    /// per-child via FK index. Used when both child's filtered field and
    /// parent's FK field are indexed.
    InvertedIndex {
        /// Name of the index on the parent's FK field
        parent_fk_index_name: String,
        /// Index of the FK field in the parent's document mapping
        parent_fk_field_index: usize,
    },
    /// Ordered inverted join (primary-first): child has FK and drives iteration
    /// in sorted order via index. Parent is looked up by docID for each child.
    /// Used when ordering by a child field that has an index and the child
    /// holds the FK to the parent (e.g., Device._ownerID → User).
    OrderedInvertedPrimary {
        /// Index of the FK field in the child's document mapping (e.g., _ownerID index)
        child_fk_index: usize,
    },
}
