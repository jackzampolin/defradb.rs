//! Index-related types for collection schemas.
//!
//! Matches Go's client/index.go and client/encrypted_index.go

use serde::{Deserialize, Serialize};

/// Describes a field within an index.
/// Matches Go's IndexedFieldDescription.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexedFieldDescription {
    /// Name of the field being indexed.
    #[serde(rename = "Name", default)]
    pub name: String,

    /// Whether the field is indexed in descending order.
    #[serde(rename = "Descending", default)]
    pub descending: bool,
}

/// Describes a secondary index on a collection.
/// Matches Go's IndexDescription.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDescription {
    /// Name of the index.
    #[serde(rename = "Name", default)]
    pub name: String,

    /// Local identifier for this index.
    #[serde(rename = "ID", default)]
    pub id: u32,

    /// Fields that are being indexed.
    #[serde(rename = "Fields", default)]
    pub fields: Vec<IndexedFieldDescription>,

    /// Whether the index enforces uniqueness.
    #[serde(rename = "Unique", default)]
    pub unique: bool,

    /// Kind-specific configuration.
    ///
    /// `None` in a description written before kinds existed, or by a caller
    /// that set only `unique`; [`IndexDescription::normalized`] resolves that.
    #[serde(rename = "Kind", default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<IndexKind>,

    /// Internal-only marker for auto-generated schema indexes.
    #[serde(skip)]
    pub auto_generated: bool,
}

impl IndexDescription {
    /// Create a new index description.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            id: 0,
            fields: Vec::new(),
            unique: false,
            kind: None,
            auto_generated: false,
        }
    }

    /// Add a field to the index.
    pub fn with_field(mut self, name: impl Into<String>, descending: bool) -> Self {
        self.fields.push(IndexedFieldDescription {
            name: name.into(),
            descending,
        });
        self
    }

    /// Set the index as unique.
    /// The vector configuration, when this is a vector index.
    pub fn vector(&self) -> Option<&VectorIndexDescription> {
        match self.kind.as_ref()? {
            IndexKind::Vector(vector) => Some(vector),
            IndexKind::Ordered(_) => None,
        }
    }

    /// Whether this index is a vector index.
    pub fn is_vector(&self) -> bool {
        self.vector().is_some()
    }

    /// Uniqueness, taken from the kind when one is set and from the legacy
    /// field otherwise. A vector index is never unique.
    pub fn resolved_unique(&self) -> bool {
        match self.kind {
            Some(IndexKind::Ordered(ordered)) => ordered.unique,
            Some(IndexKind::Vector(_)) => false,
            None => self.unique,
        }
    }

    /// A copy whose kind and legacy `unique` flag agree.
    ///
    /// Two descriptions that mean the same thing but were built in different
    /// styles, one setting only `unique` and one setting only `kind`, compare
    /// unequal until both are normalized. Mirrors Go's `Normalize`.
    pub fn normalized(mut self) -> Self {
        let kind = self
            .kind
            .unwrap_or(IndexKind::Ordered(OrderedIndexDescription {
                unique: self.unique,
            }));
        self.unique = match kind {
            IndexKind::Ordered(ordered) => ordered.unique,
            IndexKind::Vector(_) => false,
        };
        self.kind = Some(kind);
        self
    }

    /// Marks this as a vector index with the given configuration.
    pub fn as_vector(mut self, vector: VectorIndexDescription) -> Self {
        self.unique = false;
        self.kind = Some(IndexKind::Vector(vector));
        self
    }

    pub fn as_unique(mut self) -> Self {
        self.unique = true;
        self
    }

    /// Mark the index as auto-generated.
    pub fn as_auto_generated(mut self) -> Self {
        self.auto_generated = true;
        self
    }
}

/// Type of encrypted index.
/// Matches Go's EncryptedIndexType.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum EncryptedIndexType {
    /// Equality-based searchable encryption.
    #[serde(rename = "equality")]
    #[default]
    Equality,
}

/// Describes an encrypted index for searchable encryption.
/// Matches Go's EncryptedIndexDescription.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EncryptedIndexDescription {
    /// Name of the field being indexed.
    #[serde(rename = "FieldName")]
    pub field_name: String,

    /// Type of searchable encryption.
    #[serde(rename = "Type", default)]
    pub index_type: EncryptedIndexType,
}

impl EncryptedIndexDescription {
    /// Create a new encrypted index description.
    pub fn new(field_name: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            index_type: EncryptedIndexType::Equality,
        }
    }
}

/// Describes a BM25 full-text search index on a collection field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FullTextIndexDescription {
    /// Name of the field being indexed.
    #[serde(rename = "FieldName")]
    pub field_name: String,

    /// Language for tokenization and stemming (default: "english").
    #[serde(rename = "Language", default = "default_language")]
    pub language: String,

    /// BM25 term frequency saturation parameter (default: 1.2).
    #[serde(rename = "K1", default = "default_k1")]
    pub k1: f64,

    /// BM25 document length normalization parameter (default: 0.75).
    #[serde(rename = "B", default = "default_b")]
    pub b: f64,
}

fn default_language() -> String {
    "english".to_string()
}

fn default_k1() -> f64 {
    1.2
}

fn default_b() -> f64 {
    0.75
}

impl FullTextIndexDescription {
    pub fn new(field_name: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            language: default_language(),
            k1: default_k1(),
            b: default_b(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_builder() {
        let index = IndexDescription::new("user_email_idx")
            .with_field("email", false)
            .as_unique();

        assert_eq!(index.name, "user_email_idx");
        assert!(index.unique);
        assert_eq!(index.fields.len(), 1);
        assert_eq!(index.fields[0].name, "email");
        assert!(!index.fields[0].descending);
    }

    #[test]
    fn test_index_serialization() {
        let index = IndexDescription::new("test_idx")
            .with_field("name", false)
            .with_field("created_at", true);

        let json = serde_json::to_string(&index).unwrap();
        assert!(json.contains("\"Name\""));
        assert!(json.contains("\"Fields\""));

        let parsed: IndexDescription = serde_json::from_str(&json).unwrap();
        assert_eq!(index, parsed);
    }

    #[test]
    fn test_encrypted_index_serialization() {
        let enc_idx = EncryptedIndexDescription::new("ssn");
        let json = serde_json::to_string(&enc_idx).unwrap();

        assert!(json.contains("\"FieldName\""));
        assert!(json.contains("\"Type\""));
        assert!(json.contains("equality"));

        let parsed: EncryptedIndexDescription = serde_json::from_str(&json).unwrap();
        assert_eq!(enc_idx, parsed);
    }
}

/// Algorithm a vector index is built and searched with.
///
/// Go defines only `HNSW`. `FLAT` is a wire divergence: exact and linear in the
/// corpus, so it is the right choice for a small collection and the oracle an
/// approximate index is checked against.
/// Declares the variants, their wire names, and `ALL` from one list, so an
/// engine cannot be added to the enum yet be missing from the name mapping or
/// from the coverage every consumer derives from `ALL`.
macro_rules! vector_algorithms {
    (
        $(#[doc = $first_doc:literal])* $first:ident => $first_name:literal,
        $($(#[doc = $doc:literal])* $variant:ident => $name:literal),+ $(,)?
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
        pub enum VectorAlgorithm {
            $(#[doc = $first_doc])*
            #[default]
            #[serde(rename = $first_name)]
            $first,
            $(
                $(#[doc = $doc])*
                #[serde(rename = $name)]
                $variant,
            )+
        }

        impl VectorAlgorithm {
            /// Every algorithm the engine can build and search.
            ///
            /// The SDL parser, the CLI, and the query-path tests all derive from
            /// this, so adding a variant makes it selectable and covered rather
            /// than reachable only through its own engine tests.
            pub const ALL: &'static [Self] = &[Self::$first, $(Self::$variant),+];

            /// The name used in `@vectorIndex(algorithm:)` and on the wire.
            pub fn as_str(self) -> &'static str {
                match self {
                    Self::$first => $first_name,
                    $(Self::$variant => $name),+
                }
            }
        }
    };
}

// The first entry is the default.
vector_algorithms! {
    /// Hierarchical Navigable Small World graph.
    Hnsw => "HNSW",
    /// Exhaustive scan. Exact, no build parameters, no tuning.
    Flat => "FLAT",
    /// Coarse lists of product-quantized codes. Approximate and lossy, in
    /// exchange for a code far smaller than the vector it stands for.
    IvfPq => "IVF_PQ",
    /// Satellite System Graph: one flat layer, edges pruned by angle.
    Ssg => "SSG",
}

impl VectorAlgorithm {
    pub fn from_sdl_name(name: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|algorithm| algorithm.as_str() == name)
    }

    pub fn is_go_compatible(self) -> bool {
        matches!(self, Self::Hnsw)
    }

    /// Whether this algorithm can rank by `metric`.
    ///
    /// IVF-PQ scores through its codebooks by squared distance, which does not
    /// order a dot product, so the pair is refused when the definition is
    /// written rather than when the first document is indexed.
    pub fn supports_metric(self, metric: DistanceMetric) -> bool {
        match self {
            Self::IvfPq => metric == DistanceMetric::Cosine,
            Self::Hnsw | Self::Flat | Self::Ssg => true,
        }
    }
}

/// How a vector index compares two vectors.
///
/// Go defines only `COSINE`. `DOT` is a deliberate wire divergence: a
/// definition carrying it is not parseable by a Go node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DistanceMetric {
    #[default]
    #[serde(rename = "COSINE")]
    Cosine,
    #[serde(rename = "DOT")]
    Dot,
}

impl DistanceMetric {
    pub const ALL: &'static [Self] = &[Self::Cosine, Self::Dot];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cosine => "COSINE",
            Self::Dot => "DOT",
        }
    }

    pub fn from_sdl_name(name: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|metric| metric.as_str() == name)
    }

    pub fn is_go_compatible(self) -> bool {
        matches!(self, Self::Cosine)
    }
}

/// HNSW build and search parameters.
///
/// `u32` rather than `usize` because these replicate inside a collection
/// definition, and a width that differs between runtimes is a value that
/// survives one and not the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HnswParams {
    /// Maximum connections per node above layer 0.
    #[serde(rename = "M", default = "default_hnsw_m")]
    pub m: u32,
    /// Candidate-list size while building.
    #[serde(rename = "EfConstruction", default = "default_hnsw_ef_construction")]
    pub ef_construction: u32,
    /// Candidate-list size while searching.
    #[serde(rename = "EfSearch", default = "default_hnsw_ef_search")]
    pub ef_search: u32,
}

fn default_hnsw_m() -> u32 {
    16
}

fn default_hnsw_ef_construction() -> u32 {
    128
}

fn default_hnsw_ef_search() -> u32 {
    64
}

impl Default for HnswParams {
    fn default() -> Self {
        Self {
            m: default_hnsw_m(),
            ef_construction: default_hnsw_ef_construction(),
            ef_search: default_hnsw_ef_search(),
        }
    }
}

/// Configuration of a vector (approximate nearest neighbor) index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct VectorIndexDescription {
    #[serde(rename = "Algorithm", default)]
    pub algorithm: VectorAlgorithm,
    #[serde(rename = "Metric", default)]
    pub metric: DistanceMetric,
    /// Length of the vectors indexed. May be `0` on an embedding field, where
    /// the model fixes the length.
    #[serde(rename = "Dimensions", default)]
    pub dimensions: u32,
    /// Present when `algorithm` is HNSW.
    #[serde(rename = "HNSW", default, skip_serializing_if = "Option::is_none")]
    pub hnsw: Option<HnswParams>,
    /// Present when `algorithm` is IVF_PQ.
    #[serde(rename = "IVFPQ", default, skip_serializing_if = "Option::is_none")]
    pub ivfpq: Option<IvfPqParams>,
    /// Present when `algorithm` is SSG.
    #[serde(rename = "SSG", default, skip_serializing_if = "Option::is_none")]
    pub ssg: Option<SsgParams>,
}

/// SSG build and search parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SsgParams {
    /// Maximum edges kept per node.
    #[serde(rename = "R", default = "default_ssg_r")]
    pub r: u32,
    /// Minimum angle between two kept edges, in degrees.
    #[serde(rename = "Angle", default = "default_ssg_angle")]
    pub angle: u32,
    /// Candidate pool size during search.
    #[serde(rename = "Pool", default = "default_ssg_pool")]
    pub pool: u32,
}

fn default_ssg_r() -> u32 {
    50
}

fn default_ssg_angle() -> u32 {
    60
}

fn default_ssg_pool() -> u32 {
    100
}

impl Default for SsgParams {
    fn default() -> Self {
        Self {
            r: default_ssg_r(),
            angle: default_ssg_angle(),
            pool: default_ssg_pool(),
        }
    }
}

/// IVF-PQ build and search parameters.
///
/// `0` means derive from the corpus: `nlist` from its size, `m` from the
/// vector width.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IvfPqParams {
    /// Coarse centroids, and therefore inverted lists.
    #[serde(rename = "NList", default)]
    pub nlist: u32,
    /// Lists probed per query.
    #[serde(rename = "NProbe", default = "default_ivfpq_nprobe")]
    pub nprobe: u32,
    /// Subquantizers, and therefore bytes per code.
    #[serde(rename = "M", default)]
    pub m: u32,
    /// Cap on the resident training sample.
    #[serde(rename = "SampleBytes", default = "default_ivfpq_sample_bytes")]
    pub sample_bytes: u64,
}

fn default_ivfpq_nprobe() -> u32 {
    8
}

fn default_ivfpq_sample_bytes() -> u64 {
    128 << 20
}

impl Default for IvfPqParams {
    fn default() -> Self {
        Self {
            nlist: 0,
            nprobe: default_ivfpq_nprobe(),
            m: 0,
            sample_bytes: default_ivfpq_sample_bytes(),
        }
    }
}

impl VectorIndexDescription {
    /// A description carrying the chosen algorithm's default parameters.
    ///
    /// The per-algorithm block belongs to exactly one algorithm, so filling it
    /// here keeps every caller that has no tuning to express from repeating the
    /// match and forgetting an engine.
    pub fn with_defaults(
        algorithm: VectorAlgorithm,
        metric: DistanceMetric,
        dimensions: u32,
    ) -> Self {
        Self {
            algorithm,
            metric,
            dimensions,
            hnsw: matches!(algorithm, VectorAlgorithm::Hnsw).then(HnswParams::default),
            ivfpq: matches!(algorithm, VectorAlgorithm::IvfPq).then(IvfPqParams::default),
            ssg: matches!(algorithm, VectorAlgorithm::Ssg).then(SsgParams::default),
        }
    }
}

/// Configuration of an ordered index: one that stores field values in key
/// order, which is every index kind that predates vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct OrderedIndexDescription {
    #[serde(rename = "Unique", default)]
    pub unique: bool,
}

/// Which kind of index a description configures.
///
/// The concrete variant *is* the kind, so an index can never be in a state
/// where a kind tag and its config disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "IndexKindWire", into = "IndexKindWire")]
pub enum IndexKind {
    Ordered(OrderedIndexDescription),
    Vector(VectorIndexDescription),
}

impl Default for IndexKind {
    fn default() -> Self {
        IndexKind::Ordered(OrderedIndexDescription::default())
    }
}

/// The flat form both kinds share on the wire.
///
/// There is no discriminator: the kind is sniffed from whether a vector-only
/// field is present, matching Go's `parseIndexKind`. A tag would have been
/// easier to read, but adding one here would mean a description this runtime
/// writes is not one the other can parse.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct IndexKindWire {
    #[serde(rename = "Algorithm", default, skip_serializing_if = "Option::is_none")]
    algorithm: Option<VectorAlgorithm>,
    #[serde(rename = "Metric", default, skip_serializing_if = "Option::is_none")]
    metric: Option<DistanceMetric>,
    #[serde(
        rename = "Dimensions",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    dimensions: Option<u32>,
    #[serde(rename = "HNSW", default, skip_serializing_if = "Option::is_none")]
    hnsw: Option<HnswParams>,
    #[serde(rename = "IVFPQ", default, skip_serializing_if = "Option::is_none")]
    ivfpq: Option<IvfPqParams>,
    #[serde(rename = "SSG", default, skip_serializing_if = "Option::is_none")]
    ssg: Option<SsgParams>,
    #[serde(rename = "Unique", default, skip_serializing_if = "Option::is_none")]
    unique: Option<bool>,
}

impl From<IndexKindWire> for IndexKind {
    fn from(wire: IndexKindWire) -> Self {
        if wire.algorithm.is_some() || wire.dimensions.is_some() {
            IndexKind::Vector(VectorIndexDescription {
                algorithm: wire.algorithm.unwrap_or_default(),
                metric: wire.metric.unwrap_or_default(),
                dimensions: wire.dimensions.unwrap_or_default(),
                hnsw: wire.hnsw,
                ivfpq: wire.ivfpq,
                ssg: wire.ssg,
            })
        } else {
            IndexKind::Ordered(OrderedIndexDescription {
                unique: wire.unique.unwrap_or_default(),
            })
        }
    }
}

impl From<IndexKind> for IndexKindWire {
    fn from(kind: IndexKind) -> Self {
        match kind {
            IndexKind::Ordered(ordered) => Self {
                algorithm: None,
                metric: None,
                dimensions: None,
                hnsw: None,
                ivfpq: None,
                ssg: None,
                unique: Some(ordered.unique),
            },
            IndexKind::Vector(vector) => Self {
                algorithm: Some(vector.algorithm),
                metric: Some(vector.metric),
                dimensions: Some(vector.dimensions),
                hnsw: vector.hnsw,
                ivfpq: vector.ivfpq,
                ssg: vector.ssg,
                unique: None,
            },
        }
    }
}
