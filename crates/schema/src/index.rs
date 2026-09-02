//! Index-related types for collection schemas.
//!
//! Matches Go's client/index.go and client/encrypted_index.go

use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
#[derive(Debug, Clone, PartialEq)]
pub struct IndexDescription {
    /// Name of the index.
    pub name: String,

    /// Local identifier for this index.
    pub id: u32,

    /// Fields that are being indexed.
    pub fields: Vec<IndexedFieldDescription>,

    /// Whether the index enforces uniqueness.
    pub unique: bool,

    /// Kind-specific configuration.
    ///
    /// `None` in a description written before kinds existed, or by a caller
    /// that set only `unique`; [`IndexDescription::normalized`] resolves that.
    pub kind: Option<IndexKind>,

    /// Internal-only marker for auto-generated schema indexes.
    pub auto_generated: bool,
}

#[derive(Serialize)]
struct IndexDescriptionRef<'a> {
    #[serde(rename = "Name")]
    name: &'a str,
    #[serde(rename = "ID")]
    id: u32,
    #[serde(rename = "Fields")]
    fields: &'a [IndexedFieldDescription],
    #[serde(rename = "Kind")]
    kind: u8,
    #[serde(rename = "KindDescription")]
    kind_description: IndexKindDescriptionWire,
    #[serde(rename = "Unique")]
    unique: bool,
}

#[derive(Deserialize)]
struct IndexDescriptionWire {
    #[serde(rename = "Name", default)]
    name: String,
    #[serde(rename = "ID", default)]
    id: u32,
    #[serde(rename = "Fields", default)]
    fields: Vec<IndexedFieldDescription>,
    #[serde(rename = "Kind", default)]
    kind: u8,
    #[serde(rename = "KindDescription", default)]
    kind_description: Option<serde_json::Value>,
    #[serde(rename = "Unique", default)]
    unique: bool,
}

impl Serialize for IndexDescription {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let kind = self
            .kind
            .unwrap_or(IndexKind::Ordered(OrderedIndexDescription {
                unique: self.unique,
            }));
        let unique = match kind {
            IndexKind::Ordered(ordered) => ordered.unique,
            IndexKind::Vector(_) => false,
        };
        let (kind, kind_description) = kind.into_wire();

        IndexDescriptionRef {
            name: &self.name,
            id: self.id,
            fields: &self.fields,
            kind,
            kind_description,
            unique,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for IndexDescription {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = IndexDescriptionWire::deserialize(deserializer)?;
        let kind = IndexKind::from_wire(wire.kind, wire.kind_description, wire.unique)?;
        let unique = match kind {
            IndexKind::Ordered(ordered) => ordered.unique,
            IndexKind::Vector(_) => false,
        };

        Ok(Self {
            name: wire.name,
            id: wire.id,
            fields: wire.fields,
            unique,
            kind: Some(kind),
            auto_generated: false,
        })
    }
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
        assert_eq!(index.normalized(), parsed);
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
    /// IVF-PQ scores through its codebooks by squared distance. Under cosine
    /// the vectors are unit length, so that squared distance orders them the
    /// way the cosine does; a dot product it cannot order at all. `EUCLIDEAN`
    /// is the metric that squared distance *is*, so the restriction is not
    /// inherent there, but the quantized path has no measured recall for it, so
    /// it stays refused rather than shipped unmeasured.
    ///
    /// Refused when the definition is written rather than when the first
    /// document is indexed.
    pub fn supports_metric(self, metric: DistanceMetric) -> bool {
        match self {
            Self::IvfPq => metric == DistanceMetric::Cosine,
            Self::Hnsw | Self::Flat | Self::Ssg => true,
        }
    }
}

/// How a vector index compares two vectors.
///
/// The three Go defines, spelled its way, so a description carrying any of them
/// crosses runtimes unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DistanceMetric {
    /// Direction only: magnitude is normalized away before anything is
    /// compared.
    #[default]
    #[serde(rename = "COSINE")]
    Cosine,
    /// Straight-line (L2) distance. Compares direction and magnitude.
    #[serde(rename = "EUCLIDEAN")]
    Euclidean,
    /// Dot product, which grows with magnitude, so a longer vector pointing the
    /// same way is nearer.
    #[serde(rename = "DOT")]
    Dot,
}

impl DistanceMetric {
    pub const ALL: &'static [Self] = &[Self::Cosine, Self::Euclidean, Self::Dot];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cosine => "COSINE",
            Self::Euclidean => "EUCLIDEAN",
            Self::Dot => "DOT",
        }
    }

    pub fn from_sdl_name(name: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|metric| metric.as_str() == name)
    }

    /// Every metric here is one Go defines, so a description carrying any of
    /// them parses on either runtime. Exhaustive rather than `true` so a
    /// Rust-only metric cannot be added without answering the question.
    pub fn is_go_compatible(self) -> bool {
        match self {
            Self::Cosine | Self::Euclidean | Self::Dot => true,
        }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Ordered(OrderedIndexDescription),
    Vector(VectorIndexDescription),
}

impl Default for IndexKind {
    fn default() -> Self {
        IndexKind::Ordered(OrderedIndexDescription::default())
    }
}

const INDEX_KIND_ORDERED: u8 = 0;
const INDEX_KIND_VECTOR: u8 = 1;

#[derive(Serialize)]
#[serde(untagged)]
enum IndexKindDescriptionWire {
    Ordered(OrderedIndexDescription),
    Vector(VectorIndexDescription),
}

#[derive(Serialize)]
struct IndexKindRef {
    #[serde(rename = "Kind")]
    kind: u8,
    #[serde(rename = "KindDescription")]
    kind_description: IndexKindDescriptionWire,
}

#[derive(Deserialize)]
struct IndexKindWire {
    #[serde(rename = "Kind")]
    kind: u8,
    #[serde(rename = "KindDescription", default)]
    kind_description: Option<serde_json::Value>,
}

impl IndexKind {
    fn into_wire(self) -> (u8, IndexKindDescriptionWire) {
        match self {
            Self::Ordered(ordered) => (
                INDEX_KIND_ORDERED,
                IndexKindDescriptionWire::Ordered(ordered),
            ),
            Self::Vector(vector) => (INDEX_KIND_VECTOR, IndexKindDescriptionWire::Vector(vector)),
        }
    }

    fn from_wire<E: serde::de::Error>(
        kind: u8,
        kind_description: Option<serde_json::Value>,
        legacy_unique: bool,
    ) -> std::result::Result<Self, E> {
        match kind {
            INDEX_KIND_ORDERED => {
                let ordered = match kind_description {
                    Some(value) => serde_json::from_value(value).map_err(E::custom)?,
                    None => OrderedIndexDescription {
                        unique: legacy_unique,
                    },
                };
                Ok(Self::Ordered(ordered))
            }
            INDEX_KIND_VECTOR => {
                let vector = match kind_description {
                    Some(value) => serde_json::from_value(value).map_err(E::custom)?,
                    None => VectorIndexDescription::default(),
                };
                Ok(Self::Vector(vector))
            }
            _ => Err(E::custom(format!("unknown index kind: {kind}"))),
        }
    }
}

impl Serialize for IndexKind {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let (kind, kind_description) = self.into_wire();
        IndexKindRef {
            kind,
            kind_description,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for IndexKind {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = IndexKindWire::deserialize(deserializer)?;
        Self::from_wire(wire.kind, wire.kind_description, false)
    }
}
