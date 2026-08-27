use schema::CollectionVersion;
use schema::DistanceMetric;
use schema::FieldDescription;
use schema::FieldKind;
use schema::HnswParams;
use schema::IndexKind;
use schema::VectorAlgorithm;
use schema::VectorIndexDescription;

pub fn test_schema() -> CollectionVersion {
    CollectionVersion::new(
        "Users",
        "v1",
        "col-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    )
}

pub fn users_schema() -> CollectionVersion {
    let mut schema = CollectionVersion::new(
        "Users",
        "users-v1",
        "users-collection",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    );
    schema.is_materialized = true;
    schema
}

pub const COLLECTION_SHORT_ID: u32 = 1;
pub const DIMENSIONS: u32 = 4;

pub fn docs_schema() -> CollectionVersion {
    CollectionVersion::new(
        "docs",
        "v1",
        "col-docs",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "embedding", FieldKind::float64_array()),
        ],
    )
}

pub fn vector_kind() -> IndexKind {
    IndexKind::Vector(VectorIndexDescription {
        algorithm: VectorAlgorithm::Hnsw,
        metric: DistanceMetric::Cosine,
        dimensions: DIMENSIONS,
        hnsw: Some(HnswParams::default()),
        ivfpq: None,
        ssg: None,
    })
}

pub fn test_collection() -> CollectionVersion {
    CollectionVersion::new(
        "TestDoc",
        "v1",
        "col-test-doc",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "x", FieldKind::int()),
        ],
    )
}
