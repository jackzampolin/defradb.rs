use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use query::mapper::Field;
use query::{Filter, Planner, Select};
use schema::{
    CollectionVersion, FieldDescription, FieldKind, IndexDescription, IndexedFieldDescription,
};
use std::hint::black_box;

fn map<const N: usize>(
    entries: [(String, serde_json::Value); N],
) -> serde_json::Map<String, serde_json::Value> {
    entries.into_iter().collect()
}

fn make_test_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Users",
        "v1",
        "coll-1",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    )
}

fn make_test_collection_with_index() -> CollectionVersion {
    CollectionVersion::new(
        "Users",
        "v1",
        "coll-1",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    )
    .with_index(IndexDescription {
        id: 1,
        name: "name_idx".to_string(),
        unique: false,
        fields: vec![IndexedFieldDescription {
            name: "name".to_string(),
            descending: false,
        }],
        auto_generated: false,
    })
    .with_index(IndexDescription {
        id: 2,
        name: "age_idx".to_string(),
        unique: false,
        fields: vec![IndexedFieldDescription {
            name: "age".to_string(),
            descending: false,
        }],
        auto_generated: false,
    })
}

fn make_users_collection() -> CollectionVersion {
    CollectionVersion::new(
        "users",
        "v1",
        "coll-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "posts", FieldKind::relation("posts", true))
                .with_relation_name("author_posts"),
        ],
    )
}

fn make_posts_collection() -> CollectionVersion {
    CollectionVersion::new(
        "posts",
        "v1",
        "coll-posts",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "author", FieldKind::relation("users", false))
                .with_relation_name("author_posts")
                .as_primary(),
            FieldDescription::new("4", "_authorID", FieldKind::doc_id())
                .with_relation_name("author_posts")
                .as_primary(),
        ],
    )
}

fn bench_planner(c: &mut Criterion) {
    let mut group = c.benchmark_group("planner");

    let simple_planner = Planner::new(vec![make_test_collection()]);
    let simple_select = Select::new("Users")
        .with_field(Field::new("_docID"))
        .with_field(Field::new("name"));
    group.bench_function(BenchmarkId::from_parameter("simple_scan"), |b| {
        b.iter(|| black_box(simple_planner.plan(black_box(&simple_select)).unwrap()));
    });

    let equality_planner = Planner::new(vec![make_test_collection_with_index()]);
    let equality_filter = Filter::from_conditions(map([(
        "name".to_string(),
        serde_json::json!({"_eq": "Alice"}),
    )]));
    let equality_select = Select::new("Users")
        .with_field(Field::new("name"))
        .with_filter(equality_filter);
    group.bench_function(BenchmarkId::from_parameter("with_equality_filter"), |b| {
        b.iter(|| {
            black_box(
                equality_planner
                    .plan_with_index_info(black_box(&equality_select))
                    .unwrap(),
            );
        });
    });

    let range_planner = Planner::new(vec![make_test_collection_with_index()]);
    let range_filter = Filter::from_conditions(map([(
        "age".to_string(),
        serde_json::json!({"_gte": 18, "_lt": 65}),
    )]));
    let range_select = Select::new("Users")
        .with_field(Field::new("age"))
        .with_filter(range_filter);
    group.bench_function(BenchmarkId::from_parameter("with_range_filter"), |b| {
        b.iter(|| {
            black_box(
                range_planner
                    .plan_with_index_info(black_box(&range_select))
                    .unwrap(),
            );
        });
    });

    let nested_planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);
    let posts_select = Select::new("posts")
        .with_field_name("posts")
        .with_field(Field::new("title"));
    let nested_select = Select::new("users")
        .with_field(Field::new("name"))
        .with_select(posts_select);
    group.bench_function(BenchmarkId::from_parameter("nested_join"), |b| {
        b.iter(|| black_box(nested_planner.plan(black_box(&nested_select)).unwrap()));
    });

    group.finish();
}

criterion_group!(benches, bench_planner);
criterion_main!(benches);
