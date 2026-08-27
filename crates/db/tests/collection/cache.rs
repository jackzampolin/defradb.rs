use db::collection::cache::*;
use db::collection::Collection;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;

fn test_collection(name: &str) -> Collection {
    Collection::new(CollectionVersion::new(
        name,
        "v1",
        format!("col-{}", name.to_lowercase()),
        vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
    ))
}

#[test]
fn test_cache_new_is_empty() {
    let cache = CollectionCache::new();
    assert!(cache.is_empty());
    assert_eq!(cache.len(), 0);
    assert!(!cache.is_fully_populated());
}

#[test]
fn test_cache_add_and_get() {
    let mut cache = CollectionCache::new();
    let col = test_collection("Users");

    cache.add(col);

    assert!(cache.contains("Users"));
    assert!(!cache.contains("Posts"));
    assert_eq!(cache.len(), 1);

    let retrieved = cache.get("Users");
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().name(), "Users");
}

#[test]
fn test_cache_remove() {
    let mut cache = CollectionCache::new();
    cache.add(test_collection("Users"));
    cache.add(test_collection("Posts"));

    assert_eq!(cache.len(), 2);

    let removed = cache.remove("Users");
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().name(), "Users");
    assert_eq!(cache.len(), 1);
    assert!(!cache.contains("Users"));
    assert!(cache.contains("Posts"));
}

#[test]
fn test_cache_remove_resets_fully_populated() {
    let mut cache = CollectionCache::new();
    cache.populate(vec![test_collection("Users"), test_collection("Posts")]);

    assert!(cache.is_fully_populated());
    assert_eq!(cache.len(), 2);

    cache.remove("Users");

    assert!(
        !cache.is_fully_populated(),
        "Cache should no longer be fully populated after remove"
    );
}

#[test]
fn test_cache_names() {
    let mut cache = CollectionCache::new();
    cache.add(test_collection("Users"));
    cache.add(test_collection("Posts"));

    let mut names = cache.names();
    names.sort();
    assert_eq!(names, vec!["Posts", "Users"]);
}

#[test]
fn test_cache_populate() {
    let mut cache = CollectionCache::new();
    assert!(!cache.is_fully_populated());

    cache.populate(vec![test_collection("Users"), test_collection("Posts")]);

    assert!(cache.is_fully_populated());
    assert_eq!(cache.len(), 2);
    assert!(cache.contains("Users"));
    assert!(cache.contains("Posts"));
}

#[tokio::test]
async fn active_collection_versions_exclude_inactive_cache_entries() {
    let db = db::DB::open(storage::backends::MemoryStore::new())
        .await
        .expect("open");

    let active = test_collection("Users").schema().clone();
    let mut inactive = test_collection("Orders").schema().clone();
    inactive.is_active = false;
    db.add_collection_to_cache(active).expect("cache active");
    db.add_collection_to_cache(inactive)
        .expect("cache inactive");

    let versions = db
        .get_active_collection_versions()
        .expect("read active versions");
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].name, "Users");
}
