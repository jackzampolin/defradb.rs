use db::database::DB;
use schema::FieldKind;
use storage::backends::MemoryStore;

#[tokio::test]
async fn patch_collection_preserves_runtime_root_id() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();

    let collections = query::parse_sdl(
        r#"
        type Users {
            name: String
        }
        "#,
    )
    .unwrap();
    db.create_collections_atomic(collections).await.unwrap();

    let original = db.get_collection("Users").unwrap().unwrap();
    let root_id = original.resolved_root_id();
    assert_ne!(root_id, 0);

    let patched = db
        .patch_collection(
            "Users",
            r#"
            [
                { "op": "add", "path": "/Users/Fields/-", "value": {
                    "Name": "age", "Kind": "Int"
                }}
            ]
            "#,
            None,
        )
        .await
        .unwrap();

    assert_eq!(patched.root_id, root_id);
    assert_eq!(
        db.get_collection("Users")
            .unwrap()
            .unwrap()
            .resolved_root_id(),
        root_id
    );
}

#[tokio::test]
async fn patch_collection_rejects_numeric_kind_before_it_decodes_as_int_array() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();

    let collections = query::parse_sdl(
        r#"
        type AgentResponse {
            message: String
        }
        "#,
    )
    .unwrap();
    db.create_collections_atomic(collections).await.unwrap();

    let err = db
        .patch_collection(
            "AgentResponse",
            r#"
            [
                { "op": "add", "path": "/AgentResponse/Fields/-", "value": {
                    "Name": "reasoning_progress_seq", "Kind": 5
                }}
            ]
            "#,
            None,
        )
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid patch: numeric Kind values are not supported in schema patches. Field: reasoning_progress_seq, Kind: 5. Use canonical string \"[Int!]\" instead."
    );

    let patched = db
        .patch_collection(
            "AgentResponse",
            r#"
            [
                { "op": "add", "path": "/AgentResponse/Fields/-", "value": {
                    "Name": "reasoning_progress_seq", "Kind": "Int"
                }}
            ]
            "#,
            None,
        )
        .await
        .unwrap();

    let reasoning_progress_seq = patched
        .fields
        .iter()
        .find(|field| field.name == "reasoning_progress_seq")
        .unwrap();
    assert_eq!(reasoning_progress_seq.kind, FieldKind::int());
}

#[tokio::test]
async fn patch_relation_version_switching_preserves_go_canonical_versions() {
    const AUTHOR_V1: &str = "bafyreibvcavbxqwimz5vdxe5q5href63g3skc6ytg45hm4fqh6wsx57wmq";
    const AUTHOR_V2: &str = "bafyreihv2jdbz3sipc7tqdoycerkcjn6gehr5aleiroqlewvsmjd26unfq";

    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();

    let collections = query::parse_sdl(
        r#"
        type Author {
            name: String
        }
        type Book {
            title: String
        }
        "#,
    )
    .unwrap();
    db.create_collections_atomic(collections).await.unwrap();

    let author = db.get_collection("Author").unwrap().unwrap();
    assert_eq!(author.version_id(), AUTHOR_V1);

    db.patch_collection(
        "Author",
        r#"
        [
            { "op": "add", "path": "/Author/Fields/-", "value": {
                "Name": "published", "Kind": "Book", "RelationName": "author_book", "IsPrimary": true
            }},
            { "op": "add", "path": "/Author/Fields/-", "value": {
                "Name": "_publishedID", "Kind": "ID", "RelationName": "author_book", "IsPrimary": true
            }}
        ]
        "#,
        None,
    )
    .await
    .unwrap();

    db.patch_collection(
        "Book",
        r#"
        [
            { "op": "add", "path": "/Book/Fields/-", "value": {
                "Name": "author", "Kind": "Author", "RelationName": "author_book"
            }}
        ]
        "#,
        None,
    )
    .await
    .unwrap();

    let author_v2 = db
        .get_collection_by_version_id_full(AUTHOR_V2)
        .await
        .unwrap();
    assert!(author_v2.is_some());

    db.set_active_collection_version(AUTHOR_V1).await.unwrap();
    assert!(db
        .get_collection("Author")
        .unwrap()
        .unwrap()
        .get_indexes()
        .is_empty());

    db.set_active_collection_version(AUTHOR_V2).await.unwrap();
    let author = db.get_collection("Author").unwrap().unwrap();
    assert_eq!(author.version_id(), AUTHOR_V2);
    assert_eq!(author.get_indexes().len(), 1);
    assert_eq!(author.get_indexes()[0].name, "Author__publishedID_ASC");
}
