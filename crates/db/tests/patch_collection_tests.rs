use db::database::DB;
use schema::FieldKind;
use storage::backends::MemoryStore;

async fn agent_response_db() -> DB<MemoryStore> {
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
    db
}

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
    let db = agent_response_db().await;

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
        "invalid patch: numeric Kind values are not supported in schema patches. Field: reasoning_progress_seq, Kind: 5. It maps to \"[Int!]\"; use that string only if that type is intended, otherwise use the intended type's canonical string."
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
async fn patch_collection_rejects_numeric_kind_in_whole_fields_replacement() {
    let db = agent_response_db().await;
    let collection = db.get_collection("AgentResponse").unwrap().unwrap();
    let mut fields = serde_json::to_value(&collection.schema().fields).unwrap();

    for field in fields.as_array_mut().unwrap() {
        let map = field.as_object_mut().unwrap();
        let canonical = match map.get("Name").and_then(|name| name.as_str()).unwrap() {
            "_docID" => "ID",
            "message" => "String",
            name => panic!("unexpected field: {name}"),
        };
        map.insert("Kind".to_string(), serde_json::json!(canonical));
    }
    fields.as_array_mut().unwrap().push(serde_json::json!({
        "FieldID": "9",
        "Name": "reasoning_progress_seq",
        "Kind": 5
    }));

    let patch = serde_json::json!([{
        "op": "replace",
        "path": "/AgentResponse/Fields",
        "value": fields
    }]);
    let err = db
        .patch_collection("AgentResponse", &patch.to_string(), None)
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid patch: numeric Kind values are not supported in schema patches. Field: reasoning_progress_seq, Kind: 5. It maps to \"[Int!]\"; use that string only if that type is intended, otherwise use the intended type's canonical string."
    );
}

#[tokio::test]
async fn patch_collection_rejects_numeric_kind_in_direct_kind_replacement() {
    let db = agent_response_db().await;
    let err = db
        .patch_collection(
            "AgentResponse",
            r#"
            [{
                "op": "replace",
                "path": "/AgentResponse/Fields/message/Kind",
                "value": 3
            }]
            "#,
            None,
        )
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid patch: numeric Kind values are not supported in schema patches. Field: message, Kind: 3. It maps to \"[Boolean!]\"; use that string only if that type is intended, otherwise use the intended type's canonical string."
    );
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
