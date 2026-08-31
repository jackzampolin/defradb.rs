use integration_test::{assert_query_equivalent, TestCluster};

const SCHEMA: &str = r#"
    type ScalarCase {
        tag: String!
        text: String!
        optionalText: String
        integer: Int!
        optionalInteger: Int
        ratio: Float64!
        optionalRatio: Float64
        enabled: Boolean!
        optionalEnabled: Boolean
        observed: DateTime!
        optionalObserved: DateTime
        payload: Blob!
        optionalPayload: Blob
        metadata: JSON!
        optionalMetadata: JSON
    }

    type QueryCase {
        name: String
        category: String
        rank: Int
        score: Float
        optionalRank: Int
    }

    type Book {
        name: String
        rating: Float
        author: Author
    }

    type Author {
        name: String
        published: [Book]
    }
"#;

#[tokio::test]
async fn go_cross_runtime_crud_and_query_equivalence() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .build()
        .await
        .unwrap();
    let rust = cluster.client(0);
    let go = cluster.client(1);
    rust.schema_add(SCHEMA).expect("add Rust schema");
    go.schema_add(SCHEMA).expect("add Go schema");

    for document in [
        r#"{
            "tag": "boundaries",
            "text": "Grusse 東京",
            "optionalText": "",
            "integer": 9223372036854775807,
            "optionalInteger": -9223372036854775808,
            "ratio": 1.23456789012345,
            "optionalRatio": -0.0,
            "enabled": true,
            "optionalEnabled": false,
            "observed": "2400-01-01T00:00:00Z",
            "optionalObserved": "1600-01-01T00:00:00Z",
            "payload": "00FF",
            "optionalPayload": "00",
            "metadata": {"nested": {"value": 1}, "list": [true, null, "x"]},
            "optionalMetadata": {}
        }"#,
        r#"{
            "tag": "nulls",
            "text": "",
            "integer": -9223372036854775808,
            "ratio": 0.0,
            "enabled": false,
            "observed": "2000-01-01T00:00:00Z",
            "payload": "00",
            "metadata": {}
        }"#,
    ] {
        rust.collection_create("ScalarCase", document)
            .expect("create Rust scalar fixture");
        go.collection_create("ScalarCase", document)
            .expect("create Go scalar fixture");
    }
    assert_query_equivalent(
        &rust,
        &go,
        r#"query {
            ScalarCase(order: {tag: ASC}) {
                tag text optionalText integer optionalInteger ratio optionalRatio
                enabled optionalEnabled observed optionalObserved payload optionalPayload
                metadata optionalMetadata
            }
        }"#,
    );

    for input in [
        r#"{name: "alpha", category: "a", rank: 1, score: 10.5}"#,
        r#"{name: "beta", category: "a", rank: 2, score: 20, optionalRank: 2}"#,
        r#"{name: "gamma", category: "b", rank: 2, score: 30.25}"#,
        r#"{name: "alphabet", category: "b", rank: 3, score: 40, optionalRank: 3}"#,
        r#"{name: "omega", category: "a", rank: 4, score: 50.25, optionalRank: 4}"#,
    ] {
        assert_query_equivalent(
            &rust,
            &go,
            &format!(r#"mutation {{ add_QueryCase(input: {input}) {{ _docID }} }}"#),
        );
    }

    for query in [
        "query { QueryCase { name rank } }",
        "query { QueryCase(order: [{rank: ASC}, {name: DESC}]) { name rank } }",
        "query { QueryCase(order: {optionalRank: ASC}) { name optionalRank } }",
        r#"query { QueryCase(filter: {name: {_eq: "alpha"}}) { name } }"#,
        r#"query { QueryCase(filter: {name: {_neq: "alpha"}}, order: {name: ASC}) { name } }"#,
        "query { QueryCase(filter: {_and: [{rank: {_gt: 1, _geq: 2}}, {rank: {_lt: 4, _leq: 3}}]}, order: {name: ASC}) { name rank } }",
        "query { QueryCase(filter: {rank: {_in: [1, 3], _nin: [2]}}, order: {rank: ASC}) { name rank } }",
        r#"query { QueryCase(filter: {name: {_like: "alpha%"}}, order: {name: ASC}) { name } }"#,
        r#"query { QueryCase(filter: {name: {_nlike: "%ha%"}}, order: {name: ASC}) { name } }"#,
        "query { QueryCase(filter: {optionalRank: {_eq: null}}, order: {name: ASC}) { name optionalRank } }",
        "query { QueryCase(filter: {optionalRank: {_in: [null, 3]}}, order: {name: ASC}) { name optionalRank } }",
        "query { QueryCase(order: {rank: ASC}, limit: 2, offset: 1) { name rank } }",
        "query { count: COUNT(QueryCase: {}) sum: SUM(QueryCase: {field: score}) avg: AVG(QueryCase: {field: score}) }",
        "query { count: COUNT(QueryCase: {filter: {rank: {_gt: 100}}}) sum: SUM(QueryCase: {field: score, filter: {rank: {_gt: 100}}}) avg: AVG(QueryCase: {field: score, filter: {rank: {_gt: 100}}}) }",
    ] {
        assert_query_equivalent(&rust, &go, query);
    }

    assert_query_equivalent(
        &rust,
        &go,
        r#"mutation {
            update_QueryCase(filter: {name: {_eq: "beta"}}, input: {score: 21.5}) {
                name score
            }
        }"#,
    );
    assert_query_equivalent(
        &rust,
        &go,
        r#"mutation {
            delete_QueryCase(filter: {name: {_eq: "omega"}}) {
                name rank
            }
        }"#,
    );
    assert_query_equivalent(
        &rust,
        &go,
        "query { QueryCase(order: {name: ASC}) { name rank score } }",
    );

    let ada = assert_query_equivalent(
        &rust,
        &go,
        r#"mutation { add_Author(input: {name: "Ada"}) { _docID } }"#,
    )["add_Author"][0]["_docID"]
        .as_str()
        .expect("Ada document ID")
        .to_string();
    let grace = assert_query_equivalent(
        &rust,
        &go,
        r#"mutation { add_Author(input: {name: "Grace"}) { _docID } }"#,
    )["add_Author"][0]["_docID"]
        .as_str()
        .expect("Grace document ID")
        .to_string();

    for (name, rating, author) in [
        ("Compiler", 4.9, ada.as_str()),
        ("Notes", 4.2, ada.as_str()),
        ("Systems", 4.8, grace.as_str()),
        ("COBOL", 4.0, grace.as_str()),
    ] {
        assert_query_equivalent(
            &rust,
            &go,
            &format!(
                r#"mutation {{ add_Book(input: {{name: "{name}", rating: {rating}, author: "{author}"}}) {{ _docID }} }}"#
            ),
        );
    }

    for query in [
        r#"query {
            Author(filter: {name: {_like: "A%"}}) {
                name
                published(filter: {rating: {_gt: 4}}, order: {rating: DESC}, limit: 1) {
                    name rating author { name }
                }
            }
        }"#,
        "query { Book(order: {rating: ASC}, limit: 2, offset: 1) { name rating author { name } } }",
    ] {
        assert_query_equivalent(&rust, &go, query);
    }
}
