use std::collections::BTreeSet;

use integration_test::TestCluster;

async fn unsupported_index_filters_fall_back_to_scan_test(cluster: TestCluster) {
    let node = cluster.client(0);

    node.schema_add(
        r#"
        type Users {
            name: String
            custom: JSON @index
            likedIndexes: [Boolean] @index
        }
        "#,
    )
    .expect("add schema");

    node.query(
        r#"mutation {
            add_Users(input: {name: "Dany", custom: "Daenerys Stormborn", likedIndexes: [false]}) { _docID }
        }"#,
    )
    .expect("add Dany");
    node.query(
        r#"mutation {
            add_Users(input: {name: "Viserys", custom: "Viserys I Targaryen", likedIndexes: [false]}) { _docID }
        }"#,
    )
    .expect("add Viserys");
    node.query(
        r#"mutation {
            add_Users(input: {name: "Object", custom: {one: 1}, likedIndexes: [false]}) { _docID }
        }"#,
    )
    .expect("add object");
    node.query(
        r#"mutation {
            add_Users(input: {name: "Array", custom: [1, 2], likedIndexes: [false]}) { _docID }
        }"#,
    )
    .expect("add array");
    node.query(
        r#"mutation {
            add_Users(input: {name: "EmptyObject", custom: {}, likedIndexes: [false]}) { _docID }
        }"#,
    )
    .expect("add empty object");
    node.query(
        r#"mutation {
            add_Users(input: {name: "EmptyArray", custom: [], likedIndexes: [false]}) { _docID }
        }"#,
    )
    .expect("add empty array");
    node.query(
        r#"mutation {
            add_Users(input: {name: "Boolean", custom: false, likedIndexes: [false]}) { _docID }
        }"#,
    )
    .expect("add bool");
    node.query(
        r#"mutation {
            add_Users(input: {name: "Number", custom: 32, likedIndexes: [false]}) { _docID }
        }"#,
    )
    .expect("add number");
    node.query(
        r#"mutation {
            add_Users(input: {name: "Shahzad", custom: "array owner", likedIndexes: [true, false]}) { _docID }
        }"#,
    )
    .expect("add Shahzad");
    node.query(
        r#"mutation {
            add_Users(input: {name: "Fred", custom: "array miss", likedIndexes: [true, true]}) { _docID }
        }"#,
    )
    .expect("add Fred");

    let nlike_result = node
        .query(
            r#"
            query {
                Users(filter: {custom: {_nlike: "%Stormborn%"}}) {
                    name
                }
            }
            "#,
        )
        .expect("query JSON _nlike");
    let names: BTreeSet<_> = nlike_result["Users"]
        .as_array()
        .unwrap()
        .iter()
        .map(|user| user["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        names,
        BTreeSet::from([
            "Array".to_string(),
            "Boolean".to_string(),
            "EmptyArray".to_string(),
            "EmptyObject".to_string(),
            "Fred".to_string(),
            "Number".to_string(),
            "Object".to_string(),
            "Shahzad".to_string(),
            "Viserys".to_string(),
        ])
    );

    let array_eq_result = node
        .query(
            r#"
            query {
                Users(filter: {likedIndexes: {_eq: [true, false]}}) {
                    name
                }
            }
            "#,
        )
        .expect("query array literal equality");
    assert_eq!(
        array_eq_result["Users"],
        serde_json::json!([{ "name": "Shahzad" }])
    );

    let err = node
        .query(
            r#"
            query {
                Users(filter: {custom: {_gt: false}}) {
                    name
                }
            }
            "#,
        )
        .expect_err("non-numeric JSON ordering filter should be rejected");
    assert!(
        err.to_string()
            .contains("unexpected type. Property: condition, Actual: bool"),
        "unexpected non-numeric JSON ordering error: {err}"
    );
}

#[tokio::test]
async fn rust_unsupported_index_filters_fall_back_to_scan() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    unsupported_index_filters_fall_back_to_scan_test(cluster).await;
}
