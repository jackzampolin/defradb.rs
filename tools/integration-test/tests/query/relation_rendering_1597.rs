use crate::one_to_many_common::{add_author, add_book, add_schema};
use integration_test::TestCluster;

/// Mirrors Go `TestQueryOneToManyWithParentGroupByOnRelationAndDuplicateRelationSelection`.
async fn parent_group_by_relation_with_duplicate_selection_test(cluster: TestCluster) {
    let node = cluster.client(0);
    add_schema(&node);

    let john = add_author(&node, "John Grisham", 65, true);
    add_book(&node, "Painted House", 4.9, &john);

    let result = node
        .query(
            r#"query {
                Book(groupBy: [author]) {
                    author {
                        name
                    }
                    author {
                        name
                    }
                    GROUP {
                        name
                    }
                }
            }"#,
        )
        .expect("group by relation with duplicate selection");

    assert_eq!(
        result["Book"],
        serde_json::json!([
            {
                "author": {"name": "John Grisham"},
                "GROUP": [{"name": "Painted House"}],
            }
        ])
    );
}

/// Mirrors Go `TestQueryOneToManyWithDuplicateRelationSelectionEachWithInnerGroupByOnRelation`:
/// the duplicated relation selection must plan as exactly one `typeIndexJoin`.
///
/// Go's `ExpectedFullGraph` also nests `groupNode` and `pipeNode` inside the
/// `typeJoinMany` subType, which our planner does not emit there. That gap is
/// independent of the duplication, so this asserts the join topology the
/// duplication owns: the duplicated request must plan exactly like the
/// single-selection request, with one join and an unshared `scanNode` root.
async fn duplicate_relation_selection_plans_one_join_test(cluster: TestCluster) {
    let node = cluster.client(0);
    add_schema(&node);

    let published_group = r#"published(groupBy: [author]) {
        author {
            name
        }
        GROUP {
            name
        }
    }"#;

    let explain_for = |selections: String| {
        node.query(&format!(
            "query @explain(type: debug) {{ Author {{ {selections} }} }}"
        ))
        .expect("debug explain")
    };

    let duplicated = explain_for(format!("{published_group} {published_group}"));

    assert_eq!(
        duplicated,
        serde_json::json!({
            "explain": {
                "operationNode": [
                    {
                        "selectTopNode": {
                            "selectNode": {
                                "typeIndexJoin": {
                                    "typeJoinMany": {
                                        "root": {"scanNode": {}},
                                        "subType": {
                                            "selectTopNode": {
                                                "selectNode": {
                                                    "typeIndexJoin": {
                                                        "typeJoinOne": {
                                                            "root": {"scanNode": {}},
                                                            "subType": {
                                                                "selectTopNode": {
                                                                    "selectNode": {
                                                                        "scanNode": {}
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        }),
        "duplicate relation selection must dedup into a single join"
    );

    assert_eq!(
        duplicated,
        explain_for(published_group.to_string()),
        "duplicating a relation selection must not change the plan"
    );
}

/// Mirrors Go `TestQueryOneToManyWithGroupByAndFilterOnParentRelation`: the `_docID`
/// the relation filter needs internally must not leak into the rendered GROUP items.
async fn group_by_with_parent_relation_filter_test(cluster: TestCluster) {
    let node = cluster.client(0);
    add_schema(&node);

    let john = add_author(&node, "John Grisham", 65, true);
    let voltaire = add_author(&node, "Voltaire", 327, true);

    add_book(&node, "Painted House", 4.9, &john);
    add_book(&node, "A Time for Mercy", 4.5, &john);
    add_book(&node, "Candide", 4.95, &voltaire);
    add_book(&node, "Zadig", 4.91, &voltaire);

    let result = node
        .query(
            r#"query {
                Book(filter: {author: {name: {_like: "John%"}}}, groupBy: [rating]) {
                    rating
                    GROUP {
                        name
                        author { name }
                    }
                }
            }"#,
        )
        .expect("group by rating with parent relation filter");

    let mut books = result["Book"]
        .as_array()
        .unwrap_or_else(|| panic!("expected a Book array, got: {result}"))
        .clone();
    books.sort_by(|a, b| {
        a["rating"]
            .as_f64()
            .unwrap()
            .partial_cmp(&b["rating"].as_f64().unwrap())
            .unwrap()
    });

    assert_eq!(
        serde_json::Value::Array(books),
        serde_json::json!([
            {
                "rating": 4.5,
                "GROUP": [
                    {"name": "A Time for Mercy", "author": {"name": "John Grisham"}}
                ],
            },
            {
                "rating": 4.9,
                "GROUP": [
                    {"name": "Painted House", "author": {"name": "John Grisham"}}
                ],
            },
        ])
    );
}

/// The duplicate can arrive through a fragment spread, not just as a literal
/// repeat, and must dedup the same way.
async fn duplicate_selection_via_fragment_spread_test(cluster: TestCluster) {
    let node = cluster.client(0);
    add_schema(&node);

    let john = add_author(&node, "John Grisham", 65, true);
    add_book(&node, "Painted House", 4.9, &john);

    let expected = serde_json::json!([
        {
            "author": {"name": "John Grisham"},
            "GROUP": [{"name": "Painted House"}],
        }
    ]);

    let inline_plus_fragment = node
        .query(
            r#"query {
                Book(groupBy: [author]) {
                    author { name }
                    ...AuthorName
                    GROUP { name }
                }
            }
            fragment AuthorName on Book { author { name } }"#,
        )
        .expect("group by relation with fragment duplicate");
    assert_eq!(inline_plus_fragment["Book"], expected);

    let fragment_twice = node
        .query(
            r#"query {
                Book(groupBy: [author]) {
                    ...AuthorName
                    ...AuthorName
                    GROUP { name }
                }
            }
            fragment AuthorName on Book { author { name } }"#,
        )
        .expect("group by relation with repeated fragment spread");
    assert_eq!(fragment_twice["Book"], expected);
}

/// Same, through an inline fragment.
async fn duplicate_selection_via_inline_fragment_test(cluster: TestCluster) {
    let node = cluster.client(0);
    add_schema(&node);

    let john = add_author(&node, "John Grisham", 65, true);
    add_book(&node, "Painted House", 4.9, &john);

    let result = node
        .query(
            r#"query {
                Book(groupBy: [author]) {
                    author { name }
                    ... on Book { author { name } }
                    GROUP { name }
                }
            }"#,
        )
        .expect("group by relation with inline fragment duplicate");

    assert_eq!(
        result["Book"],
        serde_json::json!([
            {
                "author": {"name": "John Grisham"},
                "GROUP": [{"name": "Painted House"}],
            }
        ])
    );
}

/// GraphQL arguments are unordered, so two selections differing only in argument
/// order are the same selection and must dedup.
async fn duplicate_selection_with_reordered_arguments_test(cluster: TestCluster) {
    let node = cluster.client(0);
    add_schema(&node);

    let john = add_author(&node, "John Grisham", 65, true);
    add_book(&node, "Painted House", 4.9, &john);
    add_book(&node, "A Time for Mercy", 4.5, &john);
    add_book(&node, "The Associate", 4.2, &john);

    let result = node
        .query(
            r#"query {
                Author {
                    name
                    published(limit: 2, order: {rating: ASC}) { name }
                    published(order: {rating: ASC}, limit: 2) { name }
                }
            }"#,
        )
        .expect("relation selection with reordered arguments");

    assert_eq!(
        result["Author"],
        serde_json::json!([
            {
                "name": "John Grisham",
                "published": [{"name": "The Associate"}, {"name": "A Time for Mercy"}],
            }
        ])
    );
}

#[tokio::test]
async fn rust_relation_rendering_1597_duplicate_selection_via_fragment_spread() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    duplicate_selection_via_fragment_spread_test(cluster).await;
}

#[tokio::test]
async fn rust_relation_rendering_1597_duplicate_selection_via_inline_fragment() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    duplicate_selection_via_inline_fragment_test(cluster).await;
}

#[tokio::test]
async fn rust_relation_rendering_1597_duplicate_selection_with_reordered_arguments() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    duplicate_selection_with_reordered_arguments_test(cluster).await;
}

#[tokio::test]
async fn rust_relation_rendering_1597_parent_group_by_relation_with_duplicate_selection() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    parent_group_by_relation_with_duplicate_selection_test(cluster).await;
}

#[tokio::test]
async fn rust_relation_rendering_1597_duplicate_relation_selection_plans_one_join() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    duplicate_relation_selection_plans_one_join_test(cluster).await;
}

#[tokio::test]
async fn rust_relation_rendering_1597_group_by_with_parent_relation_filter() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    group_by_with_parent_relation_filter_test(cluster).await;
}
