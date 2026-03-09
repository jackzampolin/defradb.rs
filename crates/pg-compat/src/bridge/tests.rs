use schema::ScalarKind;

use super::*;

// ── SELECT tests ──

#[test]
fn simple_select_all() {
    let stmt = sql_to_graphql("SELECT name, age FROM User").unwrap();
    assert_eq!(
        stmt,
        SqlStatement::Query("query { User { name age } }".into())
    );
}

#[test]
fn select_with_where() {
    let stmt = sql_to_graphql("SELECT name FROM User WHERE age > 25").unwrap();
    assert_eq!(
        stmt,
        SqlStatement::Query("query { User(filter: {age: {_gt: 25}}) { name } }".into())
    );
}

#[test]
fn select_with_order() {
    let stmt = sql_to_graphql("SELECT name FROM User ORDER BY name").unwrap();
    assert_eq!(
        stmt,
        SqlStatement::Query("query { User(order: {name: ASC}) { name } }".into())
    );
}

#[test]
fn select_with_limit_offset() {
    let stmt = sql_to_graphql("SELECT name FROM User LIMIT 10 OFFSET 5").unwrap();
    assert_eq!(
        stmt,
        SqlStatement::Query("query { User(limit: 10, offset: 5) { name } }".into())
    );
}

#[test]
fn select_with_string_where() {
    let stmt = sql_to_graphql("SELECT name FROM User WHERE name = 'Alice'").unwrap();
    assert_eq!(
        stmt,
        SqlStatement::Query("query { User(filter: {name: {_eq: \"Alice\"}}) { name } }".into())
    );
}

#[test]
fn select_with_and() {
    let stmt = sql_to_graphql("SELECT name FROM User WHERE age > 25 AND name = 'Alice'").unwrap();
    match stmt {
        SqlStatement::Query(gql) => {
            assert!(gql.contains("_and"));
            assert!(gql.contains("_gt: 25"));
            assert!(gql.contains("_eq: \"Alice\""));
        }
        _ => panic!("expected Query"),
    }
}

// ── INSERT tests ──

#[test]
fn insert_single_row() {
    let stmt = sql_to_graphql("INSERT INTO User (name, age) VALUES ('Alice', 30)").unwrap();
    match stmt {
        SqlStatement::Mutation {
            graphql,
            table_name,
            mutation_name,
            kind,
        } => {
            assert_eq!(kind, MutationKind::Insert);
            assert_eq!(table_name, "User");
            assert_eq!(mutation_name, "add_User");
            assert_eq!(
                graphql,
                "mutation { add_User(input: {name: \"Alice\", age: 30}) { _docID } }"
            );
        }
        _ => panic!("expected Mutation"),
    }
}

#[test]
fn insert_multi_row() {
    let stmt =
        sql_to_graphql("INSERT INTO User (name, age) VALUES ('Alice', 30), ('Bob', 25)").unwrap();
    match stmt {
        SqlStatement::Mutation { graphql, kind, .. } => {
            assert_eq!(kind, MutationKind::Insert);
            assert!(graphql.contains("[{name: \"Alice\", age: 30}, {name: \"Bob\", age: 25}]"));
        }
        _ => panic!("expected Mutation"),
    }
}

#[test]
fn insert_with_returning() {
    let stmt =
        sql_to_graphql("INSERT INTO User (name, age) VALUES ('Alice', 30) RETURNING _docID, name")
            .unwrap();
    match stmt {
        SqlStatement::Mutation { graphql, .. } => {
            assert!(graphql.contains("{ _docID name }"));
        }
        _ => panic!("expected Mutation"),
    }
}

#[test]
fn insert_without_columns_fails() {
    let result = sql_to_graphql("INSERT INTO User VALUES ('Alice', 30)");
    assert!(result.is_err());
}

// ── UPDATE tests ──

#[test]
fn update_with_where() {
    let stmt =
        sql_to_graphql("UPDATE User SET age = 31, name = 'Bob' WHERE name = 'Alice'").unwrap();
    match stmt {
        SqlStatement::Mutation {
            graphql,
            table_name,
            mutation_name,
            kind,
        } => {
            assert_eq!(kind, MutationKind::Update);
            assert_eq!(table_name, "User");
            assert_eq!(mutation_name, "update_User");
            assert_eq!(
                graphql,
                "mutation { update_User(filter: {name: {_eq: \"Alice\"}}, input: {age: 31, name: \"Bob\"}) { _docID } }"
            );
        }
        _ => panic!("expected Mutation"),
    }
}

#[test]
fn update_with_docid() {
    let stmt = sql_to_graphql("UPDATE User SET age = 31 WHERE _docID = 'bae-abc123'").unwrap();
    match stmt {
        SqlStatement::Mutation { graphql, kind, .. } => {
            assert_eq!(kind, MutationKind::Update);
            assert!(graphql.contains("docID: \"bae-abc123\""));
            assert!(graphql.contains("input: {age: 31}"));
        }
        _ => panic!("expected Mutation"),
    }
}

#[test]
fn update_without_where() {
    let stmt = sql_to_graphql("UPDATE User SET age = 0").unwrap();
    match stmt {
        SqlStatement::Mutation { graphql, kind, .. } => {
            assert_eq!(kind, MutationKind::Update);
            assert_eq!(
                graphql,
                "mutation { update_User(input: {age: 0}) { _docID } }"
            );
        }
        _ => panic!("expected Mutation"),
    }
}

#[test]
fn update_arithmetic_rejected() {
    let result = sql_to_graphql("UPDATE User SET age = age + 1 WHERE name = 'Alice'");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("arithmetic"));
}

// ── DELETE tests ──

#[test]
fn delete_with_where() {
    let stmt = sql_to_graphql("DELETE FROM User WHERE name = 'Alice'").unwrap();
    match stmt {
        SqlStatement::Mutation {
            graphql,
            table_name,
            mutation_name,
            kind,
        } => {
            assert_eq!(kind, MutationKind::Delete);
            assert_eq!(table_name, "User");
            assert_eq!(mutation_name, "delete_User");
            assert_eq!(
                graphql,
                "mutation { delete_User(filter: {name: {_eq: \"Alice\"}}) { _docID } }"
            );
        }
        _ => panic!("expected Mutation"),
    }
}

#[test]
fn delete_with_docid() {
    let stmt = sql_to_graphql("DELETE FROM User WHERE _docID = 'bae-abc123'").unwrap();
    match stmt {
        SqlStatement::Mutation { graphql, kind, .. } => {
            assert_eq!(kind, MutationKind::Delete);
            assert!(graphql.contains("docID: \"bae-abc123\""));
        }
        _ => panic!("expected Mutation"),
    }
}

#[test]
fn delete_without_where() {
    let stmt = sql_to_graphql("DELETE FROM User").unwrap();
    match stmt {
        SqlStatement::Mutation { graphql, kind, .. } => {
            assert_eq!(kind, MutationKind::Delete);
            assert_eq!(graphql, "mutation { delete_User { _docID } }");
        }
        _ => panic!("expected Mutation"),
    }
}

// ── Transaction tests ──

#[test]
fn begin_commit_rollback() {
    assert_eq!(sql_to_graphql("BEGIN").unwrap(), SqlStatement::Begin);
    assert_eq!(
        sql_to_graphql("START TRANSACTION").unwrap(),
        SqlStatement::Begin
    );
    assert_eq!(sql_to_graphql("COMMIT").unwrap(), SqlStatement::Commit);
    assert_eq!(sql_to_graphql("ROLLBACK").unwrap(), SqlStatement::Rollback);
}

// ── Parameter substitution tests ──

#[test]
fn substitute_string_param() {
    let sql = "SELECT * FROM users WHERE name = $1";
    let result = substitute_params(sql, &[Some("Alice".into())]);
    assert_eq!(result, "SELECT * FROM users WHERE name = 'Alice'");
}

#[test]
fn substitute_numeric_param() {
    let sql = "SELECT * FROM users WHERE age > $1";
    let result = substitute_params(sql, &[Some("25".into())]);
    assert_eq!(result, "SELECT * FROM users WHERE age > 25");
}

#[test]
fn substitute_null_param() {
    let sql = "INSERT INTO users (name) VALUES ($1)";
    let result = substitute_params(sql, &[None]);
    assert_eq!(result, "INSERT INTO users (name) VALUES (NULL)");
}

#[test]
fn substitute_multiple_params() {
    let sql = "INSERT INTO users (name, age) VALUES ($1, $2)";
    let result = substitute_params(sql, &[Some("Bob".into()), Some("30".into())]);
    assert_eq!(result, "INSERT INTO users (name, age) VALUES ('Bob', 30)");
}

#[test]
fn substitute_escapes_quotes() {
    let sql = "SELECT * FROM users WHERE name = $1";
    let result = substitute_params(sql, &[Some("O'Brien".into())]);
    assert_eq!(result, "SELECT * FROM users WHERE name = 'O''Brien'");
}

#[test]
fn substitute_no_params() {
    let sql = "SELECT * FROM users";
    let result = substitute_params(sql, &[]);
    assert_eq!(result, "SELECT * FROM users");
}

#[test]
fn substitute_boolean_param() {
    let sql = "SELECT * FROM users WHERE active = $1";
    let result = substitute_params(sql, &[Some("true".into())]);
    assert_eq!(result, "SELECT * FROM users WHERE active = true");
}

#[test]
fn count_params_basic() {
    assert_eq!(
        count_params("SELECT * FROM users WHERE name = $1 AND age > $2"),
        2
    );
    assert_eq!(count_params("SELECT * FROM users"), 0);
    assert_eq!(
        count_params("INSERT INTO t (a, b, c) VALUES ($1, $2, $3)"),
        3
    );
}

#[test]
fn extract_table_from_select() {
    assert_eq!(
        extract_table_from_sql("SELECT name FROM users WHERE id = 1"),
        Some("users".into())
    );
}

#[test]
fn extract_table_from_insert() {
    assert_eq!(
        extract_table_from_sql("INSERT INTO users (name) VALUES ('a')"),
        Some("users".into())
    );
}

#[test]
fn extract_table_from_begin() {
    assert_eq!(extract_table_from_sql("BEGIN"), None);
}

// ── ILIKE / BETWEEN / NOT tests ──

#[test]
fn ilike_basic() {
    let stmt = sql_to_graphql("SELECT name FROM User WHERE name ILIKE '%alice%'").unwrap();
    match stmt {
        SqlStatement::Query(gql) => {
            assert!(gql.contains("_ilike: \"%alice%\""), "got: {}", gql);
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn not_ilike() {
    let stmt = sql_to_graphql("SELECT name FROM User WHERE name NOT ILIKE '%bob%'").unwrap();
    match stmt {
        SqlStatement::Query(gql) => {
            assert!(gql.contains("_nilike: \"%bob%\""), "got: {}", gql);
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn between_basic() {
    let stmt = sql_to_graphql("SELECT name FROM User WHERE age BETWEEN 10 AND 30").unwrap();
    match stmt {
        SqlStatement::Query(gql) => {
            assert!(gql.contains("_and:"), "got: {}", gql);
            assert!(gql.contains("_ge: 10"), "got: {}", gql);
            assert!(gql.contains("_le: 30"), "got: {}", gql);
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn not_between() {
    let stmt = sql_to_graphql("SELECT name FROM User WHERE age NOT BETWEEN 10 AND 30").unwrap();
    match stmt {
        SqlStatement::Query(gql) => {
            assert!(gql.contains("_or:"), "got: {}", gql);
            assert!(gql.contains("_lt: 10"), "got: {}", gql);
            assert!(gql.contains("_gt: 30"), "got: {}", gql);
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn not_condition() {
    let stmt = sql_to_graphql("SELECT name FROM User WHERE NOT (age > 25)").unwrap();
    match stmt {
        SqlStatement::Query(gql) => {
            assert!(gql.contains("_not:"), "got: {}", gql);
            assert!(gql.contains("_gt: 25"), "got: {}", gql);
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn count_distinct_detection() {
    let stmt = sql_to_graphql("SELECT COUNT(DISTINCT status) AS cnt FROM todo").unwrap();
    match stmt {
        SqlStatement::Aggregate { aggregates, .. } => {
            assert_eq!(aggregates.len(), 1);
            assert!(aggregates[0].distinct, "expected distinct=true");
            assert_eq!(aggregates[0].field, Some("status".to_string()));
            assert_eq!(aggregates[0].alias, "cnt");
        }
        _ => panic!("expected Aggregate, got: {:?}", stmt),
    }
}

// ── Schema-aware type coercion tests ──

#[test]
fn insert_coerces_number_to_string_for_string_field() {
    let mut types = FieldTypeMap::new();
    types.insert("version".to_string(), ScalarKind::String);
    types.insert("count".to_string(), ScalarKind::Int);

    let stmt = sql_to_graphql_typed(
        "INSERT INTO session (version, count) VALUES (2, 42)",
        Some(&types),
    )
    .unwrap();

    match stmt {
        SqlStatement::Mutation { graphql, .. } => {
            assert!(
                graphql.contains("version: \"2\""),
                "expected version: \"2\", got: {}",
                graphql
            );
            assert!(
                graphql.contains("count: 42"),
                "expected count: 42, got: {}",
                graphql
            );
        }
        _ => panic!("expected Mutation"),
    }
}

#[test]
fn insert_without_types_preserves_numbers() {
    let stmt = sql_to_graphql_typed("INSERT INTO session (version) VALUES (2)", None).unwrap();

    match stmt {
        SqlStatement::Mutation { graphql, .. } => {
            assert!(
                graphql.contains("version: 2"),
                "expected bare version: 2, got: {}",
                graphql
            );
        }
        _ => panic!("expected Mutation"),
    }
}

#[test]
fn update_coerces_number_to_string_for_string_field() {
    let mut types = FieldTypeMap::new();
    types.insert("version".to_string(), ScalarKind::String);

    let stmt = sql_to_graphql_typed(
        "UPDATE session SET version = 3 WHERE id = 'abc'",
        Some(&types),
    )
    .unwrap();

    match stmt {
        SqlStatement::Mutation { graphql, .. } => {
            assert!(
                graphql.contains("version: \"3\""),
                "expected version: \"3\", got: {}",
                graphql
            );
        }
        _ => panic!("expected Mutation"),
    }
}
