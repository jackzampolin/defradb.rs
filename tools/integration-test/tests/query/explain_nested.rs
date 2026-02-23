use integration_test::{for_each_runtime, TestCluster};
use serde_json::Value;

/// Verify that a node object contains the standard scanNode metrics as numbers.
fn assert_scan_node_metrics(node: &Value, context: &str) {
    for key in &["iterations", "docFetches", "fieldFetches", "indexFetches"] {
        let val = node
            .get(key)
            .unwrap_or_else(|| panic!("{context}: scanNode missing '{key}'"));
        assert!(
            val.is_number(),
            "{context}: scanNode '{key}' should be numeric, got {val}"
        );
    }
}

/// Test recursive execute explain with 2-level deep nested one-to-one joins.
///
/// Mirrors Go's TestExecuteExplainWithTwoLevelDeepNestedJoins from PR #4471.
/// Schema: Author -> AuthorContact -> ContactAddress (3-level, all one-to-one)
///
/// Expected structure:
/// ```
/// selectTopNode > selectNode > typeIndexJoin > typeJoinOne > {
///   root: { scanNode: {...} },
///   subType: { selectTopNode > selectNode > typeIndexJoin > typeJoinOne > {
///     root: { scanNode: {...} },
///     subType: { selectTopNode > selectNode > scanNode: {...} }
///   }}
/// }
/// ```
async fn explain_nested_execute_test(cluster: TestCluster) {
    let client = cluster.client(0);

    // Schema matching Go's explain test fixture (one-to-one with @primary)
    client
        .schema_add(
            r#"
            type Author {
                name: String
                age: Int
                verified: Boolean
                contact: AuthorContact @primary
            }
            type AuthorContact {
                cell: String
                email: String
                author: Author
                address: ContactAddress @primary
            }
            type ContactAddress {
                city: String
                country: String
                contact: AuthorContact
            }
            "#,
        )
        .expect("schema add failed");

    // Create test data (bottom-up to satisfy FK constraints)

    // 2 ContactAddress documents
    client
        .query(r#"mutation { create_ContactAddress(input: {city: "Waterloo", country: "Canada"}) { _docID } }"#)
        .expect("create address 1");
    let addr2 = client
        .query(r#"mutation { create_ContactAddress(input: {city: "Brampton", country: "Canada"}) { _docID } }"#)
        .expect("create address 2");
    let _ = addr2["create_ContactAddress"][0]["_docID"]
        .as_str()
        .expect("address 2 _docID");

    // 2 AuthorContact documents linked to addresses
    // First query for address IDs
    let addresses = client
        .query(r#"query { ContactAddress { _docID city } }"#)
        .expect("query addresses");
    let addr_arr = addresses["ContactAddress"]
        .as_array()
        .expect("address array");
    let addr_id_0 = addr_arr[0]["_docID"].as_str().expect("addr 0 id");
    let addr_id_1 = addr_arr[1]["_docID"].as_str().expect("addr 1 id");

    client
        .query(&format!(
            r#"mutation {{ create_AuthorContact(input: {{cell: "5197212301", email: "john@example.com", _addressID: "{addr_id_0}"}}) {{ _docID }} }}"#,
        ))
        .expect("create contact 1");
    client
        .query(&format!(
            r#"mutation {{ create_AuthorContact(input: {{cell: "5197212302", email: "cornelia@example.com", _addressID: "{addr_id_1}"}}) {{ _docID }} }}"#,
        ))
        .expect("create contact 2");

    // 2 Author documents linked to contacts
    let contacts = client
        .query(r#"query { AuthorContact { _docID email } }"#)
        .expect("query contacts");
    let contact_arr = contacts["AuthorContact"].as_array().expect("contact array");
    let contact_id_0 = contact_arr[0]["_docID"].as_str().expect("contact 0 id");
    let contact_id_1 = contact_arr[1]["_docID"].as_str().expect("contact 1 id");

    client
        .query(&format!(
            r#"mutation {{ create_Author(input: {{name: "John Grisham", age: 65, verified: true, _contactID: "{contact_id_0}"}}) {{ _docID }} }}"#,
        ))
        .expect("create author 1");
    client
        .query(&format!(
            r#"mutation {{ create_Author(input: {{name: "Cornelia Funke", age: 62, verified: false, _contactID: "{contact_id_1}"}}) {{ _docID }} }}"#,
        ))
        .expect("create author 2");

    // Run execute explain on the 3-level nested query (matches Go test exactly)
    let explain_result = client
        .query(
            r#"query @explain(type: execute) {
                Author {
                    name
                    contact {
                        email
                        address {
                            city
                        }
                    }
                }
            }"#,
        )
        .expect("explain query");

    // The result may be wrapped in an array (explain returns operationNode array)
    let explain = if explain_result.is_array() {
        explain_result[0].clone()
    } else {
        explain_result.clone()
    };

    // Navigate the explain tree.
    // Go wraps in: explain > operationNode > [{ selectTopNode > ... }]
    // Rust may return the tree directly or wrapped.
    let select_top = explain
        .get("explain")
        .and_then(|e| {
            e.get("operationNode")
                .and_then(|op| op.as_array())
                .and_then(|arr| arr.first())
                .and_then(|first| first.get("selectTopNode"))
                .or_else(|| e.get("selectTopNode"))
        })
        .or_else(|| explain.get("selectTopNode"))
        .unwrap_or_else(|| panic!("Expected selectTopNode in explain output: {explain}"));

    let select_node = select_top
        .get("selectNode")
        .unwrap_or_else(|| panic!("Expected selectNode: {select_top}"));

    // selectNode: iterations and filterMatches
    assert!(
        select_node.get("iterations").unwrap().is_number(),
        "selectNode iterations should be numeric"
    );
    assert!(
        select_node.get("filterMatches").unwrap().is_number(),
        "selectNode filterMatches should be numeric"
    );

    // Level 1: Author -> AuthorContact (typeIndexJoin > typeJoinOne)
    let outer_join = select_node
        .get("typeIndexJoin")
        .unwrap_or_else(|| panic!("Expected typeIndexJoin in selectNode: {select_node}"));

    assert!(
        outer_join.get("iterations").unwrap().is_number(),
        "outer typeIndexJoin iterations should be numeric"
    );

    let join_one = outer_join
        .get("typeJoinOne")
        .unwrap_or_else(|| panic!("Expected typeJoinOne in outer join: {outer_join}"));

    // root: Author scanNode
    let root = join_one
        .get("root")
        .unwrap_or_else(|| panic!("Expected root in typeJoinOne: {join_one}"));
    let root_scan = root
        .get("scanNode")
        .unwrap_or_else(|| panic!("Expected scanNode in root: {root}"));
    assert_scan_node_metrics(root_scan, "Author root scanNode");

    // subType: selectTopNode > selectNode > (nested join or scanNode)
    let sub_type = join_one
        .get("subType")
        .unwrap_or_else(|| panic!("Expected subType in typeJoinOne: {join_one}"));
    let sub_select_top = sub_type
        .get("selectTopNode")
        .unwrap_or_else(|| panic!("Expected selectTopNode in subType: {sub_type}"));
    let sub_select = sub_select_top
        .get("selectNode")
        .unwrap_or_else(|| panic!("Expected selectNode in sub selectTopNode: {sub_select_top}"));

    assert!(
        sub_select.get("iterations").unwrap().is_number(),
        "sub selectNode iterations should be numeric"
    );
    assert!(
        sub_select.get("filterMatches").unwrap().is_number(),
        "sub selectNode filterMatches should be numeric"
    );

    // Level 2: AuthorContact -> ContactAddress (nested typeIndexJoin > typeJoinOne)
    let inner_join = sub_select
        .get("typeIndexJoin")
        .unwrap_or_else(|| panic!("Expected nested typeIndexJoin in sub selectNode: {sub_select}"));

    assert!(
        inner_join.get("iterations").unwrap().is_number(),
        "inner typeIndexJoin iterations should be numeric"
    );

    let inner_join_one = inner_join
        .get("typeJoinOne")
        .unwrap_or_else(|| panic!("Expected typeJoinOne in inner join: {inner_join}"));

    // inner root: AuthorContact scanNode
    let inner_root = inner_join_one
        .get("root")
        .unwrap_or_else(|| panic!("Expected root in inner typeJoinOne: {inner_join_one}"));
    let inner_root_scan = inner_root
        .get("scanNode")
        .unwrap_or_else(|| panic!("Expected scanNode in inner root: {inner_root}"));
    assert_scan_node_metrics(inner_root_scan, "AuthorContact inner root scanNode");

    // inner subType: selectTopNode > selectNode > scanNode (leaf level)
    let inner_sub_type = inner_join_one
        .get("subType")
        .unwrap_or_else(|| panic!("Expected subType in inner typeJoinOne: {inner_join_one}"));
    let inner_sub_select_top = inner_sub_type
        .get("selectTopNode")
        .unwrap_or_else(|| panic!("Expected selectTopNode in inner subType: {inner_sub_type}"));
    let inner_sub_select = inner_sub_select_top.get("selectNode").unwrap_or_else(|| {
        panic!("Expected selectNode in inner sub selectTopNode: {inner_sub_select_top}")
    });

    assert!(
        inner_sub_select.get("iterations").unwrap().is_number(),
        "inner sub selectNode iterations should be numeric"
    );
    assert!(
        inner_sub_select.get("filterMatches").unwrap().is_number(),
        "inner sub selectNode filterMatches should be numeric"
    );

    // Leaf scanNode: ContactAddress
    let leaf_scan = inner_sub_select
        .get("scanNode")
        .unwrap_or_else(|| panic!("Expected scanNode in inner sub selectNode: {inner_sub_select}"));
    assert_scan_node_metrics(leaf_scan, "ContactAddress leaf scanNode");
}

for_each_runtime!(explain_nested_execute, explain_nested_execute_test);
