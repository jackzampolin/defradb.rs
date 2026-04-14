//! ACP aggregate and relation query tests ported from Go DefraDB.
//!
//! Source:
//! - `tests/integration/acp/dac/count_test.go`
//! - `tests/integration/acp/dac/avg_test.go`
//! - `tests/integration/acp/dac/relation_objects_test.go`

use integration_test::{for_each_runtime, generate_identity, TestCluster};

fn employee_company_policy() -> &'static str {
    r#"
description: A Policy
name: test
resources:
  - name: companies
    permissions:
      - name: read
        expr: writer + reader
      - name: update
        expr: writer
      - name: delete
        expr: writer
    relations:
      - name: writer
        types:
          - actor
      - name: reader
        types:
          - actor
  - name: employees
    permissions:
      - name: read
        expr: writer + reader
      - name: update
        expr: writer
      - name: delete
        expr: writer
    relations:
      - name: writer
        types:
          - actor
      - name: reader
        types:
          - actor
"#
}

fn employee_company_schema(policy_id: &str) -> String {
    format!(
        r#"
type Employee @policy(id: "{}", resource: "employees") {{
  name: String
  salary: Int
  company: Company
}}

type Company @policy(id: "{}", resource: "companies") {{
  name: String
  capital: Int
  employees: [Employee]
}}
"#,
        policy_id, policy_id
    )
}

fn extract_policy_id(value: &serde_json::Value) -> Option<String> {
    value["PolicyID"]
        .as_str()
        .or_else(|| value["policyID"].as_str())
        .map(|s| s.to_string())
}

fn query_as(
    node: &integration_test::DefraClient,
    query: &str,
    key: Option<&str>,
) -> serde_json::Value {
    match key {
        Some(key) => node
            .query_with_identity(query, key)
            .expect("query with identity"),
        None => node.query(query).expect("anonymous query"),
    }
}

fn mutate_as(
    node: &integration_test::DefraClient,
    mutation: &str,
    key: Option<&str>,
) -> serde_json::Value {
    match key {
        Some(key) => node
            .query_with_identity(mutation, key)
            .expect("mutation with identity"),
        None => node.query(mutation).expect("anonymous mutation"),
    }
}

async fn setup_employee_company_fixture(
    node: &integration_test::DefraClient,
    owner_key: &str,
) -> (String, String) {
    let policy = node
        .acp_policy_add(employee_company_policy(), owner_key)
        .expect("add employee/company policy");
    let policy_id = extract_policy_id(&policy).expect("policy id");

    node.schema_add_with_identity(&employee_company_schema(&policy_id), owner_key)
        .expect("add employee/company schema");

    let public_company = mutate_as(
        node,
        r#"mutation { add_Company(input: {name: "Public Company", capital: 100000}) { _docID } }"#,
        None,
    );
    let public_company_id = public_company["add_Company"][0]["_docID"]
        .as_str()
        .expect("public company doc id")
        .to_string();

    let private_company = mutate_as(
        node,
        r#"mutation { add_Company(input: {name: "Private Company", capital: 200000}) { _docID } }"#,
        Some(owner_key),
    );
    let private_company_id = private_company["add_Company"][0]["_docID"]
        .as_str()
        .expect("private company doc id")
        .to_string();

    mutate_as(
        node,
        &format!(
            r#"mutation {{ add_Employee(input: {{name: "PubEmp in PubCompany", salary: 10000, company: "{}"}}) {{ _docID }} }}"#,
            public_company_id
        ),
        None,
    );
    mutate_as(
        node,
        &format!(
            r#"mutation {{ add_Employee(input: {{name: "PubEmp in PrivateCompany", salary: 20000, company: "{}"}}) {{ _docID }} }}"#,
            private_company_id
        ),
        None,
    );
    mutate_as(
        node,
        &format!(
            r#"mutation {{ add_Employee(input: {{name: "PrivateEmp in PubCompany", salary: 30000, company: "{}"}}) {{ _docID }} }}"#,
            public_company_id
        ),
        Some(owner_key),
    );
    mutate_as(
        node,
        &format!(
            r#"mutation {{ add_Employee(input: {{name: "PrivateEmp in PrivateCompany", salary: 40000, company: "{}"}}) {{ _docID }} }}"#,
            private_company_id
        ),
        Some(owner_key),
    );

    (public_company_id, private_company_id)
}

// Port of TestACP_QueryCountDocumentsWithoutIdentity / WithIdentity / WithWrongIdentity
async fn acp_count_documents(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let wrong = generate_identity(node.binary_path()).expect("wrong");
    let _ = setup_employee_company_fixture(&node, &owner.private_key_hex).await;

    let anon = query_as(&node, r#"query { COUNT(Employee: {}) }"#, None);
    assert_eq!(anon["COUNT"], 2);

    let owner_view = query_as(
        &node,
        r#"query { COUNT(Employee: {}) }"#,
        Some(&owner.private_key_hex),
    );
    assert_eq!(owner_view["COUNT"], 4);

    let wrong_view = query_as(
        &node,
        r#"query { COUNT(Employee: {}) }"#,
        Some(&wrong.private_key_hex),
    );
    assert_eq!(wrong_view["COUNT"], 2);
}

for_each_runtime!(acp_count_documents, acp_count_documents, .with_acp_local());

// Port of TestACP_QueryCountRelatedObjectsWithoutIdentity / WithIdentity / WithWrongIdentity
async fn acp_count_related_objects(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let wrong = generate_identity(node.binary_path()).expect("wrong");
    let _ = setup_employee_company_fixture(&node, &owner.private_key_hex).await;

    let query = r#"query { Company(order: {name: ASC}) { name COUNT(employees: {}) } }"#;

    let anon = query_as(&node, query, None);
    assert_eq!(
        anon["Company"],
        serde_json::json!([
            {"name": "Public Company", "COUNT": 1}
        ])
    );

    let owner_view = query_as(&node, query, Some(&owner.private_key_hex));
    assert_eq!(
        owner_view["Company"],
        serde_json::json!([
            {"name": "Private Company", "COUNT": 2},
            {"name": "Public Company", "COUNT": 2}
        ])
    );

    let wrong_view = query_as(&node, query, Some(&wrong.private_key_hex));
    assert_eq!(
        wrong_view["Company"],
        serde_json::json!([
            {"name": "Public Company", "COUNT": 1}
        ])
    );
}

for_each_runtime!(
    acp_count_related_objects,
    acp_count_related_objects,
    .with_acp_local()
);

// Port of TestACP_QueryAverageWithoutIdentity / WithIdentity / WithWrongIdentity
async fn acp_avg_documents(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let wrong = generate_identity(node.binary_path()).expect("wrong");
    let _ = setup_employee_company_fixture(&node, &owner.private_key_hex).await;

    let query = r#"query { AVG(Employee: {field: salary}) }"#;

    let anon_avg = query_as(&node, query, None)["AVG"]
        .as_f64()
        .expect("anonymous AVG should be numeric");
    let owner_avg = query_as(&node, query, Some(&owner.private_key_hex))["AVG"]
        .as_f64()
        .expect("owner AVG should be numeric");
    let wrong_avg = query_as(&node, query, Some(&wrong.private_key_hex))["AVG"]
        .as_f64()
        .expect("wrong-identity AVG should be numeric");

    assert_eq!(anon_avg, 15000.0);
    assert_eq!(owner_avg, 25000.0);
    assert_eq!(wrong_avg, 15000.0);
}

for_each_runtime!(acp_avg_documents, acp_avg_documents, .with_acp_local());

// Port of TestACP_QueryManyToOneRelationObjectsWithoutIdentity / WithIdentity / WithWrongIdentity
async fn acp_many_to_one_relation_objects(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let wrong = generate_identity(node.binary_path()).expect("wrong");
    let _ = setup_employee_company_fixture(&node, &owner.private_key_hex).await;

    let query = r#"
        query {
            Employee(order: {name: ASC}) {
                name
                company {
                    name
                }
            }
        }
    "#;

    assert_eq!(
        query_as(&node, query, None)["Employee"],
        serde_json::json!([
            {"name": "PubEmp in PrivateCompany", "company": null},
            {"name": "PubEmp in PubCompany", "company": {"name": "Public Company"}}
        ])
    );

    assert_eq!(
        query_as(&node, query, Some(&owner.private_key_hex))["Employee"],
        serde_json::json!([
            {"name": "PrivateEmp in PrivateCompany", "company": {"name": "Private Company"}},
            {"name": "PrivateEmp in PubCompany", "company": {"name": "Public Company"}},
            {"name": "PubEmp in PrivateCompany", "company": {"name": "Private Company"}},
            {"name": "PubEmp in PubCompany", "company": {"name": "Public Company"}}
        ])
    );

    assert_eq!(
        query_as(&node, query, Some(&wrong.private_key_hex))["Employee"],
        serde_json::json!([
            {"name": "PubEmp in PrivateCompany", "company": null},
            {"name": "PubEmp in PubCompany", "company": {"name": "Public Company"}}
        ])
    );
}

for_each_runtime!(
    acp_many_to_one_relation_objects,
    acp_many_to_one_relation_objects,
    .with_acp_local()
);

// Port of TestACP_QueryOneToManyRelationObjectsWithoutIdentity / WithIdentity / WithWrongIdentity
async fn acp_one_to_many_relation_objects(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let wrong = generate_identity(node.binary_path()).expect("wrong");
    let _ = setup_employee_company_fixture(&node, &owner.private_key_hex).await;

    let query = r#"
        query {
            Company(order: {name: ASC}) {
                name
                employees(order: {name: ASC}) {
                    name
                }
            }
        }
    "#;

    assert_eq!(
        query_as(&node, query, None)["Company"],
        serde_json::json!([
            {
                "name": "Public Company",
                "employees": [{"name": "PubEmp in PubCompany"}]
            }
        ])
    );

    assert_eq!(
        query_as(&node, query, Some(&owner.private_key_hex))["Company"],
        serde_json::json!([
            {
                "name": "Private Company",
                "employees": [
                    {"name": "PrivateEmp in PrivateCompany"},
                    {"name": "PubEmp in PrivateCompany"}
                ]
            },
            {
                "name": "Public Company",
                "employees": [
                    {"name": "PrivateEmp in PubCompany"},
                    {"name": "PubEmp in PubCompany"}
                ]
            }
        ])
    );

    assert_eq!(
        query_as(&node, query, Some(&wrong.private_key_hex))["Company"],
        serde_json::json!([
            {
                "name": "Public Company",
                "employees": [{"name": "PubEmp in PubCompany"}]
            }
        ])
    );
}

for_each_runtime!(
    acp_one_to_many_relation_objects,
    acp_one_to_many_relation_objects,
    .with_acp_local()
);
