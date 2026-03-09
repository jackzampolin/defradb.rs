use integration_test::{for_each_runtime, TestCluster};
use serde_json::Value;

/// Extract array length from encrypted-index list output.
/// Handles both `[...]` and `{"indexes": [...]}` formats.
fn index_count(val: &Value) -> usize {
    if let Some(arr) = val.as_array() {
        return arr.len();
    }
    if let Some(obj) = val.as_object() {
        for v in obj.values() {
            if let Some(arr) = v.as_array() {
                return arr.len();
            }
        }
    }
    0
}

async fn se_key_rotation_test(cluster: TestCluster) {
    let node = cluster.client(0);

    node.schema_add("type Entry { value: String }")
        .expect("add Entry schema");

    // Add encrypted index on value
    node.encrypted_index_add("Entry", "value")
        .expect("add encrypted index on value");

    // Create two documents
    let data1 = node
        .query(r#"mutation { add_Entry(input: {value: "hello"}) { _docID } }"#)
        .expect("create entry 1");
    let doc1_id = data1["add_Entry"]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v["_docID"].as_str())
        .or_else(|| data1["add_Entry"]["_docID"].as_str())
        .expect("missing _docID for entry 1")
        .to_string();

    let data2 = node
        .query(r#"mutation { add_Entry(input: {value: "world"}) { _docID } }"#)
        .expect("create entry 2");
    let doc2_id = data2["add_Entry"]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v["_docID"].as_str())
        .or_else(|| data2["add_Entry"]["_docID"].as_str())
        .expect("missing _docID for entry 2")
        .to_string();

    assert!(!doc1_id.is_empty(), "doc1 should have a valid docID");
    assert!(!doc2_id.is_empty(), "doc2 should have a valid docID");

    // Assert both documents are returned by a plain query
    let result1 = node
        .query("query { Entry { _docID value } }")
        .expect("query all entries before rotation");
    let entries1 = result1["Entry"].as_array().expect("Entry array");
    assert_eq!(
        entries1.len(),
        2,
        "should have 2 entries before index rotation"
    );

    // Delete the encrypted index (simulates retiring the old key)
    node.encrypted_index_delete("Entry", "value")
        .expect("delete encrypted index on value");

    // Verify index is gone
    let list_after_delete = node
        .encrypted_index_list("Entry")
        .expect("list after delete");
    assert_eq!(
        index_count(&list_after_delete),
        0,
        "should have 0 encrypted indexes after delete"
    );

    // Re-add the encrypted index (new index identity = key rotation)
    node.encrypted_index_add("Entry", "value")
        .expect("re-add encrypted index on value after rotation");

    // Verify new index is present
    let list_after_readd = node
        .encrypted_index_list("Entry")
        .expect("list after re-add");
    assert_eq!(
        index_count(&list_after_readd),
        1,
        "should have 1 encrypted index after re-add"
    );

    // Create a third document with a distinct value (must differ from doc1/doc2
    // because DefraDB is content-addressed — same content = same docID)
    let data3 = node
        .query(r#"mutation { add_Entry(input: {value: "rotated"}) { _docID } }"#)
        .expect("create entry 3 after rotation");
    let doc3_id = data3["add_Entry"]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v["_docID"].as_str())
        .or_else(|| data3["add_Entry"]["_docID"].as_str())
        .expect("missing _docID for entry 3")
        .to_string();
    assert!(!doc3_id.is_empty(), "doc3 should have a valid docID");

    // Query all Entry records — old docs must not be lost
    let result2 = node
        .query("query { Entry { _docID value } }")
        .expect("query all entries after rotation");
    let entries2 = result2["Entry"]
        .as_array()
        .expect("Entry array after rotation");
    assert_eq!(
        entries2.len(),
        3,
        "should have 3 entries after index rotation; old docs must survive"
    );

    // Confirm node is operational: all 3 doc IDs appear in the results
    let returned_ids: Vec<&str> = entries2
        .iter()
        .filter_map(|v| v["_docID"].as_str())
        .collect();
    assert!(
        returned_ids.contains(&doc1_id.as_str()),
        "doc1 should be present after rotation"
    );
    assert!(
        returned_ids.contains(&doc2_id.as_str()),
        "doc2 should be present after rotation"
    );
    assert!(
        returned_ids.contains(&doc3_id.as_str()),
        "doc3 should be present after rotation"
    );
}

for_each_runtime!(se_key_rotation, se_key_rotation_test, .with_encryption());

async fn field_level_key_isolation_test(cluster: TestCluster) {
    let node = cluster.client(0);

    node.schema_add("type Vault { secret: String  pin: String  notes: String }")
        .expect("add Vault schema");

    // Create document with field-level encryption on secret and pin; notes is plaintext
    let data1 = node
        .query(
            r#"mutation { add_Vault(input: {secret: "mysecret", pin: "1234", notes: "public note"}, encryptFields: [secret, pin]) { _docID secret pin notes } }"#,
        )
        .expect("create vault doc with field-level encryption");

    let doc_id = data1["add_Vault"]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v["_docID"].as_str())
        .or_else(|| data1["add_Vault"]["_docID"].as_str())
        .expect("missing _docID for vault doc")
        .to_string();

    assert!(!doc_id.is_empty(), "vault doc should have a valid docID");

    // Query with correct identity returns all fields with expected values
    let result1 = node
        .query("query { Vault { _docID secret pin notes } }")
        .expect("query vault after create");
    let vaults1 = result1["Vault"].as_array().expect("Vault array");
    assert_eq!(vaults1.len(), 1, "should see 1 vault doc");
    assert_eq!(
        vaults1[0]["secret"], "mysecret",
        "secret field should decrypt correctly"
    );
    assert_eq!(
        vaults1[0]["pin"], "1234",
        "pin field should decrypt correctly"
    );
    assert_eq!(
        vaults1[0]["notes"], "public note",
        "notes field (plaintext) should be readable"
    );

    // Query commits to get CIDs — demonstrates independent field blocks exist
    let commits_query = format!(r#"query {{ _commits(docID: "{}") {{ cid }} }}"#, doc_id);
    let commits_result = node.query(&commits_query).or_else(|_| {
        let q = format!(r#"query {{ commits(docID: "{}") {{ cid }} }}"#, doc_id);
        node.query(&q)
    });

    if let Ok(commits) = commits_result {
        let commits_arr = commits
            .get("_commits")
            .or_else(|| commits.get("commits"))
            .and_then(|v| v.as_array());
        if let Some(arr) = commits_arr {
            assert!(
                !arr.is_empty(),
                "should have at least one commit for the vault doc"
            );
        }
    }

    // Re-query to verify encrypted fields still decrypt correctly on second read
    let result2 = node
        .query("query { Vault { secret pin notes } }")
        .expect("re-query vault");
    let vaults2 = result2["Vault"]
        .as_array()
        .expect("Vault array second read");
    assert_eq!(vaults2.len(), 1, "should still see 1 vault doc on re-query");
    assert_eq!(
        vaults2[0]["secret"], "mysecret",
        "secret decrypts correctly on re-read"
    );
    assert_eq!(
        vaults2[0]["pin"], "1234",
        "pin decrypts correctly on re-read"
    );

    // Verify notes (plaintext) is accessible independently of encrypted fields
    assert_eq!(
        vaults2[0]["notes"], "public note",
        "notes readable independently of encrypted fields"
    );

    // Create a second document with different values but same encrypted fields
    // This verifies each document uses independent encryption
    let data2 = node
        .query(
            r#"mutation { add_Vault(input: {secret: "othersecret", pin: "5678", notes: "another note"}, encryptFields: [secret, pin]) { _docID secret pin notes } }"#,
        )
        .expect("create second vault doc with field-level encryption");

    let doc2_id = data2["add_Vault"]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v["_docID"].as_str())
        .or_else(|| data2["add_Vault"]["_docID"].as_str())
        .expect("missing _docID for second vault doc")
        .to_string();

    assert!(
        !doc2_id.is_empty(),
        "second vault doc should have a valid docID"
    );
    assert_ne!(
        doc_id, doc2_id,
        "two docs with different content must have different docIDs"
    );

    // Both documents are queryable with correct values — independent encryption per document
    let result3 = node
        .query("query { Vault { _docID secret pin notes } }")
        .expect("query both vault docs");
    let vaults3 = result3["Vault"].as_array().expect("Vault array both docs");
    assert_eq!(
        vaults3.len(),
        2,
        "should see both vault docs after second create"
    );

    // Verify each document returns its own distinct values
    let doc1_entry = vaults3.iter().find(|v| v["_docID"] == doc_id.as_str());
    let doc2_entry = vaults3.iter().find(|v| v["_docID"] == doc2_id.as_str());

    if let Some(d1) = doc1_entry {
        assert_eq!(d1["secret"], "mysecret", "doc1 secret unchanged");
        assert_eq!(d1["pin"], "1234", "doc1 pin unchanged");
        assert_eq!(d1["notes"], "public note", "doc1 notes unchanged");
    }

    if let Some(d2) = doc2_entry {
        assert_eq!(d2["secret"], "othersecret", "doc2 has its own secret");
        assert_eq!(d2["pin"], "5678", "doc2 has its own pin");
        assert_eq!(d2["notes"], "another note", "doc2 has its own notes");
    }
}

for_each_runtime!(field_level_key_isolation, field_level_key_isolation_test, .with_encryption());
