use integration_test::{for_each_runtime, TestCluster};

async fn backup_restore_test(cluster: TestCluster) {
    let client = cluster.client(0);

    // 1. Deploy 2 schemas
    client
        .schema_add("type User { name: String }")
        .expect("failed to add User schema");
    client
        .schema_add("type Post { title: String }")
        .expect("failed to add Post schema");

    // 2. Create 2 Users and 3 Posts
    let alice = client
        .query(r#"mutation { add_User(input: {name: "Alice"}) { _docID name } }"#)
        .expect("create Alice");
    let alice_id = alice["add_User"][0]["_docID"]
        .as_str()
        .expect("missing Alice _docID")
        .to_string();

    let bob = client
        .query(r#"mutation { add_User(input: {name: "Bob"}) { _docID name } }"#)
        .expect("create Bob");
    let bob_id = bob["add_User"][0]["_docID"]
        .as_str()
        .expect("missing Bob _docID")
        .to_string();

    client
        .query(r#"mutation { add_Post(input: {title: "Hello World"}) { _docID } }"#)
        .expect("create post 1");
    client
        .query(r#"mutation { add_Post(input: {title: "Rust Tips"}) { _docID } }"#)
        .expect("create post 2");
    client
        .query(r#"mutation { add_Post(input: {title: "P2P Guide"}) { _docID } }"#)
        .expect("create post 3");

    // Build backup paths inside the node's rootdir
    let node_dir = cluster.nodes[0].rootdir.to_str().unwrap().to_string();
    let full_path = format!("{}/full.json", node_dir);
    let users_path = format!("{}/users.json", node_dir);

    // 3. Full backup export
    client
        .backup_export(&full_path, &[], true)
        .expect("backup_export full failed");

    // Verify file exists with content
    let full_content = std::fs::read_to_string(&full_path).expect("failed to read full.json");
    assert!(
        full_content.contains("Alice"),
        "full backup should contain Alice"
    );
    assert!(
        full_content.contains("Hello World"),
        "full backup should contain posts"
    );

    // 4. Partial backup (Users only)
    client
        .backup_export(&users_path, &["User"], false)
        .expect("backup_export users failed");

    let users_content = std::fs::read_to_string(&users_path).expect("failed to read users.json");
    assert!(
        users_content.contains("Alice"),
        "users backup should contain Alice"
    );

    // 5. Truncate both collections
    client
        .collection_truncate("User")
        .expect("truncate User failed");
    client
        .collection_truncate("Post")
        .expect("truncate Post failed");

    // 6. Verify both empty
    let empty_users = client
        .query("query { User { _docID } }")
        .expect("query users after truncate");
    assert_eq!(
        empty_users["User"].as_array().expect("not array").len(),
        0,
        "expected 0 users after truncate"
    );

    let empty_posts = client
        .query("query { Post { _docID } }")
        .expect("query posts after truncate");
    assert_eq!(
        empty_posts["Post"].as_array().expect("not array").len(),
        0,
        "expected 0 posts after truncate"
    );

    // 7. Import full backup
    client
        .backup_import(&full_path)
        .expect("backup_import full failed");

    // 8. Verify restored data
    let restored_users = client
        .query("query { User { _docID name } }")
        .expect("query users after restore");
    let users = restored_users["User"].as_array().expect("users not array");
    assert_eq!(users.len(), 2, "expected 2 users after restore");

    let restored_posts = client
        .query("query { Post { _docID title } }")
        .expect("query posts after restore");
    let posts = restored_posts["Post"].as_array().expect("posts not array");
    assert_eq!(posts.len(), 3, "expected 3 posts after restore");

    // 9. Verify doc IDs match originals (CID stability)
    let user_ids: Vec<&str> = users.iter().filter_map(|u| u["_docID"].as_str()).collect();
    assert!(
        user_ids.contains(&alice_id.as_str()),
        "Alice doc ID should match after restore"
    );
    assert!(
        user_ids.contains(&bob_id.as_str()),
        "Bob doc ID should match after restore"
    );

    // 10. Truncate User only
    client
        .collection_truncate("User")
        .expect("truncate User for partial restore");

    // 11. Partial restore (Users only)
    client
        .backup_import(&users_path)
        .expect("backup_import users failed");

    // 12. Verify: 2 users restored, 3 posts untouched
    let final_users = client
        .query("query { User { _docID } }")
        .expect("query users after partial restore");
    assert_eq!(
        final_users["User"].as_array().expect("not array").len(),
        2,
        "expected 2 users after partial restore"
    );

    let final_posts = client
        .query("query { Post { _docID } }")
        .expect("query posts after partial restore");
    assert_eq!(
        final_posts["Post"].as_array().expect("not array").len(),
        3,
        "expected 3 posts still present"
    );
}

for_each_runtime!(backup_restore, backup_restore_test, .with_development());
