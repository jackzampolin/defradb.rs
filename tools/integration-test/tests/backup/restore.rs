use std::process::Command;

use integration_test::{for_each_runtime, DefraClient, NodeKind, TestCluster};
use serde_json::json;

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
    let node_url = cluster.nodes[0].http_addr.as_str();
    let full_path = format!("{}/full.json", node_dir);
    let users_path = format!("{}/users.json", node_dir);

    // 3. Full backup export
    backup_export(&client, node_url, &full_path, &[], true).expect("backup_export full failed");

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
    backup_export(&client, node_url, &users_path, &["User"], false)
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
    backup_import(&client, node_url, &full_path).expect("backup_import full failed");

    // 8. Verify restored data
    assert_restored_users(&client, &[(&alice_id, "Alice"), (&bob_id, "Bob")]);

    let restored_posts = client
        .query("query { Post(order: {title: ASC}) { _docID title } }")
        .expect("query posts after restore");
    let posts = restored_posts["Post"].as_array().expect("posts not array");
    assert_eq!(posts.len(), 3, "expected 3 posts after restore");

    for (post, title) in posts.iter().zip(["Hello World", "P2P Guide", "Rust Tips"]) {
        assert_eq!(post["title"], title);
    }

    // 9. Truncate User only
    client
        .collection_truncate("User")
        .expect("truncate User for partial restore");

    // 10. Partial restore (Users only)
    backup_import(&client, node_url, &users_path).expect("backup_import users failed");

    // 11. Verify: 2 users restored, 3 posts untouched
    assert_restored_users(&client, &[(&alice_id, "Alice"), (&bob_id, "Bob")]);

    let final_posts = client
        .query("query { Post(order: {title: ASC}) { _docID title } }")
        .expect("query posts after partial restore");
    assert_eq!(
        final_posts, restored_posts,
        "partial restore must leave posts unchanged"
    );
}

fn assert_restored_users(client: &DefraClient, expected: &[(&str, &str)]) {
    let restored = client
        .query("query { User(order: {name: ASC}) { _docID name } }")
        .expect("query users after restore");
    let users = restored["User"].as_array().expect("users not array");
    assert_eq!(users.len(), expected.len());

    // JSON import recreates genesis blocks, so signed DocIDs may change.
    // Original IDs must still resolve as aliases of the restored documents.
    for (user, (original_id, name)) in users.iter().zip(expected) {
        assert_eq!(user["name"], *name);
        let by_original_id = client
            .query(&format!(
                r#"query {{ User(docID: "{original_id}") {{ _docID name }} }}"#
            ))
            .expect("query restored user by original ID");
        assert_eq!(by_original_id["User"], json!([user]));
        let filtered = client
            .query(&format!(
                r#"query {{ User(docID: "{original_id}", filter: {{name: {{_neq: "{name}"}}}}) {{ _docID name }} }}"#
            ))
            .expect("query restored user with non-matching filter");
        assert_eq!(filtered["User"], json!([]));
    }
}

fn backup_export(
    client: &DefraClient,
    node_url: &str,
    file: &str,
    collections: &[&str],
    pretty: bool,
) -> Result<String, String> {
    let mut args = vec!["client", "backup", "export", file];
    for collection in collections {
        args.push("-c");
        args.push(collection);
    }
    if pretty {
        args.push("--pretty");
    }
    exec_with_development(client, node_url, &args)
}

fn backup_import(client: &DefraClient, node_url: &str, file: &str) -> Result<String, String> {
    exec_with_development(client, node_url, &["client", "backup", "import", file])
}

fn exec_with_development(
    client: &DefraClient,
    node_url: &str,
    args: &[&str],
) -> Result<String, String> {
    let mut command = Command::new(client.binary_path());
    command.arg("--url").arg(node_url);
    if client.kind() == NodeKind::Rust {
        command.arg("--development");
    }
    let output = command.args(args).output().map_err(|err| err.to_string())?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(format!(
            "command failed (exit {}): stderr={}, stdout={}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim(),
            String::from_utf8_lossy(&output.stdout).trim()
        ))
    }
}

for_each_runtime!(backup_restore, backup_restore_test, .with_development());
