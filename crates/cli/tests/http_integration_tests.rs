//! HTTP integration tests for the DefraDB server.

use std::time::Duration;

use cli::commands::start::Node;
use cli::config::{Config, DatastoreType};


/// Wait for server to be ready by polling health endpoint with retries.
async fn wait_for_server(api_url: &str, max_attempts: u32) {
    let client = reqwest::Client::new();
    for attempt in 1..=max_attempts {
        match client
            .get(format!("{}/health-check", api_url))
            .timeout(Duration::from_millis(100))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => return,
            _ => {
                if attempt < max_attempts {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }
    panic!(
        "Server at {} failed to become ready after {} attempts",
        api_url, max_attempts
    );
}

/// Create a test config with random port and P2P disabled
fn test_config(port: u16, temp_dir: &std::path::Path) -> Config {
    Config {
        rootdir: temp_dir.to_path_buf(),
        log: cli::config::LogConfig::default(),
        api: cli::config::ApiConfig {
            address: format!("127.0.0.1:{}", port),
            allowed_origins: vec![],
            pubkey_path: String::new(),
            privkey_path: String::new(),
        },
        datastore: cli::config::DatastoreConfig {
            store: DatastoreType::Memory,
            path: String::new(),
            max_txn_retries: 5,
            valuelogfilesize: 1 << 30,
            no_encryption: true,
            no_signing: true,
            no_searchable_encryption: true,
            default_key_type: "ed25519".to_string(),
        },
        net: cli::config::NetConfig {
            p2p_disabled: true, // Disable P2P for HTTP-only tests
            p2p_addresses: vec![],
            peers: vec![],
            pubsub_enabled: false,
            relay_enabled: false,
        },
        keyring: cli::config::KeyringConfig::default(),
        development: false,
        secret_file: String::new(),
        telemetry_disabled: true,
        replicator_retry_intervals: vec![],
    }
}

#[tokio::test]
async fn test_http_server_starts_and_serves_health_check() {
    let temp_dir = tempfile::tempdir().unwrap();
    let port = portpicker::pick_unused_port().expect("No free ports");
    let config = test_config(port, temp_dir.path());
    let api_url = format!("http://127.0.0.1:{}", port);

    // Create node
    let node = Node::new(config, None).await.unwrap();

    // Get shutdown sender before moving node
    let shutdown_tx = node.shutdown_tx.clone();

    // Spawn node in background
    let node_handle = tokio::spawn(async move { node.run().await });

    // Wait for server to be ready
    wait_for_server(&api_url, 20).await;

    // Test health check endpoint
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/health-check", api_url))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("Failed to connect to health check");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body = response.text().await.unwrap();
    assert_eq!(body, "Healthy");

    // Shutdown
    shutdown_tx.send(()).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), node_handle)
        .await
        .expect("Node shutdown timed out")
        .expect("Node task panicked");
    assert!(result.is_ok(), "Node shutdown failed: {:?}", result.err());
}

#[tokio::test]
async fn test_http_server_serves_version_endpoint() {
    let temp_dir = tempfile::tempdir().unwrap();
    let port = portpicker::pick_unused_port().expect("No free ports");
    let config = test_config(port, temp_dir.path());
    let api_url = format!("http://127.0.0.1:{}", port);

    let node = Node::new(config, None).await.unwrap();
    let shutdown_tx = node.shutdown_tx.clone();

    let node_handle = tokio::spawn(async move { node.run().await });

    wait_for_server(&api_url, 20).await;

    // Test version endpoint
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/v0/version", api_url))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("Failed to connect to version endpoint");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(
        body.get("version").is_some(),
        "Response should contain version"
    );
    assert!(
        body.get("commit").is_some(),
        "Response should contain commit"
    );

    // Shutdown
    shutdown_tx.send(()).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
}

#[tokio::test]
async fn test_http_server_serves_graphql_endpoint() {
    let temp_dir = tempfile::tempdir().unwrap();
    let port = portpicker::pick_unused_port().expect("No free ports");
    let config = test_config(port, temp_dir.path());
    let api_url = format!("http://127.0.0.1:{}", port);

    let node = Node::new(config, None).await.unwrap();
    let shutdown_tx = node.shutdown_tx.clone();

    let node_handle = tokio::spawn(async move { node.run().await });

    wait_for_server(&api_url, 20).await;

    // Test GraphQL endpoint - expect error since no schema is loaded
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/v0/graphql", api_url))
        .header("content-type", "application/json")
        .body(r#"{"query": "{ users { name } }"}"#)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("Failed to connect to graphql endpoint");

    // Should return 200 OK even with errors (GraphQL spec)
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    // With empty schema, we expect an error about collection not found
    assert!(
        body.get("errors").is_some() || body.get("data").is_some(),
        "Response should be valid GraphQL response"
    );

    // Shutdown
    shutdown_tx.send(()).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
}

#[tokio::test]
async fn test_http_server_schema_endpoint_returns_empty() {
    let temp_dir = tempfile::tempdir().unwrap();
    let port = portpicker::pick_unused_port().expect("No free ports");
    let config = test_config(port, temp_dir.path());
    let api_url = format!("http://127.0.0.1:{}", port);

    let node = Node::new(config, None).await.unwrap();
    let shutdown_tx = node.shutdown_tx.clone();

    let node_handle = tokio::spawn(async move { node.run().await });

    wait_for_server(&api_url, 20).await;

    // Test schema endpoint - should return empty or minimal schema
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/v0/schema", api_url))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("Failed to connect to schema endpoint");

    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // Shutdown
    shutdown_tx.send(()).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
}

/// Create a test config with RocksDB backend
fn test_config_rocksdb(port: u16, temp_dir: &std::path::Path) -> Config {
    Config {
        rootdir: temp_dir.to_path_buf(),
        log: cli::config::LogConfig::default(),
        api: cli::config::ApiConfig {
            address: format!("127.0.0.1:{}", port),
            allowed_origins: vec![],
            pubkey_path: String::new(),
            privkey_path: String::new(),
        },
        datastore: cli::config::DatastoreConfig {
            store: DatastoreType::Badger, // Use RocksDB backend
            path: String::new(),
            max_txn_retries: 5,
            valuelogfilesize: 1 << 30,
            no_encryption: true,
            no_signing: true,
            no_searchable_encryption: true,
            default_key_type: "ed25519".to_string(),
        },
        net: cli::config::NetConfig {
            p2p_disabled: true,
            p2p_addresses: vec![],
            peers: vec![],
            pubsub_enabled: false,
            relay_enabled: false,
        },
        keyring: cli::config::KeyringConfig::default(),
        development: false,
        secret_file: String::new(),
        telemetry_disabled: true,
        replicator_retry_intervals: vec![],
    }
}

#[tokio::test]
async fn test_http_server_with_rocksdb_backend() {
    let temp_dir = tempfile::tempdir().unwrap();
    let port = portpicker::pick_unused_port().expect("No free ports");
    let config = test_config_rocksdb(port, temp_dir.path());
    let api_url = format!("http://127.0.0.1:{}", port);

    // Create node with RocksDB backend
    let node = Node::new(config, None).await.unwrap();
    let shutdown_tx = node.shutdown_tx.clone();

    let node_handle = tokio::spawn(async move { node.run().await });

    wait_for_server(&api_url, 20).await;

    // Test health check works with RocksDB
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/health-check", api_url))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("Failed to connect to health check");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "Healthy");

    // Test schema endpoint - should be empty for fresh database
    let response = client
        .get(format!("{}/api/v0/schema", api_url))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("Failed to connect to schema endpoint");

    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // Shutdown
    shutdown_tx.send(()).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
}

/// End-to-end test: pre-seed database with documents, query via HTTP
#[tokio::test]
async fn test_http_graphql_returns_documents_from_database() {
    use document::NormalValue;
    use schema::{CollectionVersion, FieldDescription, FieldKind};

    let temp_dir = tempfile::tempdir().unwrap();
    let port = portpicker::pick_unused_port().expect("No free ports");
    // Use the same path that Node will use (rootdir when datastore.path is empty)
    let data_path = temp_dir.path();

    // Phase 1: Pre-seed database with collection and documents
    {
        let store = storage::RocksDBStore::open(data_path).unwrap();
        let database = db::DB::new(store);

        // Create Users collection
        let schema = CollectionVersion::new(
            "Users",
            "v1",
            "col-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        );
        database.create_collection(schema).await.unwrap();

        // Insert test documents
        let collection = database.get_collection("Users").unwrap().unwrap();
        let txn = database.new_txn(false).await.unwrap();

        let mut doc1 = document::Document::new();
        doc1.set("name", NormalValue::String("Alice".to_string()));
        doc1.set("age", NormalValue::Int(30));
        doc1.generate_and_set_doc_id().unwrap();
        collection.create(&txn, &doc1).await.unwrap();

        let mut doc2 = document::Document::new();
        doc2.set("name", NormalValue::String("Bob".to_string()));
        doc2.set("age", NormalValue::Int(25));
        doc2.generate_and_set_doc_id().unwrap();
        collection.create(&txn, &doc2).await.unwrap();

        txn.commit().await.unwrap();
        database.close().await.unwrap();
    }

    // Phase 2: Start server and query via HTTP
    let config = Config {
        rootdir: temp_dir.path().to_path_buf(),
        log: cli::config::LogConfig::default(),
        api: cli::config::ApiConfig {
            address: format!("127.0.0.1:{}", port),
            allowed_origins: vec![],
            pubkey_path: String::new(),
            privkey_path: String::new(),
        },
        datastore: cli::config::DatastoreConfig {
            store: DatastoreType::Badger, // Uses RocksDB
            path: String::new(),
            max_txn_retries: 5,
            valuelogfilesize: 1 << 30,
            no_encryption: true,
            no_signing: true,
            no_searchable_encryption: true,
            default_key_type: "ed25519".to_string(),
        },
        net: cli::config::NetConfig {
            p2p_disabled: true,
            p2p_addresses: vec![],
            peers: vec![],
            pubsub_enabled: false,
            relay_enabled: false,
        },
        keyring: cli::config::KeyringConfig::default(),
        development: false,
        secret_file: String::new(),
        telemetry_disabled: true,
        replicator_retry_intervals: vec![],
    };

    let api_url = format!("http://127.0.0.1:{}", port);
    let node = Node::new(config, None).await.unwrap();
    let shutdown_tx = node.shutdown_tx.clone();

    let node_handle = tokio::spawn(async move { node.run().await });

    wait_for_server(&api_url, 20).await;

    // Query documents via GraphQL
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/v0/graphql", api_url))
        .header("content-type", "application/json")
        .body(r#"{"query": "{ Users { name age } }"}"#)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("Failed to query graphql endpoint");

    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let body: serde_json::Value = response.json().await.unwrap();

    // Verify we got data back (not just errors)
    let data = body.get("data").expect("Response should have data field");
    let users = data.get("Users").expect("Data should have Users field");
    let users_array = users.as_array().expect("Users should be an array");

    assert_eq!(users_array.len(), 2, "Should have 2 users");

    // Verify document contents
    let names: Vec<&str> = users_array
        .iter()
        .filter_map(|u| u.get("name").and_then(|n| n.as_str()))
        .collect();
    assert!(names.contains(&"Alice"), "Should contain Alice");
    assert!(names.contains(&"Bob"), "Should contain Bob");

    // Shutdown
    shutdown_tx.send(()).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
}

// =========================================================================
// Integration tests for Issue #67: GraphQL Mutations
// =========================================================================

/// Test creating a document via GraphQL mutation through HTTP
#[tokio::test]
async fn test_http_graphql_create_mutation() {
    use schema::{CollectionVersion, FieldDescription, FieldKind};

    let temp_dir = tempfile::tempdir().unwrap();
    let port = portpicker::pick_unused_port().expect("No free ports");
    let data_path = temp_dir.path();

    // Phase 1: Pre-seed database with collection (no documents)
    {
        let store = storage::RocksDBStore::open(data_path).unwrap();
        let database = db::DB::new(store);

        let schema = CollectionVersion::new(
            "Users",
            "v1",
            "col-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        );
        database.create_collection(schema).await.unwrap();
        database.close().await.unwrap();
    }

    // Phase 2: Start server and create document via mutation
    let config = test_config_rocksdb(port, temp_dir.path());
    let api_url = format!("http://127.0.0.1:{}", port);
    let node = Node::new(config, None).await.unwrap();
    let shutdown_tx = node.shutdown_tx.clone();

    let node_handle = tokio::spawn(async move { node.run().await });
    wait_for_server(&api_url, 20).await;

    let client = reqwest::Client::new();

    // Create a document via mutation
    let create_mutation = r#"{
        "query": "mutation { create_Users(input: {name: \"Charlie\", age: 35}) { _docID name age } }"
    }"#;

    let response = client
        .post(format!("{}/api/v0/graphql", api_url))
        .header("content-type", "application/json")
        .body(create_mutation)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .expect("Failed to execute create mutation");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();

    // Verify mutation succeeded
    // Note: Response uses collection name "Users" as key, not "create_Users"
    let data = body.get("data").expect("Response should have data field");
    let created_array = data
        .get("Users")
        .and_then(|u| u.as_array())
        .expect("Data should have Users array");
    assert_eq!(created_array.len(), 1, "Should have created 1 document");
    let created = &created_array[0];
    assert_eq!(
        created.get("name").and_then(|n| n.as_str()),
        Some("Charlie")
    );
    assert_eq!(created.get("age").and_then(|n| n.as_i64()), Some(35));
    let doc_id = created
        .get("_docID")
        .and_then(|d| d.as_str())
        .expect("Should return _docID");
    assert!(doc_id.starts_with("bae-"), "DocID should start with bae-");

    // Verify document persisted by querying it back
    let query_response = client
        .post(format!("{}/api/v0/graphql", api_url))
        .header("content-type", "application/json")
        .body(r#"{"query": "{ Users { name age } }"}"#)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("Failed to query users");

    let query_body: serde_json::Value = query_response.json().await.unwrap();
    let users = query_body["data"]["Users"]
        .as_array()
        .expect("Should have Users array");
    assert_eq!(users.len(), 1, "Should have exactly 1 user");
    assert_eq!(users[0]["name"].as_str(), Some("Charlie"));

    shutdown_tx.send(()).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
}

/// Test updating a document via GraphQL mutation through HTTP
#[tokio::test]
async fn test_http_graphql_update_mutation() {
    use document::NormalValue;
    use schema::{CollectionVersion, FieldDescription, FieldKind};

    let temp_dir = tempfile::tempdir().unwrap();
    let port = portpicker::pick_unused_port().expect("No free ports");
    let data_path = temp_dir.path();

    // Phase 1: Pre-seed database with a document
    let doc_id: String;
    {
        let store = storage::RocksDBStore::open(data_path).unwrap();
        let database = db::DB::new(store);

        let schema = CollectionVersion::new(
            "Users",
            "v1",
            "col-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        );
        database.create_collection(schema).await.unwrap();

        let collection = database.get_collection("Users").unwrap().unwrap();
        let txn = database.new_txn(false).await.unwrap();

        let mut doc = document::Document::new();
        doc.set("name", NormalValue::String("Diana".to_string()));
        doc.set("age", NormalValue::Int(28));
        doc.generate_and_set_doc_id().unwrap();
        doc_id = doc.id().unwrap().to_string();
        collection.create(&txn, &doc).await.unwrap();

        txn.commit().await.unwrap();
        database.close().await.unwrap();
    }

    // Phase 2: Start server and update the document
    let config = test_config_rocksdb(port, temp_dir.path());
    let api_url = format!("http://127.0.0.1:{}", port);
    let node = Node::new(config, None).await.unwrap();
    let shutdown_tx = node.shutdown_tx.clone();

    let node_handle = tokio::spawn(async move { node.run().await });
    wait_for_server(&api_url, 20).await;

    let client = reqwest::Client::new();

    // Update the document
    let update_mutation = format!(
        r#"{{"query": "mutation {{ update_Users(docIDs: [\"{}\"], input: {{age: 29}}) {{ _docID name age }} }}"}}"#,
        doc_id
    );

    let response = client
        .post(format!("{}/api/v0/graphql", api_url))
        .header("content-type", "application/json")
        .body(update_mutation)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .expect("Failed to execute update mutation");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();

    // Debug: print full response
    println!(
        "Update mutation response: {}",
        serde_json::to_string_pretty(&body).unwrap()
    );

    // Verify mutation succeeded
    // Note: Response uses collection name "Users" as key, not "update_Users"
    let data = body.get("data").expect("Response should have data field");
    let updated = data
        .get("Users")
        .and_then(|u| u.as_array())
        .expect("Users should be an array");
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0]["age"].as_i64(), Some(29));
    assert_eq!(updated[0]["name"].as_str(), Some("Diana"));

    // Verify persistence
    let query_response = client
        .post(format!("{}/api/v0/graphql", api_url))
        .header("content-type", "application/json")
        .body(r#"{"query": "{ Users { name age } }"}"#)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();

    let query_body: serde_json::Value = query_response.json().await.unwrap();
    let users = query_body["data"]["Users"].as_array().unwrap();
    assert_eq!(users[0]["age"].as_i64(), Some(29));

    shutdown_tx.send(()).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
}

/// Test deleting a document via GraphQL mutation through HTTP
#[tokio::test]
async fn test_http_graphql_delete_mutation() {
    use document::NormalValue;
    use schema::{CollectionVersion, FieldDescription, FieldKind};

    let temp_dir = tempfile::tempdir().unwrap();
    let port = portpicker::pick_unused_port().expect("No free ports");
    let data_path = temp_dir.path();

    // Phase 1: Pre-seed database with two documents
    let doc_id_to_delete: String;
    {
        let store = storage::RocksDBStore::open(data_path).unwrap();
        let database = db::DB::new(store);

        let schema = CollectionVersion::new(
            "Users",
            "v1",
            "col-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        );
        database.create_collection(schema).await.unwrap();

        let collection = database.get_collection("Users").unwrap().unwrap();
        let txn = database.new_txn(false).await.unwrap();

        let mut doc1 = document::Document::new();
        doc1.set("name", NormalValue::String("Eve".to_string()));
        doc1.set("age", NormalValue::Int(22));
        doc1.generate_and_set_doc_id().unwrap();
        doc_id_to_delete = doc1.id().unwrap().to_string();
        collection.create(&txn, &doc1).await.unwrap();

        let mut doc2 = document::Document::new();
        doc2.set("name", NormalValue::String("Frank".to_string()));
        doc2.set("age", NormalValue::Int(40));
        doc2.generate_and_set_doc_id().unwrap();
        collection.create(&txn, &doc2).await.unwrap();

        txn.commit().await.unwrap();
        database.close().await.unwrap();
    }

    // Phase 2: Start server and delete one document
    let config = test_config_rocksdb(port, temp_dir.path());
    let api_url = format!("http://127.0.0.1:{}", port);
    let node = Node::new(config, None).await.unwrap();
    let shutdown_tx = node.shutdown_tx.clone();

    let node_handle = tokio::spawn(async move { node.run().await });
    wait_for_server(&api_url, 20).await;

    let client = reqwest::Client::new();

    // Delete the first document
    let delete_mutation = format!(
        r#"{{"query": "mutation {{ delete_Users(docIDs: [\"{}\"]) {{ _docID }} }}"}}"#,
        doc_id_to_delete
    );

    let response = client
        .post(format!("{}/api/v0/graphql", api_url))
        .header("content-type", "application/json")
        .body(delete_mutation)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .expect("Failed to execute delete mutation");

    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // Verify only one document remains
    let query_response = client
        .post(format!("{}/api/v0/graphql", api_url))
        .header("content-type", "application/json")
        .body(r#"{"query": "{ Users { name } }"}"#)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();

    let query_body: serde_json::Value = query_response.json().await.unwrap();
    let users = query_body["data"]["Users"].as_array().unwrap();
    assert_eq!(users.len(), 1, "Should have only 1 user after deletion");
    assert_eq!(users[0]["name"].as_str(), Some("Frank"));

    shutdown_tx.send(()).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
}

// =========================================================================
// Integration tests for Issue #68: Transaction HTTP Endpoints
// =========================================================================

/// Test transaction begin endpoint
#[tokio::test]
async fn test_http_transaction_begin() {
    use schema::{CollectionVersion, FieldDescription, FieldKind};

    let temp_dir = tempfile::tempdir().unwrap();
    let port = portpicker::pick_unused_port().expect("No free ports");
    let data_path = temp_dir.path();

    // Pre-seed database with collection
    {
        let store = storage::RocksDBStore::open(data_path).unwrap();
        let database = db::DB::new(store);
        let schema = CollectionVersion::new(
            "Users",
            "v1",
            "col-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
            ],
        );
        database.create_collection(schema).await.unwrap();
        database.close().await.unwrap();
    }

    let config = test_config_rocksdb(port, temp_dir.path());
    let api_url = format!("http://127.0.0.1:{}", port);
    let node = Node::new(config, None).await.unwrap();
    let shutdown_tx = node.shutdown_tx.clone();

    let node_handle = tokio::spawn(async move { node.run().await });
    wait_for_server(&api_url, 20).await;

    let client = reqwest::Client::new();

    // Begin a transaction
    let response = client
        .post(format!("{}/api/v0/tx/begin", api_url))
        .header("content-type", "application/json")
        .body(r#"{"readonly": false}"#)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("Failed to begin transaction");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    let txn_id = body
        .get("txn_id")
        .and_then(|t| t.as_str())
        .expect("Should return txn_id");
    assert!(!txn_id.is_empty(), "Transaction ID should not be empty");

    // Begin a read-only transaction
    let response = client
        .post(format!("{}/api/v0/tx/begin", api_url))
        .header("content-type", "application/json")
        .body(r#"{"readonly": true}"#)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    let readonly_txn_id = body.get("txn_id").and_then(|t| t.as_str()).unwrap();
    assert_ne!(
        txn_id, readonly_txn_id,
        "Should get different transaction IDs"
    );

    shutdown_tx.send(()).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
}

/// Test full transaction lifecycle: begin, query, commit
#[tokio::test]
async fn test_http_transaction_commit_flow() {
    use document::NormalValue;
    use schema::{CollectionVersion, FieldDescription, FieldKind};

    let temp_dir = tempfile::tempdir().unwrap();
    let port = portpicker::pick_unused_port().expect("No free ports");
    let data_path = temp_dir.path();

    // Pre-seed database with collection and document
    {
        let store = storage::RocksDBStore::open(data_path).unwrap();
        let database = db::DB::new(store);
        let schema = CollectionVersion::new(
            "Users",
            "v1",
            "col-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        );
        database.create_collection(schema).await.unwrap();

        let collection = database.get_collection("Users").unwrap().unwrap();
        let txn = database.new_txn(false).await.unwrap();
        let mut doc = document::Document::new();
        doc.set("name", NormalValue::String("Grace".to_string()));
        doc.set("age", NormalValue::Int(33));
        doc.generate_and_set_doc_id().unwrap();
        collection.create(&txn, &doc).await.unwrap();
        txn.commit().await.unwrap();
        database.close().await.unwrap();
    }

    let config = test_config_rocksdb(port, temp_dir.path());
    let api_url = format!("http://127.0.0.1:{}", port);
    let node = Node::new(config, None).await.unwrap();
    let shutdown_tx = node.shutdown_tx.clone();

    let node_handle = tokio::spawn(async move { node.run().await });
    wait_for_server(&api_url, 20).await;

    let client = reqwest::Client::new();

    // Step 1: Begin a transaction
    let begin_response = client
        .post(format!("{}/api/v0/tx/begin", api_url))
        .header("content-type", "application/json")
        .body(r#"{"readonly": true}"#)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();

    let begin_body: serde_json::Value = begin_response.json().await.unwrap();
    let txn_id = begin_body["txn_id"].as_str().unwrap();

    // Step 2: Query within the transaction
    let query_body = format!(
        r#"{{"query": "{{ Users {{ name age }} }}", "txn_id": "{}"}}"#,
        txn_id
    );
    let query_response = client
        .post(format!("{}/api/v0/graphql", api_url))
        .header("content-type", "application/json")
        .body(query_body)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();

    assert_eq!(query_response.status(), reqwest::StatusCode::OK);
    let query_result: serde_json::Value = query_response.json().await.unwrap();
    let users = query_result["data"]["Users"].as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0]["name"].as_str(), Some("Grace"));

    // Step 3: Commit the transaction
    let commit_body = format!(r#"{{"txn_id": "{}"}}"#, txn_id);
    let commit_response = client
        .post(format!("{}/api/v0/tx/commit", api_url))
        .header("content-type", "application/json")
        .body(commit_body)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();

    assert_eq!(commit_response.status(), reqwest::StatusCode::OK);
    let commit_body: serde_json::Value = commit_response.json().await.unwrap();
    assert_eq!(commit_body["status"].as_str(), Some("committed"));

    shutdown_tx.send(()).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
}

/// Test transaction rollback
#[tokio::test]
async fn test_http_transaction_rollback() {
    use schema::{CollectionVersion, FieldDescription, FieldKind};

    let temp_dir = tempfile::tempdir().unwrap();
    let port = portpicker::pick_unused_port().expect("No free ports");
    let data_path = temp_dir.path();

    // Pre-seed database with collection
    {
        let store = storage::RocksDBStore::open(data_path).unwrap();
        let database = db::DB::new(store);
        let schema = CollectionVersion::new(
            "Users",
            "v1",
            "col-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
            ],
        );
        database.create_collection(schema).await.unwrap();
        database.close().await.unwrap();
    }

    let config = test_config_rocksdb(port, temp_dir.path());
    let api_url = format!("http://127.0.0.1:{}", port);
    let node = Node::new(config, None).await.unwrap();
    let shutdown_tx = node.shutdown_tx.clone();

    let node_handle = tokio::spawn(async move { node.run().await });
    wait_for_server(&api_url, 20).await;

    let client = reqwest::Client::new();

    // Begin a transaction
    let begin_response = client
        .post(format!("{}/api/v0/tx/begin", api_url))
        .header("content-type", "application/json")
        .body(r#"{"readonly": false}"#)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();

    let begin_body: serde_json::Value = begin_response.json().await.unwrap();
    let txn_id = begin_body["txn_id"].as_str().unwrap();

    // Rollback the transaction
    let rollback_body = format!(r#"{{"txn_id": "{}"}}"#, txn_id);
    let rollback_response = client
        .post(format!("{}/api/v0/tx/rollback", api_url))
        .header("content-type", "application/json")
        .body(rollback_body)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();

    assert_eq!(rollback_response.status(), reqwest::StatusCode::OK);
    let rollback_result: serde_json::Value = rollback_response.json().await.unwrap();
    assert_eq!(rollback_result["status"].as_str(), Some("rolled_back"));

    // Verify the transaction is no longer valid (double rollback should fail)
    let double_rollback = client
        .post(format!("{}/api/v0/tx/rollback", api_url))
        .header("content-type", "application/json")
        .body(format!(r#"{{"txn_id": "{}"}}"#, txn_id))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();

    assert_eq!(double_rollback.status(), reqwest::StatusCode::BAD_REQUEST);

    shutdown_tx.send(()).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
}

/// Test invalid transaction ID returns error
#[tokio::test]
async fn test_http_transaction_invalid_id() {
    let temp_dir = tempfile::tempdir().unwrap();
    let port = portpicker::pick_unused_port().expect("No free ports");

    let config = test_config(port, temp_dir.path());
    let api_url = format!("http://127.0.0.1:{}", port);
    let node = Node::new(config, None).await.unwrap();
    let shutdown_tx = node.shutdown_tx.clone();

    let node_handle = tokio::spawn(async move { node.run().await });
    wait_for_server(&api_url, 20).await;

    let client = reqwest::Client::new();

    // Try to commit a non-existent transaction
    let response = client
        .post(format!("{}/api/v0/tx/commit", api_url))
        .header("content-type", "application/json")
        .body(r#"{"txn_id": "nonexistent-txn-id"}"#)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);

    shutdown_tx.send(()).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
}
