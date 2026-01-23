package interop

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb.rs-interop/tests/interop/framework"
)

// TestSubscriptionCreateTriggersUpdate tests that creating a document triggers
// a subscription update.
func TestSubscriptionCreateTriggersUpdate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Reserve ports for Rust node
	ports, err := framework.ReserveNodePorts()
	require.NoError(t, err, "failed to reserve ports")
	defer ports.Release()

	// Start Rust node
	node := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     ports.HTTPPort,
		P2PPort:      ports.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Rust node...")
	ports.Release()
	require.NoError(t, node.Start(ctx), "failed to start Rust node")
	defer node.Stop()
	dumpLogsOnFailure(t, "rust-node", node)

	client := node.Client()

	// Add schema
	t.Log("Adding schema...")
	schemas, err := client.AddSchema(ctx, framework.UsersSchema)
	require.NoError(t, err, "failed to add schema")
	require.Len(t, schemas, 1, "expected 1 schema")
	t.Logf("Schema added: Name=%s CollectionID=%s", schemas[0].Name, schemas[0].CollectionID)

	// Open subscription
	t.Log("Opening subscription...")
	sub, err := client.Subscribe(ctx, "subscription { Users { _docID name age } }", nil)
	require.NoError(t, err, "failed to open subscription")
	defer sub.Close()

	// Receive initial result (empty collection)
	t.Log("Waiting for initial subscription result...")
	select {
	case data := <-sub.Data():
		t.Logf("Initial result: %s", string(data.Data))
	case err := <-sub.Err():
		t.Fatalf("Subscription error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for initial subscription result")
	}

	// Create a document
	t.Log("Creating document...")
	createQuery := framework.CreateUserQuery("Alice", 30)
	createResp, err := client.GraphQL(ctx, createQuery, nil)
	require.NoError(t, err, "failed to create document")
	require.Empty(t, createResp.Errors, "Create errors: %v", createResp.Errors)

	// Parse created document
	var createData struct {
		CreateUsers []struct {
			DocID string `json:"_docID"`
			Name  string `json:"name"`
			Age   int    `json:"age"`
		} `json:"create_Users"`
	}
	err = json.Unmarshal(createResp.Data, &createData)
	require.NoError(t, err, "failed to parse create response")
	require.Len(t, createData.CreateUsers, 1, "expected 1 created document")
	docID := createData.CreateUsers[0].DocID
	t.Logf("Created document with ID: %s", docID)

	// Wait for subscription update
	t.Log("Waiting for subscription update...")
	select {
	case data := <-sub.Data():
		t.Logf("Subscription update: %s", string(data.Data))

		// Verify the update contains our document
		var subData struct {
			Users []struct {
				DocID string `json:"_docID"`
				Name  string `json:"name"`
				Age   int    `json:"age"`
			} `json:"Users"`
		}
		err = json.Unmarshal(data.Data, &subData)
		require.NoError(t, err, "failed to parse subscription data")
		require.Len(t, subData.Users, 1, "expected 1 user in subscription update")
		require.Equal(t, docID, subData.Users[0].DocID, "document ID mismatch")
		require.Equal(t, "Alice", subData.Users[0].Name, "name mismatch")
		require.Equal(t, 30, subData.Users[0].Age, "age mismatch")

	case err := <-sub.Err():
		t.Fatalf("Subscription error: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for subscription update")
	}

	t.Log("Subscription test passed!")
}

// TestSubscriptionUpdateTriggersUpdate tests that updating a document triggers
// a subscription update.
func TestSubscriptionUpdateTriggersUpdate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ports, err := framework.ReserveNodePorts()
	require.NoError(t, err, "failed to reserve ports")
	defer ports.Release()

	node := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     ports.HTTPPort,
		P2PPort:      ports.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Rust node...")
	ports.Release()
	require.NoError(t, node.Start(ctx), "failed to start Rust node")
	defer node.Stop()
	dumpLogsOnFailure(t, "rust-node", node)

	client := node.Client()

	// Add schema
	_, err = client.AddSchema(ctx, framework.UsersSchema)
	require.NoError(t, err, "failed to add schema")

	// Create initial document
	createQuery := framework.CreateUserQuery("Bob", 25)
	createResp, err := client.GraphQL(ctx, createQuery, nil)
	require.NoError(t, err, "failed to create document")
	require.Empty(t, createResp.Errors)

	var createData struct {
		CreateUsers []struct {
			DocID string `json:"_docID"`
		} `json:"create_Users"`
	}
	err = json.Unmarshal(createResp.Data, &createData)
	require.NoError(t, err)
	docID := createData.CreateUsers[0].DocID
	t.Logf("Created document: %s", docID)

	// Open subscription
	t.Log("Opening subscription...")
	sub, err := client.Subscribe(ctx, "subscription { Users { _docID name age } }", nil)
	require.NoError(t, err, "failed to open subscription")
	defer sub.Close()

	// Receive initial result
	select {
	case <-sub.Data():
		t.Log("Received initial subscription result")
	case err := <-sub.Err():
		t.Fatalf("Subscription error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for initial subscription result")
	}

	// Update the document
	t.Log("Updating document...")
	updateQuery := `mutation { update_Users(docID: "` + docID + `", input: {age: 26}) { _docID name age } }`
	updateResp, err := client.GraphQL(ctx, updateQuery, nil)
	require.NoError(t, err, "failed to update document")
	require.Empty(t, updateResp.Errors, "Update errors: %v", updateResp.Errors)

	// Wait for subscription update
	t.Log("Waiting for subscription update after update...")
	select {
	case data := <-sub.Data():
		t.Logf("Subscription update: %s", string(data.Data))

		var subData struct {
			Users []struct {
				DocID string `json:"_docID"`
				Name  string `json:"name"`
				Age   int    `json:"age"`
			} `json:"Users"`
		}
		err = json.Unmarshal(data.Data, &subData)
		require.NoError(t, err)
		require.Len(t, subData.Users, 1)
		require.Equal(t, 26, subData.Users[0].Age, "updated age should be 26")

	case err := <-sub.Err():
		t.Fatalf("Subscription error: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for subscription update")
	}

	t.Log("Update subscription test passed!")
}

// TestSubscriptionDeleteTriggersUpdate tests that deleting a document triggers
// a subscription update.
func TestSubscriptionDeleteTriggersUpdate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ports, err := framework.ReserveNodePorts()
	require.NoError(t, err, "failed to reserve ports")
	defer ports.Release()

	node := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     ports.HTTPPort,
		P2PPort:      ports.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Rust node...")
	ports.Release()
	require.NoError(t, node.Start(ctx), "failed to start Rust node")
	defer node.Stop()
	dumpLogsOnFailure(t, "rust-node", node)

	client := node.Client()

	// Add schema
	_, err = client.AddSchema(ctx, framework.UsersSchema)
	require.NoError(t, err, "failed to add schema")

	// Create initial document
	createQuery := framework.CreateUserQuery("Charlie", 40)
	createResp, err := client.GraphQL(ctx, createQuery, nil)
	require.NoError(t, err, "failed to create document")
	require.Empty(t, createResp.Errors)

	var createData struct {
		CreateUsers []struct {
			DocID string `json:"_docID"`
		} `json:"create_Users"`
	}
	err = json.Unmarshal(createResp.Data, &createData)
	require.NoError(t, err)
	docID := createData.CreateUsers[0].DocID
	t.Logf("Created document: %s", docID)

	// Open subscription
	t.Log("Opening subscription...")
	sub, err := client.Subscribe(ctx, "subscription { Users { _docID name age } }", nil)
	require.NoError(t, err, "failed to open subscription")
	defer sub.Close()

	// Receive initial result (should have 1 document)
	select {
	case data := <-sub.Data():
		t.Logf("Initial result: %s", string(data.Data))
	case err := <-sub.Err():
		t.Fatalf("Subscription error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for initial subscription result")
	}

	// Delete the document
	t.Log("Deleting document...")
	deleteQuery := `mutation { delete_Users(docID: "` + docID + `") { _docID } }`
	deleteResp, err := client.GraphQL(ctx, deleteQuery, nil)
	require.NoError(t, err, "failed to delete document")
	require.Empty(t, deleteResp.Errors, "Delete errors: %v", deleteResp.Errors)

	// Wait for subscription update
	t.Log("Waiting for subscription update after delete...")
	select {
	case data := <-sub.Data():
		t.Logf("Subscription update: %s", string(data.Data))

		var subData struct {
			Users []struct {
				DocID string `json:"_docID"`
			} `json:"Users"`
		}
		err = json.Unmarshal(data.Data, &subData)
		require.NoError(t, err)
		// After delete, the collection should be empty (or the doc should be gone)
		require.Empty(t, subData.Users, "collection should be empty after delete")

	case err := <-sub.Err():
		t.Fatalf("Subscription error: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for subscription update")
	}

	t.Log("Delete subscription test passed!")
}

// TestSubscriptionMultipleDocuments tests subscriptions with multiple documents.
func TestSubscriptionMultipleDocuments(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ports, err := framework.ReserveNodePorts()
	require.NoError(t, err, "failed to reserve ports")
	defer ports.Release()

	node := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     ports.HTTPPort,
		P2PPort:      ports.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Rust node...")
	ports.Release()
	require.NoError(t, node.Start(ctx), "failed to start Rust node")
	defer node.Stop()
	dumpLogsOnFailure(t, "rust-node", node)

	client := node.Client()

	// Add schema
	_, err = client.AddSchema(ctx, framework.UsersSchema)
	require.NoError(t, err, "failed to add schema")

	// Open subscription
	t.Log("Opening subscription...")
	sub, err := client.Subscribe(ctx, "subscription { Users { _docID name age } }", nil)
	require.NoError(t, err, "failed to open subscription")
	defer sub.Close()

	// Receive initial result (empty)
	select {
	case <-sub.Data():
		t.Log("Received initial subscription result")
	case err := <-sub.Err():
		t.Fatalf("Subscription error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for initial subscription result")
	}

	// Create multiple documents
	docs := []struct {
		name string
		age  int
	}{
		{"Alice", 25},
		{"Bob", 30},
		{"Charlie", 35},
	}

	for i, doc := range docs {
		t.Logf("Creating document %d: %s", i+1, doc.name)
		createQuery := framework.CreateUserQuery(doc.name, doc.age)
		_, err := client.GraphQL(ctx, createQuery, nil)
		require.NoError(t, err)

		// Wait for update
		select {
		case data := <-sub.Data():
			var subData struct {
				Users []struct {
					Name string `json:"name"`
				} `json:"Users"`
			}
			err = json.Unmarshal(data.Data, &subData)
			require.NoError(t, err)
			require.Equal(t, i+1, len(subData.Users), "expected %d documents", i+1)
			t.Logf("Subscription now has %d documents", len(subData.Users))

		case err := <-sub.Err():
			t.Fatalf("Subscription error: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatalf("Timeout waiting for subscription update after creating %s", doc.name)
		}
	}

	t.Log("Multiple documents subscription test passed!")
}
