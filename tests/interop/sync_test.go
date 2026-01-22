package interop

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb.rs-interop/tests/interop/framework"
)

// TestSyncRustToGoWriteRead tests writing a document to a Rust node
// and reading it from a Go node via P2P replication.
func TestSyncRustToGoWriteRead(t *testing.T) {
	t.Parallel() // Safe for parallel execution with ReserveNodePorts

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Reserve ports for both nodes (held until Release)
	rustPorts, err := framework.ReserveNodePorts()
	require.NoError(t, err, "failed to reserve ports for Rust node")
	defer rustPorts.Release()

	goPorts, err := framework.ReserveNodePorts()
	require.NoError(t, err, "failed to reserve ports for Go node")
	defer goPorts.Release()

	// Start Rust node
	rustNode := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     rustPorts.HTTPPort,
		P2PPort:      rustPorts.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Rust node...")
	rustPorts.Release() // Release ports before starting node
	require.NoError(t, rustNode.Start(ctx), "failed to start Rust node")
	defer rustNode.Stop()
	dumpLogsOnFailure(t, "rust-node", rustNode)

	t.Logf("Rust node started with peer ID: %s", rustNode.PeerID())

	// Start Go node
	goNode := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeGo,
		HTTPPort:     goPorts.HTTPPort,
		P2PPort:      goPorts.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Go node...")
	goPorts.Release() // Release ports before starting node
	require.NoError(t, goNode.Start(ctx), "failed to start Go node")
	defer goNode.Stop()
	dumpLogsOnFailure(t, "go-node", goNode)

	t.Logf("Go node started with peer ID: %s", goNode.PeerID())

	rustClient := rustNode.Client()
	goClient := goNode.Client()

	// Connect Go node to Rust node
	t.Log("Connecting Go node to Rust node...")
	err = goClient.ConnectPeer(ctx, rustNode.P2PMultiaddr())
	require.NoError(t, err, "failed to connect Go node to Rust node")

	// Wait for connection to establish
	err = framework.WaitForPeerConnected(ctx, rustClient, goNode.PeerID(), 30*time.Second)
	require.NoError(t, err, "Rust node did not see Go node connect")

	t.Log("Nodes connected successfully")

	// Add schema to both nodes using HTTP endpoint
	t.Log("Adding schema to both nodes...")

	rustSchemas, err := rustClient.AddSchema(ctx, framework.UsersSchema)
	require.NoError(t, err, "failed to add schema to Rust node")
	require.Len(t, rustSchemas, 1, "expected 1 schema from Rust node")
	t.Logf("Rust schema: Name=%s CollectionID=%s VersionID=%s", rustSchemas[0].Name, rustSchemas[0].CollectionID, rustSchemas[0].VersionID)

	goSchemas, err := goClient.AddSchema(ctx, framework.UsersSchema)
	require.NoError(t, err, "failed to add schema to Go node")
	require.Len(t, goSchemas, 1, "expected 1 schema from Go node")
	t.Logf("Go schema: Name=%s CollectionID=%s VersionID=%s", goSchemas[0].Name, goSchemas[0].CollectionID, goSchemas[0].VersionID)

	// Verify both nodes generate the same CollectionID (CID interop fix)
	require.Equal(t, rustSchemas[0].CollectionID, goSchemas[0].CollectionID,
		"CollectionID mismatch: Rust and Go should generate identical CIDs for the same schema")
	t.Logf("CollectionID match confirmed: %s", rustSchemas[0].CollectionID)

	// Add P2P collections to both nodes using collection NAME
	// Both nodes will look up the CollectionID internally and subscribe to the same topic
	t.Log("Adding P2P collections to Go node...")
	err = goClient.AddP2PCollections(ctx, []string{goSchemas[0].Name})
	require.NoError(t, err, "failed to add P2P collections to Go node")
	t.Log("P2P collections added to Go node")

	t.Log("Adding P2P collections to Rust node...")
	err = rustClient.AddP2PCollections(ctx, []string{rustSchemas[0].Name})
	require.NoError(t, err, "failed to add P2P collections to Rust node")
	t.Log("P2P collections added to Rust node")

	// Set up replication from Rust to Go
	// The Rust node will push data to the Go node via request-response protocol
	t.Log("Setting up replication...")
	err = rustClient.SetReplicator(ctx, []string{goNode.P2PMultiaddr()}, []string{rustSchemas[0].Name})
	require.NoError(t, err, "failed to set replicator on Rust node")
	t.Log("Replicator set on Rust node")

	// Create a document on the Rust node
	t.Log("Creating document on Rust node...")
	createQuery := framework.CreateUserQuery("Alice", 30)
	createResp, err := rustClient.GraphQL(ctx, createQuery, nil)
	require.NoError(t, err, "failed to create document on Rust node")
	require.Empty(t, createResp.Errors, "Create errors: %v", createResp.Errors)

	// Parse the created document to get the docID
	// Note: GraphQL mutations return arrays even for single document creation
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

	// Wait for replication using polling instead of hardcoded sleep
	t.Log("Waiting for replication...")
	err = framework.WaitForDocumentReplicated(ctx, goClient, "Users", docID, 30*time.Second)
	require.NoError(t, err, "document was not replicated to Go node")

	// Query the document from the Go node to verify full content
	t.Log("Querying document from Go node...")
	queryResp, err := goClient.GraphQL(ctx, framework.QueryUsersQuery(), nil)
	require.NoError(t, err, "failed to query Go node")
	require.Empty(t, queryResp.Errors, "Query errors: %v", queryResp.Errors)

	// Parse the query response
	var queryData struct {
		Users []struct {
			DocID string `json:"_docID"`
			Name  string `json:"name"`
			Age   int    `json:"age"`
		} `json:"Users"`
	}
	err = json.Unmarshal(queryResp.Data, &queryData)
	require.NoError(t, err, "failed to parse query response")

	t.Logf("Query response: %+v", queryData)

	// Verify the document was replicated
	require.Len(t, queryData.Users, 1, "expected 1 user document on Go node")
	require.Equal(t, docID, queryData.Users[0].DocID, "document ID mismatch")
	require.Equal(t, "Alice", queryData.Users[0].Name, "name mismatch")
	require.Equal(t, 30, queryData.Users[0].Age, "age mismatch")

	t.Log("Document successfully replicated from Rust to Go!")
}

// TestSyncGoToRustWriteRead tests writing a document to a Go node
// and reading it from a Rust node via P2P replication.
func TestSyncGoToRustWriteRead(t *testing.T) {
	t.Parallel() // Safe for parallel execution with ReserveNodePorts

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Reserve ports for both nodes (held until Release)
	rustPorts, err := framework.ReserveNodePorts()
	require.NoError(t, err, "failed to reserve ports for Rust node")
	defer rustPorts.Release()

	goPorts, err := framework.ReserveNodePorts()
	require.NoError(t, err, "failed to reserve ports for Go node")
	defer goPorts.Release()

	// Start Rust node
	rustNode := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     rustPorts.HTTPPort,
		P2PPort:      rustPorts.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Rust node...")
	rustPorts.Release() // Release ports before starting node
	require.NoError(t, rustNode.Start(ctx), "failed to start Rust node")
	defer rustNode.Stop()
	dumpLogsOnFailure(t, "rust-node", rustNode)

	t.Logf("Rust node started with peer ID: %s", rustNode.PeerID())

	// Start Go node
	goNode := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeGo,
		HTTPPort:     goPorts.HTTPPort,
		P2PPort:      goPorts.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Go node...")
	goPorts.Release() // Release ports before starting node
	require.NoError(t, goNode.Start(ctx), "failed to start Go node")
	defer goNode.Stop()
	dumpLogsOnFailure(t, "go-node", goNode)

	t.Logf("Go node started with peer ID: %s", goNode.PeerID())

	rustClient := rustNode.Client()
	goClient := goNode.Client()

	// Connect Rust node to Go node
	t.Log("Connecting Rust node to Go node...")
	err = rustClient.ConnectPeer(ctx, goNode.P2PMultiaddr())
	require.NoError(t, err, "failed to connect Rust node to Go node")

	// Wait for connection to establish
	err = framework.WaitForPeerConnected(ctx, rustClient, goNode.PeerID(), 30*time.Second)
	require.NoError(t, err, "Rust node did not see Go node connect")

	t.Log("Nodes connected successfully")

	// Add schema to both nodes using HTTP endpoint
	t.Log("Adding schema to both nodes...")

	rustSchemas, err := rustClient.AddSchema(ctx, framework.UsersSchema)
	require.NoError(t, err, "failed to add schema to Rust node")
	require.Len(t, rustSchemas, 1, "expected 1 schema from Rust node")
	t.Logf("Rust schema: Name=%s CollectionID=%s VersionID=%s", rustSchemas[0].Name, rustSchemas[0].CollectionID, rustSchemas[0].VersionID)

	goSchemas, err := goClient.AddSchema(ctx, framework.UsersSchema)
	require.NoError(t, err, "failed to add schema to Go node")
	require.Len(t, goSchemas, 1, "expected 1 schema from Go node")
	t.Logf("Go schema: Name=%s CollectionID=%s VersionID=%s", goSchemas[0].Name, goSchemas[0].CollectionID, goSchemas[0].VersionID)

	// Verify both nodes generate the same CollectionID (CID interop fix)
	require.Equal(t, rustSchemas[0].CollectionID, goSchemas[0].CollectionID,
		"CollectionID mismatch: Rust and Go should generate identical CIDs for the same schema")
	t.Logf("CollectionID match confirmed: %s", rustSchemas[0].CollectionID)

	// Add P2P collections to Rust node so it subscribes to collection topics
	// This is required for Rust to receive messages from Go via GossipSub or request-response
	t.Log("Adding P2P collections to Rust node...")
	err = rustClient.AddP2PCollections(ctx, []string{rustSchemas[0].Name})
	require.NoError(t, err, "failed to add P2P collections to Rust node")
	t.Log("P2P collections added to Rust node")

	// Set up bi-directional replicator registration:
	// 1. Go sets Rust as a replicator so Go pushes updates TO Rust
	// 2. Go also needs to allow Rust to fetch blocks via Bitswap (hasAccess check)
	//    This happens implicitly when we set replicator - Go allows replicators to fetch blocks

	// Set up replication from Go to Rust
	// The Go node will push data to the Rust node via request-response protocol
	t.Log("Setting up replication (Go -> Rust)...")
	err = goClient.SetReplicator(ctx, []string{rustNode.P2PMultiaddr()}, []string{goSchemas[0].Name})
	require.NoError(t, err, "failed to set replicator on Go node")
	t.Log("Replicator set on Go node")

	// Also set up replication from Rust to Go - this is needed because:
	// Go's Bitswap has an access control filter (hasAccess) that only serves blocks
	// to peers that are registered replicators. Without this, when Rust tries to
	// fetch linked blocks via Bitswap, Go will deny the request.
	t.Log("Setting up replication (Rust -> Go) for Bitswap access...")
	err = rustClient.SetReplicator(ctx, []string{goNode.P2PMultiaddr()}, []string{rustSchemas[0].Name})
	require.NoError(t, err, "failed to set replicator on Rust node")
	t.Log("Replicator set on Rust node (enables Bitswap access)")

	// Create a document on the Go node
	t.Log("Creating document on Go node...")
	createQuery := framework.CreateUserQuery("Bob", 25)
	createResp, err := goClient.GraphQL(ctx, createQuery, nil)
	require.NoError(t, err, "failed to create document on Go node")
	require.Empty(t, createResp.Errors, "Create errors: %v", createResp.Errors)

	// Parse the created document to get the docID
	// Note: GraphQL mutations return arrays even for single document creation
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

	// Wait for replication using polling instead of hardcoded sleep
	t.Log("Waiting for replication...")
	err = framework.WaitForDocumentReplicated(ctx, rustClient, "Users", docID, 30*time.Second)
	require.NoError(t, err, "document was not replicated to Rust node")

	// Query the document from the Rust node to verify full content
	t.Log("Querying document from Rust node...")
	queryResp, err := rustClient.GraphQL(ctx, framework.QueryUsersQuery(), nil)
	require.NoError(t, err, "failed to query Rust node")
	require.Empty(t, queryResp.Errors, "Query errors: %v", queryResp.Errors)

	// Parse the query response
	var queryData struct {
		Users []struct {
			DocID string `json:"_docID"`
			Name  string `json:"name"`
			Age   int    `json:"age"`
		} `json:"Users"`
	}
	err = json.Unmarshal(queryResp.Data, &queryData)
	require.NoError(t, err, "failed to parse query response")

	t.Logf("Query response: %+v", queryData)

	// Verify the document was replicated
	require.Len(t, queryData.Users, 1, "expected 1 user document on Rust node")
	require.Equal(t, docID, queryData.Users[0].DocID, "document ID mismatch")
	require.Equal(t, "Bob", queryData.Users[0].Name, "name mismatch")
	require.Equal(t, 25, queryData.Users[0].Age, "age mismatch")

	t.Log("Document successfully replicated from Go to Rust!")
}
