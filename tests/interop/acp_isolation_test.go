package interop

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb.rs-interop/tests/interop/framework"
)

// mirrorNodes holds the nodes and identity helpers returned by startMirrorNodes.
type mirrorNodes struct {
	Rust     *framework.Node
	Go       *framework.Node
	Identity *framework.TestIdentity // base identity (key material, no audience-specific token)
}

// RustClient returns an authenticated client for the Rust node.
func (m *mirrorNodes) RustClient(t *testing.T) *framework.Client {
	t.Helper()
	id, err := m.Identity.ForAudience(fmt.Sprintf("127.0.0.1:%d", m.Rust.Config.HTTPPort))
	require.NoError(t, err, "failed to create Rust identity")
	return m.Rust.Client().WithIdentity(id)
}

// GoClient returns an authenticated client for the Go node.
func (m *mirrorNodes) GoClient(t *testing.T) *framework.Client {
	t.Helper()
	id, err := m.Identity.ForAudience(fmt.Sprintf("127.0.0.1:%d", m.Go.Config.HTTPPort))
	require.NoError(t, err, "failed to create Go identity")
	return m.Go.Client().WithIdentity(id)
}

// ClientForIdentity returns authenticated clients for both nodes using the given identity.
func (m *mirrorNodes) ClientForIdentity(t *testing.T, id *framework.TestIdentity) (rustClient, goClient *framework.Client) {
	t.Helper()
	return m.NodeClient(t, m.Rust, id), m.NodeClient(t, m.Go, id)
}

// NodeClient returns an authenticated client for a specific node using the given identity.
func (m *mirrorNodes) NodeClient(t *testing.T, node *framework.Node, id *framework.TestIdentity) *framework.Client {
	t.Helper()
	audienceID, err := id.ForAudience(fmt.Sprintf("127.0.0.1:%d", node.Config.HTTPPort))
	require.NoError(t, err, "failed to create identity for node")
	return node.Client().WithIdentity(audienceID)
}

// startMirrorNodes starts a Rust and Go node pair for mirror-mode testing.
// Both nodes run independently (no P2P), with encryption enabled.
func startMirrorNodes(t *testing.T, ctx context.Context) *mirrorNodes {
	t.Helper()

	// Generate a default identity for node startup and API auth
	defaultIdentity, err := framework.GenerateIdentity("defradb")
	require.NoError(t, err, "failed to generate default identity")

	rustPorts, err := framework.ReserveNodePorts()
	require.NoError(t, err)
	defer rustPorts.Release()

	goPorts, err := framework.ReserveNodePorts()
	require.NoError(t, err)
	defer goPorts.Release()

	rustNode := framework.NewNode(framework.NodeConfig{
		Type:     framework.NodeTypeRust,
		HTTPPort: rustPorts.HTTPPort,
		P2PPort:  rustPorts.P2PPort,
		Store:    "memory",
		NoP2P:    true,
		Identity: defaultIdentity,
		AcpType:  "local",
	})

	rustPorts.Release()
	require.NoError(t, rustNode.Start(ctx), "failed to start Rust node")
	t.Cleanup(func() { rustNode.Stop() })
	dumpLogsOnFailure(t, "rust-node", rustNode)

	goNode := framework.NewNode(framework.NodeConfig{
		Type:     framework.NodeTypeGo,
		HTTPPort: goPorts.HTTPPort,
		P2PPort:  goPorts.P2PPort,
		Store:    "memory",
		NoP2P:    true,
		Identity: defaultIdentity,
		AcpType:  "local",
	})

	goPorts.Release()
	require.NoError(t, goNode.Start(ctx), "failed to start Go node")
	t.Cleanup(func() { goNode.Stop() })
	dumpLogsOnFailure(t, "go-node", goNode)

	return &mirrorNodes{
		Rust:     rustNode,
		Go:       goNode,
		Identity: defaultIdentity,
	}
}

// TestACPMultiUserIsolation tests that multiple users with ACP policies
// can only see their own documents, and that sharing works correctly.
func TestACPMultiUserIsolation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Generate identities
	alice, err := framework.GenerateIdentity("test")
	require.NoError(t, err, "failed to generate Alice identity")
	bob, err := framework.GenerateIdentity("test")
	require.NoError(t, err, "failed to generate Bob identity")
	carol, err := framework.GenerateIdentity("test")
	require.NoError(t, err, "failed to generate Carol identity")

	m := startMirrorNodes(t, ctx)

	type nodeInfo struct {
		name string
		node *framework.Node
	}

	nodes := []nodeInfo{
		{"Rust", m.Rust},
		{"Go", m.Go},
	}

	// Track policy IDs and doc IDs per node
	type nodeState struct {
		policyID    string
		aliceDocIDs []string
		bobDocIDs   []string
		carolDocIDs []string
	}
	states := make([]nodeState, len(nodes))

	for i, n := range nodes {
		t.Logf("Setting up %s node...", n.name)

		aliceClient := m.NodeClient(t, n.node, alice)
		bobClient := m.NodeClient(t, n.node, bob)
		carolClient := m.NodeClient(t, n.node, carol)

		// Alice adds ACP policy
		policyID, err := aliceClient.AddPolicy(ctx, framework.UserACPPolicy)
		require.NoError(t, err, "%s: failed to add ACP policy", n.name)
		t.Logf("%s: Policy ID = %s", n.name, policyID)
		states[i].policyID = policyID

		// Alice adds schema with policy
		sdl := framework.UsersSchemaWithPolicy(policyID)
		schemas, err := aliceClient.AddSchema(ctx, sdl)
		require.NoError(t, err, "%s: failed to add schema", n.name)
		require.Len(t, schemas, 1, "%s: expected 1 schema", n.name)
		t.Logf("%s: Schema added: %s", n.name, schemas[0].Name)

		// Alice creates 5 documents
		for j := 0; j < 5; j++ {
			resp, err := aliceClient.GraphQL(ctx, framework.CreateUserQuery(fmt.Sprintf("Alice-%d", j), 30+j), nil)
			require.NoError(t, err, "%s: Alice failed to create doc %d", n.name, j)
			require.Empty(t, resp.Errors, "%s: Alice create errors: %v", n.name, resp.Errors)
			docID := extractDocID(t, resp, "create_Users")
			states[i].aliceDocIDs = append(states[i].aliceDocIDs, docID)
		}

		// Bob creates 5 documents
		for j := 0; j < 5; j++ {
			resp, err := bobClient.GraphQL(ctx, framework.CreateUserQuery(fmt.Sprintf("Bob-%d", j), 20+j), nil)
			require.NoError(t, err, "%s: Bob failed to create doc %d", n.name, j)
			require.Empty(t, resp.Errors, "%s: Bob create errors: %v", n.name, resp.Errors)
			docID := extractDocID(t, resp, "create_Users")
			states[i].bobDocIDs = append(states[i].bobDocIDs, docID)
		}

		// Carol creates 5 documents
		for j := 0; j < 5; j++ {
			resp, err := carolClient.GraphQL(ctx, framework.CreateUserQuery(fmt.Sprintf("Carol-%d", j), 40+j), nil)
			require.NoError(t, err, "%s: Carol failed to create doc %d", n.name, j)
			require.Empty(t, resp.Errors, "%s: Carol create errors: %v", n.name, resp.Errors)
			docID := extractDocID(t, resp, "create_Users")
			states[i].carolDocIDs = append(states[i].carolDocIDs, docID)
		}
	}

	// Verify isolation: each user sees only their own docs
	for i, n := range nodes {
		aliceClient := m.NodeClient(t, n.node, alice)
		bobClient := m.NodeClient(t, n.node, bob)
		carolClient := m.NodeClient(t, n.node, carol)

		aliceResp, err := aliceClient.GraphQL(ctx, framework.QueryUsersQuery(), nil)
		require.NoError(t, err, "%s: Alice query failed", n.name)
		require.Empty(t, aliceResp.Errors, "%s: Alice query errors", n.name)
		aliceDocs := countDocs(t, aliceResp)
		require.Equal(t, 5, aliceDocs, "%s: Alice should see 5 docs, got %d", n.name, aliceDocs)

		bobResp, err := bobClient.GraphQL(ctx, framework.QueryUsersQuery(), nil)
		require.NoError(t, err, "%s: Bob query failed", n.name)
		require.Empty(t, bobResp.Errors, "%s: Bob query errors", n.name)
		bobDocs := countDocs(t, bobResp)
		require.Equal(t, 5, bobDocs, "%s: Bob should see 5 docs, got %d", n.name, bobDocs)

		carolResp, err := carolClient.GraphQL(ctx, framework.QueryUsersQuery(), nil)
		require.NoError(t, err, "%s: Carol query failed", n.name)
		require.Empty(t, carolResp.Errors, "%s: Carol query errors", n.name)
		carolDocs := countDocs(t, carolResp)
		require.Equal(t, 5, carolDocs, "%s: Carol should see 5 docs, got %d", n.name, carolDocs)

		t.Logf("%s: Isolation verified — Alice:%d Bob:%d Carol:%d", n.name, aliceDocs, bobDocs, carolDocs)

		// Alice grants Bob reader access to her first document
		sharedDocID := states[i].aliceDocIDs[0]
		added, err := aliceClient.AddDocRelationship(ctx, "Users", sharedDocID, "reader", bob.DID)
		require.NoError(t, err, "%s: failed to add doc relationship", n.name)
		require.True(t, added, "%s: relationship should be new", n.name)

		// Bob should now see 6 docs (5 own + 1 shared)
		bobResp2, err := bobClient.GraphQL(ctx, framework.QueryUsersQuery(), nil)
		require.NoError(t, err, "%s: Bob query after share failed", n.name)
		require.Empty(t, bobResp2.Errors, "%s: Bob query after share errors", n.name)
		bobDocs2 := countDocs(t, bobResp2)
		require.Equal(t, 6, bobDocs2, "%s: Bob should see 6 docs after share, got %d", n.name, bobDocs2)

		// Alice revokes Bob's reader access
		deleted, err := aliceClient.DeleteDocRelationship(ctx, "Users", sharedDocID, "reader", bob.DID)
		require.NoError(t, err, "%s: failed to delete doc relationship", n.name)
		require.True(t, deleted, "%s: relationship should have existed", n.name)

		// Bob should see 5 docs again
		bobResp3, err := bobClient.GraphQL(ctx, framework.QueryUsersQuery(), nil)
		require.NoError(t, err, "%s: Bob query after revoke failed", n.name)
		require.Empty(t, bobResp3.Errors, "%s: Bob query after revoke errors", n.name)
		bobDocs3 := countDocs(t, bobResp3)
		require.Equal(t, 5, bobDocs3, "%s: Bob should see 5 docs after revoke, got %d", n.name, bobDocs3)

		t.Logf("%s: Share/revoke verified", n.name)
	}

	// Compare Rust vs Go responses for each user
	for _, id := range []*framework.TestIdentity{alice, bob, carol} {
		rustClient, goClient := m.ClientForIdentity(t, id)
		rustResp, err := rustClient.GraphQL(ctx, framework.QueryUsersQuery(), nil)
		require.NoError(t, err)
		goResp, err := goClient.GraphQL(ctx, framework.QueryUsersQuery(), nil)
		require.NoError(t, err)
		framework.CompareGraphQLResponses(t, rustResp, goResp, fmt.Sprintf("user %s query parity", id.DID[:20]))
	}
}

// TestACPCrossUserWriteBlocked verifies that users cannot modify other users' documents.
func TestACPCrossUserWriteBlocked(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	alice, err := framework.GenerateIdentity("test")
	require.NoError(t, err)
	bob, err := framework.GenerateIdentity("test")
	require.NoError(t, err)

	m := startMirrorNodes(t, ctx)

	type nodeInfo struct {
		name string
		node *framework.Node
	}

	for _, n := range []nodeInfo{{"Rust", m.Rust}, {"Go", m.Go}} {
		t.Run(n.name, func(t *testing.T) {
			aliceClient := m.NodeClient(t, n.node, alice)
			bobClient := m.NodeClient(t, n.node, bob)

			// Alice creates collection with ACP
			policyID, err := aliceClient.AddPolicy(ctx, framework.UserACPPolicy)
			require.NoError(t, err, "failed to add policy")

			sdl := framework.UsersSchemaWithPolicy(policyID)
			_, err = aliceClient.AddSchema(ctx, sdl)
			require.NoError(t, err, "failed to add schema")

			// Alice creates a document
			resp, err := aliceClient.GraphQL(ctx, framework.CreateUserQuery("AliceOnly", 99), nil)
			require.NoError(t, err, "failed to create doc")
			require.Empty(t, resp.Errors)
			docID := extractDocID(t, resp, "create_Users")

			// Bob attempts to update Alice's document
			updateQuery := fmt.Sprintf(
				`mutation { update_Users(docID: %q, input: {name: "Hacked"}) { _docID name } }`,
				docID,
			)
			updateResp, err := bobClient.GraphQL(ctx, updateQuery, nil)
			require.NoError(t, err, "update request should not fail at HTTP level")

			// The update should either return an error or return empty results
			if len(updateResp.Errors) > 0 {
				t.Logf("%s: Bob update correctly returned error: %s", n.name, updateResp.Errors[0].Message)
			} else {
				// No error but should return 0 updated docs
				var data map[string][]any
				json.Unmarshal(updateResp.Data, &data)
				updates := data["update_Users"]
				require.Empty(t, updates, "%s: Bob should not be able to update Alice's doc", n.name)
				t.Logf("%s: Bob update correctly returned 0 results", n.name)
			}

			// Bob attempts to delete Alice's document
			deleteQuery := fmt.Sprintf(
				`mutation { delete_Users(docID: %q) { _docID } }`,
				docID,
			)
			deleteResp, err := bobClient.GraphQL(ctx, deleteQuery, nil)
			require.NoError(t, err, "delete request should not fail at HTTP level")

			if len(deleteResp.Errors) > 0 {
				t.Logf("%s: Bob delete correctly returned error: %s", n.name, deleteResp.Errors[0].Message)
			} else {
				var data map[string][]any
				json.Unmarshal(deleteResp.Data, &data)
				deletes := data["delete_Users"]
				require.Empty(t, deletes, "%s: Bob should not be able to delete Alice's doc", n.name)
				t.Logf("%s: Bob delete correctly returned 0 results", n.name)
			}

			// Verify Alice's document is unchanged
			aliceResp, err := aliceClient.GraphQL(ctx, framework.QueryUsersQuery(), nil)
			require.NoError(t, err)
			require.Empty(t, aliceResp.Errors)
			docs := countDocs(t, aliceResp)
			require.Equal(t, 1, docs, "%s: Alice should still see her document", n.name)
		})
	}
}

// extractDocID extracts a _docID from a GraphQL mutation response.
func extractDocID(t *testing.T, resp *framework.GraphQLResponse, mutationField string) string {
	t.Helper()
	var data map[string][]struct {
		DocID string `json:"_docID"`
	}
	err := json.Unmarshal(resp.Data, &data)
	require.NoError(t, err, "failed to parse response for docID extraction")
	results := data[mutationField]
	require.NotEmpty(t, results, "expected at least 1 result in %s", mutationField)
	return results[0].DocID
}

// countDocs counts the number of documents in a Users query response.
func countDocs(t *testing.T, resp *framework.GraphQLResponse) int {
	t.Helper()
	var data map[string][]json.RawMessage
	err := json.Unmarshal(resp.Data, &data)
	require.NoError(t, err, "failed to parse response for doc count")
	return len(data["Users"])
}
