package framework

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// DifferentialEnv manages a pair of Go and Rust nodes for differential testing.
// It handles setup, P2P connection, schema registration, and result comparison.
type DifferentialEnv struct {
	t          *testing.T
	ctx        context.Context
	rustNode   *Node
	goNode     *Node
	rustClient *Client
	goClient   *Client
	schemas    []AddSchemaResponse
}

// DifferentialConfig configures a differential test environment.
type DifferentialConfig struct {
	// Timeout for the entire test (default: 3 minutes)
	Timeout time.Duration
}

// NewDifferentialEnv creates and starts a differential testing environment.
// It starts both Go and Rust nodes, connects them via P2P, and sets up bidirectional replication.
// Call Close() when done (typically via defer).
func NewDifferentialEnv(t *testing.T, cfg DifferentialConfig) *DifferentialEnv {
	t.Helper()

	if cfg.Timeout == 0 {
		cfg.Timeout = 3 * time.Minute
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	t.Cleanup(cancel)

	env := &DifferentialEnv{
		t:   t,
		ctx: ctx,
	}

	// Reserve ports for both nodes
	rustPorts, err := ReserveNodePorts()
	require.NoError(t, err, "failed to reserve ports for Rust node")
	t.Cleanup(func() { rustPorts.Release() })

	goPorts, err := ReserveNodePorts()
	require.NoError(t, err, "failed to reserve ports for Go node")
	t.Cleanup(func() { goPorts.Release() })

	// Start Rust node
	env.rustNode = NewNode(NodeConfig{
		Type:         NodeTypeRust,
		HTTPPort:     rustPorts.HTTPPort,
		P2PPort:      rustPorts.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Rust node...")
	rustPorts.Release()
	require.NoError(t, env.rustNode.Start(ctx), "failed to start Rust node")
	t.Cleanup(func() { env.rustNode.Stop() })
	dumpLogsOnFailure(t, "rust-node", env.rustNode)
	t.Logf("Rust node started: %s", env.rustNode.PeerID())

	// Start Go node
	env.goNode = NewNode(NodeConfig{
		Type:         NodeTypeGo,
		HTTPPort:     goPorts.HTTPPort,
		P2PPort:      goPorts.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Go node...")
	goPorts.Release()
	require.NoError(t, env.goNode.Start(ctx), "failed to start Go node")
	t.Cleanup(func() { env.goNode.Stop() })
	dumpLogsOnFailure(t, "go-node", env.goNode)
	t.Logf("Go node started: %s", env.goNode.PeerID())

	env.rustClient = env.rustNode.Client()
	env.goClient = env.goNode.Client()

	// Connect nodes
	t.Log("Connecting Go node to Rust node...")
	err = env.goClient.ConnectPeer(ctx, env.rustNode.P2PMultiaddr())
	require.NoError(t, err, "failed to connect Go to Rust")

	err = WaitForPeerConnected(ctx, env.rustClient, env.goNode.PeerID(), 30*time.Second)
	require.NoError(t, err, "Rust node did not see Go node connect")
	t.Log("Nodes connected successfully")

	return env
}

// AddSchema adds a schema to both nodes and verifies they produce identical CollectionIDs.
// Returns the schema metadata (from Rust node, but both should be identical).
func (env *DifferentialEnv) AddSchema(sdl string) []AddSchemaResponse {
	env.t.Helper()

	env.t.Log("Adding schema to both nodes...")

	rustSchemas, err := env.rustClient.AddSchema(env.ctx, sdl)
	require.NoError(env.t, err, "failed to add schema to Rust node")

	goSchemas, err := env.goClient.AddSchema(env.ctx, sdl)
	require.NoError(env.t, err, "failed to add schema to Go node")

	require.Equal(env.t, len(rustSchemas), len(goSchemas),
		"schema count mismatch: Rust=%d, Go=%d", len(rustSchemas), len(goSchemas))

	// Build map of Go schemas by name for order-independent comparison
	goSchemaMap := make(map[string]AddSchemaResponse)
	for _, s := range goSchemas {
		goSchemaMap[s.Name] = s
	}

	// Verify CollectionIDs match for all collections (order-independent)
	for _, rustSchema := range rustSchemas {
		goSchema, ok := goSchemaMap[rustSchema.Name]
		require.True(env.t, ok, "schema %s found in Rust but not in Go", rustSchema.Name)
		require.Equal(env.t, rustSchema.CollectionID, goSchema.CollectionID,
			"CollectionID mismatch for %s: Rust=%s, Go=%s",
			rustSchema.Name, rustSchema.CollectionID, goSchema.CollectionID)
		env.t.Logf("Schema %s: CollectionID=%s (verified identical)", rustSchema.Name, rustSchema.CollectionID)
	}

	// Set up P2P collections and replication for each collection
	collectionNames := make([]string, len(rustSchemas))
	for i, s := range rustSchemas {
		collectionNames[i] = s.Name
	}

	// Add P2P collections to both nodes
	err = env.rustClient.AddP2PCollections(env.ctx, collectionNames)
	require.NoError(env.t, err, "failed to add P2P collections to Rust node")

	err = env.goClient.AddP2PCollections(env.ctx, collectionNames)
	require.NoError(env.t, err, "failed to add P2P collections to Go node")

	// Set up bidirectional replication
	err = env.rustClient.SetReplicator(env.ctx, []string{env.goNode.P2PMultiaddr()}, collectionNames)
	require.NoError(env.t, err, "failed to set Rust->Go replicator")

	err = env.goClient.SetReplicator(env.ctx, []string{env.rustNode.P2PMultiaddr()}, collectionNames)
	require.NoError(env.t, err, "failed to set Go->Rust replicator")

	env.t.Log("Schema added and replication configured")
	env.schemas = append(env.schemas, rustSchemas...)
	return rustSchemas
}

// CreateOnRust creates a document on the Rust node and waits for it to replicate to Go.
// Returns the document ID.
func (env *DifferentialEnv) CreateOnRust(mutation string, collection string) string {
	env.t.Helper()
	return env.createAndReplicate(env.rustClient, env.goClient, mutation, collection, "Rust", "Go")
}

// CreateOnGo creates a document on the Go node and waits for it to replicate to Rust.
// Returns the document ID.
func (env *DifferentialEnv) CreateOnGo(mutation string, collection string) string {
	env.t.Helper()
	return env.createAndReplicate(env.goClient, env.rustClient, mutation, collection, "Go", "Rust")
}

func (env *DifferentialEnv) createAndReplicate(
	sourceClient, targetClient *Client,
	mutation, collection, sourceName, targetName string,
) string {
	env.t.Helper()

	env.t.Logf("Creating document on %s node...", sourceName)
	resp, err := sourceClient.GraphQL(env.ctx, mutation, nil)
	require.NoError(env.t, err, "failed to execute mutation on %s", sourceName)
	require.Empty(env.t, resp.Errors, "mutation errors on %s: %v", sourceName, resp.Errors)

	// Extract docID from response
	docID := extractDocID(env.t, resp.Data)
	env.t.Logf("Created document %s on %s", docID, sourceName)

	// Wait for replication
	env.t.Logf("Waiting for replication to %s...", targetName)
	err = WaitForDocumentReplicated(env.ctx, targetClient, collection, docID, 30*time.Second)
	require.NoError(env.t, err, "document not replicated to %s", targetName)

	env.t.Logf("Document %s replicated to %s", docID, targetName)
	return docID
}

// QueryBoth executes the same query on both nodes and returns both responses.
func (env *DifferentialEnv) QueryBoth(query string) (rustResp, goResp *GraphQLResponse) {
	env.t.Helper()

	var err error
	rustResp, err = env.rustClient.GraphQL(env.ctx, query, nil)
	require.NoError(env.t, err, "failed to query Rust node")

	goResp, err = env.goClient.GraphQL(env.ctx, query, nil)
	require.NoError(env.t, err, "failed to query Go node")

	return rustResp, goResp
}

// CompareQueryResults runs the same query on both nodes and asserts they return equivalent results.
// Handles unordered comparison for collections.
func (env *DifferentialEnv) CompareQueryResults(query string) {
	env.t.Helper()

	rustResp, goResp := env.QueryBoth(query)

	// Check for errors
	require.Empty(env.t, rustResp.Errors, "Rust query errors: %v", rustResp.Errors)
	require.Empty(env.t, goResp.Errors, "Go query errors: %v", goResp.Errors)

	// Parse and compare data
	var rustData, goData map[string]any
	require.NoError(env.t, json.Unmarshal(rustResp.Data, &rustData), "failed to parse Rust response")
	require.NoError(env.t, json.Unmarshal(goResp.Data, &goData), "failed to parse Go response")

	diff := compareResults(rustData, goData)
	if diff != "" {
		env.t.Errorf("Query result mismatch:\nQuery: %s\n\nRust: %s\n\nGo: %s\n\nDiff: %s",
			query,
			prettyJSON(rustData),
			prettyJSON(goData),
			diff)
	}
}

// RustClient returns the Rust node's client for custom operations.
func (env *DifferentialEnv) RustClient() *Client {
	return env.rustClient
}

// GoClient returns the Go node's client for custom operations.
func (env *DifferentialEnv) GoClient() *Client {
	return env.goClient
}

// Context returns the test context.
func (env *DifferentialEnv) Context() context.Context {
	return env.ctx
}

// extractDocID extracts _docID from a GraphQL mutation response.
// Handles both single objects and arrays.
func extractDocID(t *testing.T, data json.RawMessage) string {
	t.Helper()

	var result map[string]any
	require.NoError(t, json.Unmarshal(data, &result), "failed to parse mutation response")

	// Find the first key (mutation name) and extract docID
	for _, v := range result {
		switch val := v.(type) {
		case []any:
			if len(val) > 0 {
				if doc, ok := val[0].(map[string]any); ok {
					if docID, ok := doc["_docID"].(string); ok {
						return docID
					}
				}
			}
		case map[string]any:
			if docID, ok := val["_docID"].(string); ok {
				return docID
			}
		}
	}

	t.Fatalf("no _docID found in response: %s", string(data))
	return ""
}

// compareResults compares two result maps, handling unordered arrays.
// Returns empty string if equal, otherwise returns a description of the difference.
func compareResults(rust, go_ map[string]any) string {
	return compareValues("", rust, go_)
}

func compareValues(path string, rust, go_ any) string {
	// Normalize nil vs empty
	if rust == nil && go_ == nil {
		return ""
	}

	rustType := reflect.TypeOf(rust)
	goType := reflect.TypeOf(go_)

	// Handle type mismatches
	if rustType != goType {
		// Special case: both numeric, compare values
		if isNumeric(rust) && isNumeric(go_) {
			if toFloat64(rust) != toFloat64(go_) {
				return fmt.Sprintf("%s: numeric value mismatch (Rust=%v, Go=%v)", path, rust, go_)
			}
			return ""
		}
		return fmt.Sprintf("%s: type mismatch (Rust=%T, Go=%T)", path, rust, go_)
	}

	switch rv := rust.(type) {
	case map[string]any:
		gv := go_.(map[string]any)
		return compareMaps(path, rv, gv)
	case []any:
		gv := go_.([]any)
		return compareArrays(path, rv, gv)
	default:
		if !reflect.DeepEqual(rust, go_) {
			return fmt.Sprintf("%s: value mismatch (Rust=%v, Go=%v)", path, rust, go_)
		}
		return ""
	}
}

func compareMaps(path string, rust, go_ map[string]any) string {
	// Check for missing/extra keys
	for k := range rust {
		if _, ok := go_[k]; !ok {
			return fmt.Sprintf("%s.%s: key only in Rust", path, k)
		}
	}
	for k := range go_ {
		if _, ok := rust[k]; !ok {
			return fmt.Sprintf("%s.%s: key only in Go", path, k)
		}
	}

	// Compare values
	for k, rv := range rust {
		childPath := k
		if path != "" {
			childPath = path + "." + k
		}
		if diff := compareValues(childPath, rv, go_[k]); diff != "" {
			return diff
		}
	}
	return ""
}

func compareArrays(path string, rust, go_ []any) string {
	if len(rust) != len(go_) {
		return fmt.Sprintf("%s: array length mismatch (Rust=%d, Go=%d)", path, len(rust), len(go_))
	}

	if len(rust) == 0 {
		return ""
	}

	// Try ordered comparison first
	allMatch := true
	for i := range rust {
		if compareValues(fmt.Sprintf("%s[%d]", path, i), rust[i], go_[i]) != "" {
			allMatch = false
			break
		}
	}
	if allMatch {
		return ""
	}

	// Fall back to unordered comparison (for collections)
	// Sort both by _docID if available, otherwise by JSON representation
	rustSorted := sortArrayForComparison(rust)
	goSorted := sortArrayForComparison(go_)

	for i := range rustSorted {
		if diff := compareValues(fmt.Sprintf("%s[%d]", path, i), rustSorted[i], goSorted[i]); diff != "" {
			return diff
		}
	}
	return ""
}

func sortArrayForComparison(arr []any) []any {
	result := make([]any, len(arr))
	copy(result, arr)

	sort.Slice(result, func(i, j int) bool {
		// Try to sort by _docID first
		iDoc, iOk := result[i].(map[string]any)
		jDoc, jOk := result[j].(map[string]any)

		if iOk && jOk {
			iID, _ := iDoc["_docID"].(string)
			jID, _ := jDoc["_docID"].(string)
			if iID != "" && jID != "" {
				return iID < jID
			}
		}

		// Fall back to JSON string comparison
		iJSON, _ := json.Marshal(result[i])
		jJSON, _ := json.Marshal(result[j])
		return string(iJSON) < string(jJSON)
	})

	return result
}

func isNumeric(v any) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return true
	}
	return false
}

func toFloat64(v any) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case float64:
		return n
	case float32:
		return float64(n)
	}
	return 0
}

func prettyJSON(v any) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}

// dumpLogsOnFailure is a helper that dumps node logs if the test fails.
func dumpLogsOnFailure(t *testing.T, name string, node *Node) {
	t.Helper()
	t.Cleanup(func() {
		if t.Failed() {
			logs, _ := node.DumpLogsString()
			t.Logf("=== %s logs ===\n%s", name, logs)
		}
	})
}
