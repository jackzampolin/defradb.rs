package ffi

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetCollectionByName(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type User { name: String, age: Int }")
	require.NoError(t, err)

	// Get collection by name
	result, err := node.GetCollectionByName("User")
	require.NoError(t, err)
	assert.Contains(t, result, "User")
	assert.Contains(t, result, "name")

	// Verify it's valid JSON
	var collection map[string]interface{}
	err = json.Unmarshal([]byte(result), &collection)
	require.NoError(t, err)
	assert.Equal(t, "User", collection["Name"])
}

func TestGetCollectionByNameNotFound(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Get non-existent collection
	_, err = node.GetCollectionByName("NonExistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestHasCollection(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type Person { name: String }")
	require.NoError(t, err)

	// Check existing collection
	exists, err := node.HasCollection("Person")
	require.NoError(t, err)
	assert.True(t, exists)

	// Check non-existing collection
	exists, err = node.HasCollection("NonExistent")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestDeleteCollection(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type ToDelete { field: String }")
	require.NoError(t, err)

	// Verify it exists
	exists, err := node.HasCollection("ToDelete")
	require.NoError(t, err)
	assert.True(t, exists)

	// Delete it
	err = node.DeleteCollection("ToDelete")
	require.NoError(t, err)

	// Verify it's gone
	exists, err = node.HasCollection("ToDelete")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestDeleteCollectionNotFound(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Delete non-existent collection should fail
	err = node.DeleteCollection("NonExistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestFindCollectionByID(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema and get the collection ID
	result, err := node.AddSchema("type FindMe { data: String }")
	require.NoError(t, err)

	var collections []map[string]interface{}
	err = json.Unmarshal([]byte(result), &collections)
	require.NoError(t, err)
	require.Len(t, collections, 1)

	collectionID := collections[0]["CollectionID"].(string)

	// Find by ID
	found, err := node.FindCollectionByID(collectionID)
	require.NoError(t, err)
	assert.Contains(t, found, "FindMe")

	// Verify it's valid JSON
	var collection map[string]interface{}
	err = json.Unmarshal([]byte(found), &collection)
	require.NoError(t, err)
	assert.Equal(t, "FindMe", collection["Name"])
}

func TestFindCollectionByIDNotFound(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Find non-existent ID
	result, err := node.FindCollectionByID("bafkreibnonexistent")
	require.NoError(t, err)
	assert.Equal(t, "null", result)
}

func TestDeleteCollectionWithDocuments(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema and create documents
	_, err = node.AddSchema("type Item { name: String }")
	require.NoError(t, err)

	_, err = node.Mutate(`mutation { create_Item(input: {name: "One"}) { _docID } }`)
	require.NoError(t, err)

	_, err = node.Mutate(`mutation { create_Item(input: {name: "Two"}) { _docID } }`)
	require.NoError(t, err)

	// Verify documents exist
	result, err := node.Query("{ Item { name } }")
	require.NoError(t, err)
	var data map[string]interface{}
	err = json.Unmarshal(result.Data, &data)
	require.NoError(t, err)
	items := data["Item"].([]interface{})
	assert.Len(t, items, 2)

	// Delete collection
	err = node.DeleteCollection("Item")
	require.NoError(t, err)

	// Collection should be gone
	exists, err := node.HasCollection("Item")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestSetActiveCollectionVersion(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema
	result, err := node.AddSchema("type VersionedCollection { data: String }")
	require.NoError(t, err)

	// Extract version ID
	var collections []map[string]interface{}
	err = json.Unmarshal([]byte(result), &collections)
	require.NoError(t, err)
	require.Len(t, collections, 1)

	versionID := collections[0]["VersionID"].(string)

	// Set active version (should succeed even for already-active version)
	err = node.SetActiveCollectionVersion(versionID)
	require.NoError(t, err)
}

func TestSetActiveCollectionVersionNotFound(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Try to set non-existent version
	err = node.SetActiveCollectionVersion("nonexistent-version")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestPatchCollection(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type Patchable { original: String }")
	require.NoError(t, err)

	// Patch the collection - change is_active to false
	patch := `[{"op":"replace","path":"/IsActive","value":false}]`
	result, err := node.PatchCollection("Patchable", patch)
	require.NoError(t, err)
	assert.Contains(t, result, "Patchable")
	assert.Contains(t, result, `"IsActive":false`)
}

func TestPatchCollectionNotFound(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	patch := `[{"op":"replace","path":"/IsActive","value":false}]`
	_, err = node.PatchCollection("NonExistent", patch)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestPatchCollectionInvalidPatch(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type PatchTest { field: String }")
	require.NoError(t, err)

	// Invalid patch
	_, err = node.PatchCollection("PatchTest", "not valid json")
	assert.Error(t, err)
}

func TestGetCollectionByVersionID(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema
	result, err := node.AddSchema("type VersionTest { field: String }")
	require.NoError(t, err)

	// Extract version ID
	var collections []map[string]interface{}
	err = json.Unmarshal([]byte(result), &collections)
	require.NoError(t, err)
	require.Len(t, collections, 1)

	versionID := collections[0]["VersionID"].(string)

	// Get by version ID
	found, err := node.GetCollectionByVersionID(versionID)
	require.NoError(t, err)
	assert.Contains(t, found, "VersionTest")
}

func TestGetCollectionByVersionIDNotFound(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Get non-existent version
	result, err := node.GetCollectionByVersionID("nonexistent")
	require.NoError(t, err)
	assert.Equal(t, "null", result)
}

// Tests for unimplemented APIs - verify they return appropriate errors

func TestAddViewNotImplemented(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddView("{ User { name } }", "type UserView { name: String }", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not yet implemented")
}

func TestRefreshViewsNotImplemented(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	err = node.RefreshViews("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not yet implemented")
}

func TestSetMigration_InvalidConfig(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Test with malformed JSON - missing required fields
	_, err = node.SetMigration(`{"source": "v1", "destination": "v2"}`)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse lens config")
}

func TestSetMigration_ValidConfig(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// First create a schema
	sdl := "type User { name: String }"
	_, err = node.AddSchema(sdl)
	require.NoError(t, err)

	// Patch the collection to create a new version (add a field)
	patchJSON := `[{"op": "add", "path": "/User/Fields/-", "value": {"Name": "verified", "Kind": "Boolean"}}]`
	_, err = node.PatchCollection("User", patchJSON)
	require.NoError(t, err)

	// Get the collection versions to get actual version IDs
	collectionsJSON, err := node.GetCollections()
	require.NoError(t, err)
	t.Logf("Collections after patch: %s", collectionsJSON)

	// Set a migration with valid config format
	// Note: This will fail if there's no WASM module at the path, but it should
	// parse successfully and attempt to load the module
	lensConfig := `{
		"SourceSchemaVersionID": "bafyreiciz2hrrmt7ritk5gf5fyruw46v2tfhq5dc7qto4wgpzluben2smu",
		"DestinationSchemaVersionID": "bafyreigqfjat435ghyt66tdaucp7oi2mke5jafx3jw3rozanopihr2vf44",
		"Lens": {
			"Path": "/path/to/nonexistent/transform.wasm"
		}
	}`

	_, err = node.SetMigration(lensConfig)
	// We expect an error because the WASM file doesn't exist, but it should NOT be
	// a "not yet implemented" error - it should be a file loading error
	if err != nil {
		assert.NotContains(t, err.Error(), "not yet implemented", "SetMigration should be implemented")
		// The error should be about loading the WASM module
		t.Logf("SetMigration error (expected for missing WASM): %s", err.Error())
	}
}
