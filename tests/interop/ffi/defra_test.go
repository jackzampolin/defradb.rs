package ffi

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	// Should not panic
	Init()
	// Should be idempotent
	Init()
	Init()
}

func TestVersion(t *testing.T) {
	Init()

	version := Version()
	assert.NotEmpty(t, version)
	assert.True(t, strings.HasPrefix(version, "0."), "version should start with 0.")
}

func TestNewNodeAndClose(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	require.NotNil(t, node)

	err = node.Close()
	require.NoError(t, err)
}

func TestNodeDoubleClose(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)

	err = node.Close()
	require.NoError(t, err)

	// Second close should fail
	err = node.Close()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid")
}

func TestAddSchema(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add a simple schema
	sdl := "type User { name: String, age: Int }"
	result, err := node.AddSchema(sdl)
	require.NoError(t, err)
	assert.NotEmpty(t, result)

	// Result should be valid JSON
	var collections []interface{}
	err = json.Unmarshal([]byte(result), &collections)
	require.NoError(t, err)
	assert.NotEmpty(t, collections)
}

func TestGetCollections(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema first
	_, err = node.AddSchema("type Person { name: String }")
	require.NoError(t, err)

	// Get collections
	result, err := node.GetCollections()
	require.NoError(t, err)
	assert.Contains(t, result, "Person")
}

func TestQuery(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type User { name: String }")
	require.NoError(t, err)

	// Query empty collection
	result, err := node.Query("{ User { name } }")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.NotNil(t, result.Data)
	assert.Empty(t, result.Errors)
}

func TestMutation(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type User { name: String, age: Int }")
	require.NoError(t, err)

	// Create a document
	result, err := node.Mutate(`mutation { create_User(input: {name: "Alice", age: 30}) { _docID name age } }`)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Errors)

	// Verify the data contains Alice
	var data map[string]interface{}
	err = json.Unmarshal(result.Data, &data)
	require.NoError(t, err)

	// create_User returns an array of created documents
	createUserArr := data["create_User"].([]interface{})
	require.Len(t, createUserArr, 1)
	createUser := createUserArr[0].(map[string]interface{})
	assert.Equal(t, "Alice", createUser["name"])
	assert.Equal(t, float64(30), createUser["age"])
}

func TestQueryAfterMutation(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type Book { title: String, author: String }")
	require.NoError(t, err)

	// Create documents
	_, err = node.Mutate(`mutation { create_Book(input: {title: "1984", author: "Orwell"}) { _docID } }`)
	require.NoError(t, err)

	_, err = node.Mutate(`mutation { create_Book(input: {title: "Dune", author: "Herbert"}) { _docID } }`)
	require.NoError(t, err)

	// Query all books
	result, err := node.Query("{ Book { title author } }")
	require.NoError(t, err)
	require.Empty(t, result.Errors)

	var data map[string]interface{}
	err = json.Unmarshal(result.Data, &data)
	require.NoError(t, err)

	books := data["Book"].([]interface{})
	assert.Len(t, books, 2)
}

func TestExecRequestRaw(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type Item { name: String }")
	require.NoError(t, err)

	// Test raw exec request returns valid JSON
	responseJSON, err := node.ExecRequest(
		`mutation { create_Item(input: {name: "RawTest"}) { _docID name } }`,
		"",
		"",
	)
	require.NoError(t, err)
	assert.Contains(t, responseJSON, "RawTest")
	assert.Contains(t, responseJSON, "data")
}

func TestInvalidQuery(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Query without schema should return error in response
	result, err := node.Query("{ NonExistent { field } }")
	require.NoError(t, err) // FFI call succeeds
	require.NotNil(t, result)
	// But response should contain errors
	assert.NotEmpty(t, result.Errors)
}

func TestMultipleNodes(t *testing.T) {
	Init()

	// Create two nodes
	node1, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)

	node2, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)

	// Add different schemas
	_, err = node1.AddSchema("type Foo { x: Int }")
	require.NoError(t, err)

	_, err = node2.AddSchema("type Bar { y: String }")
	require.NoError(t, err)

	// Each node should only see its own collections
	cols1, err := node1.GetCollections()
	require.NoError(t, err)
	assert.Contains(t, cols1, "Foo")
	assert.NotContains(t, cols1, "Bar")

	cols2, err := node2.GetCollections()
	require.NoError(t, err)
	assert.Contains(t, cols2, "Bar")
	assert.NotContains(t, cols2, "Foo")

	// Clean up
	require.NoError(t, node1.Close())
	require.NoError(t, node2.Close())
}
