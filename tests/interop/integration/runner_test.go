// Package integration provides integration tests that run DefraDB test patterns
// against the Rust FFI implementation.
package integration

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	defraclient "github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb.rs-interop/tests/interop/ffi"
)

func init() {
	// Initialize the FFI library
	ffi.Init()
}

// TestSimpleQuery tests basic schema creation, document insertion, and querying.
func TestSimpleQuery(t *testing.T) {
	ctx := context.Background()

	// Create an in-memory FFI node
	node, err := ffi.NewNode(ffi.NodeOptions{InMemory: true})
	require.NoError(t, err, "failed to create node")
	defer node.Close()

	// Wrap in client interface
	client := ffi.NewClientWrapper(node)
	defer client.Close()

	// Add schema
	schema := `
		type User {
			name: String
			age: Int
		}
	`
	versions, err := client.AddSchema(ctx, schema)
	require.NoError(t, err, "failed to add schema")
	require.NotEmpty(t, versions, "expected at least one collection version")

	t.Logf("Created collection: %s (version: %s)", versions[0].Name, versions[0].VersionID)

	// Create a document via GraphQL mutation
	createMutation := `mutation {
		create_User(input: {name: "Alice", age: 30}) {
			_docID
			name
			age
		}
	}`

	result := client.ExecRequest(ctx, createMutation)
	require.Empty(t, result.GQL.Errors, "create mutation failed: %v", result.GQL.Errors)
	require.NotNil(t, result.GQL.Data, "expected data in response")

	// Extract docID from create result
	data, ok := result.GQL.Data.(map[string]any)
	require.True(t, ok, "expected map data")

	// create_User returns an array
	createResults, ok := data["create_User"].([]any)
	require.True(t, ok, "expected create_User array in result")
	require.Len(t, createResults, 1, "expected one result")

	createResult, ok := createResults[0].(map[string]any)
	require.True(t, ok, "expected map in create result")
	docID, ok := createResult["_docID"].(string)
	require.True(t, ok, "expected _docID in create result")
	t.Logf("Created document with ID: %s", docID)

	// Query the document
	query := `{
		User {
			_docID
			name
			age
		}
	}`

	result = client.ExecRequest(ctx, query)
	require.Empty(t, result.GQL.Errors, "query failed: %v", result.GQL.Errors)
	require.NotNil(t, result.GQL.Data, "expected data in response")

	// Verify query results
	data, ok = result.GQL.Data.(map[string]any)
	require.True(t, ok, "expected map data")
	users, ok := data["User"].([]any)
	require.True(t, ok, "expected User array in result")
	require.Len(t, users, 1, "expected one user")

	user, ok := users[0].(map[string]any)
	require.True(t, ok, "expected user map")

	assert.Equal(t, "Alice", user["name"], "name mismatch")
	// Age might be returned as json.Number or float64
	switch v := user["age"].(type) {
	case json.Number:
		age, _ := v.Int64()
		assert.Equal(t, int64(30), age, "age mismatch")
	case float64:
		assert.Equal(t, float64(30), v, "age mismatch")
	default:
		t.Fatalf("unexpected age type: %T", v)
	}

	t.Log("Simple query test passed!")
}

// TestQueryWithFilter tests filtering documents.
func TestQueryWithFilter(t *testing.T) {
	ctx := context.Background()

	node, err := ffi.NewNode(ffi.NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	client := ffi.NewClientWrapper(node)
	defer client.Close()

	// Add schema
	schema := `
		type User {
			name: String
			age: Int
		}
	`
	_, err = client.AddSchema(ctx, schema)
	require.NoError(t, err)

	// Create multiple documents
	users := []struct {
		name string
		age  int
	}{
		{"Alice", 30},
		{"Bob", 25},
		{"Charlie", 35},
	}

	for _, u := range users {
		mutation := `mutation {
			create_User(input: {name: "` + u.name + `", age: ` + string(rune('0'+u.age/10)) + string(rune('0'+u.age%10)) + `}) {
				_docID
			}
		}`
		result := client.ExecRequest(ctx, mutation)
		require.Empty(t, result.GQL.Errors, "failed to create user %s", u.name)
	}

	// Query with filter
	query := `{
		User(filter: {age: {_gt: 28}}) {
			name
			age
		}
	}`

	result := client.ExecRequest(ctx, query)
	require.Empty(t, result.GQL.Errors, "query failed: %v", result.GQL.Errors)

	data, ok := result.GQL.Data.(map[string]any)
	require.True(t, ok)
	filteredUsers, ok := data["User"].([]any)
	require.True(t, ok)

	// Should have Alice (30) and Charlie (35)
	assert.Len(t, filteredUsers, 2, "expected 2 users with age > 28")

	t.Log("Query with filter test passed!")
}

// TestRelationship tests basic relationship queries.
func TestRelationship(t *testing.T) {
	ctx := context.Background()

	node, err := ffi.NewNode(ffi.NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	client := ffi.NewClientWrapper(node)
	defer client.Close()

	// Add schema with relationship
	schema := `
		type Author {
			name: String
			books: [Book]
		}
		type Book {
			title: String
			author: Author
		}
	`
	_, err = client.AddSchema(ctx, schema)
	require.NoError(t, err, "failed to add schema")

	// Create an author
	createAuthor := `mutation {
		create_Author(input: {name: "Jane Doe"}) {
			_docID
			name
		}
	}`
	result := client.ExecRequest(ctx, createAuthor)
	require.Empty(t, result.GQL.Errors, "failed to create author: %v", result.GQL.Errors)

	data, ok := result.GQL.Data.(map[string]any)
	require.True(t, ok)
	authorResults, ok := data["create_Author"].([]any)
	require.True(t, ok, "expected create_Author array")
	require.Len(t, authorResults, 1)
	authorResult, ok := authorResults[0].(map[string]any)
	require.True(t, ok)
	authorID, ok := authorResult["_docID"].(string)
	require.True(t, ok)

	// Create a book linked to the author
	createBook := `mutation {
		create_Book(input: {title: "Great Adventures", author_id: "` + authorID + `"}) {
			_docID
			title
		}
	}`
	result = client.ExecRequest(ctx, createBook)
	require.Empty(t, result.GQL.Errors, "failed to create book: %v", result.GQL.Errors)

	// Query author with books
	query := `{
		Author {
			name
			books {
				title
			}
		}
	}`
	result = client.ExecRequest(ctx, query)
	require.Empty(t, result.GQL.Errors, "query failed: %v", result.GQL.Errors)

	data, ok = result.GQL.Data.(map[string]any)
	require.True(t, ok)
	authors, ok := data["Author"].([]any)
	require.True(t, ok)
	require.Len(t, authors, 1)

	author, ok := authors[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Jane Doe", author["name"])

	books, ok := author["books"].([]any)
	require.True(t, ok)
	require.Len(t, books, 1)

	book, ok := books[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Great Adventures", book["title"])

	t.Log("Relationship test passed!")
}

// TestTransaction tests basic transaction support.
func TestTransaction(t *testing.T) {
	ctx := context.Background()

	node, err := ffi.NewNode(ffi.NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	client := ffi.NewClientWrapper(node)
	defer client.Close()

	// Add schema
	schema := `
		type Counter {
			value: Int
		}
	`
	_, err = client.AddSchema(ctx, schema)
	require.NoError(t, err)

	// Start a transaction
	txn, err := client.NewTxn(false)
	require.NoError(t, err, "failed to create transaction")

	// Create document in transaction
	mutation := `mutation {
		create_Counter(input: {value: 100}) {
			_docID
			value
		}
	}`
	result := txn.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors, "create in txn failed: %v", result.GQL.Errors)

	// Query before commit - document should be visible in transaction
	query := `{ Counter { value } }`
	result = txn.ExecRequest(ctx, query)
	require.Empty(t, result.GQL.Errors)

	// Commit transaction
	err = txn.Commit()
	require.NoError(t, err, "failed to commit transaction")

	// Query after commit - document should be visible outside transaction
	result = client.ExecRequest(ctx, query)
	require.Empty(t, result.GQL.Errors)

	data, ok := result.GQL.Data.(map[string]any)
	require.True(t, ok)
	counters, ok := data["Counter"].([]any)
	require.True(t, ok)
	require.Len(t, counters, 1)

	t.Log("Transaction test passed!")
}

// TestIndex tests index creation and usage.
func TestIndex(t *testing.T) {
	ctx := context.Background()

	node, err := ffi.NewNode(ffi.NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	client := ffi.NewClientWrapper(node)
	defer client.Close()

	// Add schema
	schema := `
		type Product {
			name: String
			price: Float
		}
	`
	_, err = client.AddSchema(ctx, schema)
	require.NoError(t, err)

	// Get collection
	collection, err := client.GetCollectionByName(ctx, "Product")
	require.NoError(t, err)

	// Create an index on price
	indexDesc, err := collection.CreateIndex(ctx, defraclient.IndexCreateRequest{
		Name: "price_idx",
		Fields: []defraclient.IndexedFieldDescription{
			{Name: "price", Descending: false},
		},
		Unique: false,
	})
	require.NoError(t, err, "failed to create index")
	t.Logf("Created index: %s (ID: %d)", indexDesc.Name, indexDesc.ID)

	// Verify index exists
	indexes, err := collection.GetIndexes(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, indexes, "expected at least one index")

	found := false
	for _, idx := range indexes {
		if idx.Name == "price_idx" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected to find price_idx")

	t.Log("Index test passed!")
}
