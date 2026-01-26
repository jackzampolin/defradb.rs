// Package integration tests DefraDB mutation patterns against the Rust FFI implementation.
//
// These tests are ported from DefraDB's tests/integration/mutation/ directory
// and validate behavioral compatibility between Go and Rust implementations.
package integration

import (
	"testing"

	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

// =============================================================================
// CREATE MUTATIONS
// =============================================================================

// TestMutationCreate tests basic document creation.
// Ported from: tests/integration/mutation/create/simple_test.go
func TestMutationCreate(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
						age: Int
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"age": 27
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
						age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "John",
							"age":  int64(27),
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationCreate_WithMultipleFields tests creating a document with various field types.
func TestMutationCreate_WithMultipleFields(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
						age: Int
						points: Float
						verified: Boolean
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Alice",
					"age": 30,
					"points": 99.5,
					"verified": true
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
						age
						points
						verified
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":     "Alice",
							"age":      int64(30),
							"points":   99.5,
							"verified": true,
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationCreate_WithNullFields tests creating a document with null/missing fields.
func TestMutationCreate_WithNullFields(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
						age: Int
						email: String
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Bob"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
						age
						email
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":  "Bob",
							"age":   nil,
							"email": nil,
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationCreate_MultipleDocuments tests creating multiple documents.
func TestMutationCreate_MultipleDocuments(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
						age: Int
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"age": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Bob",
					"age": 32
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Alice",
					"age": 28
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
						age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "John", "age": int64(21)},
						{"name": "Bob", "age": int64(32)},
						{"name": "Alice", "age": int64(28)},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationCreate_WithEmptyInput tests creating a document with empty input.
func TestMutationCreate_WithEmptyInput(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
					}
				`,
			},
			// Create document with empty JSON object
			testUtils.CreateDoc{
				Doc: `{}`,
			},
			// Verify the document was created by querying
			testUtils.Request{
				Request: `query {
					Users {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": nil,
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationCreate_WithRelationship tests creating documents with relationships.
func TestMutationCreate_WithRelationship(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Author {
						name: String
						books: [Book]
					}
					type Book {
						title: String
						author: Author
					}
				`,
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Author
				DocMap: map[string]any{
					"name": "Jane Austen",
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1, // Book
				DocMap: map[string]any{
					"title":     "Pride and Prejudice",
					"author_id": testUtils.DocIndex{CollectionIndex: 0, Index: 0},
				},
			},
			testUtils.Request{
				Request: `query {
					Author {
						name
						books {
							title
						}
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name": "Jane Austen",
							"books": []map[string]any{
								{"title": "Pride and Prejudice"},
							},
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// =============================================================================
// UPDATE MUTATIONS
// =============================================================================

// TestMutationUpdate_WithDocID tests updating a document by ID.
// Ported from: tests/integration/mutation/update/with_id_test.go
func TestMutationUpdate_WithDocID(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
						points: Float
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"points": 42.1
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Bob",
					"points": 66.6
				}`,
			},
			testUtils.UpdateDoc{
				CollectionID: 0,
				DocID:        0, // John
				Doc:          `{points: 59.0}`,
			},
			testUtils.Request{
				Request: `query {
					Users(order: {name: ASC}) {
						name
						points
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":   "Bob",
							"points": 66.6,
						},
						{
							"name":   "John",
							"points": 59.0,
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationUpdate_MultipleFields tests updating multiple fields at once.
func TestMutationUpdate_MultipleFields(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
						age: Int
						verified: Boolean
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Alice",
					"age": 25,
					"verified": false
				}`,
			},
			testUtils.UpdateDoc{
				CollectionID: 0,
				DocID:        0,
				Doc:          `{age: 26, verified: true}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
						age
						verified
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":     "Alice",
							"age":      int64(26),
							"verified": true,
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationUpdate_StringField tests updating a string field.
func TestMutationUpdate_StringField(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
						email: String
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"email": "john@old.com"
				}`,
			},
			testUtils.UpdateDoc{
				CollectionID: 0,
				DocID:        0,
				Doc:          `{email: "john@new.com"}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
						email
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":  "John",
							"email": "john@new.com",
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationUpdate_IntField tests updating an integer field.
func TestMutationUpdate_IntField(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Counter {
						name: String
						value: Int
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "clicks",
					"value": 100
				}`,
			},
			testUtils.UpdateDoc{
				CollectionID: 0,
				DocID:        0,
				Doc:          `{value: 150}`,
			},
			testUtils.Request{
				Request: `query {
					Counter {
						name
						value
					}
				}`,
				Results: map[string]any{
					"Counter": []map[string]any{
						{
							"name":  "clicks",
							"value": int64(150),
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationUpdate_FloatField tests updating a float field.
func TestMutationUpdate_FloatField(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Measurement {
						name: String
						value: Float
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "temperature",
					"value": 22.5
				}`,
			},
			testUtils.UpdateDoc{
				CollectionID: 0,
				DocID:        0,
				Doc:          `{value: 23.7}`,
			},
			testUtils.Request{
				Request: `query {
					Measurement {
						name
						value
					}
				}`,
				Results: map[string]any{
					"Measurement": []map[string]any{
						{
							"name":  "temperature",
							"value": 23.7,
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationUpdate_BoolField tests updating a boolean field.
func TestMutationUpdate_BoolField(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Task {
						title: String
						completed: Boolean
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"title": "Write tests",
					"completed": false
				}`,
			},
			testUtils.UpdateDoc{
				CollectionID: 0,
				DocID:        0,
				Doc:          `{completed: true}`,
			},
			testUtils.Request{
				Request: `query {
					Task {
						title
						completed
					}
				}`,
				Results: map[string]any{
					"Task": []map[string]any{
						{
							"title":     "Write tests",
							"completed": true,
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationUpdate_SetFieldToNull tests setting a field to null.
func TestMutationUpdate_SetFieldToNull(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
						nickname: String
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"nickname": "Johnny"
				}`,
			},
			testUtils.UpdateDoc{
				CollectionID: 0,
				DocID:        0,
				Doc:          `{nickname: null}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
						nickname
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":     "John",
							"nickname": nil,
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// =============================================================================
// DELETE MUTATIONS
// =============================================================================

// TestMutationDelete_WithDocID tests deleting a document by ID.
// Ported from: tests/integration/mutation/delete/with_id_test.go
func TestMutationDelete_WithDocID(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "John"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Bob"
				}`,
			},
			testUtils.DeleteDoc{
				CollectionID: 0,
				DocID:        0, // Delete John
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Bob"},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationDelete_AllDocuments tests deleting all documents.
func TestMutationDelete_AllDocuments(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "John"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Bob"
				}`,
			},
			testUtils.DeleteDoc{
				CollectionID: 0,
				DocID:        0,
			},
			testUtils.DeleteDoc{
				CollectionID: 0,
				DocID:        1,
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationDelete_FromMultipleCollections tests that deleting from one collection
// doesn't affect another collection.
func TestMutationDelete_FromMultipleCollections(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String
					}
					type Product {
						name: String
					}
				`,
			},
			testUtils.CreateDoc{
				CollectionID: 0, // User
				DocMap: map[string]any{
					"name": "John",
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1, // Product
				DocMap: map[string]any{
					"name": "Widget",
				},
			},
			testUtils.DeleteDoc{
				CollectionID: 1, // Delete Product
				DocID:        0,
			},
			// User should still exist
			testUtils.Request{
				Request: `query {
					User {
						name
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "John"},
					},
				},
			},
			// Product should be empty
			testUtils.Request{
				Request: `query {
					Product {
						name
					}
				}`,
				Results: map[string]any{
					"Product": []map[string]any{},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationDelete_ThenCreate tests creating a new document after deletion.
func TestMutationDelete_ThenCreate(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "John"
				}`,
			},
			testUtils.DeleteDoc{
				CollectionID: 0,
				DocID:        0,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Alice"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Alice"},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// =============================================================================
// UPSERT MUTATIONS (via GraphQL)
// Go DefraDB upsert syntax: filter, create, update (all required)
// =============================================================================

// TestMutationUpsert_CreateNew tests upsert creating a new document when no match.
// Ported from: tests/integration/mutation/upsert/simple_test.go
func TestMutationUpsert_CreateNew(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
						age: Int
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Alice",
					"age": 40
				}`,
			},
			testUtils.Request{
				Request: `mutation {
					upsert_Users(
						filter: {name: {_eq: "Bob"}},
						create: {name: "Bob", age: 40},
						update: {age: 40}
					) {
						name
						age
					}
				}`,
				Results: map[string]any{
					"upsert_Users": []map[string]any{
						{
							"name": "Bob",
							"age":  int64(40),
						},
					},
				},
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
						age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Alice", "age": int64(40)},
						{"name": "Bob", "age": int64(40)},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationUpsert_UpdateExisting tests upsert updating when a match exists.
func TestMutationUpsert_UpdateExisting(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
						age: Int
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Alice",
					"age": 40
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Bob",
					"age": 30
				}`,
			},
			testUtils.Request{
				Request: `mutation {
					upsert_Users(
						filter: {name: {_eq: "Bob"}},
						create: {name: "Bob", age: 40},
						update: {age: 40}
					) {
						name
						age
					}
				}`,
				Results: map[string]any{
					"upsert_Users": []map[string]any{
						{
							"name": "Bob",
							"age":  int64(40),
						},
					},
				},
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
						age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Alice", "age": int64(40)},
						{"name": "Bob", "age": int64(40)},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutationUpsert_MultipleMatches_Error tests that upsert fails when multiple docs match.
func TestMutationUpsert_MultipleMatches_Error(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
						age: Int
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Bob",
					"age": 30
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Alice",
					"age": 40
				}`,
			},
			testUtils.Request{
				Request: `mutation {
					upsert_Users(
						filter: {},
						create: {name: "Alice", age: 40},
						update: {age: 50}
					) {
						name
						age
					}
				}`,
				ExpectedError: "cannot upsert multiple matching documents",
			},
		},
	}

	ExecuteTestCase(t, test)
}

// =============================================================================
// COMBINED MUTATIONS
// =============================================================================

// TestMutation_CreateUpdateDelete tests a full lifecycle of create, update, delete.
func TestMutation_CreateUpdateDelete(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Task {
						title: String
						status: String
					}
				`,
			},
			// Create
			testUtils.CreateDoc{
				Doc: `{
					"title": "Write documentation",
					"status": "pending"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Task {
						title
						status
					}
				}`,
				Results: map[string]any{
					"Task": []map[string]any{
						{"title": "Write documentation", "status": "pending"},
					},
				},
			},
			// Update
			testUtils.UpdateDoc{
				CollectionID: 0,
				DocID:        0,
				Doc:          `{status: "completed"}`,
			},
			testUtils.Request{
				Request: `query {
					Task {
						title
						status
					}
				}`,
				Results: map[string]any{
					"Task": []map[string]any{
						{"title": "Write documentation", "status": "completed"},
					},
				},
			},
			// Delete
			testUtils.DeleteDoc{
				CollectionID: 0,
				DocID:        0,
			},
			testUtils.Request{
				Request: `query {
					Task {
						title
					}
				}`,
				Results: map[string]any{
					"Task": []map[string]any{},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestMutation_WithRelationships tests mutations involving relationships.
func TestMutation_WithRelationships(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Author {
						name: String
						books: [Book]
					}
					type Book {
						title: String
						author: Author
					}
				`,
			},
			// Create author
			testUtils.CreateDoc{
				CollectionID: 0, // Author
				DocMap: map[string]any{
					"name": "George Orwell",
				},
			},
			// Create first book
			testUtils.CreateDoc{
				CollectionID: 1, // Book
				DocMap: map[string]any{
					"title":     "1984",
					"author_id": testUtils.DocIndex{CollectionIndex: 0, Index: 0},
				},
			},
			// Create second book
			testUtils.CreateDoc{
				CollectionID: 1, // Book
				DocMap: map[string]any{
					"title":     "Animal Farm",
					"author_id": testUtils.DocIndex{CollectionIndex: 0, Index: 0},
				},
			},
			// Query author with books
			testUtils.Request{
				Request: `query {
					Author {
						name
						books {
							title
						}
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name": "George Orwell",
							"books": []map[string]any{
								{"title": "1984"},
								{"title": "Animal Farm"},
							},
						},
					},
				},
				NonOrderedResults: true,
			},
			// Delete one book
			testUtils.DeleteDoc{
				CollectionID: 1,
				DocID:        0, // Delete 1984
			},
			// Query should show remaining book
			testUtils.Request{
				Request: `query {
					Author {
						name
						books {
							title
						}
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name": "George Orwell",
							"books": []map[string]any{
								{"title": "Animal Farm"},
							},
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}
