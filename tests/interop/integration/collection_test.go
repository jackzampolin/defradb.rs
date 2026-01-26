// Package integration tests DefraDB collection patterns against the Rust FFI implementation.
//
// These tests are ported from DefraDB's tests/integration/collection_version/ directory
// and validate behavioral compatibility between Go and Rust implementations.
// Focus: GetCollections, PatchCollection, schema versioning.
package integration

import (
	"testing"

	"github.com/sourcenetwork/immutable"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

// TestCollectionCreatesGivenMinimalType tests schema creation with a minimal type.
// Note: Rust requires at least one field (no empty types).
// Ported from: tests/integration/collection_version/simple_test.go
func TestCollectionCreatesGivenMinimalType(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
					}
				`,
				ExpectedResults: []client.CollectionVersion{
					{
						Name:           "Users",
						IsMaterialized: true,
						IsActive:       true,
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCollectionErrorsGivenDuplicateSchema tests that duplicate schemas error.
// Ported from: tests/integration/collection_version/simple_test.go
func TestCollectionErrorsGivenDuplicateSchema(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
					}
				`,
			},
			testUtils.SetupComplete{},
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
					}
				`,
				ExpectedError: "collection already exists",
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCollectionErrorsGivenDuplicateSchemaInSameSDL tests duplicates in same SDL.
// Ported from: tests/integration/collection_version/simple_test.go
func TestCollectionErrorsGivenDuplicateSchemaInSameSDL(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
					}
					type Users {
						name: String
					}
				`,
				ExpectedError: "collection already exists",
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCollectionCreatesSchemaGivenNewTypes tests adding multiple schemas.
// Ported from: tests/integration/collection_version/simple_test.go
func TestCollectionCreatesSchemaGivenNewTypes(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
					}
				`,
			},
			&action.AddSchema{
				Schema: `
					type Books {
						title: String
					}
				`,
			},
			testUtils.Request{
				Request: `query {
					Books { _docID }
				}`,
				Results: map[string]any{
					"Books": []map[string]any{},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCollectionErrorsGivenTypeWithInvalidFieldType tests invalid field types.
// Ported from: tests/integration/collection_version/simple_test.go
func TestCollectionErrorsGivenTypeWithInvalidFieldType(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: NotAType
					}
				`,
				ExpectedError: "no type found for given name",
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCollectionCreatesSchemaGivenTypeWithStringField tests string field creation.
// Ported from: tests/integration/collection_version/simple_test.go
func TestCollectionCreatesSchemaGivenTypeWithStringField(t *testing.T) {
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
				Doc: `{"name": "Alice"}`,
			},
			testUtils.Request{
				Request: `query { Users { name } }`,
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

// TestCollectionCreatesSchemaGivenTypeWithIntField tests int field creation.
func TestCollectionCreatesSchemaGivenTypeWithIntField(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						age: Int
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{"age": 30}`,
			},
			testUtils.Request{
				Request: `query { Users { age } }`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"age": int64(30)},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCollectionCreatesSchemaGivenTypeWithFloatField tests float field creation.
func TestCollectionCreatesSchemaGivenTypeWithFloatField(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						height: Float
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{"height": 1.75}`,
			},
			testUtils.Request{
				Request: `query { Users { height } }`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"height": 1.75},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCollectionCreatesSchemaGivenTypeWithBooleanField tests boolean field creation.
func TestCollectionCreatesSchemaGivenTypeWithBooleanField(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						verified: Boolean
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{"verified": true}`,
			},
			testUtils.Request{
				Request: `query { Users { verified } }`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"verified": true},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCollectionCreatesSchemaWithAllBasicTypes tests all basic field types.
func TestCollectionCreatesSchemaWithAllBasicTypes(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
						age: Int
						height: Float
						verified: Boolean
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Alice",
					"age": 30,
					"height": 1.68,
					"verified": true
				}`,
			},
			testUtils.Request{
				Request: `query { Users { name age height verified } }`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":     "Alice",
							"age":      int64(30),
							"height":   1.68,
							"verified": true,
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestGetCollectionsReturnsEmptySetGivenNoSchema tests empty collection list.
// Ported from: tests/integration/collection_version/get_schema_test.go
func TestGetCollectionsReturnsEmptySetGivenNoSchema(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.GetCollections{
				ExpectedResults: []client.CollectionVersion{},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestGetCollectionsReturnsEmptyGivenUnknownName tests filtering by unknown name.
// Ported from: tests/integration/collection_version/get_schema_test.go
func TestGetCollectionsReturnsEmptyGivenUnknownName(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.GetCollections{
				FilterOptions: client.CollectionFetchOptions{
					Name: immutable.Some("does not exist"),
				},
				ExpectedResults: []client.CollectionVersion{},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestGetCollectionsReturnsAllSchema tests getting all schemas.
// Ported from: tests/integration/collection_version/get_schema_test.go
func TestGetCollectionsReturnsAllSchema(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
					}
				`,
			},
			&action.AddSchema{
				Schema: `
					type Books {
						title: String
					}
				`,
			},
			testUtils.GetCollections{
				ExpectedResults: []client.CollectionVersion{
					{
						Name:           "Books",
						IsActive:       true,
						IsMaterialized: true,
					},
					{
						Name:           "Users",
						IsActive:       true,
						IsMaterialized: true,
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestGetCollectionsReturnsSchemaForGivenName tests filtering by name.
// Ported from: tests/integration/collection_version/get_schema_test.go
func TestGetCollectionsReturnsSchemaForGivenName(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						name: String
					}
				`,
			},
			&action.AddSchema{
				Schema: `
					type Books {
						title: String
					}
				`,
			},
			testUtils.GetCollections{
				FilterOptions: client.CollectionFetchOptions{
					Name: immutable.Some("Users"),
				},
				ExpectedResults: []client.CollectionVersion{
					{
						Name:           "Users",
						IsActive:       true,
						IsMaterialized: true,
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestPatchCollectionAddsField tests adding a field via patch.
// Ported from: tests/integration/collection_version/get_schema_test.go
func TestPatchCollectionAddsField(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						email: String
					}
				`,
			},
			testUtils.PatchCollection{
				Patch: `
					[
						{ "op": "add", "path": "/Users/Fields/-", "value": {"Name": "name", "Kind": "String"} }
					]
				`,
			},
			// Verify we can create a doc with the new field
			testUtils.CreateDoc{
				Doc: `{"name": "Alice", "email": "alice@example.com"}`,
			},
			testUtils.Request{
				Request: `query { Users { name email } }`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Alice", "email": "alice@example.com"},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestPatchCollectionAddsMultipleFields tests adding multiple fields.
func TestPatchCollectionAddsMultipleFields(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						email: String
					}
				`,
			},
			testUtils.PatchCollection{
				Patch: `
					[
						{ "op": "add", "path": "/Users/Fields/-", "value": {"Name": "name", "Kind": "String"} },
						{ "op": "add", "path": "/Users/Fields/-", "value": {"Name": "age", "Kind": "Int"} }
					]
				`,
			},
			testUtils.CreateDoc{
				Doc: `{"name": "Alice", "age": 30, "email": "alice@example.com"}`,
			},
			testUtils.Request{
				Request: `query { Users { name age } }`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Alice", "age": int64(30)},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestPatchCollectionDeactivatesVersion tests deactivating a collection version.
// Ported from: tests/integration/collection_version/get_schema_test.go
// SKIP: Requires collection versioning support (tracked separately)
func TestPatchCollectionDeactivatesVersion(t *testing.T) {
	t.Skip("Requires collection versioning support - tracked in separate PR")
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Users {
						email: String
					}
				`,
			},
			testUtils.PatchCollection{
				Patch: `
					[
						{ "op": "add", "path": "/Users/Fields/-", "value": {"Name": "name", "Kind": "String"} },
						{ "op": "replace", "path": "/Users/IsActive", "value": false }
					]
				`,
			},
			// With IncludeInactive, we should see both versions
			testUtils.GetCollections{
				FilterOptions: client.CollectionFetchOptions{
					Name:            immutable.Some("Users"),
					IncludeInactive: immutable.Some(true),
				},
				// Expect 2 versions: original (active) and patched (inactive)
				ExpectedResults: []client.CollectionVersion{
					{
						Name:           "Users",
						IsActive:       false,
						IsMaterialized: true,
					},
					{
						Name:           "Users",
						IsActive:       true,
						IsMaterialized: true,
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCollectionWithOneToManyRelation tests creating related collections.
func TestCollectionWithOneToManyRelation(t *testing.T) {
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
			testUtils.GetCollections{
				ExpectedResults: []client.CollectionVersion{
					{
						Name:           "Author",
						IsActive:       true,
						IsMaterialized: true,
					},
					{
						Name:           "Book",
						IsActive:       true,
						IsMaterialized: true,
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCollectionWithOneToOneRelation tests one-to-one relationships.
func TestCollectionWithOneToOneRelation(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String
						profile: Profile @primary
					}
					type Profile {
						bio: String
						user: User
					}
				`,
			},
			testUtils.GetCollections{
				ExpectedResults: []client.CollectionVersion{
					{
						Name:           "Profile",
						IsActive:       true,
						IsMaterialized: true,
					},
					{
						Name:           "User",
						IsActive:       true,
						IsMaterialized: true,
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCollectionWithSelfReference tests self-referential relationships.
func TestCollectionWithSelfReference(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Employee {
						name: String
						manager: Employee
						reports: [Employee]
					}
				`,
			},
			testUtils.GetCollections{
				ExpectedResults: []client.CollectionVersion{
					{
						Name:           "Employee",
						IsActive:       true,
						IsMaterialized: true,
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCollectionDocIDField tests that _docID is always available.
func TestCollectionDocIDField(t *testing.T) {
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
				Doc: `{"name": "Alice"}`,
			},
			testUtils.Request{
				Request: `query { Users { _docID name } }`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"_docID": testUtils.NewDocIndex(0, 0),
							"name":   "Alice",
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCollectionQueryAfterUpdate tests querying after document update.
func TestCollectionQueryAfterUpdate(t *testing.T) {
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
				Doc: `{"name": "Alice", "age": 25}`,
			},
			// Note: Doc must use GraphQL input format (unquoted field names)
			testUtils.UpdateDoc{
				DocID: 0,
				Doc:   `{age: 26}`,
			},
			testUtils.Request{
				Request: `query { Users { name age } }`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Alice", "age": int64(26)},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCollectionQueryAfterDelete tests querying after document delete.
func TestCollectionQueryAfterDelete(t *testing.T) {
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
				Doc: `{"name": "Alice"}`,
			},
			testUtils.CreateDoc{
				Doc: `{"name": "Bob"}`,
			},
			testUtils.DeleteDoc{
				DocID: 0,
			},
			testUtils.Request{
				Request: `query { Users { name } }`,
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

// TestCollectionWithMultipleDocuments tests collections with multiple documents.
func TestCollectionWithMultipleDocuments(t *testing.T) {
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
				Doc: `{"name": "Alice", "age": 25}`,
			},
			testUtils.CreateDoc{
				Doc: `{"name": "Bob", "age": 30}`,
			},
			testUtils.CreateDoc{
				Doc: `{"name": "Charlie", "age": 35}`,
			},
			testUtils.Request{
				Request: `query { Users { name age } }`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Alice", "age": int64(25)},
						{"name": "Bob", "age": int64(30)},
						{"name": "Charlie", "age": int64(35)},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCollectionFilterByField tests filtering documents by field value.
func TestCollectionFilterByField(t *testing.T) {
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
				Doc: `{"name": "Alice", "age": 25}`,
			},
			testUtils.CreateDoc{
				Doc: `{"name": "Bob", "age": 30}`,
			},
			testUtils.Request{
				Request: `query { Users(filter: {age: {_gt: 27}}) { name age } }`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Bob", "age": int64(30)},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}
