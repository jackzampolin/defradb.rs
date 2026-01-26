// Package integration tests DefraDB index patterns against the Rust FFI implementation.
//
// These tests are ported from DefraDB's tests/integration/index/ directory
// and validate behavioral compatibility between Go and Rust implementations.
package integration

import (
	"testing"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

// ============================================================================
// Index Creation Tests
// Ported from: tests/integration/index/create_test.go
// ============================================================================

// TestIndexCreateWithCollection_ShouldNotHinderQuerying tests that creating an index
// via schema directive doesn't break querying.
func TestIndexCreateWithCollection_ShouldNotHinderQuerying(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String @index
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
			testUtils.Request{
				Request: `query {
					User {
						name
						age
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{
							"name": "John",
							"age":  int64(21),
						},
					},
				},
			},
			testUtils.GetIndexes{
				ExpectedIndexes: []client.IndexDescription{
					{
						// Rust uses "_idx" suffix, Go uses "_ASC" suffix
						Name: "User_name_idx",
						ID:   1,
						Fields: []client.IndexedFieldDescription{
							{Name: "name"},
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestIndexCreate_ShouldNotHinderQuerying tests that creating an index
// via CreateIndex action doesn't break querying.
func TestIndexCreate_ShouldNotHinderQuerying(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
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
			testUtils.CreateIndex{
				IndexName: "some_index",
				FieldName: "name",
			},
			testUtils.Request{
				Request: `query {
					User {
						name
						age
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{
							"name": "John",
							"age":  int64(21),
						},
					},
				},
			},
			testUtils.GetIndexes{
				ExpectedIndexes: []client.IndexDescription{
					{
						Name: "some_index",
						ID:   1,
						Fields: []client.IndexedFieldDescription{
							{Name: "name"},
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestIndexCreate_OnExistingDocs tests that creating an index on existing documents works.
func TestIndexCreate_OnExistingDocs(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
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
			testUtils.CreateIndex{
				IndexName: "name_index",
				FieldName: "name",
			},
			testUtils.Request{
				Request: `query {
					User(filter: {name: {_eq: "John"}}) {
						name
						age
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{
							"name": "John",
							"age":  int64(21),
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// ============================================================================
// Index Drop Tests
// Ported from: tests/integration/index/drop_test.go
// ============================================================================

// TestIndexDrop_ShouldNotHinderQuerying tests that dropping an index
// doesn't break querying.
func TestIndexDrop_ShouldNotHinderQuerying(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String @index
						age: Int
					}
				`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "John",
					"age": 21
				}`,
			},
			testUtils.DropIndex{
				// Rust uses "_idx" suffix
				IndexName: "User_name_idx",
			},
			testUtils.Request{
				Request: `query {
					User {
						name
						age
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{
							"name": "John",
							"age":  int64(21),
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestIndexDrop_ShouldRemoveIndexFromCollection tests that dropping indexes
// removes them from the collection.
func TestIndexDrop_ShouldRemoveIndexFromCollection(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String @index
						age: Int @index
					}
				`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "John",
					"age": 21
				}`,
			},
			testUtils.DropIndex{
				// Rust uses "_idx" suffix
				IndexName: "User_age_idx",
			},
			testUtils.GetIndexes{
				CollectionID: 0,
				ExpectedIndexes: []client.IndexDescription{
					{
						ID: 1,
						// Rust uses "_idx" suffix
						Name: "User_name_idx",
						Fields: []client.IndexedFieldDescription{
							{Name: "name"},
						},
					},
				},
			},
			testUtils.DropIndex{
				// Rust uses "_idx" suffix
				IndexName: "User_name_idx",
			},
			testUtils.GetIndexes{
				CollectionID:    0,
				ExpectedIndexes: []client.IndexDescription{},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestIndexDrop_IfIndexDoesNotExist_ReturnError tests that dropping a
// non-existent index returns an error.
func TestIndexDrop_IfIndexDoesNotExist_ReturnError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String
						age: Int
					}
				`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "John",
					"age": 21
				}`,
			},
			testUtils.DropIndex{
				CollectionID:  0,
				IndexName:     "non_existing_index",
				ExpectedError: "not found",
			},
		},
	}

	ExecuteTestCase(t, test)
}

// ============================================================================
// Composite Index Tests
// Ported from: tests/integration/index/create_composite_test.go
// ============================================================================

// TestCompositeIndexCreate_WhenCreated_CanRetrieve tests creating and
// retrieving a composite index.
func TestCompositeIndexCreate_WhenCreated_CanRetrieve(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String
						age: Int
					}
				`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "John",
					"age": 21
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Shahzad",
					"age": 22
				}`,
			},
			testUtils.CreateIndex{
				CollectionID: 0,
				IndexName:    "name_age_index",
				Fields:       []testUtils.IndexedField{{Name: "name"}, {Name: "age"}},
			},
			testUtils.GetIndexes{
				CollectionID: 0,
				ExpectedIndexes: []client.IndexDescription{
					{
						Name: "name_age_index",
						ID:   1,
						Fields: []client.IndexedFieldDescription{
							{Name: "name"},
							{Name: "age"},
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCompositeIndexCreate_WithDescending tests creating a composite index
// with descending field order.
func TestCompositeIndexCreate_WithDescending(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String
						age: Int
					}
				`,
			},
			testUtils.CreateIndex{
				CollectionID: 0,
				IndexName:    "name_age_desc_index",
				Fields: []testUtils.IndexedField{
					{Name: "name", Descending: false},
					{Name: "age", Descending: true},
				},
			},
			testUtils.GetIndexes{
				CollectionID: 0,
				ExpectedIndexes: []client.IndexDescription{
					{
						Name: "name_age_desc_index",
						ID:   1,
						Fields: []client.IndexedFieldDescription{
							{Name: "name", Descending: false},
							{Name: "age", Descending: true},
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// ============================================================================
// Unique Index Tests
// Ported from: tests/integration/index/create_unique_test.go
// ============================================================================

// TestUniqueIndexCreate_IfFieldValuesAreUnique_Succeed tests that unique
// index creation succeeds when field values are unique.
func TestUniqueIndexCreate_IfFieldValuesAreUnique_Succeed(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String
						age: Int
					}
				`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "John",
					"age": 21
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Shahzad",
					"age": 22
				}`,
			},
			testUtils.CreateIndex{
				CollectionID: 0,
				IndexName:    "age_unique_index",
				FieldName:    "age",
				Unique:       true,
			},
			testUtils.GetIndexes{
				CollectionID: 0,
				ExpectedIndexes: []client.IndexDescription{
					{
						Name:   "age_unique_index",
						ID:     1,
						Unique: true,
						Fields: []client.IndexedFieldDescription{
							{Name: "age"},
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestCreateUniqueIndex_IfFieldValuesAreNotUnique_ReturnError tests that
// creating a unique index fails when values are not unique.
func TestCreateUniqueIndex_IfFieldValuesAreNotUnique_ReturnError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String
						age: Int
					}
				`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "John",
					"age": 21
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Andy",
					"age": 22
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Shahzad",
					"age": 21
				}`,
			},
			testUtils.CreateIndex{
				CollectionID:  0,
				FieldName:     "age",
				Unique:        true,
				ExpectedError: "constraint violation",
			},
			testUtils.GetIndexes{
				CollectionID:    0,
				ExpectedIndexes: []client.IndexDescription{},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestUniqueIndexCreate_UponAddingDocWithExistingFieldValue_ReturnError tests
// that adding a doc with duplicate value on unique index fails.
func TestUniqueIndexCreate_UponAddingDocWithExistingFieldValue_ReturnError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String
						age: Int
					}
				`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Shahzad",
					"age": 21
				}`,
			},
			// Create unique index AFTER first doc
			testUtils.CreateIndex{
				CollectionID: 0,
				IndexName:    "age_unique_index",
				FieldName:    "age",
				Unique:       true,
			},
			// Now try to add a doc with duplicate value
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "John",
					"age": 21
				}`,
				ExpectedError: "constraint violation",
			},
			testUtils.Request{
				Request: `query {
					User(filter: {name: {_eq: "John"}}) {
						name
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestUniqueIndexCreate_WithMultipleNilFields_ShouldSucceed tests that
// unique index allows multiple null values.
func TestUniqueIndexCreate_WithMultipleNilFields_ShouldSucceed(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String
						age: Int
					}
				`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "John",
					"age": 21
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Andy"
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Keenan"
				}`,
			},
			testUtils.CreateIndex{
				CollectionID: 0,
				IndexName:    "age_unique_index",
				FieldName:    "age",
				Unique:       true,
			},
			testUtils.GetIndexes{
				CollectionID: 0,
				ExpectedIndexes: []client.IndexDescription{
					{
						Name:   "age_unique_index",
						ID:     1,
						Unique: true,
						Fields: []client.IndexedFieldDescription{
							{Name: "age"},
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// ============================================================================
// Query With Index Tests
// Ported from: tests/integration/index/query_with_index_only_filter_test.go
// ============================================================================

// TestQueryWithIndex_WithEqualFilter_ShouldFetch tests basic indexed query.
func TestQueryWithIndex_WithEqualFilter_ShouldFetch(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String @index
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
					"name": "Islam",
					"age": 32
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Bob",
					"age": 44
				}`,
			},
			testUtils.Request{
				Request: `query {
					User(filter: {name: {_eq: "Islam"}}) {
						name
						age
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{
							"name": "Islam",
							"age":  int64(32),
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestQueryWithIndex_WithNonIndexedFields_ShouldFetchAllOfThem tests that
// indexed queries can still return non-indexed fields.
func TestQueryWithIndex_WithNonIndexedFields_ShouldFetchAllOfThem(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String @index
						age: Int
						email: String
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Islam",
					"age": 32,
					"email": "islam@example.com"
				}`,
			},
			testUtils.Request{
				Request: `query {
					User(filter: {name: {_eq: "Islam"}}) {
						name
						age
						email
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{
							"name":  "Islam",
							"age":   int64(32),
							"email": "islam@example.com",
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestQueryWithIndex_IfSeveralDocsWithEqFilter_ShouldFetchAll tests that
// indexed queries return all matching documents.
func TestQueryWithIndex_IfSeveralDocsWithEqFilter_ShouldFetchAll(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String @index
						age: Int
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Islam",
					"age": 32
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Islam",
					"age": 18
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"age": 25
				}`,
			},
			testUtils.Request{
				Request: `query {
					User(filter: {name: {_eq: "Islam"}}) {
						age
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"age": int64(32)},
						{"age": int64(18)},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestQueryWithIndex_WithGreaterThanFilter tests indexed query with _gt filter.
func TestQueryWithIndex_WithGreaterThanFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String
						age: Int @index
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
					"age": 55
				}`,
			},
			testUtils.Request{
				Request: `query {
					User(filter: {age: {_gt: 30}}) {
						name
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "Bob"},
						{"name": "Alice"},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestQueryWithIndex_WithLessThanFilter tests indexed query with _lt filter.
func TestQueryWithIndex_WithLessThanFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String
						age: Int @index
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
					"age": 55
				}`,
			},
			testUtils.Request{
				Request: `query {
					User(filter: {age: {_lt: 30}}) {
						name
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "John"},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestQueryWithIndex_WithNotEqualFilter tests indexed query with _ne filter.
func TestQueryWithIndex_WithNotEqualFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String @index
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
			testUtils.Request{
				Request: `query {
					User(filter: {name: {_ne: "John"}}) {
						name
						age
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{
							"name": "Bob",
							"age":  int64(32),
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestQueryWithIndex_WithInFilter tests indexed query with _in filter.
func TestQueryWithIndex_WithInFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String @index
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
					"age": 44
				}`,
			},
			testUtils.Request{
				Request: `query {
					User(filter: {name: {_in: ["John", "Alice"]}}) {
						name
						age
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{
							"name": "John",
							"age":  int64(21),
						},
						{
							"name": "Alice",
							"age":  int64(44),
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestQueryWithIndex_WithOrderByIndexedField tests ordering by indexed field.
func TestQueryWithIndex_WithOrderByIndexedField(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String
						age: Int @index
					}
				`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"age": 32
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Bob",
					"age": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Alice",
					"age": 44
				}`,
			},
			testUtils.Request{
				Request: `query {
					User(order: {age: ASC}) {
						name
						age
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "Bob", "age": int64(21)},
						{"name": "John", "age": int64(32)},
						{"name": "Alice", "age": int64(44)},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// ============================================================================
// Multiple Collection Index Tests
// ============================================================================

// TestIndex_WithMultipleCollections tests indexes on multiple collections.
func TestIndex_WithMultipleCollections(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String @index
					}
					type Product {
						title: String @index
					}
				`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name": "John",
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"title": "Widget",
				},
			},
			testUtils.GetIndexes{
				CollectionID: 0,
				ExpectedIndexes: []client.IndexDescription{
					{
						// Rust uses "_idx" suffix
						Name: "User_name_idx",
						ID:   1,
						Fields: []client.IndexedFieldDescription{
							{Name: "name"},
						},
					},
				},
			},
			testUtils.GetIndexes{
				CollectionID: 1,
				ExpectedIndexes: []client.IndexDescription{
					{
						// Rust uses "_idx" suffix
						Name: "Product_title_idx",
						ID:   1,
						Fields: []client.IndexedFieldDescription{
							{Name: "title"},
						},
					},
				},
			},
			testUtils.Request{
				Request: `query {
					User(filter: {name: {_eq: "John"}}) {
						name
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "John"},
					},
				},
			},
			testUtils.Request{
				Request: `query {
					Product(filter: {title: {_eq: "Widget"}}) {
						title
					}
				}`,
				Results: map[string]any{
					"Product": []map[string]any{
						{"title": "Widget"},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// ============================================================================
// Index After Document Mutation Tests
// ============================================================================

// TestIndex_AfterDocumentUpdate tests that indexes reflect document updates.
func TestIndex_AfterDocumentUpdate(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String @index
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
			// First verify initial state
			testUtils.Request{
				Request: `query {
					User(filter: {name: {_eq: "John"}}) {
						name
						age
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "John", "age": int64(21)},
					},
				},
			},
			// Update the document - use GraphQL input format (unquoted field names)
			testUtils.UpdateDoc{
				CollectionID: 0,
				DocID:        0,
				Doc:          `{name: "Johnny"}`,
			},
			// Query should reflect update
			testUtils.Request{
				Request: `query {
					User(filter: {name: {_eq: "Johnny"}}) {
						name
						age
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "Johnny", "age": int64(21)},
					},
				},
			},
			// Old name should not match
			testUtils.Request{
				Request: `query {
					User(filter: {name: {_eq: "John"}}) {
						name
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestIndex_AfterDocumentDelete tests that indexes reflect document deletions.
func TestIndex_AfterDocumentDelete(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String @index
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
			// Verify both exist
			testUtils.Request{
				Request: `query {
					User {
						name
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "John"},
						{"name": "Bob"},
					},
				},
				NonOrderedResults: true,
			},
			// Delete first document
			testUtils.DeleteDoc{
				CollectionID: 0,
				DocID:        0,
			},
			// John should not be found via index
			testUtils.Request{
				Request: `query {
					User(filter: {name: {_eq: "John"}}) {
						name
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{},
				},
			},
			// Bob should still be found
			testUtils.Request{
				Request: `query {
					User(filter: {name: {_eq: "Bob"}}) {
						name
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "Bob"},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}
