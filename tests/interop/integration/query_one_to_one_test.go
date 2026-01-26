// Package integration tests DefraDB one-to-one relationship queries against the Rust FFI implementation.
//
// These tests are ported from DefraDB's tests/integration/query/one_to_one/ directory
// and validate behavioral compatibility between Go and Rust implementations.
package integration

import (
	"testing"

	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

// bookAuthorGQLSchema is the schema used by one_to_one query tests.
// Matches DefraDB's tests/integration/query/one_to_one/utils.go
var bookAuthorGQLSchema = `
	type Book {
		name: String
		rating: Float
		author: Author
	}

	type Author {
		name: String
		age: Int
		verified: Boolean
		published: Book @primary
	}
`

// executeOneToOneTestCase wraps the test with the Book/Author schema.
func executeOneToOneTestCase(t *testing.T, test testUtils.TestCase) {
	ExecuteTestCase(
		t,
		testUtils.TestCase{
			SupportedMutationTypes: test.SupportedMutationTypes,
			SupportedClientTypes:   test.SupportedClientTypes,
			Actions: append(
				[]any{
					&action.AddSchema{
						Schema: bookAuthorGQLSchema,
					},
				},
				test.Actions...,
			),
		},
	)
}

// TestQueryOneToOneWithMultipleRecords tests querying one-to-one relationships with multiple records.
// Ported from: tests/integration/query/one_to_one/simple_test.go
func TestQueryOneToOneWithMultipleRecords(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":   "Painted House",
					"rating": 4.9,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":   "Go Guide for Rust developers",
					"rating": 5.01, // Use non-integer to ensure float type
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "John Grisham",
					"age":          65,
					"verified":     true,
					"published_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "Andrew Lone",
					"age":          30,
					"verified":     true,
					"published_id": testUtils.NewDocIndex(0, 1),
				},
			},
			testUtils.Request{
				Request: `query {
					Book {
						name
						author {
							name
						}
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name": "Painted House",
							"author": map[string]any{
								"name": "John Grisham",
							},
						},
						{
							"name": "Go Guide for Rust developers",
							"author": map[string]any{
								"name": "Andrew Lone",
							},
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeOneToOneTestCase(t, test)
}

// TestQueryOneToOneWithMultipleRecordsSecondaryDirection tests querying from the secondary side.
// Ported from: tests/integration/query/one_to_one/simple_test.go
func TestQueryOneToOneWithMultipleRecordsSecondaryDirection(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Painted House"
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Theif Lord"
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "John Grisham",
					"published_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "Cornelia Funke",
					"published_id": testUtils.NewDocIndex(0, 1),
				},
			},
			testUtils.Request{
				Request: `query {
					Author {
						name
						published {
							name
						}
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name": "John Grisham",
							"published": map[string]any{
								"name": "Painted House",
							},
						},
						{
							"name": "Cornelia Funke",
							"published": map[string]any{
								"name": "Theif Lord",
							},
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeOneToOneTestCase(t, test)
}

// TestQueryOneToOneWithNilChild tests querying when the related document doesn't exist.
// Ported from: tests/integration/query/one_to_one/simple_test.go
func TestQueryOneToOneWithNilChild(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 1,
				Doc: `{
					"name": "John Grisham"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Author {
						name
						published {
							name
						}
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name":      "John Grisham",
							"published": nil,
						},
					},
				},
			},
		},
	}

	executeOneToOneTestCase(t, test)
}

// TestQueryOneToOneWithNilParent tests querying when the parent document has no relation.
// Ported from: tests/integration/query/one_to_one/simple_test.go
func TestQueryOneToOneWithNilParent(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Painted House"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Book {
						name
						author {
							name
						}
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name":   "Painted House",
							"author": nil,
						},
					},
				},
			},
		},
	}

	executeOneToOneTestCase(t, test)
}

// TestQueryOneToOneWithNumericFilterOnParent tests filtering on related document fields.
// Ported from: tests/integration/query/one_to_one/with_filter_test.go
func TestQueryOneToOneWithNumericFilterOnParent(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Painted House",
					"rating": 4.9
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "John Grisham",
					"age":          65,
					"verified":     true,
					"published_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.Request{
				Request: `query {
					Book {
						name
						rating
						author(filter: {age: {_eq: 65}}) {
							name
							age
						}
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name":   "Painted House",
							"rating": 4.9,
							"author": map[string]any{
								"name": "John Grisham",
								"age":  int64(65),
							},
						},
					},
				},
			},
		},
	}

	executeOneToOneTestCase(t, test)
}

// TestQueryOneToOneWithStringFilterOnChild tests filtering on the primary collection.
// Ported from: tests/integration/query/one_to_one/with_filter_test.go
func TestQueryOneToOneWithStringFilterOnChild(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Painted House",
					"rating": 4.9
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "John Grisham",
					"age":          65,
					"verified":     true,
					"published_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.Request{
				Request: `query {
					Book(filter: {name: {_eq: "Painted House"}}) {
						name
						rating
						author {
							name
							age
						}
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name":   "Painted House",
							"rating": 4.9,
							"author": map[string]any{
								"name": "John Grisham",
								"age":  int64(65),
							},
						},
					},
				},
			},
		},
	}

	executeOneToOneTestCase(t, test)
}

// TestQueryOneToOneWithBooleanFilterOnChild tests filtering on boolean fields of related documents.
// Ported from: tests/integration/query/one_to_one/with_filter_test.go
func TestQueryOneToOneWithBooleanFilterOnChild(t *testing.T) {

	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Painted House",
					"rating": 4.9
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "John Grisham",
					"age":          65,
					"verified":     true,
					"published_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.Request{
				Request: `query {
					Book(filter: {author: {verified: {_eq: true}}}) {
						name
						rating
						author {
							name
							age
						}
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name":   "Painted House",
							"rating": 4.9,
							"author": map[string]any{
								"name": "John Grisham",
								"age":  int64(65),
							},
						},
					},
				},
			},
		},
	}

	executeOneToOneTestCase(t, test)
}

// TestQueryOneToOneWithBooleanFilterOnChildWithNoSubTypeSelection tests filtering without selecting subfields.
// Ported from: tests/integration/query/one_to_one/with_filter_test.go
func TestQueryOneToOneWithBooleanFilterOnChildWithNoSubTypeSelection(t *testing.T) {

	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Painted House",
					"rating": 4.9
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "John Grisham",
					"age":          65,
					"verified":     true,
					"published_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.Request{
				Request: `query {
					Book(filter: {author: {verified: {_eq: true}}}) {
						name
						rating
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name":   "Painted House",
							"rating": 4.9,
						},
					},
				},
			},
		},
	}

	executeOneToOneTestCase(t, test)
}

// TestQueryOneToOneWithFilterThroughChildBackToParent tests filtering through a circular reference.
// Ported from: tests/integration/query/one_to_one/with_filter_test.go
func TestQueryOneToOneWithFilterThroughChildBackToParent(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Painted House",
					"rating": 4.9
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Theif Lord",
					"rating": 4.8
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "John Grisham",
					"age":          65,
					"verified":     true,
					"published_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "Cornelia Funke",
					"age":          62,
					"verified":     false,
					"published_id": testUtils.NewDocIndex(0, 1),
				},
			},
			testUtils.Request{
				Request: `query {
					Book(filter: {author: {published: {rating: {_eq: 4.9}}}}) {
						name
						rating
						author {
							name
							age
						}
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name":   "Painted House",
							"rating": 4.9,
							"author": map[string]any{
								"name": "John Grisham",
								"age":  int64(65),
							},
						},
					},
				},
			},
		},
	}

	executeOneToOneTestCase(t, test)
}

// TestQueryOneToOneWithCompoundAndFilterThatIncludesRelation tests _and filters with relations.
// Ported from: tests/integration/query/one_to_one/with_filter_test.go
func TestQueryOneToOneWithCompoundAndFilterThatIncludesRelation(t *testing.T) {

	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Painted House",
					"rating": 4.9
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Some Book",
					"rating": 4.01
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Some Other Book",
					"rating": 3.01
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "John Grisham",
					"age":          65,
					"verified":     true,
					"published_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "Some Writer",
					"age":          45,
					"verified":     false,
					"published_id": testUtils.NewDocIndex(0, 1),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "Some Other Writer",
					"age":          30,
					"verified":     true,
					"published_id": testUtils.NewDocIndex(0, 2),
				},
			},
			testUtils.Request{
				Request: `query {
					Book(filter: {_and: [
						{rating: {_ge: 4.0}},
						{author: {verified: {_eq: true}}}
					]}) {
						name
						rating
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name":   "Painted House",
							"rating": 4.9,
						},
					},
				},
			},
		},
	}

	executeOneToOneTestCase(t, test)
}

// TestQueryOneToOneWithCompoundOrFilterThatIncludesRelation tests _or filters with relations.
// Ported from: tests/integration/query/one_to_one/with_filter_test.go
func TestQueryOneToOneWithCompoundOrFilterThatIncludesRelation(t *testing.T) {

	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Painted House",
					"rating": 4.9
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Some Book",
					"rating": 4.01
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Some Other Book",
					"rating": 3.01
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "John Grisham",
					"age":          65,
					"verified":     true,
					"published_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "Some Writer",
					"age":          45,
					"verified":     false,
					"published_id": testUtils.NewDocIndex(0, 1),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "Some Other Writer",
					"age":          30,
					"verified":     true,
					"published_id": testUtils.NewDocIndex(0, 2),
				},
			},
			testUtils.Request{
				Request: `query {
					Book(filter: {_or: [
						{rating: {_gt: 4.0}},
						{author: {age: {_eq: 30}}}
					]}) {
						name
						rating
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name":   "Painted House",
							"rating": 4.9,
						},
						{
							"name":   "Some Book",
							"rating": 4.01,
						},
						{
							"name":   "Some Other Book",
							"rating": 3.01,
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeOneToOneTestCase(t, test)
}

// TestQueryOneToOneWithChildIntOrderDescendingWithNoSubTypeFieldsSelected tests ordering by related field.
// Ported from: tests/integration/query/one_to_one/with_order_test.go
func TestQueryOneToOneWithChildIntOrderDescendingWithNoSubTypeFieldsSelected(t *testing.T) {

	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Painted House",
					"rating": 4.9
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Theif Lord",
					"rating": 4.8
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "John Grisham",
					"age":          65,
					"verified":     true,
					"published_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "Cornelia Funke",
					"age":          62,
					"verified":     false,
					"published_id": testUtils.NewDocIndex(0, 1),
				},
			},
			testUtils.Request{
				Request: `query {
					Book(order: {author: {age: DESC}}) {
						name
						rating
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name":   "Painted House",
							"rating": 4.9,
						},
						{
							"name":   "Theif Lord",
							"rating": 4.8,
						},
					},
				},
			},
		},
	}

	executeOneToOneTestCase(t, test)
}

// TestQueryOneToOneWithChildIntOrderAscendingWithNoSubTypeFieldsSelected tests ascending order by related field.
// Ported from: tests/integration/query/one_to_one/with_order_test.go
func TestQueryOneToOneWithChildIntOrderAscendingWithNoSubTypeFieldsSelected(t *testing.T) {

	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Painted House",
					"rating": 4.9
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Theif Lord",
					"rating": 4.8
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "John Grisham",
					"age":          65,
					"verified":     true,
					"published_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "Cornelia Funke",
					"age":          62,
					"verified":     false,
					"published_id": testUtils.NewDocIndex(0, 1),
				},
			},
			testUtils.Request{
				Request: `query {
					Book(order: {author: {age: ASC}}) {
						name
						rating
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name":   "Theif Lord",
							"rating": 4.8,
						},
						{
							"name":   "Painted House",
							"rating": 4.9,
						},
					},
				},
			},
		},
	}

	executeOneToOneTestCase(t, test)
}

// TestQueryOneToOneWithChildBooleanOrderDescending tests ordering by boolean field of related document.
// Ported from: tests/integration/query/one_to_one/with_order_test.go
func TestQueryOneToOneWithChildBooleanOrderDescending(t *testing.T) {

	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Painted House",
					"rating": 4.9
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Theif Lord",
					"rating": 4.8
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "John Grisham",
					"age":          65,
					"verified":     true,
					"published_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "Cornelia Funke",
					"age":          62,
					"verified":     false,
					"published_id": testUtils.NewDocIndex(0, 1),
				},
			},
			testUtils.Request{
				Request: `query {
					Book(order: {author: {verified: DESC}}) {
						name
						rating
						author {
							name
							age
						}
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name":   "Painted House",
							"rating": 4.9,
							"author": map[string]any{
								"name": "John Grisham",
								"age":  int64(65),
							},
						},
						{
							"name":   "Theif Lord",
							"rating": 4.8,
							"author": map[string]any{
								"name": "Cornelia Funke",
								"age":  int64(62),
							},
						},
					},
				},
			},
		},
	}

	executeOneToOneTestCase(t, test)
}

// TestQueryOneToOneWithChildBooleanOrderAscending tests ascending order by boolean field.
// Ported from: tests/integration/query/one_to_one/with_order_test.go
func TestQueryOneToOneWithChildBooleanOrderAscending(t *testing.T) {

	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Painted House",
					"rating": 4.9
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Theif Lord",
					"rating": 4.8
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "John Grisham",
					"age":          65,
					"verified":     true,
					"published_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":         "Cornelia Funke",
					"age":          62,
					"verified":     false,
					"published_id": testUtils.NewDocIndex(0, 1),
				},
			},
			testUtils.Request{
				Request: `query {
					Book(order: {author: {verified: ASC}}) {
						name
						rating
						author {
							name
							age
						}
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name":   "Theif Lord",
							"rating": 4.8,
							"author": map[string]any{
								"name": "Cornelia Funke",
								"age":  int64(62),
							},
						},
						{
							"name":   "Painted House",
							"rating": 4.9,
							"author": map[string]any{
								"name": "John Grisham",
								"age":  int64(65),
							},
						},
					},
				},
			},
		},
	}

	executeOneToOneTestCase(t, test)
}
