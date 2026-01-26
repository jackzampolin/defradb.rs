// Package integration tests DefraDB one-to-many relationship queries against the Rust FFI implementation.
//
// These tests are ported from DefraDB's tests/integration/query/one_to_many/ directory
// and validate behavioral compatibility between Go and Rust implementations.
package integration

import (
	"testing"

	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

// bookAuthorOneToManyGQLSchema is the schema used by one_to_many query tests.
// Matches DefraDB's tests/integration/query/one_to_many/utils.go
var bookAuthorOneToManyGQLSchema = `
	type Book {
		name: String
		rating: Float
		author: Author
	}

	type Author {
		name: String
		age: Int
		verified: Boolean
		published: [Book]
	}
`

// executeOneToManyTestCase wraps the test with the Book/Author schema.
func executeOneToManyTestCase(t *testing.T, test testUtils.TestCase) {
	ExecuteTestCase(
		t,
		testUtils.TestCase{
			SupportedMutationTypes: test.SupportedMutationTypes,
			SupportedClientTypes:   test.SupportedClientTypes,
			Actions: append(
				[]any{
					&action.AddSchema{
						Schema: bookAuthorOneToManyGQLSchema,
					},
				},
				test.Actions...,
			),
		},
	)
}

// TestQueryOneToMany_PrimaryDirection tests querying from Book to Author (primary direction).
// Ported from: tests/integration/query/one_to_many/simple_test.go
func TestQueryOneToMany_PrimaryDirection(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 1,
				Doc: `{
					"name": "John Grisham",
					"age": 65,
					"verified": true
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Painted House",
					"rating":    4.9,
					"author_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.Request{
				Request: `query {
					Book {
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

	executeOneToManyTestCase(t, test)
}

// TestQueryOneToMany_SecondaryDirection tests querying from Author to Books (secondary direction, returns array).
// Ported from: tests/integration/query/one_to_many/simple_test.go
func TestQueryOneToMany_SecondaryDirection(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 1,
				Doc: `{
					"name": "John Grisham",
					"age": 65,
					"verified": true
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				Doc: `{
					"name": "Cornelia Funke",
					"age": 62,
					"verified": false
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Painted House",
					"rating":    4.9,
					"author_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "A Time for Mercy",
					"rating":    4.5,
					"author_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Theif Lord",
					"rating":    4.8,
					"author_id": testUtils.NewDocIndex(1, 1),
				},
			},
			testUtils.Request{
				Request: `query {
					Author {
						name
						age
						published {
							name
							rating
						}
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name": "John Grisham",
							"age":  int64(65),
							"published": []map[string]any{
								{
									"name":   "Painted House",
									"rating": 4.9,
								},
								{
									"name":   "A Time for Mercy",
									"rating": 4.5,
								},
							},
						},
						{
							"name": "Cornelia Funke",
							"age":  int64(62),
							"published": []map[string]any{
								{
									"name":   "Theif Lord",
									"rating": 4.8,
								},
							},
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeOneToManyTestCase(t, test)
}

// TestQueryOneToManyWithNonExistantParent tests querying when the related Author doesn't exist.
// Ported from: tests/integration/query/one_to_many/simple_test.go
func TestQueryOneToManyWithNonExistantParent(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Painted House",
					"rating": 4.9,
					"author_id": "bae-9d52c335-c8e3-5782-8daa-e359c106e0ab"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Book {
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
							"author": nil,
						},
					},
				},
			},
		},
	}

	executeOneToManyTestCase(t, test)
}

// TestQueryOneToManyWithNumericGreaterThanFilterOnParent tests filtering on parent fields.
// Ported from: tests/integration/query/one_to_many/with_filter_test.go
func TestQueryOneToManyWithNumericGreaterThanFilterOnParent(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 1,
				Doc: `{
					"name": "John Grisham",
					"age": 65,
					"verified": true
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				Doc: `{
					"name": "Cornelia Funke",
					"age": 62,
					"verified": false
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Painted House",
					"rating":    4.9,
					"author_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "A Time for Mercy",
					"rating":    4.5,
					"author_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Theif Lord",
					"rating":    4.8,
					"author_id": testUtils.NewDocIndex(1, 1),
				},
			},
			testUtils.Request{
				Request: `query {
					Author(filter: {age: {_gt: 63}}) {
						name
						age
						published {
							name
							rating
						}
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name": "John Grisham",
							"age":  int64(65),
							"published": []map[string]any{
								{
									"name":   "Painted House",
									"rating": 4.9,
								},
								{
									"name":   "A Time for Mercy",
									"rating": 4.5,
								},
							},
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeOneToManyTestCase(t, test)
}

// TestQueryOneToManyWithNumericGreaterThanChildFilterOnParentWithUnrenderedChild tests filtering by child fields without selecting child.
// Ported from: tests/integration/query/one_to_many/with_filter_test.go
func TestQueryOneToManyWithNumericGreaterThanChildFilterOnParentWithUnrenderedChild(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 1,
				Doc: `{
					"name": "John Grisham",
					"age": 65,
					"verified": true
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				Doc: `{
					"name": "Cornelia Funke",
					"age": 62,
					"verified": false
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Painted House",
					"rating":    4.9,
					"author_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "A Time for Mercy",
					"rating":    4.5,
					"author_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Theif Lord",
					"rating":    4.8,
					"author_id": testUtils.NewDocIndex(1, 1),
				},
			},
			testUtils.Request{
				Request: `query {
					Author(filter: {published: {rating: {_gt: 4.8}}, age: {_gt: 63}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name": "John Grisham",
						},
					},
				},
			},
		},
	}

	executeOneToManyTestCase(t, test)
}

// TestQueryOneToManyWithNumericGreaterThanFilterOnParentAndChild tests filtering on both parent and child.
// Ported from: tests/integration/query/one_to_many/with_filter_test.go
func TestQueryOneToManyWithNumericGreaterThanFilterOnParentAndChild(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 1,
				Doc: `{
					"name": "John Grisham",
					"age": 65,
					"verified": true
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				Doc: `{
					"name": "Cornelia Funke",
					"age": 62,
					"verified": false
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Painted House",
					"rating":    4.9,
					"author_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "A Time for Mercy",
					"rating":    4.5,
					"author_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Theif Lord",
					"rating":    4.8,
					"author_id": testUtils.NewDocIndex(1, 1),
				},
			},
			testUtils.Request{
				Request: `query {
					Author(filter: {age: {_gt: 63}}) {
						name
						age
						published(filter: {rating: {_gt: 4.6}}) {
							name
							rating
						}
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name": "John Grisham",
							"age":  int64(65),
							"published": []map[string]any{
								{
									"name":   "Painted House",
									"rating": 4.9,
								},
							},
						},
					},
				},
			},
		},
	}

	executeOneToManyTestCase(t, test)
}

// TestQueryOneToManyWithMultipleAliasedFilteredChildren tests multiple aliased child selections with different filters.
// Ported from: tests/integration/query/one_to_many/with_filter_test.go
func TestQueryOneToManyWithMultipleAliasedFilteredChildren(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 1,
				Doc: `{
					"name": "John Grisham",
					"age": 65,
					"verified": true
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				Doc: `{
					"name": "Cornelia Funke",
					"age": 62,
					"verified": false
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Painted House",
					"rating":    4.9,
					"author_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "A Time for Mercy",
					"rating":    4.5,
					"author_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Theif Lord",
					"rating":    4.8,
					"author_id": testUtils.NewDocIndex(1, 1),
				},
			},
			testUtils.Request{
				Request: `query {
					Author {
						name
						age
						p1: published(filter: {rating: {_gt: 4.6}}) {
							name
							rating
						}
						p2: published(filter: {rating: {_lt: 4.6}}) {
							name
							rating
						}
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name": "John Grisham",
							"age":  int64(65),
							"p1": []map[string]any{
								{
									"name":   "Painted House",
									"rating": 4.9,
								},
							},
							"p2": []map[string]any{
								{
									"name":   "A Time for Mercy",
									"rating": 4.5,
								},
							},
						},
						{
							"name": "Cornelia Funke",
							"age":  int64(62),
							"p1": []map[string]any{
								{
									"name":   "Theif Lord",
									"rating": 4.8,
								},
							},
							"p2": []map[string]any{},
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeOneToManyTestCase(t, test)
}

// TestQueryOneToManyWithCompoundOperatorInFilterAndRelation tests compound _or/_and filters with relations.
// Ported from: tests/integration/query/one_to_many/with_filter_test.go
func TestQueryOneToManyWithCompoundOperatorInFilterAndRelation(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				CollectionID: 1,
				Doc: `{
					"name": "John Grisham",
					"age": 65,
					"verified": true
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				Doc: `{
					"name": "Cornelia Funke",
					"age": 62,
					"verified": false
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				Doc: `{
					"name": "John Tolkien",
					"age": 70,
					"verified": true
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Painted House",
					"rating":    4.9,
					"author_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "A Time for Mercy",
					"rating":    4.5,
					"author_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Theif Lord",
					"rating":    4.8,
					"author_id": testUtils.NewDocIndex(1, 1),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "The Lord of the Rings",
					"rating":    5.0,
					"author_id": testUtils.NewDocIndex(1, 2),
				},
			},
			testUtils.Request{
				Request: `query {
					Author(filter: {_or: [
						{_and: [
							{published: {rating: {_lt: 5.0}}},
							{published: {rating: {_gt: 4.8}}}
						]},
						{_and: [
							{age: {_le: 65}},
							{published: {name: {_like: "%Lord%"}}}
						]},
					]}) {
						name
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name": "John Grisham",
						},
						{
							"name": "Cornelia Funke",
						},
					},
				},
				NonOrderedResults: true,
			},
			testUtils.Request{
				Request: `query {
					Author(filter: {_and: [
						{ _not: {published: {rating: {_gt: 4.8}}}},
						{ _not: {published: {rating: {_lt: 4.8}}}}
					]}) {
						name
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{"name": "Cornelia Funke"},
					},
				},
			},
		},
	}

	executeOneToManyTestCase(t, test)
}
