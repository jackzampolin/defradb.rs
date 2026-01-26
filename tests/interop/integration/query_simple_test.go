// Package integration tests DefraDB query patterns against the Rust FFI implementation.
//
// These tests are ported from DefraDB's tests/integration/query/simple/ directory
// and validate behavioral compatibility between Go and Rust implementations.
package integration

import (
	"testing"

	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

// userCollectionGQLSchema is the schema used by simple query tests.
// Matches DefraDB's tests/integration/query/simple/utils.go
var userCollectionGQLSchema = `
	type Users {
		Name: String
		Email: String
		Age: Int
		HeightM: Float
		Verified: Boolean
		CreatedAt: DateTime
	}
`

// executeSimpleTestCase wraps the test with the Users schema.
func executeSimpleTestCase(t *testing.T, test testUtils.TestCase) {
	ExecuteTestCase(
		t,
		testUtils.TestCase{
			SupportedMutationTypes: test.SupportedMutationTypes,
			SupportedClientTypes:   test.SupportedClientTypes,
			Actions: append(
				[]any{
					&action.AddSchema{
						Schema: userCollectionGQLSchema,
					},
				},
				test.Actions...,
			),
		},
	)
}

// TestQuerySimple tests basic document creation and querying.
// Ported from: tests/integration/query/simple/simple_test.go
func TestQuerySimple(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						Name
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
							"Age":  int64(21),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// TestQuerySimpleWithAlias tests querying with field aliases.
func TestQuerySimpleWithAlias(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						username: Name
						age: Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"username": "John",
							"age":      int64(21),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// TestQuerySimpleWithMultipleRows tests querying multiple documents.
func TestQuerySimpleWithMultipleRows(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 27
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						Name
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
							"Age":  int64(21),
						},
						{
							"Name": "Bob",
							"Age":  int64(27),
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// TestQuerySimpleWithUndefinedField tests error handling for undefined fields.
// BUG: Rust returns empty results for undefined fields instead of an error.
// Go DefraDB returns: "Cannot query field \"ThisFieldDoesNotExists\" on type \"Users\"."
func TestQuerySimpleWithUndefinedField(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.Request{
				Request: `query {
					Users {
						Name
						ThisFieldDoesNotExists
					}
				}`,
				ExpectedError: "Cannot query field",
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// TestQuerySimpleWithSomeDefaultValues tests querying documents with null fields.
func TestQuerySimpleWithSomeDefaultValues(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						Name
						Email
						Age
						HeightM
						Verified
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name":     "John",
							"Email":    nil,
							"Age":      nil,
							"HeightM":  nil,
							"Verified": nil,
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// TestQuerySimpleWithDefaultValue tests querying an empty document.
func TestQuerySimpleWithDefaultValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{ }`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						Name
						Email
						Age
						HeightM
						Verified
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name":     nil,
							"Email":    nil,
							"Age":      nil,
							"HeightM":  nil,
							"Verified": nil,
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// TestQuerySimpleWithFilter tests basic filter queries.
func TestQuerySimpleWithFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Age: {_gt: 25}}) {
						Name
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
							"Age":  int64(32),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// TestQuerySimpleWithFilterEq tests equality filter.
func TestQuerySimpleWithFilterEq(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_eq: "John"}}) {
						Name
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
							"Age":  int64(21),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// TestQuerySimpleWithLimit tests limit queries.
func TestQuerySimpleWithLimit(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Alice",
					"Age": 19
				}`,
			},
			// Query with limit and order to get deterministic results
			testUtils.Request{
				Request: `query {
					Users(limit: 2, order: {Age: ASC}) {
						Name
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"Name": "Alice", "Age": int64(19)},
						{"Name": "John", "Age": int64(21)},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// TestQuerySimpleWithOrderAsc tests ascending order.
func TestQuerySimpleWithOrderAsc(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 32
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 21
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(order: {Age: ASC}) {
						Name
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
							"Age":  int64(21),
						},
						{
							"Name": "John",
							"Age":  int64(32),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// TestQuerySimpleWithOrderDesc tests descending order.
func TestQuerySimpleWithOrderDesc(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(order: {Age: DESC}) {
						Name
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
							"Age":  int64(32),
						},
						{
							"Name": "John",
							"Age":  int64(21),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// TestQuerySimpleWithBooleanField tests boolean fields.
func TestQuerySimpleWithBooleanField(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Verified": true
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Verified": false
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Verified: {_eq: true}}) {
						Name
						Verified
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name":     "John",
							"Verified": true,
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// TestQuerySimpleWithFloatField tests float fields.
func TestQuerySimpleWithFloatField(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						Name
						HeightM
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name":    "John",
							"HeightM": 1.82,
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// TestQuerySimpleWithStringField tests string fields.
func TestQuerySimpleWithStringField(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Email": "john@example.com"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						Name
						Email
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name":  "John",
							"Email": "john@example.com",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// TestQuerySimpleWithAllFields tests querying all field types.
func TestQuerySimpleWithAllFields(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Email": "john@example.com",
					"Age": 25,
					"HeightM": 1.75,
					"Verified": true
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						Name
						Email
						Age
						HeightM
						Verified
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name":     "John",
							"Email":    "john@example.com",
							"Age":      int64(25),
							"HeightM":  1.75,
							"Verified": true,
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// TestQuerySimple_WithDeletedDocsInCollection2_ShouldNotYieldDeletedDocsOnCollection1Query
// tests that deleted docs from one collection don't affect queries on another collection.
// Ported from: tests/integration/query/simple/simple_test.go
func TestQuerySimple_WithDeletedDocsInCollection2(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type User {
						name: String
					}
					type Friend {
						name: String
					}
				`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name": "Shahzad",
				},
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
					"name": "Andy",
				},
			},
			testUtils.Request{
				Request: `query {
					User {
						name
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "Shahzad"},
						{"name": "John"},
					},
				},
				NonOrderedResults: true,
			},
			testUtils.DeleteDoc{
				CollectionID: 1,
				DocID:        0,
			},
			testUtils.Request{
				Request: `query {
					User {
						name
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "Shahzad"},
						{"name": "John"},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	// Note: This test uses its own schema, so we use ExecuteTestCase directly
	ExecuteTestCase(t, test)
}

// ============================================================================
// Limit/Offset Tests
// Ported from: tests/integration/query/simple/with_limit_offset_test.go
// ============================================================================

func TestQuerySimpleWithLimit0(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(limit: 0) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
						},
						{
							"Name": "John",
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithLimit1(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"Name": "John",
						"Age": 21
					}`,
			},
			testUtils.CreateDoc{
				Doc: `{
						"Name": "Bob",
						"Age": 32
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users(limit: 1) {
							Name
							Age
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
							"Age":  int64(21),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithLimit2(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"Name": "John",
						"Age": 21
					}`,
			},
			testUtils.CreateDoc{
				Doc: `{
						"Name": "Bob",
						"Age": 32
					}`,
			},
			testUtils.CreateDoc{
				Doc: `{
						"Name": "Carlo",
						"Age": 55
					}`,
			},
			testUtils.CreateDoc{
				Doc: `{
						"Name": "Alice",
						"Age": 19
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users(limit: 2) {
							Name
							Age
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
							"Age":  int64(32),
						},
						{
							"Name": "John",
							"Age":  int64(21),
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithLimitBiggerThanTotalDocuments(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"Name": "John",
						"Age": 21
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users(limit: 3) {
							Name
							Age
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
							"Age":  int64(21),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithOffset0(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"Name": "John",
						"Age": 21
					}`,
			},
			testUtils.CreateDoc{
				Doc: `{
						"Name": "Bob",
						"Age": 32
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users(offset: 0) {
							Name
							Age
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
							"Age":  int64(32),
						},
						{
							"Name": "John",
							"Age":  int64(21),
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithOffset1(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"Name": "John",
						"Age": 21
					}`,
			},
			testUtils.CreateDoc{
				Doc: `{
						"Name": "Bob",
						"Age": 32
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users(offset: 1) {
							Name
							Age
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
							"Age":  int64(32),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithOffset2(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"Name": "John",
						"Age": 21
					}`,
			},
			testUtils.CreateDoc{
				Doc: `{
						"Name": "Bob",
						"Age": 32
					}`,
			},
			testUtils.CreateDoc{
				Doc: `{
						"Name": "Carlo",
						"Age": 55
					}`,
			},
			testUtils.CreateDoc{
				Doc: `{
						"Name": "Alice",
						"Age": 19
					}`,
			},
			testUtils.CreateDoc{
				Doc: `{
						"Name": "Melynda",
						"Age": 30
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users(offset: 2) {
							Name
							Age
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Carlo",
							"Age":  int64(55),
						},
						{
							"Name": "Alice",
							"Age":  int64(19),
						},
						{
							"Name": "Bob",
							"Age":  int64(32),
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithOffsetBiggerThanTotalDocuments(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"Name": "John",
						"Age": 21
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users(offset: 3) {
							Name
							Age
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithLimit0AndOffset0(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"Name": "John",
						"Age": 21
					}`,
			},
			testUtils.CreateDoc{
				Doc: `{
						"Name": "Bob",
						"Age": 32
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users(limit: 0, offset: 0) {
							Name
							Age
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
							"Age":  int64(32),
						},
						{
							"Name": "John",
							"Age":  int64(21),
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithLimit1AndOffset1(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"Name": "John",
						"Age": 21
					}`,
			},
			testUtils.CreateDoc{
				Doc: `{
						"Name": "Bob",
						"Age": 32
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users(limit: 1, offset: 1) {
							Name
							Age
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
							"Age":  int64(32),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithLimit2AndOffset2(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"Name": "John",
						"Age": 21
					}`,
			},
			testUtils.CreateDoc{
				Doc: `{
						"Name": "Bob",
						"Age": 32
					}`,
			},
			testUtils.CreateDoc{
				Doc: `{
						"Name": "Carlo",
						"Age": 55
					}`,
			},
			testUtils.CreateDoc{
				Doc: `{
						"Name": "Alice",
						"Age": 19
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users(limit: 2, offset: 2) {
							Name
							Age
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Carlo",
							"Age":  int64(55),
						},
						{
							"Name": "Alice",
							"Age":  int64(19),
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// ============================================================================
// Order Tests
// Ported from: tests/integration/query/simple/with_order_test.go
// ============================================================================

func TestQuerySimpleWithNumericOrderAscending(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Carlo",
					"Age": 55
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Alice",
					"Age": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(order: {Age: ASC}) {
						Name
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Alice",
							"Age":  int64(19),
						},
						{
							"Name": "John",
							"Age":  int64(21),
						},
						{
							"Name": "Bob",
							"Age":  int64(32),
						},
						{
							"Name": "Carlo",
							"Age":  int64(55),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithNumericOrderDescending(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Carlo",
					"Age": 55
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Alice",
					"Age": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(order: {Age: DESC}) {
						Name
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Carlo",
							"Age":  int64(55),
						},
						{
							"Name": "Bob",
							"Age":  int64(32),
						},
						{
							"Name": "John",
							"Age":  int64(21),
						},
						{
							"Name": "Alice",
							"Age":  int64(19),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithDateTimeOrderAscending(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2021-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2032-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Carlo",
					"Age": 55,
					"CreatedAt": "2055-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Alice",
					"Age": 19,
					"CreatedAt": "2019-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(order: {CreatedAt: ASC}) {
						Name
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Alice",
							"Age":  int64(19),
						},
						{
							"Name": "John",
							"Age":  int64(21),
						},
						{
							"Name": "Bob",
							"Age":  int64(32),
						},
						{
							"Name": "Carlo",
							"Age":  int64(55),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithOrderLimitAndOffset(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Carlo",
					"Age": 55
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Alice",
					"Age": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(order: {Age: ASC}, limit: 2, offset: 1) {
						Name
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
							"Age":  int64(21),
						},
						{
							"Name": "Bob",
							"Age":  int64(32),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}
