// Copyright 2022 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package integration tests DefraDB inline array query patterns against the Rust FFI implementation.
//
// These tests are ported from DefraDB's tests/integration/query/inline_array/ directory
// and validate behavioral compatibility between Go and Rust implementations.
package integration

import (
	"testing"

	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

// userCollectionGQLSchemaInlineArray is the schema used by inline array query tests.
// Matches DefraDB's tests/integration/query/inline_array/utils.go
var userCollectionGQLSchemaInlineArray = (`
	type Users {
		name: String
		likedIndexes: [Boolean!]
		indexLikesDislikes: [Boolean]
		favouriteIntegers: [Int!]
		testScores: [Int]
		favouriteFloats: [Float!]
		pageRatings: [Float]
		preferredStrings: [String!]
		pageHeaders: [String]
	}
`)

// executeInlineArrayTestCase wraps the test with the Users schema for inline arrays.
func executeInlineArrayTestCase(t *testing.T, test testUtils.TestCase) {
	ExecuteTestCase(
		t,
		testUtils.TestCase{
			SupportedMutationTypes: test.SupportedMutationTypes,
			SupportedClientTypes:   test.SupportedClientTypes,
			Actions: append(
				[]any{
					&action.AddSchema{
						Schema: userCollectionGQLSchemaInlineArray,
					},
				},
				test.Actions...,
			),
		},
	)
}

// ====================
// simple_test.go tests
// ====================

func TestQueryInlineArrayWithBooleans_Null(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"name": "John",
						"likedIndexes": null
					}`,
			},
			testUtils.Request{
				Request: `query {
			 			Users {
			 				name
			 				likedIndexes
			 			}
			 		}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":         "John",
							"likedIndexes": nil,
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithBooleans_EmptyList(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"name": "John",
						"likedIndexes": []
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users {
							name
							likedIndexes
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":         "John",
							"likedIndexes": []any{},
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithBooleans_NotEmpty(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"name": "John",
						"likedIndexes": [true, true, false, true]
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users {
							name
							likedIndexes
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":         "John",
							"likedIndexes": []any{true, true, false, true},
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithNillableBooleans(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"indexLikesDislikes": [true, true, false, null]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
						indexLikesDislikes
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":               "John",
							"indexLikesDislikes": []any{true, true, false, nil},
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithIntegers_Missing(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"name": "John"
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users {
							name
							favouriteIntegers
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":              "John",
							"favouriteIntegers": nil,
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithIntegers_Null(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"name": "John",
						"favouriteIntegers": null
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users {
							name
							favouriteIntegers
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":              "John",
							"favouriteIntegers": nil,
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithIntegers_EmptyList(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"name": "John",
						"favouriteIntegers": []
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users {
							name
							favouriteIntegers
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":              "John",
							"favouriteIntegers": []any{},
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithIntegers_NotEmptyList(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"name": "John",
						"favouriteIntegers": [1, 2, 3, 5, 8]
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users {
							name
							favouriteIntegers
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":              "John",
							"favouriteIntegers": []any{int64(1), int64(2), int64(3), int64(5), int64(8)},
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithNegativeIntegers_NotEmptyList(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"name": "Andy",
						"favouriteIntegers": [-1, -2, -3, -5, -8]
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users {
							name
							favouriteIntegers
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":              "Andy",
							"favouriteIntegers": []any{int64(-1), int64(-2), int64(-3), int64(-5), int64(-8)},
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithMixIntegers_NotEmptyList(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"name": "Shahzad",
						"favouriteIntegers": [-1, 2, -1, 1, 0]
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users {
							name
							favouriteIntegers
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":              "Shahzad",
							"favouriteIntegers": []any{int64(-1), int64(2), int64(-1), int64(1), int64(0)},
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithNillableInts(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"testScores": [-1, null, -1, 2, 0]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
						testScores
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":       "John",
							"testScores": []any{int64(-1), nil, int64(-1), int64(2), int64(0)},
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithFloats_Null(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"name": "John",
						"favouriteFloats": null
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users {
							name
							favouriteFloats
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":            "John",
							"favouriteFloats": nil,
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithFloats_EmptyList(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"name": "John",
						"favouriteFloats": []
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users {
							name
							favouriteFloats
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":            "John",
							"favouriteFloats": []any{},
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithFloats_NotEmpty(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"name": "John",
						"favouriteFloats": [3.1425, 0.00000000001, 10]
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users {
							name
							favouriteFloats
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":            "John",
							"favouriteFloats": []any{3.1425, 0.00000000001, float64(10)},
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithNillableFloats(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"pageRatings": [3.1425, null, -0.00000000001, 10]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
						pageRatings
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":        "John",
							"pageRatings": []any{3.1425, nil, -0.00000000001, float64(10)},
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithStrings_Null(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"name": "John",
						"preferredStrings": null
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users {
							name
							preferredStrings
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":             "John",
							"preferredStrings": nil,
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithStrings_EmptyList(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"name": "John",
						"preferredStrings": []
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users {
							name
							preferredStrings
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":             "John",
							"preferredStrings": []any{},
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithStrings_NotEmpty(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
						"name": "John",
						"preferredStrings": ["", "the previous", "the first", "empty string"]
					}`,
			},
			testUtils.Request{
				Request: `query {
						Users {
							name
							preferredStrings
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":             "John",
							"preferredStrings": []any{"", "the previous", "the first", "empty string"},
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineArrayWithNillableString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"pageHeaders": ["", "the previous", "the first", "empty string", null]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users {
						name
						pageHeaders
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name":        "John",
							"pageHeaders": []any{"", "the previous", "the first", "empty string", nil},
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

// ====================
// with_filter_any_test.go tests
// ====================

func TestQueryInlineStringArray_WithAnyFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"pageHeaders": ["first", "second"]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"pageHeaders": [null, "second"]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {pageHeaders: {_any: {_eq: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Fred",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineNotNullStringArray_WithAnyFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"preferredStrings": ["first", "second"]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"preferredStrings": ["", "second"]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {preferredStrings: {_any: {_eq: ""}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Fred",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineIntArray_WithAnyFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"testScores": [50, 80]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"testScores": [null, 60]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {testScores: {_any: {_eq: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Fred",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineNotNullIntArray_WithAnyFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"testScores": [50, 80]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"testScores": [0, 60]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {testScores: {_any: {_gt: 70}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Shahzad",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineFloatArray_WithAnyFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"pageRatings": [50, 80]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"pageRatings": [null, 60]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {pageRatings: {_any: {_eq: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Fred",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineNotNullFloatArray_WithAnyFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"pageRatings": [50, 80]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"pageRatings": [0, 60]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {pageRatings: {_any: {_gt: 70}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Shahzad",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineBooleanArray_WithAnyFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"indexLikesDislikes": [false, false]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"indexLikesDislikes": [null, true]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {indexLikesDislikes: {_any: {_eq: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Fred",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineNotNullBooleanArray_WithAnyFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"likedIndexes": [false, false]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"likedIndexes": [true, true]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {likedIndexes: {_any: {_eq: true}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Fred",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineStringArray_WithAnyFilterAndNullValue_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Islam",
					"pageHeaders": null
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {pageHeaders: {_any: {_eq: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

// ====================
// with_filter_all_test.go tests
// ====================

func TestQueryInlineStringArray_WithAllFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"pageHeaders": ["first", "second"]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"pageHeaders": [null, "second"]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {pageHeaders: {_all: {_ne: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Shahzad",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineNotNullStringArray_WithAllFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"preferredStrings": ["first", "second"]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"preferredStrings": ["", "second"]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {preferredStrings: {_all: {_ne: ""}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Shahzad",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineIntArray_WithAllFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"testScores": [50, 80]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"testScores": [null, 60]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {testScores: {_all: {_ne: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Shahzad",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineNotNullIntArray_WithAllFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"testScores": [50, 80]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"testScores": [0, 60]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {testScores: {_all: {_lt: 70}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Fred",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineFloatArray_WithAllFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"pageRatings": [50, 80]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"pageRatings": [null, 60]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {pageRatings: {_all: {_ne: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Shahzad",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineNotNullFloatArray_WithAllFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"pageRatings": [50, 80]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"pageRatings": [0, 60]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {pageRatings: {_all: {_lt: 70}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Fred",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineBooleanArray_WithAllFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"indexLikesDislikes": [false, false]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"indexLikesDislikes": [null, true]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {indexLikesDislikes: {_all: {_ne: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Shahzad",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineNotNullBooleanArray_WithAllFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"likedIndexes": [false, false]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"likedIndexes": [true, true]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {likedIndexes: {_all: {_eq: true}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Fred",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineStringArray_WithAllFilterAndNullValue_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Islam",
					"pageHeaders": null
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {pageHeaders: {_all: {_eq: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

// ====================
// with_filter_none_test.go tests
// ====================

func TestQueryInlineStringArrayWithNoneFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"pageHeaders": ["first", "second"]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"pageHeaders": [null, "second"]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {pageHeaders: {_none: {_eq: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Shahzad",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineNonNullStringArrayWithNoneFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"preferredStrings": ["first", "second"]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"preferredStrings": ["", "second"]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {preferredStrings: {_none: {_eq: ""}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Shahzad",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineIntArrayWithNoneFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"testScores": [50, 80]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"testScores": [null, 60]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {testScores: {_none: {_eq: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Shahzad",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineNonNullIntArrayWithNoneFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"testScores": [50, 80]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"testScores": [0, 60]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {testScores: {_none: {_gt: 70}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Fred",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineFloatArrayWithNoneFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"pageRatings": [50, 80]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"pageRatings": [null, 60]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {pageRatings: {_none: {_eq: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Shahzad",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineNonNullFloatArrayWithNoneFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"pageRatings": [50, 80]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"pageRatings": [0, 60]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {pageRatings: {_none: {_gt: 70}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Fred",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineBooleanArrayWithNoneFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"indexLikesDislikes": [false, false]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"indexLikesDislikes": [null, true]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {indexLikesDislikes: {_none: {_eq: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Shahzad",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}

func TestQueryInlineNonNullBooleanArrayWithNoneFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"likedIndexes": [false, false]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"likedIndexes": [true, true]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {likedIndexes: {_none: {_ne: true}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "Fred",
						},
					},
				},
			},
		},
	}

	executeInlineArrayTestCase(t, test)
}
