// Copyright 2022 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package integration tests DefraDB JSON field query patterns against the Rust FFI implementation.
//
// These tests are ported from DefraDB's tests/integration/query/json/ directory
// and validate behavioral compatibility between Go and Rust implementations.
package integration

import (
	"testing"

	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

// userCollectionGQLSchemaJSON is the schema used by JSON query tests.
var userCollectionGQLSchemaJSON = (`
	type Users {
		name: String
		custom: JSON
	}
`)

// userCollectionGQLSchemaJSONCased is the schema used by JSON query tests (with cased field names).
var userCollectionGQLSchemaJSONCased = (`
	type Users {
		Name: String
		Custom: JSON
	}
`)

// executeJSONTestCase wraps the test with the Users schema for JSON fields.
func executeJSONTestCase(t *testing.T, test testUtils.TestCase) {
	ExecuteTestCase(
		t,
		testUtils.TestCase{
			SupportedMutationTypes: test.SupportedMutationTypes,
			SupportedClientTypes:   test.SupportedClientTypes,
			Actions: append(
				[]any{
					&action.AddSchema{
						Schema: userCollectionGQLSchemaJSON,
					},
				},
				test.Actions...,
			),
		},
	)
}

// executeJSONTestCaseCased wraps the test with the Users schema (cased fields) for JSON.
func executeJSONTestCaseCased(t *testing.T, test testUtils.TestCase) {
	ExecuteTestCase(
		t,
		testUtils.TestCase{
			SupportedMutationTypes: test.SupportedMutationTypes,
			SupportedClientTypes:   test.SupportedClientTypes,
			Actions: append(
				[]any{
					&action.AddSchema{
						Schema: userCollectionGQLSchemaJSONCased,
					},
				},
				test.Actions...,
			),
		},
	)
}

// ====================
// with_eq_test.go tests
// ====================

func TestQueryJSON_WithEqualFilterWithObject_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"custom": {
						"tree": "maple",
						"age": 250
					}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Andy",
					"custom": {
						"tree": "oak",
						"age": 450
					}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"custom": null
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_eq: {tree:"oak",age:450}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Andy"},
					},
				},
			},
		},
	}

	executeJSONTestCase(t, test)
}

func TestQueryJSON_WithCompoundFilterCondition_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Andy",
					"custom": {
						"tree": "maple",
						"age": 450
					}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"custom": {
						"tree": "maple",
						"age": 250
					}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"custom": {
						"tree": "maple",
						"age": 20
					}
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {_and: [
						{custom: {tree: {_eq: "maple"}}},
						{custom: {age: {_eq: 250}}}
					]}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "John"},
					},
				},
			},
		},
	}

	executeJSONTestCase(t, test)
}

func TestQueryJSON_WithEqualFilterWithNestedObjects_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"custom": {
						"level_1": {
							"level_2": {
								"level_3": [true, false]
							}
						}
					}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Andy",
					"custom": {
						"level_1": {
							"level_2": {
								"level_3": [false, true]
							}
						}
					}
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_eq: {level_1: {level_2: {level_3: [true, false]}}}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "John"},
					},
				},
			},
		},
	}

	executeJSONTestCase(t, test)
}

func TestQueryJSON_WithEqualFilterWithNullValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"custom": null
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Andy",
					"custom": {}
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_eq: null}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "John"},
					},
				},
			},
		},
	}

	executeJSONTestCase(t, test)
}

func TestQueryJSON_WithEqualFilterWithAllTypes_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Shahzad",
					"Custom": "32"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Andy",
					"Custom": [1, 2]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Custom": {"one": 1}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": false
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_eq: {one: 1}}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Fred",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

// ====================
// with_ne_test.go tests
// ====================

func TestQueryJSON_WithNotEqualFilterWithObject_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"custom": {
						"tree": "maple",
						"age": 250
					}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Andy",
					"custom": {
						"tree": "oak",
						"age": 450
					}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"custom": null
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_ne: {tree:"oak",age:450}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Shahzad"},
						{"name": "John"},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeJSONTestCase(t, test)
}

func TestQueryJSON_WithNotEqualFilterWithNestedObjects_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"custom": {
						"level_1": {
							"level_2": {
								"level_3": [true, false]
							}
						}
					}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Andy",
					"custom": {
						"level_1": {
							"level_2": {
								"level_3": [false, true]
							}
						}
					}
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_ne: {level_1: {level_2: {level_3: [true, false]}}}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Andy"},
					},
				},
			},
		},
	}

	executeJSONTestCase(t, test)
}

func TestQueryJSON_WithNotEqualFilterWithNullValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"custom": null
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Andy",
					"custom": {}
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_ne: null}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Andy"},
					},
				},
			},
		},
	}

	executeJSONTestCase(t, test)
}

func TestQueryJSON_WithNeFilterAgainstNumberField_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				DocMap: map[string]any{
					"name": "John",
					"custom": map[string]any{
						"age": 48,
					},
				},
			},
			testUtils.CreateDoc{
				DocMap: map[string]any{
					"name": "Andy",
					"custom": map[string]any{
						"age": nil,
					},
				},
			},
			testUtils.CreateDoc{
				DocMap: map[string]any{
					"name": "Shahzad",
					"custom": map[string]any{
						"age": 42,
					},
				},
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {age: {_ne: 48}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Andy"},
						{"name": "Shahzad"},
					},
				},
				NonOrderedResults: true,
			},
		},
	}
	executeJSONTestCase(t, test)
}

func TestQueryJSON_WithNeFilterAgainstStringField_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				DocMap: map[string]any{
					"name": "John",
					"custom": map[string]any{
						"city": "Istanbul",
					},
				},
			},
			testUtils.CreateDoc{
				DocMap: map[string]any{
					"name": "Andy",
					"custom": map[string]any{
						"city": nil,
					},
				},
			},
			testUtils.CreateDoc{
				DocMap: map[string]any{
					"name": "Shahzad",
					"custom": map[string]any{
						"city": "Lucerne",
					},
				},
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {city: {_ne: "Istanbul"}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Shahzad"},
						{"name": "Andy"},
					},
				},
			},
		},
	}
	executeJSONTestCase(t, test)
}

func TestQueryJSON_WithNeFilterAgainstBooleanField_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				DocMap: map[string]any{
					"name": "John",
					"custom": map[string]any{
						"verified": true,
					},
				},
			},
			testUtils.CreateDoc{
				DocMap: map[string]any{
					"name": "Andy",
					"custom": map[string]any{
						"verified": nil,
					},
				},
			},
			testUtils.CreateDoc{
				DocMap: map[string]any{
					"name": "Shahzad",
					"custom": map[string]any{
						"verified": false,
					},
				},
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {verified: {_ne: true}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Shahzad"},
						{"name": "Andy"},
					},
				},
			},
		},
	}
	executeJSONTestCase(t, test)
}

func TestQueryJSON_WithNeFilterAgainstNullField_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				DocMap: map[string]any{
					"name": "John",
					"custom": map[string]any{
						"age": 48,
					},
				},
			},
			testUtils.CreateDoc{
				DocMap: map[string]any{
					"name": "Andy",
					"custom": map[string]any{
						"age": nil,
					},
				},
			},
			testUtils.CreateDoc{
				DocMap: map[string]any{
					"name": "Shahzad",
					"custom": map[string]any{
						"age": 42,
					},
				},
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {age: {_ne: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "John"},
						{"name": "Shahzad"},
					},
				},
				NonOrderedResults: true,
			},
		},
	}
	executeJSONTestCase(t, test)
}

// ====================
// with_gt_test.go tests
// ====================

func TestQueryJSON_WithGreaterThanFilterBlockWithGreaterValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_gt: 20}}) {
						Name
						Custom
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name":   "John",
							"Custom": int64(21),
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterThanFilterBlockWithLesserValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_gt: 22}}) {
						Name
						Custom
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterThanFilterBlockWithNullFilterValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_gt: null}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterThanFilterBlockWithNestedGreaterValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": {"age": 21}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": {"age": 19}
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {age: {_gt: 20}}}) {
						Name
						Custom
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
							"Custom": map[string]any{
								"age": float64(21),
							},
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterThanFilterBlockWithNestedLesserValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": {"age": 21}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": {"age": 19}
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {age: {_gt: 22}}}) {
						Name
						Custom
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterThanFilterBlockWithNestedNullFilterValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": {"age": 21}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {age: {_gt: null}}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterThanFilterBlockWithBoolValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_gt: false}}) {
						Name
						Custom
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: bool`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterThanFilterBlockWithStringValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_gt: ""}}) {
						Name
						Custom
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: string`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterThanFilterBlockWithObjectValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_gt: {one: 1}}}) {
						Name
						Custom
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: map[string]interface {}`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterThanFilterBlockWithArrayValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_gt: [1,2]}}) {
						Name
						Custom
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: []interface {}`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterThanFilterWithAllTypes_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Shahzad",
					"Custom": "32"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Andy",
					"Custom": [1, 2]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Custom": {"one": 1}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": false
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_gt: 30}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "David",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

// ====================
// with_ge_test.go tests
// ====================

func TestQueryJSON_WithGreaterEqualFilterWithEqualValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_ge: 32}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "David",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterEqualFilterWithGreaterValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_ge: 31}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "David",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterEqualFilterWithNullValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_ge: null}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "David",
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

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterEqualFilterWithNestedEqualValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": {"age": 21}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": {"age": 32}
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {age: {_ge: 32}}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "David",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterEqualFilterWithNestedGreaterValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": {"age": 21}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": {"age": 32}
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {age: {_ge: 31}}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "David",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterEqualFilterWithNestedNullValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": {"age": 21}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {age: {_ge: null}}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "David",
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

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterEqualFilterWithBoolValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_ge: true}}) {
						Name
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: bool`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterEqualFilterWithStringValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_ge: ""}}) {
						Name
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: string`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterEqualFilterWithObjectValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_ge: {one: 1}}}) {
						Name
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: map[string]interface {}`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterEqualFilterWithArrayValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_ge: [1, 2]}}) {
						Name
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: []interface {}`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithGreaterEqualFilterWithAllTypes_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Shahzad",
					"Custom": "32"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Andy",
					"Custom": [1, 2]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Custom": {"one": 1}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": false
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_ge: 32}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "David",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

// ====================
// with_lt_test.go tests
// ====================

func TestQueryJSON_WithLesserThanFilterBlockWithGreaterValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Custom": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_lt: 20}}) {
						Name
						Custom
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name":   "Bob",
							"Custom": int64(19),
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserThanFilterBlockWithLesserValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Custom": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_lt: 19}}) {
						Name
						Custom
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserThanFilterBlockWithNullFilterValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_lt: null}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserThanFilterBlockWithNestedGreaterValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": {"age": 21}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Custom": {"age": 19}
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {age: {_lt: 20}}}) {
						Name
						Custom
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
							"Custom": map[string]any{
								"age": float64(19),
							},
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserThanFilterBlockWithNestedLesserValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": {"age": 21}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Custom": {"age": 19}
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {age: {_lt: 19}}}) {
						Name
						Custom
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserThanFilterBlockWithNestedNullFilterValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": {"age": 21}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {age: {_lt: null}}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserThanFilterBlockWithBoolValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Custom": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_lt: false}}) {
						Name
						Custom
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: bool`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserThanFilterBlockWithStringValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Custom": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_lt: ""}}) {
						Name
						Custom
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: string`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserThanFilterBlockWithObjectValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Custom": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_lt: {one: 1}}}) {
						Name
						Custom
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: map[string]interface {}`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserThanFilterBlockWithArrayValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Custom": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_lt: [1,2]}}) {
						Name
						Custom
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: []interface {}`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserThanFilterWithAllTypes_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Shahzad",
					"Custom": "32"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Andy",
					"Custom": [1, 2]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Custom": {"one": 1}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": false
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_lt: 33}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "David",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

// ====================
// with_le_test.go tests
// ====================

func TestQueryJSON_WithLesserEqualFilterWithEqualValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_le: 21}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserEqualFilterWithLesserValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_le: 31}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserEqualFilterWithNullValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_le: null}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "David",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserEqualFilterWithNestedEqualValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": {"age": 21}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": {"age": 32}
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {age: {_le: 21}}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserEqualFilterWithNestedLesserValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": {"age": 21}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": {"age": 32}
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {age: {_le: 31}}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserEqualFilterWithNestedNullValue_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": {"age": 21}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {age: {_le: null}}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "David",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserEqualFilterWithBoolValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_le: true}}) {
						Name
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: bool`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserEqualFilterWithStringValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_le: ""}}) {
						Name
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: string`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserEqualFilterWithObjectValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_le: {one: 1}}}) {
						Name
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: map[string]interface {}`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserEqualFilterWithArrayValue_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": 21
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_le: [1, 2]}}) {
						Name
					}
				}`,
				ExpectedError: `unexpected type. Property: condition, Actual: []interface {}`,
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

func TestQueryJSON_WithLesserEqualFilterWithAllTypes_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Shahzad",
					"Custom": "32"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Andy",
					"Custom": [1, 2]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Custom": {"one": 1}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Custom": false
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "David",
					"Custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Custom: {_le: 32}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "David",
						},
					},
				},
			},
		},
	}

	executeJSONTestCaseCased(t, test)
}

// ====================
// with_in_test.go tests
// ====================

func TestQueryJSON_WithInFilter_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"custom": {
						"tree": "maple",
						"age": 250
					}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Andy",
					"custom": {
						"tree": "oak",
						"age": 450
					}
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_in: [{tree:"oak",age:450}]}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Andy"},
					},
				},
			},
		},
	}

	executeJSONTestCase(t, test)
}

// ====================
// with_nin_test.go tests
// ====================

func TestQueryJSON_WithNotInFilter_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"custom": {
						"tree": "maple",
						"age": 250
					}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Andy",
					"custom": {
						"tree": "oak",
						"age": 450
					}
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_nin: [{tree:"oak",age:450}]}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "John"},
					},
				},
			},
		},
	}

	executeJSONTestCase(t, test)
}

// ====================
// with_like_test.go tests
// ====================

func TestQueryJSON_WithLikeFilter_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				DocMap: map[string]any{
					"custom": "Daenerys Stormborn of House Targaryen, the First of Her Name",
				},
			},
			testUtils.CreateDoc{
				DocMap: map[string]any{
					"custom": "Viserys I Targaryen, King of the Andals",
				},
			},
			testUtils.CreateDoc{
				Doc: `{
					"custom": [1, 2]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"custom": {"one": 1}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"custom": false
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_like: "Daenerys%Name"}}) {
						custom
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"custom": "Daenerys Stormborn of House Targaryen, the First of Her Name",
						},
					},
				},
			},
		},
	}

	executeJSONTestCase(t, test)
}

// ====================
// with_nlike_test.go tests
// ====================

func TestQueryJSON_WithNotLikeFilter_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"custom": "Daenerys Stormborn of House Targaryen, the First of Her Name"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"custom": "Viserys I Targaryen, King of the Andals"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"custom": [1, 2]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"custom": {"one": 1}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"custom": false
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"custom": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_nlike: "%Stormborn%"}}) {
						custom
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"custom": false,
						},
						{
							"custom": "Viserys I Targaryen, King of the Andals",
						},
						{
							"custom": map[string]any{"one": float64(1)},
						},
						{
							"custom": float64(32),
						},
						{
							"custom": []any{float64(1), float64(2)},
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeJSONTestCase(t, test)
}

// ====================
// with_any_test.go tests
// ====================

func TestQueryJSON_WithAnyFilterWithAllTypes_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"custom": [1, false, "second", {"one": 1}, [1, 2]]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"custom": [null, false, "second", {"one": 1}, [1, 2]]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Islam",
					"custom": null
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Keenan",
					"custom": 0
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Andy",
					"custom": ""
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"custom": true
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_any: {_eq: null}}}) {
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

	executeJSONTestCase(t, test)
}

func TestQueryJSON_WithAnyFilterAndNestedArray_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"custom": [1, false, "second", {"one": 1}, [1, 2]]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"custom": [null, false, "second", {"one": 1}, [1, [2, 3]]]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Islam",
					"custom": null
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Keenan",
					"custom": 3
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Bruno",
					"custom": [null, 3]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Andy",
					"custom": ""
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"custom": true
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_any: {_eq: 3}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Bruno"},
					},
				},
			},
		},
	}

	executeJSONTestCase(t, test)
}

// ====================
// with_all_test.go tests
// ====================

func TestQueryJSON_WithAllFilterWithAllTypes_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"custom": [1, false, "second", {"one": 1}, [1, 2]]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"custom": [null, false, "second", {"one": 1}, [1, 2]]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"custom": [false, "second", {"one": 1}, [1, [2, null]]]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Islam",
					"custom": null
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Keenan",
					"custom": 0
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Andy",
					"custom": ""
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"custom": true
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_all: {_ne: null}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Shahzad"},
						{"name": "Fred"},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeJSONTestCase(t, test)
}

func TestQueryJSON_WithAllFilterAndNestedArray_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"custom": [1]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"custom": [1, 2, 1]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Islam",
					"custom": [1, [1, [1]]]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Keenan",
					"custom": 1
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Andy",
					"custom": [1, "1"]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_all: {_eq: 1}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Shahzad"},
					},
				},
			},
		},
	}

	executeJSONTestCase(t, test)
}

// ====================
// with_none_test.go tests
// ====================

func TestQueryJSON_WithNoneFilter_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"custom": [1, false, "second", {"one": 1}, [1, 2]]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"custom": [null, false, "second", {"one": 1}, [1, 2]]
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_none: {_eq: null}}}) {
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

	executeJSONTestCase(t, test)
}

func TestQueryJSON_WithNoneFilterAndNestedArray_ShouldFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"custom": [1, false, "second", {"one": 3}, [1, 3]]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Fred",
					"custom": [null, false, "second", 3, {"one": 1}, [1, 2]]
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Islam",
					"custom": 3
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Bruno",
					"custom": null
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"custom": false
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {custom: {_none: {_eq: 3}}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Shahzad"},
					},
				},
			},
		},
	}

	executeJSONTestCase(t, test)
}

// ====================
// with_aggregate_test.go tests
// ====================

func TestQueryJSON_WithAggregateFilter_Succeeds(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"name": "John",
					"custom": {
						"tree": "maple",
						"age": 250
					}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Andy",
					"custom": {
						"tree": "oak",
						"age": 450
					}
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"name": "Shahzad",
					"custom": null
				}`,
			},
			testUtils.Request{
				Request: `query {
					_count(Users: {filter: {custom: {tree: {_eq: "oak"}}}})
				}`,
				Results: map[string]any{
					"_count": 1,
				},
			},
		},
	}

	executeJSONTestCase(t, test)
}
