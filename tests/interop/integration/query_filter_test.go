// Package integration tests DefraDB filter query patterns against the Rust FFI implementation.
//
// These tests are ported from DefraDB's tests/integration/query/simple/with_filter/ directory
// and validate behavioral compatibility between Go and Rust implementations.
package integration

import (
	"testing"

	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

// ============================================================================
// Integer Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_eq_int_test.go
// ============================================================================

func TestQuerySimpleWithIntEqualsFilterBlock(t *testing.T) {
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
					Users(filter: {Age: {_eq: 21}}) {
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

func TestQuerySimpleWithIntEqualsNilFilterBlock(t *testing.T) {
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
					"Name": "Fred"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Age: {_eq: null}}) {
						Name
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Fred",
							"Age":  nil,
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// ============================================================================
// String Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_eq_string_test.go
// ============================================================================

func TestQuerySimpleWithStringFilterBlock(t *testing.T) {
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

func TestQuerySimpleWithStringEqualsNilFilterBlock(t *testing.T) {
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
					"Age": 60
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_eq: null}}) {
						Name
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": nil,
							"Age":  int64(60),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithStringFilterBlockAndSelect_SelectSameFieldAsFilterWithMatch(t *testing.T) {
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

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithStringFilterBlockAndSelect_SelectDifferentFieldThanFilterWithMatch(t *testing.T) {
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
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Age": int64(21),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithStringFilterBlockAndSelect_SelectMultipleFieldsButNoMatch(t *testing.T) {
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
					Users(filter: {Name: {_eq: "Bob"}}) {
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

// ============================================================================
// LIKE String Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_like_string_test.go
// ============================================================================

func TestQuerySimpleWithLikeStringContainsFilterBlockContainsString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_like: "%Stormborn%"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimple_WithCaseInsensitiveLike_ShouldMatchString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_ilike: "%stormborn%"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithLikeStringContainsFilterBlockAsPrefixString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_like: "Viserys%"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Viserys I Targaryen, King of the Andals",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimple_WithCaseInsensitiveLikeString_ShouldMatchPrefixString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_ilike: "viserys%"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Viserys I Targaryen, King of the Andals",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithLikeStringContainsFilterBlockAsSuffixString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_like: "%Andals"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Viserys I Targaryen, King of the Andals",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimple_WithCaseInsensitiveLikeString_ShouldMatchSuffixString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_ilike: "%andals"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Viserys I Targaryen, King of the Andals",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithLikeStringContainsFilterBlockExactString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_like: "Daenerys Stormborn of House Targaryen, the First of Her Name"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimple_WithCaseInsensitiveLikeString_ShouldMatchExactString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_ilike: "daenerys stormborn of house targaryen, the first of her name"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithLikeStringContainsFilterBlockContainsStringMultipleResults(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_like: "%Targaryen%"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
						},
						{
							"Name": "Viserys I Targaryen, King of the Andals",
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithLikeStringContainsFilterBlockHasStartAndEnd(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_like: "Daenerys%Name"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithLikeStringContainsFilterBlockHasBoth(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {_and: [{Name: {_like: "%Baratheon%"}}, {Name: {_like: "%Stormborn%"}}]}) {
						Name
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

func TestQuerySimpleWithLikeStringContainsFilterBlockHasEither(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {_or: [{Name: {_like: "%Baratheon%"}}, {Name: {_like: "%Stormborn%"}}]}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithLikeStringContainsFilterBlockPropNotSet(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"HeightM": 1.92
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_like: "%King%"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Viserys I Targaryen, King of the Andals",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// ============================================================================
// AND Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_and_test.go
// ============================================================================

func TestQuerySimpleWithIntGreaterThanAndIntLessThanFilter(t *testing.T) {
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
					Users(filter: {_and: [{Age: {_gt: 20}}, {Age: {_lt: 50}}]}) {
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

// ============================================================================
// OR Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_or_test.go
// ============================================================================

func TestQuerySimpleWithIntEqualToXOrYFilter(t *testing.T) {
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
					Users(filter: {_or: [{Age: {_eq: 55}}, {Age: {_eq: 19}}]}) {
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
							"Name": "Carlo",
							"Age":  int64(55),
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
// Order Tests (additional)
// Ported from: tests/integration/query/simple/with_order_test.go
// ============================================================================

func TestQuerySimpleWithEmptyOrder(t *testing.T) {
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
			testUtils.Request{
				Request: `query {
					Users(order: {}) {
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
						{
							"Name": "Carlo",
							"Age":  int64(55),
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithDateTimeOrderDescending(t *testing.T) {
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
					Users(order: {CreatedAt: DESC}) {
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

func TestQuerySimpleWithFloatOrderAscending(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"HeightM": 1.82
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Carlo",
					"HeightM": 1.91
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Alice",
					"HeightM": 1.55
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(order: {HeightM: ASC}) {
						Name
						HeightM
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name":    "Alice",
							"HeightM": 1.55,
						},
						{
							"Name":    "Bob",
							"HeightM": 1.65,
						},
						{
							"Name":    "John",
							"HeightM": 1.82,
						},
						{
							"Name":    "Carlo",
							"HeightM": 1.91,
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithFloatOrderDescending(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"HeightM": 1.82
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Carlo",
					"HeightM": 1.91
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Alice",
					"HeightM": 1.55
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(order: {HeightM: DESC}) {
						Name
						HeightM
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name":    "Carlo",
							"HeightM": 1.91,
						},
						{
							"Name":    "John",
							"HeightM": 1.82,
						},
						{
							"Name":    "Bob",
							"HeightM": 1.65,
						},
						{
							"Name":    "Alice",
							"HeightM": 1.55,
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// ============================================================================
// Error Cases
// ============================================================================

func TestQuerySimple_WithInvalidOrderEnum_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.Request{
				Request: `query {
					Users(order: {Age: INVALID}) {
						Name
						Age
					}
				}`,
				ExpectedError: `Argument "order" has invalid value`,
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimple_WithMultipleOrderFields_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.Request{
				Request: `query {
					Users(order: {Age: ASC, Name: DESC}) {
						Name
						Age
					}
				}`,
				ExpectedError: "each order argument can only define one field",
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// ============================================================================
// Integer Not Equals Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_ne_int_test.go
// ============================================================================

func TestQuerySimpleWithIntNotEqualsFilterBlock(t *testing.T) {
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
					Users(filter: {Age: {_ne: 21}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithIntNotEqualsNilFilterBlock(t *testing.T) {
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
					"Name": "Fred"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Age: {_ne: null}}) {
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

// ============================================================================
// Integer Greater Than Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_gt_int_test.go
// ============================================================================

func TestQuerySimpleWithIntGreaterThanFilterBlock_ReturnOneAsOneMatches(t *testing.T) {
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
					"Age": 19
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Age: {_gt: 20}}) {
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

func TestQuerySimpleWithIntGreaterThanFilterBlock_ReturnNoneAsNoMatch(t *testing.T) {
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
					Users(filter: {Age: {_gt: 40}}) {
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

func TestQuerySimpleWithIntGreaterThanFilterBlock_ReturnAllMultiMatches(t *testing.T) {
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
					Users(filter: {Age: {_gt: 20}}) {
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

func TestQuerySimpleWithIntGreaterThanFilterBlockWithNullFilterValue(t *testing.T) {
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
					"Name": "Bob"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Age: {_gt: null}}) {
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

	executeSimpleTestCase(t, test)
}

// ============================================================================
// Integer Less Than Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_lt_int_test.go
// ============================================================================

func TestQuerySimpleWithIntLessThanFilterBlockWithGreaterValue(t *testing.T) {
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
					Users(filter: {Age: {_lt: 22}}) {
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

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithIntLessThanFilterBlockWithNullValue(t *testing.T) {
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
					"Name": "Bob"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Age: {_lt: null}}) {
						Name
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

// ============================================================================
// Integer Greater Than or Equal Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_ge_int_test.go
// ============================================================================

func TestQuerySimpleWithIntGEFilterBlockWithEqualValue(t *testing.T) {
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
					Users(filter: {Age: {_ge: 32}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithIntGEFilterBlockWithGreaterValue(t *testing.T) {
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
					Users(filter: {Age: {_ge: 31}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithIntGEFilterBlockWithNilValue(t *testing.T) {
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
					"Name": "Bob"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Age: {_ge: null}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
						},
						{
							"Name": "Bob",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// ============================================================================
// Integer Less Than or Equal Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_le_int_test.go
// ============================================================================

func TestQuerySimpleWithIntLEFilterBlockWithEqualValue(t *testing.T) {
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
					Users(filter: {Age: {_le: 21}}) {
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

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithIntLEFilterBlockWithGreaterValue(t *testing.T) {
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
					Users(filter: {Age: {_le: 22}}) {
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

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithIntLEFilterBlockWithNullValue(t *testing.T) {
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
					"Name": "Bob"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Age: {_le: null}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// ============================================================================
// IN Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_in_test.go
// ============================================================================

func TestQuerySimpleWithIntInFilter(t *testing.T) {
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
					Users(filter: {Age: {_in: [19, 40, 55]}}) {
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

func TestQuerySimpleWithIntInFilterOnFloat(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"HeightM": 21.0
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"HeightM": 21.1
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Carlo",
					"HeightM": 21.2
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Alice",
					"HeightM": 21.3
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {HeightM: {_in: [21, 21.2]}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
						},
						{
							"Name": "Carlo",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithIntInFilterWithNullValue(t *testing.T) {
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
					"Name": "Fred"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Age: {_in: [19, 40, 55, null]}}) {
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
							"Name": "Fred",
							"Age":  nil,
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
// NOT IN Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_nin_test.go
// ============================================================================

func TestQuerySimpleWithNotInFilter(t *testing.T) {
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
					"Name": "Fred"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Age: {_nin: [19, 40, 55, null]}}) {
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

// ============================================================================
// NOT Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_not_test.go
// ============================================================================

func TestQuerySimple_WithNotEqualToXFilter_NoError(t *testing.T) {
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
					Users(filter: {_not: {Age: {_eq: 55}}}) {
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
							"Name": "Alice",
							"Age":  int64(19),
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

func TestQuerySimple_WithNotAndComparisonXFilter_NoError(t *testing.T) {
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
					Users(filter: {_not: {Age: {_gt: 20}}}) {
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
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimple_WithNotEqualToXorYFilter_NoError(t *testing.T) {
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
					Users(filter: {_not: {_or: [{Age: {_eq: 55}}, {Name: {_eq: "Alice"}}]}}) {
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

func TestQuerySimple_WithEmptyNotFilter_ReturnError(t *testing.T) {
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
					Users(filter: {_not: {}}) {
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

func TestQuerySimple_WithNotEqualToXAndNotYFilter_NoError(t *testing.T) {
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
					"Name": "Frank",
					"Age": 55
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {_not: {Age: {_eq: 55}, _not: {Name: {_eq: "Carlo"}}}}) {
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
							"Name": "Alice",
							"Age":  int64(19),
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

// ============================================================================
// String Not Equals Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_ne_string_test.go
// ============================================================================

func TestQuerySimpleWithStringNotEqualsFilterBlock(t *testing.T) {
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
					Users(filter: {Name: {_ne: "John"}}) {
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Age": int64(32),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithStringNotEqualsNilFilterBlock(t *testing.T) {
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
					"Age": 36
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_ne: null}}) {
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Age": int64(32),
						},
						{
							"Age": int64(21),
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
// Float Greater Than Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_gt_float_test.go
// ============================================================================

func TestQuerySimpleWithFloatGreaterThanFilterBlock_OneMatchingResult(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"HeightM": 2.1
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {HeightM: {_gt: 2.0999999999999}}) {
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

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithFloatGreaterThanFilterBlock_NoMatchingResult(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"HeightM": 2.1
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {HeightM: {_gt: 40}}) {
						Name
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

func TestQuerySimpleWithFloatGreaterThanFilterBlock_AllMatchingResult(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"HeightM": 2.1
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {HeightM: {_gt: 1.8199999999999}}) {
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

func TestQuerySimpleWithFloatGreaterThanFilterBlockWithIntFilterValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"HeightM": 2.1
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {HeightM: {_gt: 2}}) {
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

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithFloatGreaterThanFilterBlockWithNullFilterValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"HeightM": 2.1
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {HeightM: {_gt: null}}) {
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

	executeSimpleTestCase(t, test)
}

// ============================================================================
// Float Less Than Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_lt_float_test.go
// ============================================================================

func TestQuerySimpleWithFloatLessThanFilterBlockWithGreaterValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"HeightM": 2.1
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {HeightM: {_lt: 1.820000000001}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithFloatLessThanFilterBlockWithGreaterIntValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"HeightM": 2.1
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {HeightM: {_lt: 2}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithFloatLessThanFilterBlockWithNullValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"HeightM": 2.1
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {HeightM: {_lt: null}}) {
						Name
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

// ============================================================================
// Not Like String Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_nlike_string_test.go
// ============================================================================

func TestQuerySimpleWithNotLikeStringContainsFilterBlockContainsString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_nlike: "%Stormborn%"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Viserys I Targaryen, King of the Andals",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimple_WithNotCaseInsensitiveLikeString_ShouldMatchString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_nilike: "%stormborn%"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Viserys I Targaryen, King of the Andals",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithNotLikeStringContainsFilterBlockAsPrefixString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_nlike: "Viserys%"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimple_WithNotCaseInsensitiveLikeString_ShouldMatchPrefixString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_nilike: "viserys%"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithNotLikeStringContainsFilterBlockAsSuffixString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_nlike: "%Andals"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimple_WithNotCaseInsensitiveLikeString_ShouldMatchSuffixString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_nilike: "%andals"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithNotLikeStringContainsFilterBlockExactString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_nlike: "Daenerys Stormborn of House Targaryen, the First of Her Name"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Viserys I Targaryen, King of the Andals",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimple_WithNotCaseInsensitiveLikeString_MatchExactString(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_nilike: "daenerys stormborn of house targaryen, the first of her name"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Viserys I Targaryen, King of the Andals",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithNotLikeStringContainsFilterBlockContainsStringMuplitpleResults(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_nlike: "%Targaryen%"}}) {
						Name
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

func TestQuerySimpleWithNotLikeStringContainsFilterBlockHasStartAndEnd(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_nlike: "Daenerys%Name"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Viserys I Targaryen, King of the Andals",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithNotLikeStringContainsFilterBlockHasBoth(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {_and: [{Name: {_nlike: "%Baratheon%"}}, {Name: {_nlike: "%Stormborn%"}}]}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Viserys I Targaryen, King of the Andals",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithNotLikeStringContainsFilterBlockHasEither(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {_or: [{Name: {_nlike: "%Baratheon%"}}, {Name: {_nlike: "%Stormborn%"}}]}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
						},
						{
							"Name": "Viserys I Targaryen, King of the Andals",
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithNotLikeStringContainsFilterBlockPropNotSet(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
					"HeightM": 1.65
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Viserys I Targaryen, King of the Andals",
					"HeightM": 1.82
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"HeightM": 1.92
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Name: {_nlike: "%King%"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": nil,
						},
						{
							"Name": "Daenerys Stormborn of House Targaryen, the First of Her Name",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// ============================================================================
// Float Equals Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_eq_float_test.go
// ============================================================================

func TestQuerySimpleWithFloatEqualsFilterBlock(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"HeightM": 2.1
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"HeightM": 1.82
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {HeightM: {_eq: 2.1}}) {
						Name
						HeightM
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name":    "John",
							"HeightM": float64(2.1),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithFloatEqualsNilFilterBlock(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"HeightM": 2.1
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"HeightM": 1.82
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {HeightM: {_eq: null}}) {
						Name
						HeightM
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name":    "Fred",
							"HeightM": nil,
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// ============================================================================
// DocID Filter Tests
// Ported from: tests/integration/query/simple/with_doc_id_test.go
// ============================================================================

func TestQuerySimpleWithDocIDFilter_TargetNotFound(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21
				}`,
			},
			testUtils.Request{
				// Use a valid UUID format but non-existent docID
				Request: `query {
					Users(docID: "bae-52b9170d-b77a-5887-b877-cbdbb99b0090") {
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

func TestQuerySimpleWithDocIDFilter_SingleDocumentTargetFound(t *testing.T) {
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
					Users(docID: "bae-619ea0d2-35ba-5e8c-ac4d-2b769937213b") {
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

func TestQuerySimpleWithDocIDFilter_MultipleDocumentsTargetFound(t *testing.T) {
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
					Users(docID: "bae-619ea0d2-35ba-5e8c-ac4d-2b769937213b") {
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

// ============================================================================
// DateTime Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_eq_datetime_test.go
// ============================================================================

func TestQuerySimpleWithDateTimeEqualsFilterBlock(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2016-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_eq: "2017-07-23T03:46:56-05:00"}}) {
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

func TestQuerySimpleWithDateTimeEqualsNilFilterBlock(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2016-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Age": 44
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_eq: null}}) {
						Name
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Fred",
							"Age":  int64(44),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimple_WithNilDateTimeEqualsAndNonNilFilterBlock_ShouldSucceed(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2016-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Age": 44
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_eq: "2016-07-23T03:46:56-05:00"}}) {
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

// ============================================================================
// DateTime Not Equals Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_neq_datetime_test.go
// ============================================================================

func TestQuerySimpleWithDateTimeNotEqualsFilterBlock(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2011-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_neq: "2017-07-23T03:46:56-05:00"}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithDateTimeNotEqualsNilFilterBlock(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2011-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Age": 32
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_neq: null}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
						},
						{
							"Name": "Bob",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimple_WithNilDateTimeNotEqualAndNonNilFilterBlock_ShouldSucceed(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2016-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Age": 44
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_neq: "2016-07-23T03:46:56-05:00"}}) {
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
							"Name": "Fred",
							"Age":  int64(44),
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

// ============================================================================
// DateTime Greater Than Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_gt_datetime_test.go
// ============================================================================

func TestQuerySimpleWithDateTimeGTFilterBlockWithEqualValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2010-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_gt: "2017-07-20T03:46:56-05:00"}}) {
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

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithDateTimeGTFilterBlockWithGreaterValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2010-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_gt: "2017-07-22T03:46:56-05:00"}}) {
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

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithDateTimeGTFilterBlockWithLesserValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2010-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_gt: "2017-07-25T03:46:56-05:00"}}) {
						Name
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

func TestQuerySimpleWithDateTimeGTFilterBlockWithNilValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"CreatedAt": "2010-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_gt: null}}) {
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

	executeSimpleTestCase(t, test)
}

func TestQuerySimple_WithNilDateTimeGTAndNonNilFilterBlock_ShouldSucceed(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2016-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Age": 44
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_gt: "2016-07-23T03:46:56-05:00"}}) {
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

// ============================================================================
// DateTime Less Than Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_lt_datetime_test.go
// ============================================================================

func TestQuerySimpleWithDateTimeLTFilterBlockWithGreaterValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2019-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_lt: "2017-07-25T03:46:56-05:00"}}) {
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

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithDateTimeLTFilterBlockWithNullValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
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
					Users(filter: {CreatedAt: {_lt: null}}) {
						Name
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

func TestQuerySimple_WithNilDateTimeLTAndNonNilFilterBlock_ShouldSucceed(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2016-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Age": 44
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_lt: "2017-07-23T03:46:56-05:00"}}) {
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

// ============================================================================
// DateTime Greater Than or Equal Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_geq_datetime_test.go
// ============================================================================

func TestQuerySimpleWithDateTimeGEFilterBlockWithEqualValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2010-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_ge: "2017-07-23T03:46:56-05:00"}}) {
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

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithDateTimeGEFilterBlockWithGreaterValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2010-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_ge: "2017-07-22T03:46:56-05:00"}}) {
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

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithDateTimeGEFilterBlockWithLesserValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2010-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_ge: "2017-07-25T03:46:56-05:00"}}) {
						Name
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

func TestQuerySimpleWithDateTimeGEFilterBlockWithNilValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"CreatedAt": "2010-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_ge: null}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
						},
						{
							"Name": "Bob",
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimple_WithNilDateTimeGEAndNonNilFilterBlock_ShouldSucceed(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2016-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Age": 44
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_ge: "2016-07-23T03:46:56-05:00"}}) {
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

// ============================================================================
// DateTime Less Than or Equal Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_leq_datetime_test.go
// ============================================================================

func TestQuerySimpleWithDateTimeLEFilterBlockWithEqualValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2019-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_le: "2017-07-23T03:46:56-05:00"}}) {
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

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithDateTimeLEFilterBlockWithGreaterValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2019-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_le: "2018-07-23T03:46:56-05:00"}}) {
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

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithDateTimeLEFilterBlockWithNullValue(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
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
					Users(filter: {CreatedAt: {_le: null}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Bob",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimple_WithNilDateTimeLEAndNonNilFilterBlock_ShouldSucceed(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			testUtils.CreateDoc{
				Doc: `{
					"Name": "John",
					"Age": 21,
					"CreatedAt": "2017-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Bob",
					"Age": 32,
					"CreatedAt": "2016-07-23T03:46:56-05:00"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Age": 44
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {CreatedAt: {_le: "2017-07-23T03:46:56-05:00"}}) {
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

// ============================================================================
// Boolean Not Equals Filter Tests
// Ported from: tests/integration/query/simple/with_filter/with_neq_bool_test.go
// ============================================================================

func TestQuerySimpleWithBoolNotEqualsTrueFilterBlock(t *testing.T) {
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
					"Name": "Bob"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Verified": false
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Verified: {_neq: true}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Fred",
						},
						{
							"Name": "Bob",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}

func TestQuerySimpleWithBoolNotEqualsNilFilterBlock(t *testing.T) {
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
					"Name": "Bob"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Verified": false
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Verified: {_neq: null}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "Fred",
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

func TestQuerySimpleWithBoolNotEqualsFalseFilterBlock(t *testing.T) {
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
					"Name": "Bob"
				}`,
			},
			testUtils.CreateDoc{
				Doc: `{
					"Name": "Fred",
					"Verified": false
				}`,
			},
			testUtils.Request{
				Request: `query {
					Users(filter: {Verified: {_neq: false}}) {
						Name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Name": "John",
						},
						{
							"Name": "Bob",
						},
					},
				},
			},
		},
	}

	executeSimpleTestCase(t, test)
}
