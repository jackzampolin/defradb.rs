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
