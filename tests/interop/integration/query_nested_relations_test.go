// Package integration tests DefraDB nested relationship queries against the Rust FFI implementation.
//
// These tests are ported from DefraDB's tests/integration/query/ directory for:
// - one_to_many_multiple/ (multiple relation types per entity)
// - one_to_many_to_one/ (3-level deep relations: Author -> Book -> Publisher)
// - one_to_many_to_many/ (3-level deep with array at the end)
// - many_to_many/ (join table patterns)
// - one_to_two_many/ (multiple named relations to same type)
package integration

import (
	"testing"

	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

// =============================================================================
// ONE-TO-MANY-TO-ONE TESTS
// Schema: Author -> [Book] -> Publisher (where Book has single Publisher via @primary)
// =============================================================================

var oneToManyToOneSchema = `
	type Author {
		name: String
		age: Int
		verified: Boolean
		book: [Book]
	}

	type Book {
		name: String
		rating: Float
		author: Author
		publisher: Publisher
	}

	type Publisher {
		name: String
		address: String
		yearOpened: Int
		book: Book @primary
	}
`

// createOneToManyToOneFixture creates the standard fixture for one-to-many-to-one tests.
func createOneToManyToOneFixture() []any {
	return []any{
		// Authors
		testUtils.CreateDoc{
			CollectionID: 0,
			Doc: `{
				"name": "John Grisham",
				"age": 65,
				"verified": true
			}`,
		},
		testUtils.CreateDoc{
			CollectionID: 0,
			Doc: `{
				"name": "Cornelia Funke",
				"age": 62,
				"verified": false
			}`,
		},
		testUtils.CreateDoc{
			CollectionID: 0,
			Doc: `{
				"name": "Not a Writer",
				"age": 6,
				"verified": false
			}`,
		},
		// Books
		testUtils.CreateDoc{
			CollectionID: 1,
			DocMap: map[string]any{
				"name":      "The Rooster Bar",
				"rating":    4.0,
				"author_id": testUtils.NewDocIndex(0, 1),
			},
		},
		testUtils.CreateDoc{
			CollectionID: 1,
			DocMap: map[string]any{
				"name":      "Theif Lord",
				"rating":    4.8,
				"author_id": testUtils.NewDocIndex(0, 0),
			},
		},
		testUtils.CreateDoc{
			CollectionID: 1,
			DocMap: map[string]any{
				"name":      "The Associate",
				"rating":    4.2,
				"author_id": testUtils.NewDocIndex(0, 0),
			},
		},
		testUtils.CreateDoc{
			CollectionID: 1,
			DocMap: map[string]any{
				"name":      "Painted House",
				"rating":    4.9,
				"author_id": testUtils.NewDocIndex(0, 0),
			},
		},
		testUtils.CreateDoc{
			CollectionID: 1,
			DocMap: map[string]any{
				"name":      "A Time for Mercy",
				"rating":    4.5,
				"author_id": testUtils.NewDocIndex(0, 0),
			},
		},
		testUtils.CreateDoc{
			CollectionID: 1,
			DocMap: map[string]any{
				"name":      "Sooley",
				"rating":    3.2,
				"author_id": testUtils.NewDocIndex(0, 0),
			},
		},
		// Publishers
		testUtils.CreateDoc{
			CollectionID: 2,
			DocMap: map[string]any{
				"name":       "Only Publisher of The Rooster Bar",
				"address":    "1 Rooster Ave., Waterloo, Ontario",
				"yearOpened": 2022,
				"book_id":    testUtils.NewDocIndex(1, 0),
			},
		},
		testUtils.CreateDoc{
			CollectionID: 2,
			DocMap: map[string]any{
				"name":       "Only Publisher of Theif Lord",
				"address":    "1 Theif Lord, Waterloo, Ontario",
				"yearOpened": 2020,
				"book_id":    testUtils.NewDocIndex(1, 1),
			},
		},
		testUtils.CreateDoc{
			CollectionID: 2,
			DocMap: map[string]any{
				"name":       "Only Publisher of Painted House",
				"address":    "600 Madison Ave., New York, New York",
				"yearOpened": 1995,
				"book_id":    testUtils.NewDocIndex(1, 3),
			},
		},
		testUtils.CreateDoc{
			CollectionID: 2,
			DocMap: map[string]any{
				"name":       "Only Publisher of A Time for Mercy",
				"address":    "123 Andrew Street, Flin Flon, Manitoba",
				"yearOpened": 2013,
				"book_id":    testUtils.NewDocIndex(1, 4),
			},
		},
		testUtils.CreateDoc{
			CollectionID: 2,
			DocMap: map[string]any{
				"name":       "Only Publisher of Sooley",
				"address":    "11 Sooley Ave., Waterloo, Ontario",
				"yearOpened": 1999,
				"book_id":    testUtils.NewDocIndex(1, 5),
			},
		},
	}
}

// executeOneToManyToOneTestCase wraps the test with the schema and fixture.
func executeOneToManyToOneTestCase(t *testing.T, test testUtils.TestCase) {
	actions := []any{
		&action.AddSchema{
			Schema: oneToManyToOneSchema,
		},
	}
	actions = append(actions, test.Actions...)

	ExecuteTestCase(
		t,
		testUtils.TestCase{
			SupportedMutationTypes: test.SupportedMutationTypes,
			SupportedClientTypes:   test.SupportedClientTypes,
			Actions:                actions,
		},
	)
}

// TestNestedOneToManyToOne_SimpleQuery tests basic 3-level deep query.
// Ported from: tests/integration/query/one_to_many_to_one/simple_test.go
func TestNestedOneToManyToOne_SimpleQuery(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(
			[]any{
				// Authors
				testUtils.CreateDoc{
					CollectionID: 0,
					Doc: `{
						"name": "John Grisham",
						"age": 65,
						"verified": true
					}`,
				},
				testUtils.CreateDoc{
					CollectionID: 0,
					Doc: `{
						"name": "Cornelia Funke",
						"age": 62,
						"verified": false
					}`,
				},
				testUtils.CreateDoc{
					CollectionID: 0,
					Doc: `{
						"name": "Not a Writer",
						"age": 6,
						"verified": false
					}`,
				},
				// Books
				testUtils.CreateDoc{
					CollectionID: 1,
					DocMap: map[string]any{
						"name":      "The Rooster Bar",
						"rating":    4.0,
						"author_id": testUtils.NewDocIndex(0, 1),
					},
				},
				testUtils.CreateDoc{
					CollectionID: 1,
					DocMap: map[string]any{
						"name":      "Theif Lord",
						"rating":    4.8,
						"author_id": testUtils.NewDocIndex(0, 0),
					},
				},
				testUtils.CreateDoc{
					CollectionID: 1,
					DocMap: map[string]any{
						"name":      "The Associate",
						"rating":    4.2,
						"author_id": testUtils.NewDocIndex(0, 0),
					},
				},
				// Publishers
				testUtils.CreateDoc{
					CollectionID: 2,
					DocMap: map[string]any{
						"name":       "Only Publisher of The Rooster Bar",
						"address":    "1 Rooster Ave., Waterloo, Ontario",
						"yearOpened": 2022,
						"book_id":    testUtils.NewDocIndex(1, 0),
					},
				},
				testUtils.CreateDoc{
					CollectionID: 2,
					DocMap: map[string]any{
						"name":       "Only Publisher of Theif Lord",
						"address":    "1 Theif Lord, Waterloo, Ontario",
						"yearOpened": 2020,
						"book_id":    testUtils.NewDocIndex(1, 1),
					},
				},
			},
			testUtils.Request{
				Request: `query {
					Book {
						name
						author {
							name
						}
						publisher {
							name
						}
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name": "The Associate",
							"author": map[string]any{
								"name": "John Grisham",
							},
							"publisher": nil,
						},
						{
							"name": "The Rooster Bar",
							"author": map[string]any{
								"name": "Cornelia Funke",
							},
							"publisher": map[string]any{
								"name": "Only Publisher of The Rooster Bar",
							},
						},
						{
							"name": "Theif Lord",
							"author": map[string]any{
								"name": "John Grisham",
							},
							"publisher": map[string]any{
								"name": "Only Publisher of Theif Lord",
							},
						},
					},
				},
				NonOrderedResults: true,
			},
		),
	}

	executeOneToManyToOneTestCase(t, test)
}

// TestNestedOneToManyToOne_JoinsLinkedProperly tests that nested joins return correct data.
// Ported from: tests/integration/query/one_to_many_to_one/joins_test.go
func TestNestedOneToManyToOne_JoinsLinkedProperly(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(
			createOneToManyToOneFixture(),
			testUtils.Request{
				Request: `query {
					Author {
						name
						book {
							name
							publisher {
								name
							}
						}
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"book": []map[string]any{},
							"name": "Not a Writer",
						},
						{
							"name": "John Grisham",
							"book": []map[string]any{
								{
									"name":      "The Associate",
									"publisher": nil,
								},
								{
									"name": "Sooley",
									"publisher": map[string]any{
										"name": "Only Publisher of Sooley",
									},
								},
								{
									"name": "Painted House",
									"publisher": map[string]any{
										"name": "Only Publisher of Painted House",
									},
								},
								{
									"name": "A Time for Mercy",
									"publisher": map[string]any{
										"name": "Only Publisher of A Time for Mercy",
									},
								},
								{
									"name": "Theif Lord",
									"publisher": map[string]any{
										"name": "Only Publisher of Theif Lord",
									},
								},
							},
						},
						{
							"name": "Cornelia Funke",
							"book": []map[string]any{
								{
									"name": "The Rooster Bar",
									"publisher": map[string]any{
										"name": "Only Publisher of The Rooster Bar",
									},
								},
							},
						},
					},
				},
				NonOrderedResults: true,
			},
		),
	}

	executeOneToManyToOneTestCase(t, test)
}

// TestNestedOneToManyToOne_DeepFilter tests filtering on deeply nested fields.
// Ported from: tests/integration/query/one_to_many_to_one/with_filter_test.go
func TestNestedOneToManyToOne_DeepFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(
			[]any{
				// Authors
				testUtils.CreateDoc{
					CollectionID: 0,
					Doc: `{
						"name": "John Grisham",
						"age": 65,
						"verified": true
					}`,
				},
				testUtils.CreateDoc{
					CollectionID: 0,
					Doc: `{
						"name": "Cornelia Funke",
						"age": 62,
						"verified": false
					}`,
				},
				testUtils.CreateDoc{
					CollectionID: 0,
					Doc: `{
						"name": "Not a Writer",
						"age": 6,
						"verified": false
					}`,
				},
				// Books
				testUtils.CreateDoc{
					CollectionID: 1,
					DocMap: map[string]any{
						"name":      "The Rooster Bar",
						"rating":    4.0,
						"author_id": testUtils.NewDocIndex(0, 1),
					},
				},
				testUtils.CreateDoc{
					CollectionID: 1,
					DocMap: map[string]any{
						"name":      "Theif Lord",
						"rating":    4.8,
						"author_id": testUtils.NewDocIndex(0, 0),
					},
				},
				testUtils.CreateDoc{
					CollectionID: 1,
					DocMap: map[string]any{
						"name":      "The Associate",
						"rating":    4.2,
						"author_id": testUtils.NewDocIndex(0, 0),
					},
				},
				// Publishers
				testUtils.CreateDoc{
					CollectionID: 2,
					DocMap: map[string]any{
						"name":       "Only Publisher of The Rooster Bar",
						"address":    "1 Rooster Ave., Waterloo, Ontario",
						"yearOpened": 2022,
						"book_id":    testUtils.NewDocIndex(1, 0),
					},
				},
				testUtils.CreateDoc{
					CollectionID: 2,
					DocMap: map[string]any{
						"name":       "Only Publisher of Theif Lord",
						"address":    "1 Theif Lord, Waterloo, Ontario",
						"yearOpened": 2020,
						"book_id":    testUtils.NewDocIndex(1, 1),
					},
				},
			},
			testUtils.Request{
				Request: `query {
					Author (filter: {book: {publisher: {yearOpened: {_gt: 2021}}}}) {
						name
						book {
							publisher {
								yearOpened
							}
						}
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name": "Cornelia Funke",
							"book": []map[string]any{
								{
									"publisher": map[string]any{
										"yearOpened": int64(2022),
									},
								},
							},
						},
					},
				},
			},
		),
	}

	executeOneToManyToOneTestCase(t, test)
}

// TestNestedOneToManyToOne_TwoLevelDeepFilter tests filtering across two levels of nesting.
// Ported from: tests/integration/query/one_to_many_to_one/with_filter_test.go
func TestNestedOneToManyToOne_TwoLevelDeepFilter(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(
			createOneToManyToOneFixture(),
			testUtils.Request{
				Request: `query {
					Author (filter: {book: {publisher: {yearOpened: { _ge: 2020}}}}){
						name
						book {
							name
							publisher {
								yearOpened
							}
						}
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"book": []map[string]any{
								{
									"name":      "The Associate",
									"publisher": nil,
								},
								{
									"name": "Sooley",
									"publisher": map[string]any{
										"yearOpened": int64(1999),
									},
								},
								{
									"name": "Painted House",
									"publisher": map[string]any{
										"yearOpened": int64(1995),
									},
								},
								{
									"name": "A Time for Mercy",
									"publisher": map[string]any{
										"yearOpened": int64(2013),
									},
								},
								{
									"name": "Theif Lord",
									"publisher": map[string]any{
										"yearOpened": int64(2020),
									},
								},
							},
							"name": "John Grisham",
						},
						{
							"book": []map[string]any{
								{
									"name": "The Rooster Bar",
									"publisher": map[string]any{
										"yearOpened": int64(2022),
									},
								},
							},
							"name": "Cornelia Funke",
						},
					},
				},
				NonOrderedResults: true,
			},
		),
	}

	executeOneToManyToOneTestCase(t, test)
}

// TestNestedOneToManyToOne_OrderByNestedField tests ordering by nested relation fields.
// Ported from: tests/integration/query/one_to_many_to_one/with_order_test.go
func TestNestedOneToManyToOne_OrderByNestedField(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(
			createOneToManyToOneFixture(),
			testUtils.Request{
				Request: `query {
					Book (order: [{rating: ASC}, {publisher: {yearOpened: DESC}}]) {
						name
						rating
						publisher{
							name
							yearOpened
						}
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name":   "Sooley",
							"rating": 3.2,
							"publisher": map[string]any{
								"name":       "Only Publisher of Sooley",
								"yearOpened": int64(1999),
							},
						},
						{
							"name":   "The Rooster Bar",
							"rating": 4.0,
							"publisher": map[string]any{
								"name":       "Only Publisher of The Rooster Bar",
								"yearOpened": int64(2022),
							},
						},
						{
							"name":      "The Associate",
							"rating":    4.2,
							"publisher": nil,
						},
						{
							"name":   "A Time for Mercy",
							"rating": 4.5,
							"publisher": map[string]any{
								"name":       "Only Publisher of A Time for Mercy",
								"yearOpened": int64(2013),
							},
						},
						{
							"name":   "Theif Lord",
							"rating": 4.8,
							"publisher": map[string]any{
								"name":       "Only Publisher of Theif Lord",
								"yearOpened": int64(2020),
							},
						},
						{
							"name":   "Painted House",
							"rating": 4.9,
							"publisher": map[string]any{
								"name":       "Only Publisher of Painted House",
								"yearOpened": int64(1995),
							},
						},
					},
				},
			},
		),
	}

	executeOneToManyToOneTestCase(t, test)
}

// =============================================================================
// ONE-TO-MANY-TO-MANY TESTS
// Schema: Author -> [Book] -> [Publisher] (where Book has multiple Publishers)
// =============================================================================

var oneToManyToManySchema = `
	type Author {
		name: String
		age: Int
		verified: Boolean
		book: [Book]
	}

	type Book {
		name: String
		rating: Float
		author: Author
		publisher: [Publisher]
	}

	type Publisher {
		name: String
		address: String
		yearOpened: Int
		book: Book
	}
`

// TestNestedOneToManyToMany_JoinsLinkedProperly tests 3-level deep query with array at end.
// Ported from: tests/integration/query/one_to_many_to_many/joins_test.go
func TestNestedOneToManyToMany_JoinsLinkedProperly(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: oneToManyToManySchema,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "John Grisham",
					"age": 65,
					"verified": true
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Cornelia Funke",
					"age": 62,
					"verified": false
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
					"name": "Not a Writer",
					"age": 6,
					"verified": false
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "The Rooster Bar",
					"rating":    4.0,
					"author_id": testUtils.NewDocIndex(0, 1),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "Theif Lord",
					"rating":    4.8,
					"author_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "The Associate",
					"rating":    4.2,
					"author_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "Painted House",
					"rating":    4.9,
					"author_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "A Time for Mercy",
					"rating":    4.5,
					"author_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "Sooley",
					"rating":    3.2,
					"author_id": testUtils.NewDocIndex(0, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 2,
				DocMap: map[string]any{
					"name":       "Only Publisher of The Rooster Bar",
					"address":    "1 Rooster Ave., Waterloo, Ontario",
					"yearOpened": 2022,
					"book_id":    testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 2,
				DocMap: map[string]any{
					"name":       "Only Publisher of Theif Lord",
					"address":    "1 Theif Lord, Waterloo, Ontario",
					"yearOpened": 2020,
					"book_id":    testUtils.NewDocIndex(1, 1),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 2,
				DocMap: map[string]any{
					"name":       "Only Publisher of Painted House",
					"address":    "600 Madison Ave., New York, New York",
					"yearOpened": 1995,
					"book_id":    testUtils.NewDocIndex(1, 3),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 2,
				DocMap: map[string]any{
					"name":       "Only Publisher of A Time for Mercy",
					"address":    "123 Andrew Street, Flin Flon, Manitoba",
					"yearOpened": 2013,
					"book_id":    testUtils.NewDocIndex(1, 4),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 2,
				DocMap: map[string]any{
					"name":       "First of Two Publishers of Sooley",
					"address":    "11 Sooley Ave., Waterloo, Ontario",
					"yearOpened": 1999,
					"book_id":    testUtils.NewDocIndex(1, 5),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 2,
				DocMap: map[string]any{
					"name":       "Second of Two Publishers of Sooley",
					"address":    "22 Sooley Ave., Waterloo, Ontario",
					"yearOpened": 2000,
					"book_id":    testUtils.NewDocIndex(1, 5),
				},
			},
			testUtils.Request{
				Request: `query {
					Author {
						name
						book {
							name
							publisher {
								name
							}
						}
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"book": []map[string]any{},
							"name": "Not a Writer",
						},
						{
							"name": "John Grisham",
							"book": []map[string]any{
								{
									"name":      "The Associate",
									"publisher": []map[string]any{},
								},
								{
									"name": "Painted House",
									"publisher": []map[string]any{
										{
											"name": "Only Publisher of Painted House",
										},
									},
								},
								{
									"name": "Theif Lord",
									"publisher": []map[string]any{
										{
											"name": "Only Publisher of Theif Lord",
										},
									},
								},
								{
									"name": "A Time for Mercy",
									"publisher": []map[string]any{
										{
											"name": "Only Publisher of A Time for Mercy",
										},
									},
								},
								{
									"name": "Sooley",
									"publisher": []map[string]any{
										{
											"name": "First of Two Publishers of Sooley",
										},
										{
											"name": "Second of Two Publishers of Sooley",
										},
									},
								},
							},
						},
						{
							"name": "Cornelia Funke",
							"book": []map[string]any{
								{
									"name": "The Rooster Bar",
									"publisher": []map[string]any{
										{
											"name": "Only Publisher of The Rooster Bar",
										},
									},
								},
							},
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	ExecuteTestCase(t, test)
}

// =============================================================================
// ONE-TO-MANY-MULTIPLE TESTS
// Schema: Author with multiple relation types (books + articles)
// =============================================================================

var oneToManyMultipleSchema = `
	type Article {
		name: String
		author: Author
		rating: Int
	}

	type Book {
		name: String
		author: Author
		score: Int
	}

	type Author {
		name: String
		age: Int
		verified: Boolean
		books: [Book]
		articles: [Article]
	}
`

// createOneToManyMultipleFixture creates fixture with Author having both books and articles.
func createOneToManyMultipleFixture() []any {
	return []any{
		testUtils.CreateDoc{
			CollectionID: 2, // Author
			DocMap: map[string]any{
				"name":     "John Grisham",
				"age":      65,
				"verified": true,
			},
		},
		testUtils.CreateDoc{
			CollectionID: 2, // Author
			DocMap: map[string]any{
				"name":     "Cornelia Funke",
				"age":      62,
				"verified": false,
			},
		},
		// Articles
		testUtils.CreateDoc{
			CollectionID: 0, // Article
			DocMap: map[string]any{
				"name":      "After Guantanamo, Another Injustice",
				"author_id": testUtils.NewDocIndex(2, 0),
				"rating":    3,
			},
		},
		testUtils.CreateDoc{
			CollectionID: 0, // Article
			DocMap: map[string]any{
				"name":      "To my dear readers",
				"author_id": testUtils.NewDocIndex(2, 1),
				"rating":    2,
			},
		},
		testUtils.CreateDoc{
			CollectionID: 0, // Article
			DocMap: map[string]any{
				"name":      "Twinklestars Favourite Xmas Cookie",
				"author_id": testUtils.NewDocIndex(2, 1),
				"rating":    1,
			},
		},
		// Books
		testUtils.CreateDoc{
			CollectionID: 1, // Book
			DocMap: map[string]any{
				"name":      "Painted House",
				"author_id": testUtils.NewDocIndex(2, 0),
				"score":     1,
			},
		},
		testUtils.CreateDoc{
			CollectionID: 1, // Book
			DocMap: map[string]any{
				"name":      "A Time for Mercy",
				"author_id": testUtils.NewDocIndex(2, 0),
				"score":     2,
			},
		},
		testUtils.CreateDoc{
			CollectionID: 1, // Book
			DocMap: map[string]any{
				"name":      "Theif Lord",
				"author_id": testUtils.NewDocIndex(2, 1),
				"score":     4,
			},
		},
	}
}

// TestNestedOneToManyMultiple_CountMultipleRelations tests _count on multiple relation types.
// Ported from: tests/integration/query/one_to_many_multiple/with_count_test.go
func TestNestedOneToManyMultiple_CountMultipleRelations(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: oneToManyMultipleSchema,
			},
			testUtils.CreateDoc{
				CollectionID: 2,
				DocMap: map[string]any{
					"name":     "John Grisham",
					"age":      65,
					"verified": true,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 2,
				DocMap: map[string]any{
					"name":     "Cornelia Funke",
					"age":      62,
					"verified": false,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "After Guantanamo, Another Injustice",
					"author_id": testUtils.NewDocIndex(2, 0),
					"rating":    3,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "To my dear readers",
					"author_id": testUtils.NewDocIndex(2, 1),
					"rating":    2,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Twinklestars Favourite Xmas Cookie",
					"author_id": testUtils.NewDocIndex(2, 1),
					"rating":    1,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "Painted House",
					"author_id": testUtils.NewDocIndex(2, 0),
					"score":     1,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "A Time for Mercy",
					"author_id": testUtils.NewDocIndex(2, 0),
					"score":     2,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "Theif Lord",
					"author_id": testUtils.NewDocIndex(2, 1),
					"score":     4,
				},
			},
			testUtils.Request{
				Request: `query {
					Author {
						name
						numberOfBooks: _count(books: {})
						numberOfArticles: _count(articles: {})
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name":             "John Grisham",
							"numberOfBooks":    2,
							"numberOfArticles": 1,
						},
						{
							"name":             "Cornelia Funke",
							"numberOfBooks":    1,
							"numberOfArticles": 2,
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestNestedOneToManyMultiple_SumMultipleRelations tests _sum across multiple relation types.
// Ported from: tests/integration/query/one_to_many_multiple/with_sum_test.go
func TestNestedOneToManyMultiple_SumMultipleRelations(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: oneToManyMultipleSchema,
			},
			testUtils.CreateDoc{
				CollectionID: 2,
				DocMap: map[string]any{
					"name":     "John Grisham",
					"age":      65,
					"verified": true,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 2,
				DocMap: map[string]any{
					"name":     "Cornelia Funke",
					"age":      62,
					"verified": false,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "After Guantanamo, Another Injustice",
					"author_id": testUtils.NewDocIndex(2, 0),
					"rating":    3,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "To my dear readers",
					"author_id": testUtils.NewDocIndex(2, 1),
					"rating":    2,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Twinklestars Favourite Xmas Cookie",
					"author_id": testUtils.NewDocIndex(2, 1),
					"rating":    1,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "Painted House",
					"author_id": testUtils.NewDocIndex(2, 0),
					"score":     1,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "A Time for Mercy",
					"author_id": testUtils.NewDocIndex(2, 0),
					"score":     2,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "Sooley",
					"author_id": testUtils.NewDocIndex(2, 0),
					"score":     3,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "Theif Lord",
					"author_id": testUtils.NewDocIndex(2, 1),
					"score":     4,
				},
			},
			testUtils.Request{
				Request: `query {
					Author {
						name
						_sum(books: {field: score}, articles: {field: rating})
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name": "John Grisham",
							"_sum": int64(9),
						},
						{
							"name": "Cornelia Funke",
							"_sum": int64(7),
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestNestedOneToManyMultiple_AverageMultipleRelations tests _avg across multiple relation types.
// Ported from: tests/integration/query/one_to_many_multiple/with_average_test.go
func TestNestedOneToManyMultiple_AverageMultipleRelations(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: oneToManyMultipleSchema,
			},
			testUtils.CreateDoc{
				CollectionID: 2,
				DocMap: map[string]any{
					"name":     "John Grisham",
					"age":      65,
					"verified": true,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 2,
				DocMap: map[string]any{
					"name":     "Cornelia Funke",
					"age":      62,
					"verified": false,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "After Guantanamo, Another Injustice",
					"author_id": testUtils.NewDocIndex(2, 0),
					"rating":    3,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "To my dear readers",
					"author_id": testUtils.NewDocIndex(2, 1),
					"rating":    2,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":      "Twinklestars Favourite Xmas Cookie",
					"author_id": testUtils.NewDocIndex(2, 1),
					"rating":    1,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "Painted House",
					"author_id": testUtils.NewDocIndex(2, 0),
					"score":     1,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "A Time for Mercy",
					"author_id": testUtils.NewDocIndex(2, 0),
					"score":     2,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "Sooley",
					"author_id": testUtils.NewDocIndex(2, 0),
					"score":     3,
				},
			},
			testUtils.CreateDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "Theif Lord",
					"author_id": testUtils.NewDocIndex(2, 1),
					"score":     4,
				},
			},
			testUtils.Request{
				Request: `query {
					Author {
						name
						_avg(books: {field: score}, articles: {field: rating})
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name": "John Grisham",
							"_avg": float64(2.25),
						},
						{
							"name": "Cornelia Funke",
							"_avg": float64(2.3333333333333335),
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	ExecuteTestCase(t, test)
}

// =============================================================================
// MANY-TO-MANY TESTS
// Schema: Student <-> Course via Enrollment join table
// =============================================================================

var manyToManySchema = `
	type Student {
		name: String
	}

	type Course {
		name: String
	}

	type Enrollment {
		student: Student @relation(name: "student_enrollments")
		course: Course @relation(name: "course_enrollments")
	}
`

// TestNestedManyToMany_QueryFromJoinTable tests querying through a join table.
// Ported from: tests/integration/query/many_to_many/simple_test.go
func TestNestedManyToMany_QueryFromJoinTable(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: manyToManySchema,
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Student
				Doc:          `{"name": "Alice"}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Student
				Doc:          `{"name": "Bob"}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1, // Course
				Doc:          `{"name": "Math"}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1, // Course
				Doc:          `{"name": "Science"}`,
			},
			testUtils.CreateDoc{
				CollectionID: 2, // Enrollment: Alice -> Math
				DocMap: map[string]any{
					"student": testUtils.NewDocIndex(0, 0),
					"course":  testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 2, // Enrollment: Alice -> Science
				DocMap: map[string]any{
					"student": testUtils.NewDocIndex(0, 0),
					"course":  testUtils.NewDocIndex(1, 1),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 2, // Enrollment: Bob -> Math
				DocMap: map[string]any{
					"student": testUtils.NewDocIndex(0, 1),
					"course":  testUtils.NewDocIndex(1, 0),
				},
			},
			// Query course-to-students direction via join table
			testUtils.Request{
				Request: `query {
					Enrollment(
						filter: {course: {name: {_eq: "Math"}}}
						order: {student: {name: ASC}}
					) {
						student { name }
					}
				}`,
				Results: map[string]any{
					"Enrollment": []map[string]any{
						{"student": map[string]any{"name": "Alice"}},
						{"student": map[string]any{"name": "Bob"}},
					},
				},
			},
			// Query student-to-courses direction via join table
			testUtils.Request{
				Request: `query {
					Enrollment(
						filter: {student: {name: {_eq: "Alice"}}}
						order: {course: {name: ASC}}
					) {
						course { name }
					}
				}`,
				Results: map[string]any{
					"Enrollment": []map[string]any{
						{"course": map[string]any{"name": "Math"}},
						{"course": map[string]any{"name": "Science"}},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestNestedManyToMany_QueryFromSecondary tests querying from the secondary side.
// Ported from: tests/integration/query/many_to_many/with_nested_query_test.go
func TestNestedManyToMany_QueryFromSecondary(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Student {
						name: String
						enrollments: [Enrollment] @relation(name: "student_enrollments")
					}

					type Course {
						name: String
					}

					type Enrollment {
						student: Student @relation(name: "student_enrollments")
						course: Course @relation(name: "course_enrollments")
					}
				`,
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Student
				Doc:          `{"name": "Alice"}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Student
				Doc:          `{"name": "Bob"}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1, // Course
				Doc:          `{"name": "Math"}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1, // Course
				Doc:          `{"name": "Science"}`,
			},
			testUtils.CreateDoc{
				CollectionID: 2, // Enrollment
				DocMap: map[string]any{
					"student": testUtils.NewDocIndex(0, 0), // Alice
					"course":  testUtils.NewDocIndex(1, 0), // Math
				},
			},
			testUtils.CreateDoc{
				CollectionID: 2, // Enrollment
				DocMap: map[string]any{
					"student": testUtils.NewDocIndex(0, 0), // Alice
					"course":  testUtils.NewDocIndex(1, 1), // Science
				},
			},
			testUtils.CreateDoc{
				CollectionID: 2, // Enrollment
				DocMap: map[string]any{
					"student": testUtils.NewDocIndex(0, 1), // Bob
					"course":  testUtils.NewDocIndex(1, 0), // Math
				},
			},
			// Query Alice and access her course names through enrollments
			testUtils.Request{
				Request: `query {
					Student(filter: {name: {_eq: "Alice"}}) {
						name
						enrollments(order: {course: {name: ASC}}) {
							course { name }
						}
					}
				}`,
				Results: map[string]any{
					"Student": []map[string]any{
						{
							"name": "Alice",
							"enrollments": []map[string]any{
								{"course": map[string]any{"name": "Math"}},
								{"course": map[string]any{"name": "Science"}},
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
// ONE-TO-TWO-MANY TESTS
// Schema: Book with author + reviewedBy both pointing to Author (named relations)
// =============================================================================

var oneToTwoManySchema = `
	type Book {
		name: String
		rating: Float
		author: Author @relation(name: "written_books")
		reviewedBy: Author @relation(name: "reviewed_books")
	}

	type Author {
		name: String
		age: Int
		verified: Boolean
		written: [Book] @relation(name: "written_books")
		reviewed: [Book] @relation(name: "reviewed_books")
	}
`

// TestNestedOneToTwoMany_FromOneSide tests querying multiple relations to same type.
// Ported from: tests/integration/query/one_to_two_many/simple_test.go
func TestNestedOneToTwoMany_FromOneSide(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: oneToTwoManySchema,
			},
			testUtils.CreateDoc{
				CollectionID: 1, // Author
				Doc: `{
					"name": "John Grisham",
					"age": 65,
					"verified": true
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1, // Author
				Doc: `{
					"name": "Cornelia Funke",
					"age": 62,
					"verified": false
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Book
				DocMap: map[string]any{
					"name":          "Painted House",
					"rating":        4.9,
					"author_id":     testUtils.NewDocIndex(1, 0),
					"reviewedBy_id": testUtils.NewDocIndex(1, 1),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Book
				DocMap: map[string]any{
					"name":          "A Time for Mercy",
					"rating":        4.5,
					"author_id":     testUtils.NewDocIndex(1, 0),
					"reviewedBy_id": testUtils.NewDocIndex(1, 1),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Book
				DocMap: map[string]any{
					"name":          "Theif Lord",
					"rating":        4.8,
					"author_id":     testUtils.NewDocIndex(1, 1),
					"reviewedBy_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.Request{
				Request: `query {
					Book {
						name
						rating
						author {
							name
						}
						reviewedBy {
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
							},
							"reviewedBy": map[string]any{
								"name": "Cornelia Funke",
								"age":  int64(62),
							},
						},
						{
							"name":   "Theif Lord",
							"rating": 4.8,
							"author": map[string]any{
								"name": "Cornelia Funke",
							},
							"reviewedBy": map[string]any{
								"name": "John Grisham",
								"age":  int64(65),
							},
						},
						{
							"name":   "A Time for Mercy",
							"rating": 4.5,
							"author": map[string]any{
								"name": "John Grisham",
							},
							"reviewedBy": map[string]any{
								"name": "Cornelia Funke",
								"age":  int64(62),
							},
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestNestedOneToTwoMany_FromManySide tests querying from the many side with two relation paths.
// Ported from: tests/integration/query/one_to_two_many/simple_test.go
func TestNestedOneToTwoMany_FromManySide(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: oneToTwoManySchema,
			},
			testUtils.CreateDoc{
				CollectionID: 1, // Author
				Doc: `{
					"name": "John Grisham",
					"age": 65,
					"verified": true
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1, // Author
				Doc: `{
					"name": "Cornelia Funke",
					"age": 62,
					"verified": false
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Book
				DocMap: map[string]any{
					"name":          "Painted House",
					"rating":        4.9,
					"author_id":     testUtils.NewDocIndex(1, 0),
					"reviewedBy_id": testUtils.NewDocIndex(1, 1),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Book
				DocMap: map[string]any{
					"name":          "A Time for Mercy",
					"rating":        4.5,
					"author_id":     testUtils.NewDocIndex(1, 0),
					"reviewedBy_id": testUtils.NewDocIndex(1, 1),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Book
				DocMap: map[string]any{
					"name":          "Theif Lord",
					"rating":        4.8,
					"author_id":     testUtils.NewDocIndex(1, 1),
					"reviewedBy_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.Request{
				Request: `query {
					Author {
						name
						age
						written {
							name
						}
						reviewed {
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
							"reviewed": []map[string]any{
								{
									"name":   "Theif Lord",
									"rating": 4.8,
								},
							},
							"written": []map[string]any{
								{
									"name": "Painted House",
								},
								{
									"name": "A Time for Mercy",
								},
							},
						},
						{
							"name": "Cornelia Funke",
							"age":  int64(62),
							"reviewed": []map[string]any{
								{
									"name":   "Painted House",
									"rating": 4.9,
								},
								{
									"name":   "A Time for Mercy",
									"rating": 4.5,
								},
							},
							"written": []map[string]any{
								{
									"name": "Theif Lord",
								},
							},
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestNestedOneToTwoMany_WithOrder tests ordering on multiple named relations.
// Ported from: tests/integration/query/one_to_two_many/with_order_test.go
func TestNestedOneToTwoMany_WithOrder(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: oneToTwoManySchema,
			},
			testUtils.CreateDoc{
				CollectionID: 1, // Author
				Doc: `{
					"name": "John Grisham",
					"age": 65,
					"verified": true
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1, // Author
				Doc: `{
					"name": "Cornelia Funke",
					"age": 62,
					"verified": false
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Book
				DocMap: map[string]any{
					"name":          "Painted House",
					"rating":        4.9,
					"author_id":     testUtils.NewDocIndex(1, 0),
					"reviewedBy_id": testUtils.NewDocIndex(1, 1),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Book
				DocMap: map[string]any{
					"name":          "A Time for Mercy",
					"rating":        4.5,
					"author_id":     testUtils.NewDocIndex(1, 0),
					"reviewedBy_id": testUtils.NewDocIndex(1, 0), // Self-reviewed
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Book
				DocMap: map[string]any{
					"name":          "Theif Lord",
					"rating":        4.8,
					"author_id":     testUtils.NewDocIndex(1, 1),
					"reviewedBy_id": testUtils.NewDocIndex(1, 0),
				},
			},
			testUtils.Request{
				Request: `query {
					Author {
						name
						written (order: {rating: ASC}) {
							name
						}
						reviewed (order: {rating: DESC}){
							name
							rating
						}
					}
				}`,
				Results: map[string]any{
					"Author": []map[string]any{
						{
							"name": "Cornelia Funke",
							"reviewed": []map[string]any{
								{
									"name":   "Painted House",
									"rating": 4.9,
								},
							},
							"written": []map[string]any{
								{
									"name": "Theif Lord",
								},
							},
						},
						{
							"name": "John Grisham",
							"reviewed": []map[string]any{
								{
									"name":   "Theif Lord",
									"rating": 4.8,
								},
								{
									"name":   "A Time for Mercy",
									"rating": 4.5,
								},
							},
							"written": []map[string]any{
								{
									"name": "A Time for Mercy",
								},
								{
									"name": "Painted House",
								},
							},
						},
					},
				},
			},
		},
	}

	ExecuteTestCase(t, test)
}

// TestNestedOneToTwoMany_WithNamedAndUnnamedRelationships tests mixing named and unnamed relations.
// Ported from: tests/integration/query/one_to_two_many/simple_test.go
func TestNestedOneToTwoMany_WithNamedAndUnnamedRelationships(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddSchema{
				Schema: `
					type Book {
						name: String
						rating: Float
						price: Price
						author: Author @relation(name: "written_books")
						reviewedBy: Author @relation(name: "reviewed_books")
					}

					type Author {
						name: String
						age: Int
						verified: Boolean
						written: [Book] @relation(name: "written_books")
						reviewed: [Book] @relation(name: "reviewed_books")
					}

					type Price {
						currency: String
						value: Float
						books: [Book]
					}
				`,
			},
			testUtils.CreateDoc{
				CollectionID: 2, // Price
				Doc: `{
					"currency": "GBP",
					"value": 12.99
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 2, // Price
				Doc: `{
					"currency": "SEK",
					"value": 129
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1, // Author
				Doc: `{
					"name": "John Grisham",
					"age": 65,
					"verified": true
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 1, // Author
				Doc: `{
					"name": "Cornelia Funke",
					"age": 62,
					"verified": false
				}`,
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Book
				DocMap: map[string]any{
					"name":          "Painted House",
					"rating":        4.9,
					"author_id":     testUtils.NewDocIndex(1, 0),
					"reviewedBy_id": testUtils.NewDocIndex(1, 1),
					"price_id":      testUtils.NewDocIndex(2, 0),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Book
				DocMap: map[string]any{
					"name":          "A Time for Mercy",
					"rating":        4.5,
					"author_id":     testUtils.NewDocIndex(1, 0),
					"reviewedBy_id": testUtils.NewDocIndex(1, 1),
					"price_id":      testUtils.NewDocIndex(2, 1),
				},
			},
			testUtils.CreateDoc{
				CollectionID: 0, // Book
				DocMap: map[string]any{
					"name":          "Theif Lord",
					"rating":        4.8,
					"author_id":     testUtils.NewDocIndex(1, 1),
					"reviewedBy_id": testUtils.NewDocIndex(1, 0),
					"price_id":      testUtils.NewDocIndex(2, 0),
				},
			},
			testUtils.Request{
				Request: `query {
					Book {
						name
						rating
						author {
							name
						}
						reviewedBy {
							name
							age
						}
						price {
							currency
							value
						}
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{
							"name":   "A Time for Mercy",
							"rating": 4.5,
							"author": map[string]any{
								"name": "John Grisham",
							},
							"reviewedBy": map[string]any{
								"name": "Cornelia Funke",
								"age":  int64(62),
							},
							"price": map[string]any{
								"currency": "SEK",
								"value":    float64(129),
							},
						},
						{
							"name":   "Theif Lord",
							"rating": 4.8,
							"author": map[string]any{
								"name": "Cornelia Funke",
							},
							"reviewedBy": map[string]any{
								"name": "John Grisham",
								"age":  int64(65),
							},
							"price": map[string]any{
								"currency": "GBP",
								"value":    12.99,
							},
						},
						{
							"name":   "Painted House",
							"rating": 4.9,
							"author": map[string]any{
								"name": "John Grisham",
							},
							"reviewedBy": map[string]any{
								"name": "Cornelia Funke",
								"age":  int64(62),
							},
							"price": map[string]any{
								"currency": "GBP",
								"value":    12.99,
							},
						},
					},
				},
				NonOrderedResults: true,
			},
		},
	}

	ExecuteTestCase(t, test)
}
