package interop

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb.rs-interop/tests/interop/framework"
)

// TestIndexCreateDropParity tests creating, listing, querying, and dropping indexes.
func TestIndexCreateDropParity(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	m := startMirrorNodes(t, ctx)
	rustClient := m.RustClient(t)
	goClient := m.GoClient(t)

	// Add Article schema
	_, err := rustClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err, "Rust: failed to add schema")
	_, err = goClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err, "Go: failed to add schema")

	// Create index on rating field
	indexName := "idx_article_rating"
	err = rustClient.CreateIndex(ctx, "Article", []string{"rating"}, indexName, false)
	require.NoError(t, err, "Rust: failed to create index")
	err = goClient.CreateIndex(ctx, "Article", []string{"rating"}, indexName, false)
	require.NoError(t, err, "Go: failed to create index")

	// List indexes and compare
	rustIndexes, err := rustClient.ListIndexes(ctx, "Article")
	require.NoError(t, err, "Rust: failed to list indexes")
	goIndexes, err := goClient.ListIndexes(ctx, "Article")
	require.NoError(t, err, "Go: failed to list indexes")

	// Both should have at least the index we created
	rustFound := false
	for _, idx := range rustIndexes {
		if idx.Name == indexName {
			rustFound = true
			break
		}
	}
	require.True(t, rustFound, "Rust: index %s not found in list", indexName)

	goFound := false
	for _, idx := range goIndexes {
		if idx.Name == indexName {
			goFound = true
			break
		}
	}
	require.True(t, goFound, "Go: index %s not found in list", indexName)

	// Insert 50 documents
	seedArticles(t, ctx, rustClient, goClient, 50)

	// Query with filter on indexed field
	filterQuery := framework.QueryArticlesWithFilterQuery(`{rating: {_gt: 3}}`)
	rustResp, err := rustClient.GraphQL(ctx, filterQuery, nil)
	require.NoError(t, err, "Rust: filtered query failed")
	goResp, err := goClient.GraphQL(ctx, filterQuery, nil)
	require.NoError(t, err, "Go: filtered query failed")
	framework.CompareGraphQLResponses(t, rustResp, goResp, "indexed filter query")

	// Drop the index
	err = rustClient.DropIndex(ctx, "Article", indexName)
	require.NoError(t, err, "Rust: failed to drop index")
	err = goClient.DropIndex(ctx, "Article", indexName)
	require.NoError(t, err, "Go: failed to drop index")

	// Query again — same data expected
	rustResp2, err := rustClient.GraphQL(ctx, filterQuery, nil)
	require.NoError(t, err, "Rust: post-drop query failed")
	goResp2, err := goClient.GraphQL(ctx, filterQuery, nil)
	require.NoError(t, err, "Go: post-drop query failed")
	framework.CompareGraphQLResponses(t, rustResp2, goResp2, "post-drop filter query")

	// Results should be the same before and after dropping index
	framework.CompareGraphQLResponses(t, rustResp, rustResp2, "Rust pre/post drop")
	framework.CompareGraphQLResponses(t, goResp, goResp2, "Go pre/post drop")

	t.Log("Index create/drop parity verified")
}

// TestUniqueIndexParity tests unique index enforcement.
func TestUniqueIndexParity(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	m := startMirrorNodes(t, ctx)
	rustClient := m.RustClient(t)
	goClient := m.GoClient(t)

	_, err := rustClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err)
	_, err = goClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err)

	// Create unique index on title
	err = rustClient.CreateIndex(ctx, "Article", []string{"title"}, "idx_article_title_unique", true)
	require.NoError(t, err, "Rust: failed to create unique index")
	err = goClient.CreateIndex(ctx, "Article", []string{"title"}, "idx_article_title_unique", true)
	require.NoError(t, err, "Go: failed to create unique index")

	// Insert a document
	createQuery := framework.CreateArticleQuery("Unique Title", "body", 5, true)
	rustResp, err := rustClient.GraphQL(ctx, createQuery, nil)
	require.NoError(t, err, "Rust: first insert failed")
	require.Empty(t, rustResp.Errors, "Rust: first insert errors")

	goResp, err := goClient.GraphQL(ctx, createQuery, nil)
	require.NoError(t, err, "Go: first insert failed")
	require.Empty(t, goResp.Errors, "Go: first insert errors")

	// Insert duplicate title — should fail on both
	dupQuery := framework.CreateArticleQuery("Unique Title", "different body", 3, false)
	rustDupResp, err := rustClient.GraphQL(ctx, dupQuery, nil)
	require.NoError(t, err, "Rust: duplicate request should not fail at HTTP level")

	goDupResp, err := goClient.GraphQL(ctx, dupQuery, nil)
	require.NoError(t, err, "Go: duplicate request should not fail at HTTP level")

	// Both should have errors
	rustHasError := len(rustDupResp.Errors) > 0
	goHasError := len(goDupResp.Errors) > 0

	if rustHasError {
		t.Logf("Rust duplicate error: %s", rustDupResp.Errors[0].Message)
	}
	if goHasError {
		t.Logf("Go duplicate error: %s", goDupResp.Errors[0].Message)
	}

	// Verify both implementations reject the duplicate
	require.True(t, rustHasError, "Rust should reject duplicate unique index value")
	require.True(t, goHasError, "Go should reject duplicate unique index value")

	// Verify only 1 document exists on each
	allQuery := framework.QueryArticlesQuery()
	rustAll, err := rustClient.GraphQL(ctx, allQuery, nil)
	require.NoError(t, err)
	goAll, err := goClient.GraphQL(ctx, allQuery, nil)
	require.NoError(t, err)

	var rustData, goData map[string][]json.RawMessage
	json.Unmarshal(rustAll.Data, &rustData)
	json.Unmarshal(goAll.Data, &goData)

	require.Len(t, rustData["Article"], 1, "Rust should have exactly 1 article")
	require.Len(t, goData["Article"], 1, "Go should have exactly 1 article")

	t.Log("Unique index parity verified")
}
