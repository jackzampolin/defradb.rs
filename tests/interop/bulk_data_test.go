package interop

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb.rs-interop/tests/interop/framework"
)

// TestBulkInsertQueryParity inserts 500 documents and verifies query parity.
func TestBulkInsertQueryParity(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	rustNode, goNode := startMirrorNodes(t, ctx)
	rustClient := rustNode.Client()
	goClient := goNode.Client()

	_, err := rustClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err, "Rust: failed to add schema")
	_, err = goClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err, "Go: failed to add schema")

	// Insert 500 documents in batches of 50
	totalDocs := 500
	batchSize := 50
	for batch := 0; batch < totalDocs/batchSize; batch++ {
		for i := 0; i < batchSize; i++ {
			idx := batch*batchSize + i
			title := fmt.Sprintf("Bulk Article %d about %s", idx, []string{"rust", "go", "python", "javascript", "typescript"}[idx%5])
			body := fmt.Sprintf("Bulk body content for article %d", idx)
			rating := (idx % 5) + 1
			published := idx%2 == 0

			query := framework.CreateArticleQuery(title, body, rating, published)

			rustResp, err := rustClient.GraphQL(ctx, query, nil)
			require.NoError(t, err, "Rust: failed to create bulk article %d", idx)
			require.Empty(t, rustResp.Errors, "Rust: bulk create errors at %d", idx)

			goResp, err := goClient.GraphQL(ctx, query, nil)
			require.NoError(t, err, "Go: failed to create bulk article %d", idx)
			require.Empty(t, goResp.Errors, "Go: bulk create errors at %d", idx)
		}
		t.Logf("Inserted batch %d/%d", batch+1, totalDocs/batchSize)
	}

	// Verify document counts
	rustAll, err := rustClient.GraphQL(ctx, framework.QueryArticlesQuery(), nil)
	require.NoError(t, err)
	goAll, err := goClient.GraphQL(ctx, framework.QueryArticlesQuery(), nil)
	require.NoError(t, err)

	var rustData, goData map[string][]json.RawMessage
	json.Unmarshal(rustAll.Data, &rustData)
	json.Unmarshal(goAll.Data, &goData)
	require.Equal(t, totalDocs, len(rustData["Article"]), "Rust doc count mismatch")
	require.Equal(t, totalDocs, len(goData["Article"]), "Go doc count mismatch")

	// Run multiple query patterns and compare
	queries := []struct {
		name  string
		query string
	}{
		{"filter_rating_gt_3", framework.QueryArticlesWithFilterQuery(`{rating: {_gt: 3}}`)},
		{"filter_published_true", framework.QueryArticlesWithFilterQuery(`{published: {_eq: true}}`)},
		{"filter_title_like_rust", framework.QueryArticlesWithFilterQuery(`{title: {_like: "%rust%"}}`)},
		{"order_rating_asc", framework.QueryArticlesOrderedQuery(`{rating: ASC}`)},
		{"order_rating_desc", framework.QueryArticlesOrderedQuery(`{rating: DESC}`)},
		{"limit_10", framework.QueryArticlesLimitOffsetQuery(10, 0)},
		{"limit_5_offset_100", framework.QueryArticlesLimitOffsetQuery(5, 100)},
		{"count", framework.QueryArticlesAggregateQuery()},
	}

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			rustResp, err := rustClient.GraphQL(ctx, q.query, nil)
			require.NoError(t, err, "Rust: query failed")

			goResp, err := goClient.GraphQL(ctx, q.query, nil)
			require.NoError(t, err, "Go: query failed")

			framework.CompareGraphQLResponses(t, rustResp, goResp, fmt.Sprintf("bulk %s", q.name))
		})
	}

	t.Log("Bulk insert and query parity verified")
}

// TestPurgeAndRecreateParity tests purging the database and re-inserting data.
func TestPurgeAndRecreateParity(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rustNode, goNode := startMirrorNodes(t, ctx)
	rustClient := rustNode.Client()
	goClient := goNode.Client()

	_, err := rustClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err, "Rust: failed to add schema")
	_, err = goClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err, "Go: failed to add schema")

	// Insert 100 documents
	seedArticles(t, ctx, rustClient, goClient, 100)

	// Verify counts
	rustResp, err := rustClient.GraphQL(ctx, framework.QueryArticlesQuery(), nil)
	require.NoError(t, err)
	goResp, err := goClient.GraphQL(ctx, framework.QueryArticlesQuery(), nil)
	require.NoError(t, err)

	var rustData, goData map[string][]json.RawMessage
	json.Unmarshal(rustResp.Data, &rustData)
	json.Unmarshal(goResp.Data, &goData)
	require.Equal(t, 100, len(rustData["Article"]), "Rust: expected 100 articles before purge")
	require.Equal(t, 100, len(goData["Article"]), "Go: expected 100 articles before purge")

	// Purge both databases
	err = rustClient.Purge(ctx)
	require.NoError(t, err, "Rust: purge failed")
	err = goClient.Purge(ctx)
	require.NoError(t, err, "Go: purge failed")

	// Re-add schema after purge (purge clears everything)
	_, err = rustClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err, "Rust: failed to re-add schema after purge")
	_, err = goClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err, "Go: failed to re-add schema after purge")

	// Verify both return empty
	rustEmpty, err := rustClient.GraphQL(ctx, framework.QueryArticlesQuery(), nil)
	require.NoError(t, err, "Rust: post-purge query failed")
	goEmpty, err := goClient.GraphQL(ctx, framework.QueryArticlesQuery(), nil)
	require.NoError(t, err, "Go: post-purge query failed")

	var rustEmptyData, goEmptyData map[string][]json.RawMessage
	json.Unmarshal(rustEmpty.Data, &rustEmptyData)
	json.Unmarshal(goEmpty.Data, &goEmptyData)
	require.Empty(t, rustEmptyData["Article"], "Rust: expected 0 articles after purge")
	require.Empty(t, goEmptyData["Article"], "Go: expected 0 articles after purge")

	// Re-insert 50 documents
	seedArticles(t, ctx, rustClient, goClient, 50)

	// Verify matching results
	rustFinal, err := rustClient.GraphQL(ctx, framework.QueryArticlesQuery(), nil)
	require.NoError(t, err)
	goFinal, err := goClient.GraphQL(ctx, framework.QueryArticlesQuery(), nil)
	require.NoError(t, err)

	framework.CompareGraphQLResponses(t, rustFinal, goFinal, "post-purge recreate")

	var rustFinalData, goFinalData map[string][]json.RawMessage
	json.Unmarshal(rustFinal.Data, &rustFinalData)
	json.Unmarshal(goFinal.Data, &goFinalData)
	require.Equal(t, 50, len(rustFinalData["Article"]), "Rust: expected 50 articles after recreate")
	require.Equal(t, 50, len(goFinalData["Article"]), "Go: expected 50 articles after recreate")

	t.Log("Purge and recreate parity verified")
}
