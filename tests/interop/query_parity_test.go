package interop

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb.rs-interop/tests/interop/framework"
)

// seedArticles inserts a set of test articles into both Rust and Go nodes.
// Returns the count of inserted articles.
func seedArticles(t *testing.T, ctx context.Context, rustClient, goClient *framework.Client, count int) int {
	t.Helper()

	for i := 0; i < count; i++ {
		title := fmt.Sprintf("Article %d about %s", i, []string{"rust", "go", "python", "javascript", "typescript"}[i%5])
		body := fmt.Sprintf("Body content for article %d", i)
		rating := (i % 5) + 1 // 1-5
		published := i%2 == 0

		query := framework.CreateArticleQuery(title, body, rating, published)

		rustResp, err := rustClient.GraphQL(ctx, query, nil)
		require.NoError(t, err, "Rust: failed to create article %d", i)
		require.Empty(t, rustResp.Errors, "Rust: create article %d errors: %v", i, rustResp.Errors)

		goResp, err := goClient.GraphQL(ctx, query, nil)
		require.NoError(t, err, "Go: failed to create article %d", i)
		require.Empty(t, goResp.Errors, "Go: create article %d errors: %v", i, goResp.Errors)
	}

	return count
}

// TestQueryFilterParity tests that filter queries produce the same results.
func TestQueryFilterParity(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rustNode, goNode, id := startMirrorNodes(t, ctx)
	rustClient := rustNode.Client().WithIdentity(id)
	goClient := goNode.Client().WithIdentity(id)

	// Add Article schema to both
	_, err := rustClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err, "Rust: failed to add schema")
	_, err = goClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err, "Go: failed to add schema")

	// Insert 20 articles
	seedArticles(t, ctx, rustClient, goClient, 20)

	filters := []struct {
		name   string
		filter string
	}{
		{"rating_gt_3", `{rating: {_gt: 3}}`},
		{"published_eq_true", `{published: {_eq: true}}`},
		{"title_like_rust", `{title: {_like: "%rust%"}}`},
		{"and_rating_gte_2_published", `{_and: [{rating: {_gte: 2}}, {published: {_eq: true}}]}`},
		{"rating_lt_3", `{rating: {_lt: 3}}`},
		{"title_like_go", `{title: {_like: "%go%"}}`},
	}

	for _, f := range filters {
		t.Run(f.name, func(t *testing.T) {
			query := framework.QueryArticlesWithFilterQuery(f.filter)

			rustResp, err := rustClient.GraphQL(ctx, query, nil)
			require.NoError(t, err, "Rust: filter query failed")
			require.Empty(t, rustResp.Errors, "Rust: filter query errors: %v", rustResp.Errors)

			goResp, err := goClient.GraphQL(ctx, query, nil)
			require.NoError(t, err, "Go: filter query failed")
			require.Empty(t, goResp.Errors, "Go: filter query errors: %v", goResp.Errors)

			framework.CompareGraphQLResponses(t, rustResp, goResp, fmt.Sprintf("filter %s", f.name))
		})
	}
}

// TestQueryOrderParity tests that ORDER BY queries produce the same results.
func TestQueryOrderParity(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rustNode, goNode, id := startMirrorNodes(t, ctx)
	rustClient := rustNode.Client().WithIdentity(id)
	goClient := goNode.Client().WithIdentity(id)

	_, err := rustClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err)
	_, err = goClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err)

	seedArticles(t, ctx, rustClient, goClient, 20)

	orders := []struct {
		name  string
		order string
	}{
		{"rating_asc", `{rating: ASC}`},
		{"rating_desc", `{rating: DESC}`},
		{"title_asc", `{title: ASC}`},
		{"title_desc", `{title: DESC}`},
	}

	for _, o := range orders {
		t.Run(o.name, func(t *testing.T) {
			query := framework.QueryArticlesOrderedQuery(o.order)

			rustResp, err := rustClient.GraphQL(ctx, query, nil)
			require.NoError(t, err, "Rust: order query failed")

			goResp, err := goClient.GraphQL(ctx, query, nil)
			require.NoError(t, err, "Go: order query failed")

			// For ordered queries, we compare without sorting (order matters)
			if len(rustResp.Errors) == 0 && len(goResp.Errors) == 0 {
				framework.CompareJSON(t, rustResp.Data, goResp.Data, fmt.Sprintf("order %s", o.name))
			} else {
				framework.CompareGraphQLResponses(t, rustResp, goResp, fmt.Sprintf("order %s", o.name))
			}
		})
	}
}

// TestQueryLimitOffsetParity tests that LIMIT/OFFSET queries produce the same results.
func TestQueryLimitOffsetParity(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rustNode, goNode, id := startMirrorNodes(t, ctx)
	rustClient := rustNode.Client().WithIdentity(id)
	goClient := goNode.Client().WithIdentity(id)

	_, err := rustClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err)
	_, err = goClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err)

	seedArticles(t, ctx, rustClient, goClient, 20)

	cases := []struct {
		name   string
		limit  int
		offset int
	}{
		{"limit_5", 5, 0},
		{"offset_5", 20, 5},
		{"limit_3_offset_10", 3, 10},
		{"limit_1", 1, 0},
		{"limit_10_offset_15", 10, 15},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			query := framework.QueryArticlesLimitOffsetQuery(c.limit, c.offset)

			rustResp, err := rustClient.GraphQL(ctx, query, nil)
			require.NoError(t, err, "Rust: limit/offset query failed")

			goResp, err := goClient.GraphQL(ctx, query, nil)
			require.NoError(t, err, "Go: limit/offset query failed")

			framework.CompareGraphQLResponses(t, rustResp, goResp, fmt.Sprintf("limit_offset %s", c.name))
		})
	}
}

// TestQueryAggregateParity tests that aggregate queries produce the same results.
func TestQueryAggregateParity(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rustNode, goNode, id := startMirrorNodes(t, ctx)
	rustClient := rustNode.Client().WithIdentity(id)
	goClient := goNode.Client().WithIdentity(id)

	_, err := rustClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err)
	_, err = goClient.AddSchema(ctx, framework.ArticleSchema)
	require.NoError(t, err)

	seedArticles(t, ctx, rustClient, goClient, 20)

	queries := []struct {
		name  string
		query string
	}{
		{"count", framework.QueryArticlesAggregateQuery()},
		{"group_aggregate", framework.QueryArticlesGroupAggregateQuery()},
	}

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			rustResp, err := rustClient.GraphQL(ctx, q.query, nil)
			require.NoError(t, err, "Rust: aggregate query failed")

			goResp, err := goClient.GraphQL(ctx, q.query, nil)
			require.NoError(t, err, "Go: aggregate query failed")

			framework.CompareGraphQLResponses(t, rustResp, goResp, fmt.Sprintf("aggregate %s", q.name))
		})
	}
}
