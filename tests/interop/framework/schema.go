package framework

import "fmt"

// UsersSchema is a simple test schema for user documents.
const UsersSchema = `
type Users {
    name: String
    age: Int
}
`

// UserACPPolicy is an ACP policy YAML for document-level access control.
const UserACPPolicy = `
name: test-user-policy
description: Policy for user document access control
resources:
  users:
    permissions:
      read:
        expr: owner + reader + admin
      write:
        expr: owner + admin
      delete:
        expr: owner
    relations:
      owner:
        types:
          - actor
      reader:
        types:
          - actor
      admin:
        types:
          - actor
        manages:
          - reader
`

// ArticleSchema is a schema for article documents (no ACP).
const ArticleSchema = `
type Article {
    title: String
    body: String
    rating: Int
    published: Boolean
}
`

// UsersSchemaWithPolicy returns a Users SDL with @policy directive.
func UsersSchemaWithPolicy(policyID string) string {
	return fmt.Sprintf(`
type Users @policy(id: "%s", resource: "users") {
    name: String
    age: Int
}
`, policyID)
}

// ArticleSchemaWithPolicy returns an Article SDL with @policy directive.
func ArticleSchemaWithPolicy(policyID string) string {
	return fmt.Sprintf(`
type Article @policy(id: "%s", resource: "users") {
    title: String
    body: String
    rating: Int
    published: Boolean
}
`, policyID)
}

// AddSchemaQuery generates a GraphQL mutation to add a schema.
func AddSchemaQuery(schema string) string {
	return fmt.Sprintf(`mutation { addSchema(schema: %q) }`, schema)
}

// CreateUserQuery generates a GraphQL mutation to create a user document.
func CreateUserQuery(name string, age int) string {
	return fmt.Sprintf(`mutation { create_Users(input: {name: %q, age: %d}) { _docID name age } }`, name, age)
}

// QueryUsersQuery generates a GraphQL query to fetch all users.
func QueryUsersQuery() string {
	return `{ Users { _docID name age } }`
}

// CreateArticleQuery generates a GraphQL mutation to create an article.
func CreateArticleQuery(title, body string, rating int, published bool) string {
	return fmt.Sprintf(
		`mutation { create_Article(input: {title: %q, body: %q, rating: %d, published: %t}) { _docID title body rating published } }`,
		title, body, rating, published,
	)
}

// QueryArticlesQuery generates a GraphQL query to fetch all articles.
func QueryArticlesQuery() string {
	return `{ Article { _docID title body rating published } }`
}

// QueryArticlesWithFilterQuery generates a filtered article query.
func QueryArticlesWithFilterQuery(filter string) string {
	return fmt.Sprintf(`{ Article(filter: %s) { _docID title body rating published } }`, filter)
}

// QueryArticlesOrderedQuery generates an ordered article query.
func QueryArticlesOrderedQuery(orderClause string) string {
	return fmt.Sprintf(`{ Article(order: %s) { _docID title body rating published } }`, orderClause)
}

// QueryArticlesLimitOffsetQuery generates a query with limit and offset.
func QueryArticlesLimitOffsetQuery(limit, offset int) string {
	return fmt.Sprintf(`{ Article(limit: %d, offset: %d) { _docID title body rating published } }`, limit, offset)
}

// QueryArticlesAggregateQuery generates an aggregate query for articles.
func QueryArticlesAggregateQuery() string {
	return `{ _count(Article: {}) }`
}

// QueryArticlesGroupAggregateQuery generates a grouped aggregate query.
func QueryArticlesGroupAggregateQuery() string {
	return `{
		Article(groupBy: [published]) {
			published
			_group {
				title
				rating
			}
			_count(_group: {})
			_avg(_group: {field: rating})
			_sum(_group: {field: rating})
		}
	}`
}
