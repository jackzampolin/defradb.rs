package framework

import "fmt"

// UsersSchema is a simple test schema for user documents.
const UsersSchema = `
type Users {
    name: String
    age: Int
}
`

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
