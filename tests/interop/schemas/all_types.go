package schemas

import "fmt"

// AllTypesSchema tests every field type supported by DefraDB.
// This ensures both implementations handle all scalar types and arrays.
// Note: NonNull (!) fields are not yet supported by Go DefraDB, so all fields are nullable.
const AllTypesSchema = `
type AllTypes {
	stringField: String
	stringRequired: String
	intField: Int
	intRequired: Int
	floatField: Float
	floatRequired: Float
	boolField: Boolean
	boolRequired: Boolean
	dateTimeField: DateTime

	stringArray: [String]
	intArray: [Int]
	floatArray: [Float]
	boolArray: [Boolean]
}
`

// AllTypesCollectionName is the collection name for AllTypes documents.
const AllTypesCollectionName = "AllTypes"

// CreateAllTypesDoc generates a GraphQL mutation to create an AllTypes document.
func CreateAllTypesDoc(doc AllTypesDoc) string {
	return fmt.Sprintf(`mutation {
		create_AllTypes(input: {
			stringField: %s
			stringRequired: %q
			intField: %s
			intRequired: %d
			floatField: %s
			floatRequired: %f
			boolField: %s
			boolRequired: %t
			dateTimeField: %s
			stringArray: %s
			intArray: %s
			floatArray: %s
			boolArray: %s
		}) {
			_docID
		}
	}`,
		nullableString(doc.StringField),
		doc.StringRequired,
		nullableInt(doc.IntField),
		doc.IntRequired,
		nullableFloat(doc.FloatField),
		doc.FloatRequired,
		nullableBool(doc.BoolField),
		doc.BoolRequired,
		nullableString(doc.DateTimeField),
		formatStringArray(doc.StringArray),
		formatIntArray(doc.IntArray),
		formatFloatArray(doc.FloatArray),
		formatBoolArray(doc.BoolArray),
	)
}

// AllTypesDoc represents the input data for creating an AllTypes document.
type AllTypesDoc struct {
	StringField   *string
	StringRequired string
	IntField      *int
	IntRequired   int
	FloatField    *float64
	FloatRequired float64
	BoolField     *bool
	BoolRequired  bool
	DateTimeField *string
	StringArray   []string
	IntArray      []int
	FloatArray    []float64
	BoolArray     []bool
}

// QueryAllTypes generates a GraphQL query to fetch all AllTypes documents.
func QueryAllTypes() string {
	return `{
		AllTypes {
			_docID
			stringField
			stringRequired
			intField
			intRequired
			floatField
			floatRequired
			boolField
			boolRequired
			dateTimeField
			stringArray
			intArray
			floatArray
			boolArray
		}
	}`
}

// Helper functions for nullable field formatting

func nullableString(s *string) string {
	if s == nil {
		return "null"
	}
	return fmt.Sprintf("%q", *s)
}

func nullableInt(i *int) string {
	if i == nil {
		return "null"
	}
	return fmt.Sprintf("%d", *i)
}

func nullableFloat(f *float64) string {
	if f == nil {
		return "null"
	}
	return fmt.Sprintf("%f", *f)
}

func nullableBool(b *bool) string {
	if b == nil {
		return "null"
	}
	return fmt.Sprintf("%t", *b)
}

func formatStringArray(arr []string) string {
	if arr == nil {
		return "null"
	}
	result := "["
	for i, s := range arr {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%q", s)
	}
	return result + "]"
}

func formatIntArray(arr []int) string {
	if arr == nil {
		return "null"
	}
	result := "["
	for i, v := range arr {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%d", v)
	}
	return result + "]"
}

func formatFloatArray(arr []float64) string {
	if arr == nil {
		return "null"
	}
	result := "["
	for i, v := range arr {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%f", v)
	}
	return result + "]"
}

func formatBoolArray(arr []bool) string {
	if arr == nil {
		return "null"
	}
	result := "["
	for i, v := range arr {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%t", v)
	}
	return result + "]"
}
