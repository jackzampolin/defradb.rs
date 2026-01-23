// Package schemas provides a corpus of test schemas for conformance testing
// between Go and Rust DefraDB implementations.
//
// Each schema file contains:
// - GraphQL SDL schema definition (const)
// - Collection name constants
// - Document structs for typed creation
// - Mutation helper functions (CreateX)
// - Query helper functions (QueryX)
//
// Available schemas:
//   - AllTypes:     Tests every field type (scalars, arrays, nullability)
//   - IoTSensor:    Device -> SensorReading (nested, time-series)
//   - Maritime:     Vessel -> Voyage -> PortCall -> Event (4 levels deep)
//   - RetailPOS:    Store -> Transaction -> LineItem (financial pattern)
//   - Relations:    1:1, 1:N, M:N via junction, self-referential
package schemas

// SchemaInfo describes a test schema for indexing and selection.
type SchemaInfo struct {
	Name        string
	Schema      string
	Collections []string
	Description string
}

// AllSchemas returns all available test schemas for batch operations.
func AllSchemas() []SchemaInfo {
	return []SchemaInfo{
		{
			Name:        "AllTypes",
			Schema:      AllTypesSchema,
			Collections: []string{AllTypesCollectionName},
			Description: "Tests all field types: String, Int, Float, Boolean, DateTime, arrays, nullability",
		},
		{
			Name:   "IoTSensor",
			Schema: IoTSensorSchema,
			Collections: []string{
				IoTCollectionNames.Device,
				IoTCollectionNames.SensorReading,
			},
			Description: "IoT pattern with devices and time-series sensor readings",
		},
		{
			Name:   "Maritime",
			Schema: MaritimeTrackingSchema,
			Collections: []string{
				MaritimeCollectionNames.Vessel,
				MaritimeCollectionNames.Voyage,
				MaritimeCollectionNames.PortCall,
				MaritimeCollectionNames.PortEvent,
			},
			Description: "Maritime tracking with 4-level nesting (Vessel->Voyage->PortCall->Event)",
		},
		{
			Name:   "RetailPOS",
			Schema: RetailPOSSchema,
			Collections: []string{
				RetailCollectionNames.Store,
				RetailCollectionNames.Product,
				RetailCollectionNames.Transaction,
				RetailCollectionNames.LineItem,
			},
			Description: "Retail POS with transactions, line items, and products",
		},
		{
			Name:   "Relations",
			Schema: RelationsSchema,
			Collections: []string{
				RelationsCollectionNames.Person,
				RelationsCollectionNames.Profile,
				RelationsCollectionNames.Author,
				RelationsCollectionNames.Book,
				RelationsCollectionNames.Student,
				RelationsCollectionNames.Course,
				RelationsCollectionNames.Enrollment,
				RelationsCollectionNames.Employee,
			},
			Description: "Tests 1:1, 1:N, M:N (via junction), and self-referential relationships",
		},
	}
}

// GetSchema returns a specific schema by name, or nil if not found.
func GetSchema(name string) *SchemaInfo {
	for _, s := range AllSchemas() {
		if s.Name == name {
			return &s
		}
	}
	return nil
}
