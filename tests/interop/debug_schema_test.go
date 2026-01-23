package interop

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sourcenetwork/defradb.rs-interop/tests/interop/framework"
	"github.com/stretchr/testify/require"
)

// SchemaField represents a field in a schema
type SchemaField struct {
	Name         string  `json:"Name"`
	FieldID      string  `json:"FieldID"`
	RelationName *string `json:"RelationName"`
	IsPrimary    bool    `json:"IsPrimary"`
	Typ          int     `json:"Typ"`
	Kind         any     `json:"Kind"`
}

// SchemaDefinition represents a collection schema
type SchemaDefinition struct {
	Name         string        `json:"Name"`
	CollectionID string        `json:"CollectionID"`
	VersionID    string        `json:"VersionID"`
	Fields       []SchemaField `json:"Fields"`
}

// addSchemaWithFields adds schema and returns full schema with fields
func addSchemaWithFields(baseURL, sdl string) ([]SchemaDefinition, error) {
	req, err := http.NewRequest(http.MethodPost, baseURL+"/api/v0/schema", strings.NewReader(sdl))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("add schema failed: %s", string(body))
	}

	var result []SchemaDefinition
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal: %w, body: %s", err, string(body))
	}
	return result, nil
}

// TestDebugSchemaFields dumps full field info for comparison
func TestDebugSchemaFields(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Reserve ports
	rustPorts, err := framework.ReserveNodePorts()
	require.NoError(t, err)
	defer rustPorts.Release()

	goPorts, err := framework.ReserveNodePorts()
	require.NoError(t, err)
	defer goPorts.Release()

	// Start Rust node
	rustNode := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     rustPorts.HTTPPort,
		P2PPort:      rustPorts.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})
	rustPorts.Release()
	require.NoError(t, rustNode.Start(ctx))
	defer rustNode.Stop()

	// Start Go node
	goNode := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeGo,
		HTTPPort:     goPorts.HTTPPort,
		P2PPort:      goPorts.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})
	goPorts.Release()
	require.NoError(t, goNode.Start(ctx))
	defer goNode.Stop()

	// Add schema to both nodes - full RelationsSchema
	sdl := `type Person {
	name: String
	email: String
	profile: Profile @relation
}

type Profile {
	bio: String
	avatar: String
	website: String
	person: Person @relation @primary
}

type Author {
	name: String
	country: String
	books: [Book] @relation
}

type Book {
	title: String
	isbn: String
	publishYear: Int
	genre: String
	author: Author @relation
}

type Student {
	studentId: String
	name: String
	major: String
	enrollments: [Enrollment] @relation
}

type Course {
	courseCode: String
	title: String
	credits: Int
	department: String
	enrollments: [Enrollment] @relation
}

type Enrollment {
	enrollmentDate: DateTime
	grade: String
	status: String
	student: Student @relation
	course: Course @relation
}

type Employee {
	employeeId: String
	name: String
	title: String
	department: String
	hireDate: DateTime
	manager: Employee @relation
	directReports: [Employee] @relation
}`

	rustSchemas, err := addSchemaWithFields(rustNode.HTTPURL(), sdl)
	require.NoError(t, err)

	goSchemas, err := addSchemaWithFields(goNode.HTTPURL(), sdl)
	require.NoError(t, err)

	// Find Person, Profile, Employee, and Enrollment in both
	var rustPerson, goPerson, rustProfile, goProfile, rustEmployee, goEmployee, rustEnrollment, goEnrollment, rustStudent, goStudent *SchemaDefinition
	for i := range rustSchemas {
		switch rustSchemas[i].Name {
		case "Person":
			rustPerson = &rustSchemas[i]
		case "Profile":
			rustProfile = &rustSchemas[i]
		case "Employee":
			rustEmployee = &rustSchemas[i]
		case "Enrollment":
			rustEnrollment = &rustSchemas[i]
		case "Student":
			rustStudent = &rustSchemas[i]
		}
	}
	for i := range goSchemas {
		switch goSchemas[i].Name {
		case "Person":
			goPerson = &goSchemas[i]
		case "Profile":
			goProfile = &goSchemas[i]
		case "Employee":
			goEmployee = &goSchemas[i]
		case "Enrollment":
			goEnrollment = &goSchemas[i]
		case "Student":
			goStudent = &goSchemas[i]
		}
	}

	require.NotNil(t, rustPerson, "Rust Person not found")
	require.NotNil(t, goPerson, "Go Person not found")
	require.NotNil(t, rustProfile, "Rust Profile not found")
	require.NotNil(t, goProfile, "Go Profile not found")

	t.Logf("=== RUST Person ===")
	t.Logf("CollectionID: %s", rustPerson.CollectionID)
	t.Logf("Fields:")
	for _, f := range rustPerson.Fields {
		relName := "<nil>"
		if f.RelationName != nil {
			relName = *f.RelationName
		}
		kindJSON, _ := json.Marshal(f.Kind)
		t.Logf("  %s: FieldID=%s, RelationName=%s, IsPrimary=%v, Typ=%d, Kind=%s", f.Name, f.FieldID, relName, f.IsPrimary, f.Typ, string(kindJSON))
	}

	t.Logf("")
	t.Logf("=== GO Person ===")
	t.Logf("CollectionID: %s", goPerson.CollectionID)
	t.Logf("Fields:")
	for _, f := range goPerson.Fields {
		relName := "<nil>"
		if f.RelationName != nil {
			relName = *f.RelationName
		}
		kindJSON, _ := json.Marshal(f.Kind)
		t.Logf("  %s: FieldID=%s, RelationName=%s, IsPrimary=%v, Typ=%d, Kind=%s", f.Name, f.FieldID, relName, f.IsPrimary, f.Typ, string(kindJSON))
	}

	t.Logf("")
	t.Logf("=== RUST Profile ===")
	t.Logf("CollectionID: %s", rustProfile.CollectionID)
	t.Logf("Fields:")
	for _, f := range rustProfile.Fields {
		relName := "<nil>"
		if f.RelationName != nil {
			relName = *f.RelationName
		}
		kindJSON, _ := json.Marshal(f.Kind)
		t.Logf("  %s: FieldID=%s, RelationName=%s, IsPrimary=%v, Typ=%d, Kind=%s", f.Name, f.FieldID, relName, f.IsPrimary, f.Typ, string(kindJSON))
	}

	t.Logf("")
	t.Logf("=== GO Profile ===")
	t.Logf("CollectionID: %s", goProfile.CollectionID)
	t.Logf("Fields:")
	for _, f := range goProfile.Fields {
		relName := "<nil>"
		if f.RelationName != nil {
			relName = *f.RelationName
		}
		kindJSON, _ := json.Marshal(f.Kind)
		t.Logf("  %s: FieldID=%s, RelationName=%s, IsPrimary=%v, Typ=%d, Kind=%s", f.Name, f.FieldID, relName, f.IsPrimary, f.Typ, string(kindJSON))
	}

	t.Logf("")
	t.Logf("=== RUST Employee ===")
	if rustEmployee != nil {
		t.Logf("CollectionID: %s", rustEmployee.CollectionID)
		t.Logf("Fields:")
		for _, f := range rustEmployee.Fields {
			relName := "<nil>"
			if f.RelationName != nil {
				relName = *f.RelationName
			}
			kindJSON, _ := json.Marshal(f.Kind)
			t.Logf("  %s: FieldID=%s, RelationName=%s, IsPrimary=%v, Typ=%d, Kind=%s", f.Name, f.FieldID, relName, f.IsPrimary, f.Typ, string(kindJSON))
		}
	}

	t.Logf("")
	t.Logf("=== GO Employee ===")
	if goEmployee != nil {
		t.Logf("CollectionID: %s", goEmployee.CollectionID)
		t.Logf("Fields:")
		for _, f := range goEmployee.Fields {
			relName := "<nil>"
			if f.RelationName != nil {
				relName = *f.RelationName
			}
			kindJSON, _ := json.Marshal(f.Kind)
			t.Logf("  %s: FieldID=%s, RelationName=%s, IsPrimary=%v, Typ=%d, Kind=%s", f.Name, f.FieldID, relName, f.IsPrimary, f.Typ, string(kindJSON))
		}
	}

	t.Logf("")
	t.Logf("=== RUST Enrollment ===")
	if rustEnrollment != nil {
		t.Logf("CollectionID: %s", rustEnrollment.CollectionID)
		t.Logf("Fields:")
		for _, f := range rustEnrollment.Fields {
			relName := "<nil>"
			if f.RelationName != nil {
				relName = *f.RelationName
			}
			kindJSON, _ := json.Marshal(f.Kind)
			t.Logf("  %s: FieldID=%s, RelationName=%s, IsPrimary=%v, Typ=%d, Kind=%s", f.Name, f.FieldID, relName, f.IsPrimary, f.Typ, string(kindJSON))
		}
	}

	t.Logf("")
	t.Logf("=== GO Enrollment ===")
	if goEnrollment != nil {
		t.Logf("CollectionID: %s", goEnrollment.CollectionID)
		t.Logf("Fields:")
		for _, f := range goEnrollment.Fields {
			relName := "<nil>"
			if f.RelationName != nil {
				relName = *f.RelationName
			}
			kindJSON, _ := json.Marshal(f.Kind)
			t.Logf("  %s: FieldID=%s, RelationName=%s, IsPrimary=%v, Typ=%d, Kind=%s", f.Name, f.FieldID, relName, f.IsPrimary, f.Typ, string(kindJSON))
		}
	}

	t.Logf("")
	t.Logf("=== RUST Student ===")
	if rustStudent != nil {
		t.Logf("CollectionID: %s", rustStudent.CollectionID)
	}

	t.Logf("")
	t.Logf("=== GO Student ===")
	if goStudent != nil {
		t.Logf("CollectionID: %s", goStudent.CollectionID)
	}

	// Assert match
	require.Equal(t, goPerson.CollectionID, rustPerson.CollectionID, "Person CollectionID mismatch")
	require.Equal(t, goProfile.CollectionID, rustProfile.CollectionID, "Profile CollectionID mismatch")
	if goEmployee != nil && rustEmployee != nil {
		require.Equal(t, goEmployee.CollectionID, rustEmployee.CollectionID, "Employee CollectionID mismatch")
	}
	if goEnrollment != nil && rustEnrollment != nil {
		require.Equal(t, goEnrollment.CollectionID, rustEnrollment.CollectionID, "Enrollment CollectionID mismatch")
	}
}
