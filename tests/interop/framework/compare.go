package framework

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
)

// CompareGraphQLResponses compares GraphQL responses from Rust and Go nodes.
// It normalizes JSON before comparison and reports clear diffs.
func CompareGraphQLResponses(t *testing.T, rustResp, goResp *GraphQLResponse, description string) {
	t.Helper()

	// Compare errors
	if len(rustResp.Errors) > 0 || len(goResp.Errors) > 0 {
		rustErrs := formatErrors(rustResp.Errors)
		goErrs := formatErrors(goResp.Errors)
		if rustErrs != goErrs {
			t.Errorf("%s: error mismatch\n  Rust errors: %s\n  Go errors:   %s", description, rustErrs, goErrs)
		}
		return
	}

	// Compare data
	CompareJSON(t, rustResp.Data, goResp.Data, description)
}

// CompareJSON compares two JSON values after normalization.
// It sorts object keys and array elements for deterministic comparison.
func CompareJSON(t *testing.T, rustJSON, goJSON json.RawMessage, description string) bool {
	t.Helper()

	rustNorm, err := normalizeJSON(rustJSON)
	if err != nil {
		t.Errorf("%s: failed to normalize Rust JSON: %v", description, err)
		return false
	}

	goNorm, err := normalizeJSON(goJSON)
	if err != nil {
		t.Errorf("%s: failed to normalize Go JSON: %v", description, err)
		return false
	}

	if !reflect.DeepEqual(rustNorm, goNorm) {
		rustPretty, _ := json.MarshalIndent(rustNorm, "  ", "  ")
		goPretty, _ := json.MarshalIndent(goNorm, "  ", "  ")
		t.Errorf("%s: response mismatch\n  Rust: %s\n  Go:   %s", description, string(rustPretty), string(goPretty))
		return false
	}

	return true
}

// normalizeJSON unmarshals JSON and normalizes it for comparison.
// - null and missing are treated as equivalent
// - arrays of objects are sorted by _docID if present, otherwise by JSON representation
func normalizeJSON(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}

	return normalizeValue(v), nil
}

func normalizeValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		result := make(map[string]any, len(val))
		for k, child := range val {
			result[k] = normalizeValue(child)
		}
		return result
	case []any:
		normalized := make([]any, len(val))
		for i, child := range val {
			normalized[i] = normalizeValue(child)
		}
		sortArrayForComparison(normalized)
		return normalized
	default:
		return v
	}
}

// sortArrayForComparison sorts an array of values for deterministic comparison.
// If elements are objects with _docID, sort by _docID. Otherwise sort by JSON string.
func sortArrayForComparison(arr []any) {
	if len(arr) <= 1 {
		return
	}

	// Check if all elements are objects with _docID
	allHaveDocID := true
	for _, elem := range arr {
		obj, ok := elem.(map[string]any)
		if !ok {
			allHaveDocID = false
			break
		}
		if _, has := obj["_docID"]; !has {
			allHaveDocID = false
			break
		}
	}

	if allHaveDocID {
		sort.Slice(arr, func(i, j int) bool {
			iID := arr[i].(map[string]any)["_docID"].(string)
			jID := arr[j].(map[string]any)["_docID"].(string)
			return iID < jID
		})
		return
	}

	// Fallback: sort by JSON string representation
	sort.Slice(arr, func(i, j int) bool {
		iJSON, _ := json.Marshal(arr[i])
		jJSON, _ := json.Marshal(arr[j])
		return string(iJSON) < string(jJSON)
	})
}

func formatErrors(errs []GraphQLError) string {
	if len(errs) == 0 {
		return "<none>"
	}
	result := ""
	for i, e := range errs {
		if i > 0 {
			result += "; "
		}
		result += e.Message
	}
	return result
}
