// Package integration provides a test runner for executing DefraDB test cases
// against the Rust FFI implementation.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
	"github.com/sourcenetwork/defradb.rs-interop/tests/interop/ffi"
)

// TestState tracks state during test execution.
type TestState struct {
	ctx         context.Context
	t           testing.TB
	node        *ffi.Node
	client      *ffi.ClientWrapper
	collections []client.CollectionVersion
	// docIDs maps [collectionIndex][docIndex] -> docID
	docIDs [][]string
	// collectionNames maps collectionIndex -> name
	collectionNames []string
}

// NewTestState creates a new test state with an in-memory FFI node.
func NewTestState(t testing.TB) *TestState {
	node, err := ffi.NewNode(ffi.NodeOptions{InMemory: true})
	require.NoError(t, err, "failed to create FFI node")

	return &TestState{
		ctx:    context.Background(),
		t:      t,
		node:   node,
		client: ffi.NewClientWrapper(node),
	}
}

// Close cleans up test resources.
func (s *TestState) Close() {
	if s.client != nil {
		s.client.Close()
	}
	if s.node != nil {
		s.node.Close()
	}
}

// ExecuteTestCase runs a DefraDB TestCase against the Rust FFI implementation.
func ExecuteTestCase(t testing.TB, test testUtils.TestCase) {
	state := NewTestState(t)
	defer state.Close()

	for i, act := range test.Actions {
		err := state.performAction(act)
		if err != nil {
			t.Fatalf("action %d failed: %v", i, err)
		}
	}
}

// performAction executes a single test action.
func (s *TestState) performAction(act any) error {
	switch a := act.(type) {
	case *action.AddSchema:
		return s.addSchema(a)
	case testUtils.CreateDoc:
		return s.createDoc(a)
	case testUtils.DeleteDoc:
		return s.deleteDoc(a)
	case testUtils.UpdateDoc:
		return s.updateDoc(a)
	case testUtils.Request:
		return s.executeRequest(a)
	case testUtils.CreateIndex:
		return s.createIndex(a)
	case testUtils.DropIndex:
		return s.dropIndex(a)
	case testUtils.GetIndexes:
		return s.getIndexes(a)
	case testUtils.PatchCollection:
		return s.patchCollection(a)
	case testUtils.GetCollections:
		return s.getCollections(a)
	case testUtils.SetActiveCollectionVersion:
		return s.setActiveCollectionVersion(a)
	case testUtils.CreateView:
		return s.createView(a)
	case testUtils.SetupComplete:
		// No-op for our purposes
		return nil
	case testUtils.Wait:
		// No-op for single-node tests
		return nil
	default:
		return fmt.Errorf("unsupported action type: %T", act)
	}
}

// addSchema adds a schema to the database.
func (s *TestState) addSchema(a *action.AddSchema) error {
	versions, err := s.client.AddSchema(s.ctx, a.Schema)
	if err != nil {
		if a.ExpectedError != "" && strings.Contains(err.Error(), a.ExpectedError) {
			return nil
		}
		return fmt.Errorf("AddSchema failed: %w", err)
	}
	if a.ExpectedError != "" {
		return fmt.Errorf("expected error containing %q but got none", a.ExpectedError)
	}

	// Sort collections by their order of appearance in the SDL
	// This ensures consistent collection indices across Go and Rust
	orderedVersions := orderCollectionsBySDL(a.Schema, versions)

	s.collections = append(s.collections, orderedVersions...)
	for _, v := range orderedVersions {
		s.collectionNames = append(s.collectionNames, v.Name)
		s.docIDs = append(s.docIDs, []string{})
	}

	return nil
}

// orderCollectionsBySDL orders collection versions by their appearance in the SDL.
func orderCollectionsBySDL(sdl string, versions []client.CollectionVersion) []client.CollectionVersion {
	if len(versions) <= 1 {
		return versions
	}

	// Build a map of collection name -> order in SDL
	orderMap := make(map[string]int)
	for i, v := range versions {
		// Find "type <Name>" in the SDL
		idx := strings.Index(sdl, "type "+v.Name)
		if idx >= 0 {
			orderMap[v.Name] = idx
		} else {
			// If not found, use original order
			orderMap[v.Name] = i * 1000
		}
	}

	// Sort by SDL order
	ordered := make([]client.CollectionVersion, len(versions))
	copy(ordered, versions)

	for i := 0; i < len(ordered)-1; i++ {
		for j := i + 1; j < len(ordered); j++ {
			if orderMap[ordered[i].Name] > orderMap[ordered[j].Name] {
				ordered[i], ordered[j] = ordered[j], ordered[i]
			}
		}
	}

	return ordered
}

// createDoc creates a document in the specified collection.
func (s *TestState) createDoc(a testUtils.CreateDoc) error {
	if a.CollectionID >= len(s.collectionNames) {
		return fmt.Errorf("collection index %d out of range (have %d collections)", a.CollectionID, len(s.collectionNames))
	}

	collectionName := s.collectionNames[a.CollectionID]

	// Build document input for GraphQL
	var docInput string
	if a.DocMap != nil {
		// Substitute DocIndex references with actual docIDs
		resolved := s.resolveDocIndexes(a.DocMap)
		docInput = mapToGraphQLInput(resolved)
	} else {
		// Convert JSON to GraphQL input format (unquoted field names)
		docInput = jsonToGraphQLInput(a.Doc)
	}

	// Create via GraphQL mutation
	mutation := fmt.Sprintf(`mutation { create_%s(input: %s) { _docID } }`, collectionName, docInput)
	result := s.client.ExecRequest(s.ctx, mutation)

	if len(result.GQL.Errors) > 0 {
		errStr := result.GQL.Errors[0].Error()
		if a.ExpectedError != "" && strings.Contains(errStr, a.ExpectedError) {
			return nil
		}
		return fmt.Errorf("create mutation failed: %v", result.GQL.Errors)
	}
	if a.ExpectedError != "" {
		return fmt.Errorf("expected error containing %q but got none", a.ExpectedError)
	}

	// Extract and store docID
	docID, err := extractDocID(result.GQL.Data, "create_"+collectionName)
	if err != nil {
		return fmt.Errorf("failed to extract docID: %w", err)
	}

	s.docIDs[a.CollectionID] = append(s.docIDs[a.CollectionID], docID)
	return nil
}

// deleteDoc deletes a document from the specified collection.
func (s *TestState) deleteDoc(a testUtils.DeleteDoc) error {
	if a.CollectionID >= len(s.collectionNames) {
		return fmt.Errorf("collection index %d out of range", a.CollectionID)
	}
	if a.DocID >= len(s.docIDs[a.CollectionID]) {
		return fmt.Errorf("doc index %d out of range for collection %d", a.DocID, a.CollectionID)
	}

	collectionName := s.collectionNames[a.CollectionID]
	docID := s.docIDs[a.CollectionID][a.DocID]

	mutation := fmt.Sprintf(`mutation { delete_%s(docID: "%s") { _docID } }`, collectionName, docID)
	result := s.client.ExecRequest(s.ctx, mutation)

	if len(result.GQL.Errors) > 0 {
		errStr := result.GQL.Errors[0].Error()
		if a.ExpectedError != "" && strings.Contains(errStr, a.ExpectedError) {
			return nil
		}
		return fmt.Errorf("delete mutation failed: %v", result.GQL.Errors)
	}
	if a.ExpectedError != "" {
		return fmt.Errorf("expected error containing %q but got none", a.ExpectedError)
	}

	return nil
}

// updateDoc updates a document in the specified collection.
func (s *TestState) updateDoc(a testUtils.UpdateDoc) error {
	if a.CollectionID >= len(s.collectionNames) {
		return fmt.Errorf("collection index %d out of range", a.CollectionID)
	}
	if a.DocID >= len(s.docIDs[a.CollectionID]) {
		return fmt.Errorf("doc index %d out of range for collection %d", a.DocID, a.CollectionID)
	}

	collectionName := s.collectionNames[a.CollectionID]
	docID := s.docIDs[a.CollectionID][a.DocID]

	mutation := fmt.Sprintf(`mutation { update_%s(docID: "%s", input: %s) { _docID } }`,
		collectionName, docID, a.Doc)
	result := s.client.ExecRequest(s.ctx, mutation)

	if len(result.GQL.Errors) > 0 {
		errStr := result.GQL.Errors[0].Error()
		if a.ExpectedError != "" && strings.Contains(errStr, a.ExpectedError) {
			return nil
		}
		return fmt.Errorf("update mutation failed: %v", result.GQL.Errors)
	}
	if a.ExpectedError != "" {
		return fmt.Errorf("expected error containing %q but got none", a.ExpectedError)
	}

	return nil
}

// executeRequest executes a GraphQL query and verifies results.
func (s *TestState) executeRequest(a testUtils.Request) error {
	// Substitute placeholders in the request
	request := s.replaceDocIndexPlaceholders(a.Request)

	result := s.client.ExecRequest(s.ctx, request)

	// Check for expected errors
	if a.ExpectedError != "" {
		if len(result.GQL.Errors) == 0 {
			return fmt.Errorf("expected error containing %q but got none", a.ExpectedError)
		}
		errStr := result.GQL.Errors[0].Error()
		if !strings.Contains(errStr, a.ExpectedError) {
			return fmt.Errorf("expected error containing %q, got: %s", a.ExpectedError, errStr)
		}
		return nil
	}

	// Check for unexpected errors
	if len(result.GQL.Errors) > 0 {
		return fmt.Errorf("query failed: %v", result.GQL.Errors)
	}

	// Verify results if provided
	if a.Results != nil {
		data, ok := result.GQL.Data.(map[string]any)
		if !ok {
			return fmt.Errorf("expected map data, got %T", result.GQL.Data)
		}

		// Replace DocIndex in expected results with actual docIDs
		expectedResults := s.resolveDocIndexesInResults(a.Results)

		if err := s.assertResults(data, expectedResults, a.NonOrderedResults); err != nil {
			return err
		}
	}

	return nil
}

// createIndex creates an index on a collection.
func (s *TestState) createIndex(a testUtils.CreateIndex) error {
	if a.CollectionID >= len(s.collectionNames) {
		return fmt.Errorf("collection index %d out of range", a.CollectionID)
	}

	collection, err := s.client.GetCollectionByName(s.ctx, s.collectionNames[a.CollectionID])
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}

	fields := make([]client.IndexedFieldDescription, 0)
	if a.FieldName != "" {
		fields = append(fields, client.IndexedFieldDescription{Name: a.FieldName})
	}
	for _, f := range a.Fields {
		fields = append(fields, client.IndexedFieldDescription{
			Name:       f.Name,
			Descending: f.Descending,
		})
	}

	_, err = collection.CreateIndex(s.ctx, client.IndexCreateRequest{
		Name:   a.IndexName,
		Fields: fields,
		Unique: a.Unique,
	})

	if err != nil {
		if a.ExpectedError != "" && strings.Contains(err.Error(), a.ExpectedError) {
			return nil
		}
		return fmt.Errorf("CreateIndex failed: %w", err)
	}
	if a.ExpectedError != "" {
		return fmt.Errorf("expected error containing %q but got none", a.ExpectedError)
	}

	return nil
}

// dropIndex drops an index from a collection.
func (s *TestState) dropIndex(a testUtils.DropIndex) error {
	if a.CollectionID >= len(s.collectionNames) {
		return fmt.Errorf("collection index %d out of range", a.CollectionID)
	}

	collection, err := s.client.GetCollectionByName(s.ctx, s.collectionNames[a.CollectionID])
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}

	err = collection.DropIndex(s.ctx, a.IndexName)
	if err != nil {
		if a.ExpectedError != "" && strings.Contains(err.Error(), a.ExpectedError) {
			return nil
		}
		return fmt.Errorf("DropIndex failed: %w", err)
	}
	if a.ExpectedError != "" {
		return fmt.Errorf("expected error containing %q but got none", a.ExpectedError)
	}

	return nil
}

// getIndexes retrieves indexes from a collection.
func (s *TestState) getIndexes(a testUtils.GetIndexes) error {
	if a.CollectionID >= len(s.collectionNames) {
		return fmt.Errorf("collection index %d out of range", a.CollectionID)
	}

	collection, err := s.client.GetCollectionByName(s.ctx, s.collectionNames[a.CollectionID])
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}

	indexes, err := collection.GetIndexes(s.ctx)
	if err != nil {
		if a.ExpectedError != "" && strings.Contains(err.Error(), a.ExpectedError) {
			return nil
		}
		return fmt.Errorf("GetIndexes failed: %w", err)
	}
	if a.ExpectedError != "" {
		return fmt.Errorf("expected error containing %q but got none", a.ExpectedError)
	}

	// Verify expected indexes
	if len(a.ExpectedIndexes) > 0 {
		assert.Equal(s.t, len(a.ExpectedIndexes), len(indexes), "index count mismatch")
		for i, expected := range a.ExpectedIndexes {
			if i < len(indexes) {
				assert.Equal(s.t, expected.Name, indexes[i].Name, "index name mismatch")
				assert.Equal(s.t, expected.Unique, indexes[i].Unique, "index unique mismatch")
			}
		}
	}

	return nil
}

// patchCollection patches a collection.
func (s *TestState) patchCollection(a testUtils.PatchCollection) error {
	err := s.client.PatchCollection(s.ctx, a.Patch, a.Lens)
	if err != nil {
		if a.ExpectedError != "" && strings.Contains(err.Error(), a.ExpectedError) {
			return nil
		}
		return fmt.Errorf("PatchCollection failed: %w", err)
	}
	if a.ExpectedError != "" {
		return fmt.Errorf("expected error containing %q but got none", a.ExpectedError)
	}
	return nil
}

// getCollections retrieves collections.
func (s *TestState) getCollections(a testUtils.GetCollections) error {
	collections, err := s.client.GetCollections(s.ctx, a.FilterOptions)
	if err != nil {
		if a.ExpectedError != "" && strings.Contains(err.Error(), a.ExpectedError) {
			return nil
		}
		return fmt.Errorf("GetCollections failed: %w", err)
	}
	if a.ExpectedError != "" {
		return fmt.Errorf("expected error containing %q but got none", a.ExpectedError)
	}

	// Verify expected results
	if len(a.ExpectedResults) > 0 {
		assert.Equal(s.t, len(a.ExpectedResults), len(collections), "collection count mismatch")
	}

	return nil
}

// setActiveCollectionVersion sets the active collection version.
func (s *TestState) setActiveCollectionVersion(a testUtils.SetActiveCollectionVersion) error {
	err := s.client.SetActiveCollectionVersion(s.ctx, a.VersionID)
	if err != nil {
		if a.ExpectedError != "" && strings.Contains(err.Error(), a.ExpectedError) {
			return nil
		}
		return fmt.Errorf("SetActiveCollectionVersion failed: %w", err)
	}
	if a.ExpectedError != "" {
		return fmt.Errorf("expected error containing %q but got none", a.ExpectedError)
	}
	return nil
}

// createView creates a view.
func (s *TestState) createView(a testUtils.CreateView) error {
	versions, err := s.client.AddView(s.ctx, a.Query, a.SDL, a.Transform)
	if err != nil {
		if a.ExpectedError != "" && strings.Contains(err.Error(), a.ExpectedError) {
			return nil
		}
		return fmt.Errorf("CreateView failed: %w", err)
	}
	if a.ExpectedError != "" {
		return fmt.Errorf("expected error containing %q but got none", a.ExpectedError)
	}

	// Track new view collections
	for _, v := range versions {
		s.collectionNames = append(s.collectionNames, v.Name)
		s.docIDs = append(s.docIDs, []string{})
	}

	return nil
}

// resolveDocIndexes substitutes DocIndex references with actual docIDs.
func (s *TestState) resolveDocIndexes(docMap map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range docMap {
		switch val := v.(type) {
		case testUtils.DocIndex:
			if val.CollectionIndex < len(s.docIDs) && val.Index < len(s.docIDs[val.CollectionIndex]) {
				result[k] = s.docIDs[val.CollectionIndex][val.Index]
			} else {
				result[k] = v
			}
		case map[string]any:
			result[k] = s.resolveDocIndexes(val)
		case []map[string]any:
			resolved := make([]map[string]any, len(val))
			for i, item := range val {
				resolved[i] = s.resolveDocIndexes(item)
			}
			result[k] = resolved
		default:
			result[k] = v
		}
	}
	return result
}

// resolveDocIndexesInResults resolves DocIndex references in expected results.
func (s *TestState) resolveDocIndexesInResults(results map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range results {
		result[k] = s.resolveValue(v)
	}
	return result
}

func (s *TestState) resolveValue(v any) any {
	switch val := v.(type) {
	case testUtils.DocIndex:
		if val.CollectionIndex < len(s.docIDs) && val.Index < len(s.docIDs[val.CollectionIndex]) {
			return s.docIDs[val.CollectionIndex][val.Index]
		}
		return v
	case map[string]any:
		return s.resolveDocIndexesInResults(val)
	case []map[string]any:
		resolved := make([]any, len(val))
		for i, item := range val {
			resolved[i] = s.resolveDocIndexesInResults(item)
		}
		return resolved
	case []any:
		resolved := make([]any, len(val))
		for i, item := range val {
			resolved[i] = s.resolveValue(item)
		}
		return resolved
	default:
		return v
	}
}

// replaceDocIndexPlaceholders replaces %docID% style placeholders in requests.
func (s *TestState) replaceDocIndexPlaceholders(request string) string {
	// For now, just return the request as-is
	// Placeholder substitution can be added if needed
	return request
}

// assertResults compares actual results with expected results.
func (s *TestState) assertResults(actual, expected map[string]any, nonOrdered bool) error {
	for key, expectedVal := range expected {
		actualVal, ok := actual[key]
		if !ok {
			return fmt.Errorf("missing key %q in results", key)
		}

		if err := s.compareValues(actualVal, expectedVal, nonOrdered); err != nil {
			return fmt.Errorf("key %q: %w", key, err)
		}
	}
	return nil
}

// compareValues compares two values, handling arrays specially.
func (s *TestState) compareValues(actual, expected any, nonOrdered bool) error {
	switch exp := expected.(type) {
	case []map[string]any:
		actSlice, ok := actual.([]any)
		if !ok {
			return fmt.Errorf("expected array, got %T", actual)
		}
		return s.compareArrays(actSlice, exp, nonOrdered)
	case []any:
		actSlice, ok := actual.([]any)
		if !ok {
			return fmt.Errorf("expected array, got %T", actual)
		}
		return s.compareAnyArrays(actSlice, exp, nonOrdered)
	case map[string]any:
		actMap, ok := actual.(map[string]any)
		if !ok {
			return fmt.Errorf("expected map, got %T", actual)
		}
		return s.assertResults(actMap, exp, nonOrdered)
	case int64:
		return s.compareNumeric(actual, exp)
	case int:
		return s.compareNumeric(actual, int64(exp))
	case float64:
		return s.compareNumeric(actual, exp)
	default:
		if !valuesEqual(actual, expected) {
			return fmt.Errorf("value mismatch: got %v (%T), expected %v (%T)", actual, actual, expected, expected)
		}
		return nil
	}
}

// compareNumeric handles numeric comparison with type flexibility.
func (s *TestState) compareNumeric(actual any, expected any) error {
	actualNum := toFloat64(actual)
	expectedNum := toFloat64(expected)
	if actualNum != expectedNum {
		return fmt.Errorf("numeric mismatch: got %v, expected %v", actual, expected)
	}
	return nil
}

// toFloat64 converts various numeric types to float64.
func toFloat64(v any) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	case json.Number:
		f, _ := val.Float64()
		return f
	default:
		return 0
	}
}

// compareArrays compares two arrays of maps.
func (s *TestState) compareArrays(actual []any, expected []map[string]any, nonOrdered bool) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("array length mismatch: got %d, expected %d", len(actual), len(expected))
	}

	if nonOrdered {
		// For non-ordered comparison, try to find matching items
		used := make([]bool, len(expected))
		for _, actItem := range actual {
			actMap, ok := actItem.(map[string]any)
			if !ok {
				return fmt.Errorf("expected map in array, got %T", actItem)
			}
			found := false
			for i, expItem := range expected {
				if !used[i] {
					if s.assertResults(actMap, expItem, nonOrdered) == nil {
						used[i] = true
						found = true
						break
					}
				}
			}
			if !found {
				return fmt.Errorf("no match found for item: %v", actItem)
			}
		}
	} else {
		// Ordered comparison
		for i, expItem := range expected {
			actMap, ok := actual[i].(map[string]any)
			if !ok {
				return fmt.Errorf("expected map at index %d, got %T", i, actual[i])
			}
			if err := s.assertResults(actMap, expItem, nonOrdered); err != nil {
				return fmt.Errorf("index %d: %w", i, err)
			}
		}
	}
	return nil
}

// compareAnyArrays compares two arrays of any type.
func (s *TestState) compareAnyArrays(actual, expected []any, nonOrdered bool) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("array length mismatch: got %d, expected %d", len(actual), len(expected))
	}

	if nonOrdered {
		// Non-ordered comparison
		used := make([]bool, len(expected))
		for _, actItem := range actual {
			found := false
			for i, expItem := range expected {
				if !used[i] {
					if s.compareValues(actItem, expItem, nonOrdered) == nil {
						used[i] = true
						found = true
						break
					}
				}
			}
			if !found {
				return fmt.Errorf("no match found for item: %v", actItem)
			}
		}
	} else {
		for i := range expected {
			if err := s.compareValues(actual[i], expected[i], nonOrdered); err != nil {
				return fmt.Errorf("index %d: %w", i, err)
			}
		}
	}
	return nil
}

// valuesEqual compares two values for equality.
func valuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Handle json.Number
	switch va := a.(type) {
	case json.Number:
		switch vb := b.(type) {
		case int64:
			ai, _ := va.Int64()
			return ai == vb
		case float64:
			af, _ := va.Float64()
			return af == vb
		case json.Number:
			return va == vb
		}
	case float64:
		if vb, ok := b.(float64); ok {
			return va == vb
		}
		if vb, ok := b.(int64); ok {
			return va == float64(vb)
		}
	case int64:
		if vb, ok := b.(int64); ok {
			return va == vb
		}
		if vb, ok := b.(float64); ok {
			return float64(va) == vb
		}
	}

	return a == b
}

// jsonToGraphQLInput converts a JSON string to GraphQL input format.
// GraphQL input has unquoted field names: {Name: "John"} not {"Name": "John"}
func jsonToGraphQLInput(jsonStr string) string {
	var data map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		// If it's not valid JSON, return as-is
		return jsonStr
	}
	return mapToGraphQLInput(data)
}

// mapToGraphQLInput converts a map to GraphQL input format.
func mapToGraphQLInput(data map[string]any) string {
	if len(data) == 0 {
		return "{}"
	}

	var parts []string
	for k, v := range data {
		parts = append(parts, fmt.Sprintf("%s: %s", k, valueToGraphQL(v)))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// valueToGraphQL converts a Go value to its GraphQL representation.
func valueToGraphQL(v any) string {
	if v == nil {
		return "null"
	}

	switch val := v.(type) {
	case string:
		// Escape quotes in string
		escaped := strings.ReplaceAll(val, `"`, `\"`)
		return fmt.Sprintf(`"%s"`, escaped)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case int, int64, int32:
		return fmt.Sprintf("%d", val)
	case float64:
		// Check if it's actually an integer value (from JSON parsing)
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%v", val)
	case float32:
		// Check if it's actually an integer value (from JSON parsing)
		if val == float32(int32(val)) {
			return fmt.Sprintf("%d", int32(val))
		}
		return fmt.Sprintf("%v", val)
	case map[string]any:
		return mapToGraphQLInput(val)
	case []any:
		var items []string
		for _, item := range val {
			items = append(items, valueToGraphQL(item))
		}
		return "[" + strings.Join(items, ", ") + "]"
	case []map[string]any:
		var items []string
		for _, item := range val {
			items = append(items, mapToGraphQLInput(item))
		}
		return "[" + strings.Join(items, ", ") + "]"
	default:
		// Fall back to JSON marshaling for unknown types
		data, err := json.Marshal(val)
		if err != nil {
			return fmt.Sprintf("%v", val)
		}
		return string(data)
	}
}

// extractDocID extracts the document ID from a mutation result.
func extractDocID(data any, key string) (string, error) {
	dataMap, ok := data.(map[string]any)
	if !ok {
		return "", fmt.Errorf("expected map data, got %T", data)
	}

	results, ok := dataMap[key].([]any)
	if !ok {
		return "", fmt.Errorf("expected array for key %q, got %T", key, dataMap[key])
	}

	if len(results) == 0 {
		return "", fmt.Errorf("empty results array")
	}

	resultMap, ok := results[0].(map[string]any)
	if !ok {
		return "", fmt.Errorf("expected map in results, got %T", results[0])
	}

	docID, ok := resultMap["_docID"].(string)
	if !ok {
		return "", fmt.Errorf("expected string _docID, got %T", resultMap["_docID"])
	}

	return docID, nil
}
