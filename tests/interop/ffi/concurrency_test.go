package ffi

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentNodeCreation tests creating multiple nodes concurrently.
func TestConcurrentNodeCreation(t *testing.T) {
	Init()

	const numNodes = 10
	var wg sync.WaitGroup
	nodes := make([]*Node, numNodes)
	errs := make([]error, numNodes)

	// Create nodes concurrently
	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			node, err := NewNode(NodeOptions{InMemory: true})
			nodes[idx] = node
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	// Verify all succeeded
	for i := 0; i < numNodes; i++ {
		require.NoError(t, errs[i], "node %d creation failed", i)
		require.NotNil(t, nodes[i], "node %d is nil", i)
	}

	// Close all nodes concurrently
	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = nodes[idx].Close()
		}(i)
	}
	wg.Wait()

	// Verify all closed successfully
	for i := 0; i < numNodes; i++ {
		assert.NoError(t, errs[i], "node %d close failed", i)
	}
}

// TestConcurrentQueriesSingleNode tests multiple goroutines querying the same node.
func TestConcurrentQueriesSingleNode(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type Counter { name: String, value: Int }")
	require.NoError(t, err)

	// Create some initial data
	for i := 0; i < 5; i++ {
		_, err := node.Mutate(fmt.Sprintf(
			`mutation { create_Counter(input: {name: "counter_%d", value: %d}) { _docID } }`,
			i, i*10,
		))
		require.NoError(t, err)
	}

	const numGoroutines = 20
	const queriesPerGoroutine = 10
	var wg sync.WaitGroup
	var successCount atomic.Int64
	var errorCount atomic.Int64

	// Run concurrent queries
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for q := 0; q < queriesPerGoroutine; q++ {
				result, err := node.Query("{ Counter { name value } }")
				if err != nil {
					errorCount.Add(1)
					continue
				}
				if len(result.Errors) > 0 {
					errorCount.Add(1)
					continue
				}
				successCount.Add(1)
			}
		}()
	}
	wg.Wait()

	totalQueries := int64(numGoroutines * queriesPerGoroutine)
	t.Logf("Concurrent queries: %d successful, %d errors out of %d total",
		successCount.Load(), errorCount.Load(), totalQueries)

	// All queries should succeed
	assert.Equal(t, totalQueries, successCount.Load(), "all queries should succeed")
	assert.Equal(t, int64(0), errorCount.Load(), "no queries should fail")
}

// TestConcurrentMutationsSingleNode tests multiple goroutines mutating the same node.
func TestConcurrentMutationsSingleNode(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type Item { name: String, seq: Int }")
	require.NoError(t, err)

	const numGoroutines = 10
	const mutationsPerGoroutine = 5
	var wg sync.WaitGroup
	var successCount atomic.Int64
	var errorCount atomic.Int64

	// Run concurrent mutations
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for m := 0; m < mutationsPerGoroutine; m++ {
				seq := goroutineID*mutationsPerGoroutine + m
				mutation := fmt.Sprintf(
					`mutation { create_Item(input: {name: "item_g%d_m%d", seq: %d}) { _docID } }`,
					goroutineID, m, seq,
				)
				result, err := node.Mutate(mutation)
				if err != nil {
					errorCount.Add(1)
					continue
				}
				if len(result.Errors) > 0 {
					errorCount.Add(1)
					continue
				}
				successCount.Add(1)
			}
		}(g)
	}
	wg.Wait()

	totalMutations := int64(numGoroutines * mutationsPerGoroutine)
	t.Logf("Concurrent mutations: %d successful, %d errors out of %d total",
		successCount.Load(), errorCount.Load(), totalMutations)

	// All mutations should succeed
	assert.Equal(t, totalMutations, successCount.Load(), "all mutations should succeed")
	assert.Equal(t, int64(0), errorCount.Load(), "no mutations should fail")

	// Verify all items were created
	result, err := node.Query("{ Item { name seq } }")
	require.NoError(t, err)
	require.Empty(t, result.Errors)
}

// TestConcurrentMixedOperations tests concurrent reads and writes.
func TestConcurrentMixedOperations(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type Event { name: String, timestamp: Int }")
	require.NoError(t, err)

	const numWriters = 5
	const numReaders = 10
	const opsPerGoroutine = 10
	var wg sync.WaitGroup
	var writeSuccess atomic.Int64
	var readSuccess atomic.Int64
	var errors atomic.Int64

	// Start writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for op := 0; op < opsPerGoroutine; op++ {
				ts := writerID*1000 + op
				mutation := fmt.Sprintf(
					`mutation { create_Event(input: {name: "event_w%d", timestamp: %d}) { _docID } }`,
					writerID, ts,
				)
				result, err := node.Mutate(mutation)
				if err != nil || len(result.Errors) > 0 {
					errors.Add(1)
				} else {
					writeSuccess.Add(1)
				}
			}
		}(w)
	}

	// Start readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for op := 0; op < opsPerGoroutine; op++ {
				result, err := node.Query("{ Event { name timestamp } }")
				if err != nil || len(result.Errors) > 0 {
					errors.Add(1)
				} else {
					readSuccess.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	totalWrites := int64(numWriters * opsPerGoroutine)
	totalReads := int64(numReaders * opsPerGoroutine)
	t.Logf("Mixed ops: %d/%d writes, %d/%d reads, %d errors",
		writeSuccess.Load(), totalWrites,
		readSuccess.Load(), totalReads,
		errors.Load())

	assert.Equal(t, totalWrites, writeSuccess.Load(), "all writes should succeed")
	assert.Equal(t, totalReads, readSuccess.Load(), "all reads should succeed")
	assert.Equal(t, int64(0), errors.Load(), "no operations should fail")
}

// TestConcurrentSchemaAndQueries tests adding schemas while querying.
func TestConcurrentSchemaAndQueries(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Add initial schema
	_, err = node.AddSchema("type Base { name: String }")
	require.NoError(t, err)

	var wg sync.WaitGroup
	var schemaErrors atomic.Int64
	var queryErrors atomic.Int64

	// Query the base type while adding new schemas
	const numQueries = 20
	for i := 0; i < numQueries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := node.Query("{ Base { name } }")
			if err != nil {
				queryErrors.Add(1)
			}
		}()
	}

	// Add additional schemas concurrently
	const numSchemas = 3
	for i := 0; i < numSchemas; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sdl := fmt.Sprintf("type Extra%d { value: Int }", idx)
			_, err := node.AddSchema(sdl)
			if err != nil {
				schemaErrors.Add(1)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Schema+Query: %d schema errors, %d query errors",
		schemaErrors.Load(), queryErrors.Load())

	// Queries should succeed even during schema changes
	assert.Equal(t, int64(0), queryErrors.Load(), "queries should not fail during schema adds")
}

// TestConcurrentMultipleNodes tests operations across multiple nodes.
func TestConcurrentMultipleNodes(t *testing.T) {
	Init()

	const numNodes = 5
	nodes := make([]*Node, numNodes)

	// Create nodes
	for i := 0; i < numNodes; i++ {
		node, err := NewNode(NodeOptions{InMemory: true})
		require.NoError(t, err)
		nodes[i] = node

		// Each node gets its own schema
		sdl := fmt.Sprintf("type Node%dData { value: Int }", i)
		_, err = node.AddSchema(sdl)
		require.NoError(t, err)
	}
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()

	var wg sync.WaitGroup
	var errors atomic.Int64

	// Run operations on all nodes concurrently
	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go func(nodeIdx int) {
			defer wg.Done()
			node := nodes[nodeIdx]
			typeName := fmt.Sprintf("Node%dData", nodeIdx)

			// Create documents
			for j := 0; j < 10; j++ {
				mutation := fmt.Sprintf(
					`mutation { create_%s(input: {value: %d}) { _docID } }`,
					typeName, j,
				)
				result, err := node.Mutate(mutation)
				if err != nil || len(result.Errors) > 0 {
					errors.Add(1)
				}
			}

			// Query documents
			query := fmt.Sprintf("{ %s { value } }", typeName)
			result, err := node.Query(query)
			if err != nil || len(result.Errors) > 0 {
				errors.Add(1)
			}
		}(i)
	}

	wg.Wait()

	assert.Equal(t, int64(0), errors.Load(), "all multi-node operations should succeed")
}

// TestStressHighConcurrency runs a high-concurrency stress test.
func TestStressHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddSchema("type Stress { id: Int, data: String }")
	require.NoError(t, err)

	const numGoroutines = 50
	const opsPerGoroutine = 20
	var wg sync.WaitGroup
	var totalOps atomic.Int64
	var errors atomic.Int64

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for op := 0; op < opsPerGoroutine; op++ {
				// Alternate between mutations and queries
				if op%2 == 0 {
					mutation := fmt.Sprintf(
						`mutation { create_Stress(input: {id: %d, data: "g%d_op%d"}) { _docID } }`,
						gid*1000+op, gid, op,
					)
					result, err := node.Mutate(mutation)
					if err != nil || len(result.Errors) > 0 {
						errors.Add(1)
					}
				} else {
					result, err := node.Query("{ Stress { id data } }")
					if err != nil || len(result.Errors) > 0 {
						errors.Add(1)
					}
				}
				totalOps.Add(1)
			}
		}(g)
	}

	wg.Wait()

	expectedOps := int64(numGoroutines * opsPerGoroutine)
	t.Logf("Stress test: %d total ops, %d errors", totalOps.Load(), errors.Load())

	assert.Equal(t, expectedOps, totalOps.Load())
	assert.Equal(t, int64(0), errors.Load(), "stress test should have no errors")
}
