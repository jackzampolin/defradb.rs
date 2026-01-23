package ffi

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Note: Currently execute_in_txn only supports queries, not mutations.
// Mutation support in transactions requires query runner changes.
// These tests focus on what's currently working.

func TestTransactionQuery(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddSchema("type TxnQueryItem { name: String, value: Int }")
	require.NoError(t, err)

	// Create some data first (outside transaction)
	_, err = node.Mutate(`mutation { create_TxnQueryItem(input: {name: "test", value: 42}) { _docID } }`)
	require.NoError(t, err)

	// Begin transaction
	txn, err := node.BeginTxn(false)
	require.NoError(t, err)
	require.NotEmpty(t, txn.ID())

	// Query within transaction
	result, err := txn.Query("{ TxnQueryItem { name value } }")
	require.NoError(t, err)
	require.Empty(t, result.Errors)

	var data map[string]interface{}
	err = json.Unmarshal(result.Data, &data)
	require.NoError(t, err)

	items := data["TxnQueryItem"].([]interface{})
	assert.Len(t, items, 1)
	item := items[0].(map[string]interface{})
	assert.Equal(t, "test", item["name"])
	assert.Equal(t, float64(42), item["value"])

	// Commit transaction
	err = txn.Commit()
	require.NoError(t, err)
}

func TestTransactionRollback(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddSchema("type RollbackItem { name: String }")
	require.NoError(t, err)

	// Begin transaction
	txn, err := node.BeginTxn(false)
	require.NoError(t, err)

	// Query within transaction (empty result expected)
	result, err := txn.Query("{ RollbackItem { name } }")
	require.NoError(t, err)
	require.Empty(t, result.Errors)

	// Rollback transaction
	err = txn.Rollback()
	require.NoError(t, err)
}

func TestTransactionReadOnly(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddSchema("type ReadOnlyItem { name: String }")
	require.NoError(t, err)

	// Create initial data
	_, err = node.Mutate(`mutation { create_ReadOnlyItem(input: {name: "existing"}) { _docID } }`)
	require.NoError(t, err)

	// Begin readonly transaction
	txn, err := node.BeginTxn(true)
	require.NoError(t, err)

	// Query should work in readonly transaction
	result, err := txn.Query("{ ReadOnlyItem { name } }")
	require.NoError(t, err)
	require.Empty(t, result.Errors)

	var data map[string]interface{}
	err = json.Unmarshal(result.Data, &data)
	require.NoError(t, err)

	items := data["ReadOnlyItem"].([]interface{})
	assert.Len(t, items, 1)

	// Commit readonly transaction
	err = txn.Commit()
	require.NoError(t, err)
}

func TestTransactionInvalidID(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	// Create a fake transaction with invalid ID
	fakeTxn := &Transaction{node: node, id: "invalid-txn-id-12345"}

	// Commit should fail
	err = fakeTxn.Commit()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed")
}

func TestTransactionDoubleCommit(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddSchema("type DoubleCommitItem { name: String }")
	require.NoError(t, err)

	txn, err := node.BeginTxn(false)
	require.NoError(t, err)

	// First commit should succeed
	err = txn.Commit()
	require.NoError(t, err)

	// Second commit should fail (transaction no longer valid)
	err = txn.Commit()
	assert.Error(t, err)
}

func TestTransactionMultipleQueries(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddSchema("type MultiQueryItem { seq: Int }")
	require.NoError(t, err)

	// Create some data
	for i := 0; i < 3; i++ {
		_, err = node.Mutate(`mutation { create_MultiQueryItem(input: {seq: ` + string(rune('0'+i)) + `}) { _docID } }`)
		require.NoError(t, err)
	}

	// Begin transaction
	txn, err := node.BeginTxn(false)
	require.NoError(t, err)

	// Execute multiple queries in same transaction
	for i := 0; i < 5; i++ {
		result, err := txn.Query("{ MultiQueryItem { seq } }")
		require.NoError(t, err)
		require.Empty(t, result.Errors, "query %d should succeed", i)
	}

	// Commit
	err = txn.Commit()
	require.NoError(t, err)
}

func TestConcurrentTransactions(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddSchema("type ConcurrentItem { txn_id: Int, value: Int }")
	require.NoError(t, err)

	// Create some data
	for i := 0; i < 5; i++ {
		_, err = node.Mutate(`mutation { create_ConcurrentItem(input: {txn_id: ` + string(rune('0'+i)) + `, value: ` + string(rune('0'+i)) + `}) { _docID } }`)
		require.NoError(t, err)
	}

	// Create multiple concurrent transactions (readonly queries)
	const numTxns = 5
	txns := make([]*Transaction, numTxns)
	for i := 0; i < numTxns; i++ {
		txn, err := node.BeginTxn(true) // readonly
		require.NoError(t, err)
		txns[i] = txn
	}

	// Each transaction queries
	for _, txn := range txns {
		result, err := txn.Query("{ ConcurrentItem { txn_id value } }")
		require.NoError(t, err)
		require.Empty(t, result.Errors)
	}

	// Commit all transactions
	for _, txn := range txns {
		err := txn.Commit()
		require.NoError(t, err)
	}
}

func TestTransactionQueryWithExistingData(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddSchema("type ExistingDataItem { name: String, count: Int }")
	require.NoError(t, err)

	// Create multiple documents
	names := []string{"Alice", "Bob", "Charlie"}
	for i, name := range names {
		_, err = node.Mutate(`mutation { create_ExistingDataItem(input: {name: "` + name + `", count: ` + string(rune('0'+i)) + `}) { _docID } }`)
		require.NoError(t, err)
	}

	// Query in transaction
	txn, err := node.BeginTxn(true)
	require.NoError(t, err)

	result, err := txn.Query("{ ExistingDataItem { name count } }")
	require.NoError(t, err)
	require.Empty(t, result.Errors)

	var data map[string]interface{}
	err = json.Unmarshal(result.Data, &data)
	require.NoError(t, err)

	items := data["ExistingDataItem"].([]interface{})
	assert.Len(t, items, 3)

	err = txn.Commit()
	require.NoError(t, err)
}
