package ffi

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// ============================================================================
// Transaction Mutation Tests
// ============================================================================

func TestTransactionMutationCreate(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddSchema("type TxnCreateItem { name: String, value: Int }")
	require.NoError(t, err)

	// Begin transaction
	txn, err := node.BeginTxn(false)
	require.NoError(t, err)

	// Create document within transaction
	result, err := txn.Mutate(`mutation { create_TxnCreateItem(input: {name: "txn-item", value: 123}) { _docID name value } }`)
	require.NoError(t, err)
	require.Empty(t, result.Errors, "mutation should succeed")

	var data map[string]interface{}
	err = json.Unmarshal(result.Data, &data)
	require.NoError(t, err)

	items := data["create_TxnCreateItem"].([]interface{})
	assert.Len(t, items, 1)
	item := items[0].(map[string]interface{})
	assert.Equal(t, "txn-item", item["name"])
	assert.Equal(t, float64(123), item["value"])
	docID := item["_docID"].(string)
	assert.NotEmpty(t, docID)

	// Query within same transaction should see the new document
	queryResult, err := txn.Query("{ TxnCreateItem { name value } }")
	require.NoError(t, err)
	require.Empty(t, queryResult.Errors)

	var queryData map[string]interface{}
	err = json.Unmarshal(queryResult.Data, &queryData)
	require.NoError(t, err)

	queryItems := queryData["TxnCreateItem"].([]interface{})
	assert.Len(t, queryItems, 1)

	// Commit transaction
	err = txn.Commit()
	require.NoError(t, err)

	// Verify data persisted after commit
	queryResult, err = node.Query("{ TxnCreateItem { name value } }")
	require.NoError(t, err)
	require.Empty(t, queryResult.Errors)

	err = json.Unmarshal(queryResult.Data, &queryData)
	require.NoError(t, err)

	queryItems = queryData["TxnCreateItem"].([]interface{})
	assert.Len(t, queryItems, 1)
}

func TestTransactionMutationCreateRollback(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddSchema("type RollbackCreateItem { name: String }")
	require.NoError(t, err)

	// Begin transaction
	txn, err := node.BeginTxn(false)
	require.NoError(t, err)

	// Create document within transaction
	result, err := txn.Mutate(`mutation { create_RollbackCreateItem(input: {name: "will-be-rolled-back"}) { _docID } }`)
	require.NoError(t, err)
	require.Empty(t, result.Errors, "mutation should succeed")

	// Rollback transaction
	err = txn.Rollback()
	require.NoError(t, err)

	// Verify data was NOT persisted
	queryResult, err := node.Query("{ RollbackCreateItem { name } }")
	require.NoError(t, err)
	require.Empty(t, queryResult.Errors)

	var queryData map[string]interface{}
	err = json.Unmarshal(queryResult.Data, &queryData)
	require.NoError(t, err)

	items := queryData["RollbackCreateItem"].([]interface{})
	assert.Len(t, items, 0, "rolled back document should not be visible")
}

func TestTransactionMutationUpdate(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddSchema("type TxnUpdateItem { name: String, value: Int }")
	require.NoError(t, err)

	// Create initial document outside transaction
	createResult, err := node.Mutate(`mutation { create_TxnUpdateItem(input: {name: "original", value: 1}) { _docID } }`)
	require.NoError(t, err)
	require.Empty(t, createResult.Errors)

	var createData map[string]interface{}
	err = json.Unmarshal(createResult.Data, &createData)
	require.NoError(t, err)
	docID := createData["create_TxnUpdateItem"].([]interface{})[0].(map[string]interface{})["_docID"].(string)

	// Begin transaction and update
	txn, err := node.BeginTxn(false)
	require.NoError(t, err)

	// Update document within transaction
	result, err := txn.Mutate(`mutation { update_TxnUpdateItem(docIDs: ["` + docID + `"], input: {value: 999}) { _docID name value } }`)
	require.NoError(t, err)
	require.Empty(t, result.Errors, "update mutation should succeed")

	var updateData map[string]interface{}
	err = json.Unmarshal(result.Data, &updateData)
	require.NoError(t, err)

	items := updateData["update_TxnUpdateItem"].([]interface{})
	assert.Len(t, items, 1)
	item := items[0].(map[string]interface{})
	assert.Equal(t, "original", item["name"]) // name unchanged
	assert.Equal(t, float64(999), item["value"])

	// Commit transaction
	err = txn.Commit()
	require.NoError(t, err)

	// Verify update persisted
	queryResult, err := node.Query("{ TxnUpdateItem { name value } }")
	require.NoError(t, err)

	var queryData map[string]interface{}
	err = json.Unmarshal(queryResult.Data, &queryData)
	require.NoError(t, err)

	queryItems := queryData["TxnUpdateItem"].([]interface{})
	assert.Len(t, queryItems, 1)
	assert.Equal(t, float64(999), queryItems[0].(map[string]interface{})["value"])
}

func TestTransactionMutationDelete(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddSchema("type TxnDeleteItem { name: String }")
	require.NoError(t, err)

	// Create document outside transaction
	createResult, err := node.Mutate(`mutation { create_TxnDeleteItem(input: {name: "to-delete"}) { _docID } }`)
	require.NoError(t, err)
	require.Empty(t, createResult.Errors)

	var createData map[string]interface{}
	err = json.Unmarshal(createResult.Data, &createData)
	require.NoError(t, err)
	docID := createData["create_TxnDeleteItem"].([]interface{})[0].(map[string]interface{})["_docID"].(string)

	// Begin transaction and delete
	txn, err := node.BeginTxn(false)
	require.NoError(t, err)

	// Delete document within transaction
	result, err := txn.Mutate(`mutation { delete_TxnDeleteItem(docIDs: ["` + docID + `"]) { _docID } }`)
	require.NoError(t, err)
	require.Empty(t, result.Errors, "delete mutation should succeed")

	// Commit transaction
	err = txn.Commit()
	require.NoError(t, err)

	// Verify document was deleted
	queryResult, err := node.Query("{ TxnDeleteItem { name } }")
	require.NoError(t, err)

	var queryData map[string]interface{}
	err = json.Unmarshal(queryResult.Data, &queryData)
	require.NoError(t, err)

	items := queryData["TxnDeleteItem"].([]interface{})
	assert.Len(t, items, 0, "deleted document should not be visible")
}

func TestTransactionMutationMultipleOperations(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddSchema("type TxnMultiItem { name: String, count: Int }")
	require.NoError(t, err)

	// Begin transaction
	txn, err := node.BeginTxn(false)
	require.NoError(t, err)

	// Create multiple documents in same transaction
	for i := 0; i < 5; i++ {
		result, err := txn.Mutate(`mutation { create_TxnMultiItem(input: {name: "item-` + string(rune('0'+i)) + `", count: ` + string(rune('0'+i)) + `}) { _docID } }`)
		require.NoError(t, err)
		require.Empty(t, result.Errors, "mutation %d should succeed", i)
	}

	// Query within transaction
	queryResult, err := txn.Query("{ TxnMultiItem { name count } }")
	require.NoError(t, err)
	require.Empty(t, queryResult.Errors)

	var queryData map[string]interface{}
	err = json.Unmarshal(queryResult.Data, &queryData)
	require.NoError(t, err)

	items := queryData["TxnMultiItem"].([]interface{})
	assert.Len(t, items, 5)

	// Commit transaction
	err = txn.Commit()
	require.NoError(t, err)

	// Verify all documents persisted
	queryResult, err = node.Query("{ TxnMultiItem { name } }")
	require.NoError(t, err)

	err = json.Unmarshal(queryResult.Data, &queryData)
	require.NoError(t, err)

	items = queryData["TxnMultiItem"].([]interface{})
	assert.Len(t, items, 5)
}

func TestTransactionMutationReadOnlyFails(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddSchema("type ReadOnlyMutateItem { name: String }")
	require.NoError(t, err)

	// Begin readonly transaction
	txn, err := node.BeginTxn(true)
	require.NoError(t, err)

	// Attempt mutation in readonly transaction - should fail
	result, err := txn.Mutate(`mutation { create_ReadOnlyMutateItem(input: {name: "should-fail"}) { _docID } }`)
	require.NoError(t, err) // FFI call succeeds
	assert.NotEmpty(t, result.Errors, "mutation in readonly txn should return errors")
	assert.Contains(t, result.Errors[0].Message, "read-only", "error should mention read-only")

	// Rollback since commit would fail anyway
	err = txn.Rollback()
	require.NoError(t, err)
}

func TestTransactionMutationIsolation(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err)
	defer node.Close()

	_, err = node.AddSchema("type IsolationItem { name: String }")
	require.NoError(t, err)

	// Begin transaction and create document
	txn, err := node.BeginTxn(false)
	require.NoError(t, err)

	result, err := txn.Mutate(`mutation { create_IsolationItem(input: {name: "isolated"}) { _docID } }`)
	require.NoError(t, err)
	require.Empty(t, result.Errors)

	// Query outside transaction - should NOT see uncommitted data
	queryResult, err := node.Query("{ IsolationItem { name } }")
	require.NoError(t, err)
	require.Empty(t, queryResult.Errors)

	var queryData map[string]interface{}
	err = json.Unmarshal(queryResult.Data, &queryData)
	require.NoError(t, err)

	items := queryData["IsolationItem"].([]interface{})
	assert.Len(t, items, 0, "uncommitted data should not be visible outside transaction")

	// Now commit
	err = txn.Commit()
	require.NoError(t, err)

	// Query again - should now see the data
	queryResult, err = node.Query("{ IsolationItem { name } }")
	require.NoError(t, err)

	err = json.Unmarshal(queryResult.Data, &queryData)
	require.NoError(t, err)

	items = queryData["IsolationItem"].([]interface{})
	assert.Len(t, items, 1, "committed data should be visible")
}
