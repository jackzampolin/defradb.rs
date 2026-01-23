// Package ffi provides Go bindings for the DefraDB Rust FFI.
//
// This package wraps the C FFI exposed by the Rust `ffi` crate,
// providing a Go-native interface for integration testing.
//
// Build requirements:
//   - Rust library must be built first: cargo build --release -p ffi
//   - CGO must be enabled: CGO_ENABLED=1
package ffi

/*
#cgo CFLAGS: -I../../../crates/ffi
#cgo LDFLAGS: -L../../../target/release -lffi -ldl -lpthread -lm

#include "defra.h"
#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

var (
	// initOnce ensures Init is only called once
	initOnce sync.Once

	// ErrNotInitialized is returned when FFI functions are called before Init
	ErrNotInitialized = errors.New("ffi: not initialized - call Init() first")
)

// Init initializes the FFI library.
// Must be called before any other FFI functions.
// Safe to call multiple times.
func Init() {
	initOnce.Do(func() {
		C.defra_init()
	})
}

// Version returns the library version string.
func Version() string {
	cstr := C.defra_version()
	if cstr == nil {
		return ""
	}
	defer C.defra_free_string(cstr)
	return C.GoString(cstr)
}

// Node represents a DefraDB node handle.
type Node struct {
	ptr C.uintptr_t
}

// NodeOptions configures node creation.
type NodeOptions struct {
	// DBPath is the path to the database directory.
	// If empty, an in-memory database is used.
	DBPath string

	// InMemory forces in-memory storage even if DBPath is set.
	InMemory bool
}

// NewNode creates a new DefraDB node.
// The node must be closed with Close() when done.
func NewNode(opts NodeOptions) (*Node, error) {
	var cOpts C.struct_NodeInitOptions

	if opts.DBPath != "" {
		cDBPath := C.CString(opts.DBPath)
		defer C.free(unsafe.Pointer(cDBPath))
		cOpts.db_path = cDBPath
	}

	if opts.InMemory || opts.DBPath == "" {
		cOpts.in_memory = 1
	} else {
		cOpts.in_memory = 0
	}

	result := C.new_node(cOpts)

	if result.status != 0 {
		err := C.GoString(result.error)
		C.defra_free_string(result.error)
		return nil, fmt.Errorf("ffi: new_node failed: %s", err)
	}

	return &Node{ptr: result.node_ptr}, nil
}

// Close closes the node and releases resources.
// After calling Close, the node handle is no longer valid.
func (n *Node) Close() error {
	result := C.node_close(n.ptr)

	if result.status != 0 {
		err := C.GoString(result.error)
		C.defra_free_string(result.error)
		return fmt.Errorf("ffi: node_close failed: %s", err)
	}

	return nil
}

// AddSchema adds a GraphQL SDL schema to the database.
// Returns the JSON response containing created collection versions.
func (n *Node) AddSchema(sdl string) (string, error) {
	cSDL := C.CString(sdl)
	defer C.free(unsafe.Pointer(cSDL))

	result := C.add_schema(n.ptr, cSDL)

	if result.status != 0 {
		err := C.GoString(result.error)
		C.defra_free_string(result.error)
		return "", fmt.Errorf("ffi: add_schema failed: %s", err)
	}

	value := C.GoString(result.value)
	C.defra_free_string(result.value)
	return value, nil
}

// GetCollections returns all collections in the database as JSON.
func (n *Node) GetCollections() (string, error) {
	result := C.get_collections(n.ptr)

	if result.status != 0 {
		err := C.GoString(result.error)
		C.defra_free_string(result.error)
		return "", fmt.Errorf("ffi: get_collections failed: %s", err)
	}

	value := C.GoString(result.value)
	C.defra_free_string(result.value)
	return value, nil
}

// QueryResult represents a GraphQL query response.
type QueryResult struct {
	Data   json.RawMessage `json:"data,omitempty"`
	Errors []QueryError    `json:"errors,omitempty"`
}

// QueryError represents a GraphQL error.
type QueryError struct {
	Message   string          `json:"message"`
	Locations []ErrorLocation `json:"locations,omitempty"`
	Path      []interface{}   `json:"path,omitempty"`
}

// ErrorLocation indicates where an error occurred in the query.
type ErrorLocation struct {
	Line   int `json:"line"`
	Column int `json:"column"`
}

// ExecRequest executes a GraphQL query or mutation.
// Returns the raw JSON response string.
func (n *Node) ExecRequest(query string, operationName string, variables string) (string, error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	var cOpName *C.char
	if operationName != "" {
		cOpName = C.CString(operationName)
		defer C.free(unsafe.Pointer(cOpName))
	}

	var cVars *C.char
	if variables != "" {
		cVars = C.CString(variables)
		defer C.free(unsafe.Pointer(cVars))
	}

	result := C.exec_request(n.ptr, cQuery, cOpName, cVars)

	if result.status != 0 {
		err := C.GoString(result.error)
		C.defra_free_string(result.error)
		return "", fmt.Errorf("ffi: exec_request failed: %s", err)
	}

	value := C.GoString(result.value)
	C.defra_free_string(result.value)
	return value, nil
}

// Query executes a GraphQL query and returns a parsed result.
func (n *Node) Query(query string) (*QueryResult, error) {
	return n.QueryWithVars(query, "", nil)
}

// QueryWithVars executes a GraphQL query with variables.
func (n *Node) QueryWithVars(query string, operationName string, variables map[string]interface{}) (*QueryResult, error) {
	var varsJSON string
	if variables != nil {
		varsBytes, err := json.Marshal(variables)
		if err != nil {
			return nil, fmt.Errorf("ffi: failed to marshal variables: %w", err)
		}
		varsJSON = string(varsBytes)
	}

	responseJSON, err := n.ExecRequest(query, operationName, varsJSON)
	if err != nil {
		return nil, err
	}

	var result QueryResult
	if err := json.Unmarshal([]byte(responseJSON), &result); err != nil {
		return nil, fmt.Errorf("ffi: failed to parse response: %w", err)
	}

	return &result, nil
}

// Mutate executes a GraphQL mutation and returns a parsed result.
func (n *Node) Mutate(mutation string) (*QueryResult, error) {
	return n.Query(mutation)
}

// Transaction represents an active database transaction.
type Transaction struct {
	node *Node
	id   string
}

// BeginTxn starts a new transaction.
// If readonly is true, the transaction cannot perform write operations.
// The transaction must be committed or rolled back when done.
func (n *Node) BeginTxn(readonly bool) (*Transaction, error) {
	var readonlyInt C.int32_t
	if readonly {
		readonlyInt = 1
	}

	result := C.begin_txn(n.ptr, readonlyInt)

	if result.status != 0 {
		err := C.GoString(result.error)
		C.defra_free_string(result.error)
		return nil, fmt.Errorf("ffi: begin_txn failed: %s", err)
	}

	txnID := C.GoString(result.txn_id)
	C.defra_free_string(result.txn_id)

	return &Transaction{node: n, id: txnID}, nil
}

// ID returns the transaction ID.
func (t *Transaction) ID() string {
	return t.id
}

// Commit commits the transaction, making all changes permanent.
// After commit, the transaction is no longer valid.
func (t *Transaction) Commit() error {
	cTxnID := C.CString(t.id)
	defer C.free(unsafe.Pointer(cTxnID))

	result := C.commit_txn(t.node.ptr, cTxnID)

	if result.status != 0 {
		err := C.GoString(result.error)
		C.defra_free_string(result.error)
		return fmt.Errorf("ffi: commit_txn failed: %s", err)
	}

	return nil
}

// Rollback discards all changes made in the transaction.
// After rollback, the transaction is no longer valid.
func (t *Transaction) Rollback() error {
	cTxnID := C.CString(t.id)
	defer C.free(unsafe.Pointer(cTxnID))

	result := C.rollback_txn(t.node.ptr, cTxnID)

	if result.status != 0 {
		err := C.GoString(result.error)
		C.defra_free_string(result.error)
		return fmt.Errorf("ffi: rollback_txn failed: %s", err)
	}

	return nil
}

// ExecRequest executes a GraphQL query or mutation within the transaction.
func (t *Transaction) ExecRequest(query string, operationName string, variables string) (string, error) {
	cTxnID := C.CString(t.id)
	defer C.free(unsafe.Pointer(cTxnID))

	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	var cOpName *C.char
	if operationName != "" {
		cOpName = C.CString(operationName)
		defer C.free(unsafe.Pointer(cOpName))
	}

	var cVars *C.char
	if variables != "" {
		cVars = C.CString(variables)
		defer C.free(unsafe.Pointer(cVars))
	}

	result := C.exec_request_in_txn(t.node.ptr, cTxnID, cQuery, cOpName, cVars)

	if result.status != 0 {
		err := C.GoString(result.error)
		C.defra_free_string(result.error)
		return "", fmt.Errorf("ffi: exec_request_in_txn failed: %s", err)
	}

	value := C.GoString(result.value)
	C.defra_free_string(result.value)
	return value, nil
}

// Query executes a GraphQL query within the transaction.
func (t *Transaction) Query(query string) (*QueryResult, error) {
	responseJSON, err := t.ExecRequest(query, "", "")
	if err != nil {
		return nil, err
	}

	var result QueryResult
	if err := json.Unmarshal([]byte(responseJSON), &result); err != nil {
		return nil, fmt.Errorf("ffi: failed to parse response: %w", err)
	}

	return &result, nil
}

// Mutate executes a GraphQL mutation within the transaction.
func (t *Transaction) Mutate(mutation string) (*QueryResult, error) {
	return t.Query(mutation)
}
