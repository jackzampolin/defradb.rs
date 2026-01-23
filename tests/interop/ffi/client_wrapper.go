// Package ffi provides Go bindings for the DefraDB Rust FFI.
//
// This file implements the DefraDB client.TxnStore interface for integration testing.
package ffi

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/defradb/tests/clients"
	"github.com/sourcenetwork/immutable"
	lensmodel "github.com/sourcenetwork/lens/host-go/config/model"
)

// Verify interface compliance at compile time
var _ clients.Client = (*ClientWrapper)(nil)

// ClientWrapper wraps an FFI Node to implement the DefraDB client.TxnStore interface.
type ClientWrapper struct {
	node     *Node
	events   *eventBus
	txnIDGen uint64
}

// NewClientWrapper creates a new client wrapper around an FFI node.
func NewClientWrapper(node *Node) *ClientWrapper {
	return &ClientWrapper{
		node:   node,
		events: newEventBus(),
	}
}

// ============================================================================
// clients.Client interface
// ============================================================================

func (c *ClientWrapper) Close() {
	if c.node != nil {
		c.node.Close()
	}
	if c.events != nil {
		c.events.Close()
	}
}

func (c *ClientWrapper) MaxTxnRetries() int {
	return 5 // Default value, matches Go DefraDB
}

func (c *ClientWrapper) Events() event.Bus {
	return c.events
}

// ============================================================================
// client.TxnStore interface
// ============================================================================

func (c *ClientWrapper) NewTxn(readOnly bool) (client.Txn, error) {
	txn, err := c.node.BeginTxn(readOnly)
	if err != nil {
		return nil, err
	}
	id := atomic.AddUint64(&c.txnIDGen, 1)
	return &TxnWrapper{
		client:   c,
		txn:      txn,
		id:       id,
		readOnly: readOnly,
	}, nil
}

func (c *ClientWrapper) NewConcurrentTxn(readOnly bool) (client.Txn, error) {
	// Our FFI transactions are already thread-safe
	return c.NewTxn(readOnly)
}

// ============================================================================
// client.Store interface - Core methods
// ============================================================================

func (c *ClientWrapper) ExecRequest(
	ctx context.Context,
	request string,
	opts ...client.RequestOption,
) *client.RequestResult {
	gqlOpts := &client.GQLOptions{}
	for _, opt := range opts {
		opt(gqlOpts)
	}

	varsJSON := ""
	if gqlOpts.Variables != nil {
		varsBytes, err := json.Marshal(gqlOpts.Variables)
		if err != nil {
			return &client.RequestResult{
				GQL: client.GQLResult{
					Errors: []error{fmt.Errorf("failed to marshal variables: %w", err)},
				},
			}
		}
		varsJSON = string(varsBytes)
	}

	responseJSON, err := c.node.ExecRequest(request, gqlOpts.OperationName, varsJSON)
	if err != nil {
		return &client.RequestResult{
			GQL: client.GQLResult{
				Errors: []error{err},
			},
		}
	}

	var gqlResult client.GQLResult
	if err := json.Unmarshal([]byte(responseJSON), &gqlResult); err != nil {
		return &client.RequestResult{
			GQL: client.GQLResult{
				Errors: []error{fmt.Errorf("failed to parse response: %w", err)},
			},
		}
	}

	return &client.RequestResult{GQL: gqlResult}
}

func (c *ClientWrapper) AddSchema(ctx context.Context, sdl string) ([]client.CollectionVersion, error) {
	responseJSON, err := c.node.AddSchema(sdl)
	if err != nil {
		return nil, err
	}

	var versions []client.CollectionVersion
	if err := json.Unmarshal([]byte(responseJSON), &versions); err != nil {
		return nil, fmt.Errorf("failed to parse schema response: %w", err)
	}

	return versions, nil
}

func (c *ClientWrapper) GetCollectionByName(ctx context.Context, name client.CollectionName) (client.Collection, error) {
	responseJSON, err := c.node.GetCollectionByName(name)
	if err != nil {
		return nil, err
	}

	var version client.CollectionVersion
	if err := json.Unmarshal([]byte(responseJSON), &version); err != nil {
		return nil, fmt.Errorf("failed to parse collection: %w", err)
	}

	return &CollectionWrapper{
		client:  c,
		version: version,
	}, nil
}

func (c *ClientWrapper) GetCollections(
	ctx context.Context,
	options client.CollectionFetchOptions,
) ([]client.Collection, error) {
	responseJSON, err := c.node.GetCollections()
	if err != nil {
		return nil, err
	}

	var versions []client.CollectionVersion
	if err := json.Unmarshal([]byte(responseJSON), &versions); err != nil {
		return nil, fmt.Errorf("failed to parse collections: %w", err)
	}

	// Apply filters
	var filtered []client.CollectionVersion
	for _, v := range versions {
		if options.Name.HasValue() && v.Name != options.Name.Value() {
			continue
		}
		if options.VersionID.HasValue() && v.VersionID != options.VersionID.Value() {
			continue
		}
		if options.CollectionID.HasValue() && v.CollectionID != options.CollectionID.Value() {
			continue
		}
		filtered = append(filtered, v)
	}

	collections := make([]client.Collection, len(filtered))
	for i, v := range filtered {
		collections[i] = &CollectionWrapper{
			client:  c,
			version: v,
		}
	}

	return collections, nil
}

func (c *ClientWrapper) SetActiveCollectionVersion(ctx context.Context, versionID string) error {
	return c.node.SetActiveCollectionVersion(versionID)
}

func (c *ClientWrapper) PatchCollection(
	ctx context.Context,
	patch string,
	migration immutable.Option[lensmodel.Lens],
) error {
	// For now, ignore migration - our FFI PatchCollection takes a collection name
	// This needs to be updated when we properly support migrations
	_, err := c.node.PatchCollection("", patch)
	return err
}

func (c *ClientWrapper) GetAllIndexes(ctx context.Context) (map[client.CollectionName][]client.IndexDescription, error) {
	result, err := c.node.GetAllIndexes()
	if err != nil {
		return nil, err
	}

	// Convert from our IndexDescription to client.IndexDescription
	indexes := make(map[client.CollectionName][]client.IndexDescription)
	for name, descs := range result {
		converted := make([]client.IndexDescription, len(descs))
		for i, d := range descs {
			fields := make([]client.IndexedFieldDescription, len(d.Fields))
			for j, f := range d.Fields {
				fields[j] = client.IndexedFieldDescription{
					Name:       f.Name,
					Descending: f.Descending,
				}
			}
			converted[i] = client.IndexDescription{
				Name:   d.Name,
				ID:     d.ID,
				Fields: fields,
				Unique: d.Unique,
			}
		}
		indexes[name] = converted
	}

	return indexes, nil
}

// ============================================================================
// client.Store interface - ACP methods
// ============================================================================

func (c *ClientWrapper) AddDACPolicy(ctx context.Context, policy string) (client.AddPolicyResult, error) {
	return client.AddPolicyResult{}, fmt.Errorf("AddDACPolicy not yet implemented in FFI")
}

func (c *ClientWrapper) AddDACActorRelationship(
	ctx context.Context,
	collectionName string,
	docID string,
	relation string,
	targetActor string,
) (client.AddActorRelationshipResult, error) {
	added, err := c.node.AddDACActorRelationship("", targetActor, collectionName, docID, relation)
	if err != nil {
		return client.AddActorRelationshipResult{}, err
	}
	return client.AddActorRelationshipResult{ExistedAlready: !added}, nil
}

func (c *ClientWrapper) DeleteDACActorRelationship(
	ctx context.Context,
	collectionName string,
	docID string,
	relation string,
	targetActor string,
) (client.DeleteActorRelationshipResult, error) {
	deleted, err := c.node.DeleteDACActorRelationship("", targetActor, collectionName, docID, relation)
	if err != nil {
		return client.DeleteActorRelationshipResult{}, err
	}
	return client.DeleteActorRelationshipResult{RecordFound: deleted}, nil
}

func (c *ClientWrapper) AddNACActorRelationship(
	ctx context.Context,
	relation string,
	targetActor string,
) (client.AddActorRelationshipResult, error) {
	added, err := c.node.AddNACActorRelationship("", targetActor)
	if err != nil {
		return client.AddActorRelationshipResult{}, err
	}
	return client.AddActorRelationshipResult{ExistedAlready: !added}, nil
}

func (c *ClientWrapper) DeleteNACActorRelationship(
	ctx context.Context,
	relation string,
	targetActor string,
) (client.DeleteActorRelationshipResult, error) {
	deleted, err := c.node.DeleteNACActorRelationship("", targetActor)
	if err != nil {
		return client.DeleteActorRelationshipResult{}, err
	}
	return client.DeleteActorRelationshipResult{RecordFound: deleted}, nil
}

func (c *ClientWrapper) ReEnableNAC(ctx context.Context) error {
	return c.node.ReEnableNAC("")
}

func (c *ClientWrapper) DisableNAC(ctx context.Context) error {
	return c.node.DisableNAC("")
}

func (c *ClientWrapper) GetNACStatus(ctx context.Context) (client.NACStatusResult, error) {
	status, err := c.node.GetNACStatus()
	if err != nil {
		return client.NACStatusResult{}, err
	}
	return client.NACStatusResult{
		Status: status.Status,
	}, nil
}

func (c *ClientWrapper) GetNodeIdentity(ctx context.Context) (immutable.Option[identity.PublicRawIdentity], error) {
	did, err := c.node.GetNodeIdentity()
	if err != nil {
		return immutable.None[identity.PublicRawIdentity](), err
	}
	if did == "" {
		return immutable.None[identity.PublicRawIdentity](), nil
	}
	return immutable.Some(identity.PublicRawIdentity{DID: did}), nil
}

func (c *ClientWrapper) VerifySignature(ctx context.Context, blockCid string, pubKey crypto.PublicKey) error {
	return fmt.Errorf("VerifySignature not yet implemented in FFI")
}

// ============================================================================
// client.Store interface - View/Migration methods
// ============================================================================

func (c *ClientWrapper) AddView(
	ctx context.Context,
	gqlQuery string,
	sdl string,
	transform immutable.Option[lensmodel.Lens],
) ([]client.CollectionVersion, error) {
	transformJSON := ""
	if transform.HasValue() {
		data, err := json.Marshal(transform.Value())
		if err != nil {
			return nil, fmt.Errorf("failed to marshal transform: %w", err)
		}
		transformJSON = string(data)
	}

	responseJSON, err := c.node.AddView(gqlQuery, sdl, transformJSON)
	if err != nil {
		return nil, err
	}

	var versions []client.CollectionVersion
	if err := json.Unmarshal([]byte(responseJSON), &versions); err != nil {
		return nil, fmt.Errorf("failed to parse view response: %w", err)
	}

	return versions, nil
}

func (c *ClientWrapper) RefreshViews(ctx context.Context, opts client.CollectionFetchOptions) error {
	optsJSON := ""
	if opts.Name.HasValue() || opts.VersionID.HasValue() {
		data, err := json.Marshal(opts)
		if err != nil {
			return fmt.Errorf("failed to marshal options: %w", err)
		}
		optsJSON = string(data)
	}
	return c.node.RefreshViews(optsJSON)
}

func (c *ClientWrapper) SetMigration(ctx context.Context, config client.LensConfig) (string, error) {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("failed to marshal config: %w", err)
	}
	return c.node.SetMigration(string(configJSON))
}

// ============================================================================
// client.Store interface - Backup methods
// ============================================================================

func (c *ClientWrapper) BasicImport(ctx context.Context, filepath string) error {
	return fmt.Errorf("BasicImport not yet implemented in FFI")
}

func (c *ClientWrapper) BasicExport(ctx context.Context, config *client.BackupConfig) error {
	return fmt.Errorf("BasicExport not yet implemented in FFI")
}

// ============================================================================
// client.Store interface - Utility methods
// ============================================================================

func (c *ClientWrapper) PrintDump(ctx context.Context) error {
	return fmt.Errorf("PrintDump not yet implemented in FFI")
}

func (c *ClientWrapper) ListAllEncryptedIndexes(ctx context.Context) (map[client.CollectionName][]client.EncryptedIndexDescription, error) {
	return nil, fmt.Errorf("ListAllEncryptedIndexes not yet implemented in FFI")
}

// ============================================================================
// client.P2P interface
// ============================================================================

func (c *ClientWrapper) PeerInfo() ([]string, error) {
	return nil, fmt.Errorf("P2P not available in FFI client")
}

func (c *ClientWrapper) Connect(ctx context.Context, addresses []string) error {
	return fmt.Errorf("P2P not available in FFI client")
}

func (c *ClientWrapper) SetReplicator(ctx context.Context, addresses []string, collections ...string) error {
	return fmt.Errorf("P2P not available in FFI client")
}

func (c *ClientWrapper) DeleteReplicator(ctx context.Context, id string, collections ...string) error {
	return fmt.Errorf("P2P not available in FFI client")
}

func (c *ClientWrapper) GetAllReplicators(ctx context.Context) ([]client.Replicator, error) {
	return nil, fmt.Errorf("P2P not available in FFI client")
}

func (c *ClientWrapper) AddP2PCollections(ctx context.Context, collectionNames ...string) error {
	return fmt.Errorf("P2P not available in FFI client")
}

func (c *ClientWrapper) RemoveP2PCollections(ctx context.Context, collectionNames ...string) error {
	return fmt.Errorf("P2P not available in FFI client")
}

func (c *ClientWrapper) GetAllP2PCollections(ctx context.Context) ([]string, error) {
	return nil, fmt.Errorf("P2P not available in FFI client")
}

func (c *ClientWrapper) AddP2PDocuments(ctx context.Context, docIDs ...string) error {
	return fmt.Errorf("P2P not available in FFI client")
}

func (c *ClientWrapper) RemoveP2PDocuments(ctx context.Context, docIDs ...string) error {
	return fmt.Errorf("P2P not available in FFI client")
}

func (c *ClientWrapper) GetAllP2PDocuments(ctx context.Context) ([]string, error) {
	return nil, fmt.Errorf("P2P not available in FFI client")
}

func (c *ClientWrapper) SyncDocuments(ctx context.Context, collectionName string, docIDs []string) error {
	return fmt.Errorf("P2P not available in FFI client")
}

func (c *ClientWrapper) SyncCollections(ctx context.Context, versionIDs ...string) error {
	return fmt.Errorf("P2P not available in FFI client")
}

// ============================================================================
// TxnWrapper implements client.Txn
// ============================================================================

type TxnWrapper struct {
	client   *ClientWrapper
	txn      *Transaction
	id       uint64
	readOnly bool
}

var _ client.Txn = (*TxnWrapper)(nil)

func (t *TxnWrapper) ID() uint64 {
	return t.id
}

func (t *TxnWrapper) Commit() error {
	return t.txn.Commit()
}

func (t *TxnWrapper) Discard() {
	_ = t.txn.Rollback()
}

// Delegate Store methods to the underlying transaction
func (t *TxnWrapper) ExecRequest(ctx context.Context, request string, opts ...client.RequestOption) *client.RequestResult {
	gqlOpts := &client.GQLOptions{}
	for _, opt := range opts {
		opt(gqlOpts)
	}

	varsJSON := ""
	if gqlOpts.Variables != nil {
		varsBytes, err := json.Marshal(gqlOpts.Variables)
		if err != nil {
			return &client.RequestResult{
				GQL: client.GQLResult{
					Errors: []error{fmt.Errorf("failed to marshal variables: %w", err)},
				},
			}
		}
		varsJSON = string(varsBytes)
	}

	responseJSON, err := t.txn.ExecRequest(request, gqlOpts.OperationName, varsJSON)
	if err != nil {
		return &client.RequestResult{
			GQL: client.GQLResult{
				Errors: []error{err},
			},
		}
	}

	var gqlResult client.GQLResult
	if err := json.Unmarshal([]byte(responseJSON), &gqlResult); err != nil {
		return &client.RequestResult{
			GQL: client.GQLResult{
				Errors: []error{fmt.Errorf("failed to parse response: %w", err)},
			},
		}
	}

	return &client.RequestResult{GQL: gqlResult}
}

// Stub out other Store methods on transaction - most delegate to client
func (t *TxnWrapper) AddSchema(ctx context.Context, sdl string) ([]client.CollectionVersion, error) {
	return t.client.AddSchema(ctx, sdl)
}

func (t *TxnWrapper) GetCollectionByName(ctx context.Context, name client.CollectionName) (client.Collection, error) {
	return t.client.GetCollectionByName(ctx, name)
}

func (t *TxnWrapper) GetCollections(ctx context.Context, options client.CollectionFetchOptions) ([]client.Collection, error) {
	return t.client.GetCollections(ctx, options)
}

func (t *TxnWrapper) SetActiveCollectionVersion(ctx context.Context, versionID string) error {
	return t.client.SetActiveCollectionVersion(ctx, versionID)
}

func (t *TxnWrapper) PatchCollection(ctx context.Context, patch string, migration immutable.Option[lensmodel.Lens]) error {
	return t.client.PatchCollection(ctx, patch, migration)
}

func (t *TxnWrapper) GetAllIndexes(ctx context.Context) (map[client.CollectionName][]client.IndexDescription, error) {
	return t.client.GetAllIndexes(ctx)
}

func (t *TxnWrapper) AddDACPolicy(ctx context.Context, policy string) (client.AddPolicyResult, error) {
	return t.client.AddDACPolicy(ctx, policy)
}

func (t *TxnWrapper) AddDACActorRelationship(ctx context.Context, collectionName, docID, relation, targetActor string) (client.AddActorRelationshipResult, error) {
	return t.client.AddDACActorRelationship(ctx, collectionName, docID, relation, targetActor)
}

func (t *TxnWrapper) DeleteDACActorRelationship(ctx context.Context, collectionName, docID, relation, targetActor string) (client.DeleteActorRelationshipResult, error) {
	return t.client.DeleteDACActorRelationship(ctx, collectionName, docID, relation, targetActor)
}

func (t *TxnWrapper) AddNACActorRelationship(ctx context.Context, relation, targetActor string) (client.AddActorRelationshipResult, error) {
	return t.client.AddNACActorRelationship(ctx, relation, targetActor)
}

func (t *TxnWrapper) DeleteNACActorRelationship(ctx context.Context, relation, targetActor string) (client.DeleteActorRelationshipResult, error) {
	return t.client.DeleteNACActorRelationship(ctx, relation, targetActor)
}

func (t *TxnWrapper) ReEnableNAC(ctx context.Context) error {
	return t.client.ReEnableNAC(ctx)
}

func (t *TxnWrapper) DisableNAC(ctx context.Context) error {
	return t.client.DisableNAC(ctx)
}

func (t *TxnWrapper) GetNACStatus(ctx context.Context) (client.NACStatusResult, error) {
	return t.client.GetNACStatus(ctx)
}

func (t *TxnWrapper) GetNodeIdentity(ctx context.Context) (immutable.Option[identity.PublicRawIdentity], error) {
	return t.client.GetNodeIdentity(ctx)
}

func (t *TxnWrapper) VerifySignature(ctx context.Context, blockCid string, pubKey crypto.PublicKey) error {
	return t.client.VerifySignature(ctx, blockCid, pubKey)
}

func (t *TxnWrapper) AddView(ctx context.Context, gqlQuery, sdl string, transform immutable.Option[lensmodel.Lens]) ([]client.CollectionVersion, error) {
	return t.client.AddView(ctx, gqlQuery, sdl, transform)
}

func (t *TxnWrapper) RefreshViews(ctx context.Context, opts client.CollectionFetchOptions) error {
	return t.client.RefreshViews(ctx, opts)
}

func (t *TxnWrapper) SetMigration(ctx context.Context, config client.LensConfig) (string, error) {
	return t.client.SetMigration(ctx, config)
}

func (t *TxnWrapper) BasicImport(ctx context.Context, filepath string) error {
	return t.client.BasicImport(ctx, filepath)
}

func (t *TxnWrapper) BasicExport(ctx context.Context, config *client.BackupConfig) error {
	return t.client.BasicExport(ctx, config)
}

func (t *TxnWrapper) PrintDump(ctx context.Context) error {
	return t.client.PrintDump(ctx)
}

func (t *TxnWrapper) ListAllEncryptedIndexes(ctx context.Context) (map[client.CollectionName][]client.EncryptedIndexDescription, error) {
	return t.client.ListAllEncryptedIndexes(ctx)
}

// P2P methods - not available in transactions
func (t *TxnWrapper) PeerInfo() ([]string, error) { return nil, fmt.Errorf("P2P not available") }
func (t *TxnWrapper) Connect(ctx context.Context, addresses []string) error {
	return fmt.Errorf("P2P not available")
}
func (t *TxnWrapper) SetReplicator(ctx context.Context, addresses []string, collections ...string) error {
	return fmt.Errorf("P2P not available")
}
func (t *TxnWrapper) DeleteReplicator(ctx context.Context, id string, collections ...string) error {
	return fmt.Errorf("P2P not available")
}
func (t *TxnWrapper) GetAllReplicators(ctx context.Context) ([]client.Replicator, error) {
	return nil, fmt.Errorf("P2P not available")
}
func (t *TxnWrapper) AddP2PCollections(ctx context.Context, collectionNames ...string) error {
	return fmt.Errorf("P2P not available")
}
func (t *TxnWrapper) RemoveP2PCollections(ctx context.Context, collectionNames ...string) error {
	return fmt.Errorf("P2P not available")
}
func (t *TxnWrapper) GetAllP2PCollections(ctx context.Context) ([]string, error) {
	return nil, fmt.Errorf("P2P not available")
}
func (t *TxnWrapper) AddP2PDocuments(ctx context.Context, docIDs ...string) error {
	return fmt.Errorf("P2P not available")
}
func (t *TxnWrapper) RemoveP2PDocuments(ctx context.Context, docIDs ...string) error {
	return fmt.Errorf("P2P not available")
}
func (t *TxnWrapper) GetAllP2PDocuments(ctx context.Context) ([]string, error) {
	return nil, fmt.Errorf("P2P not available")
}
func (t *TxnWrapper) SyncDocuments(ctx context.Context, collectionName string, docIDs []string) error {
	return fmt.Errorf("P2P not available")
}
func (t *TxnWrapper) SyncCollections(ctx context.Context, versionIDs ...string) error {
	return fmt.Errorf("P2P not available")
}

// ============================================================================
// eventBus implements event.Bus for testing
// ============================================================================

type eventBus struct {
	closed bool
	subs   []event.Subscription
}

func newEventBus() *eventBus {
	return &eventBus{}
}

func (e *eventBus) Publish(msg event.Message) {
	// For now, we don't forward events - the FFI subscription system handles this
}

func (e *eventBus) Subscribe(events ...event.Name) (event.Subscription, error) {
	sub := &eventSubscription{
		ch: make(chan event.Message, 100),
	}
	e.subs = append(e.subs, sub)
	return sub, nil
}

func (e *eventBus) Unsubscribe(sub event.Subscription) {
	for i, s := range e.subs {
		if s == sub {
			e.subs = append(e.subs[:i], e.subs[i+1:]...)
			break
		}
	}
}

func (e *eventBus) Close() {
	e.closed = true
	for _, sub := range e.subs {
		if es, ok := sub.(*eventSubscription); ok {
			close(es.ch)
		}
	}
}

type eventSubscription struct {
	ch chan event.Message
}

func (s *eventSubscription) Message() <-chan event.Message {
	return s.ch
}

// ============================================================================
// CollectionWrapper implements client.Collection
// ============================================================================

type CollectionWrapper struct {
	client  *ClientWrapper
	version client.CollectionVersion
}

var _ client.Collection = (*CollectionWrapper)(nil)

func (c *CollectionWrapper) Name() string {
	return c.version.Name
}

func (c *CollectionWrapper) VersionID() string {
	return c.version.VersionID
}

func (c *CollectionWrapper) CollectionID() string {
	return c.version.CollectionID
}

func (c *CollectionWrapper) Version() client.CollectionVersion {
	return c.version
}

func (c *CollectionWrapper) Create(ctx context.Context, doc *client.Document, opts ...client.DocCreateOption) error {
	// Use GraphQL mutation to create document
	docJSON, err := doc.ToJSONPatch()
	if err != nil {
		return fmt.Errorf("failed to convert document to JSON: %w", err)
	}

	mutation := fmt.Sprintf(`mutation { create_%s(input: %s) { _docID } }`, c.version.Name, string(docJSON))
	result := c.client.ExecRequest(ctx, mutation)
	if len(result.GQL.Errors) > 0 {
		return result.GQL.Errors[0]
	}

	// The document ID is generated by the server and returned in the response.
	// The test framework tracks doc IDs separately through the state.
	return nil
}

func (c *CollectionWrapper) CreateMany(ctx context.Context, docs []*client.Document, opts ...client.DocCreateOption) error {
	for _, doc := range docs {
		if err := c.Create(ctx, doc, opts...); err != nil {
			return err
		}
	}
	return nil
}

func (c *CollectionWrapper) Update(ctx context.Context, doc *client.Document) error {
	docJSON, err := doc.ToJSONPatch()
	if err != nil {
		return fmt.Errorf("failed to convert document to JSON: %w", err)
	}

	mutation := fmt.Sprintf(`mutation { update_%s(docID: "%s", input: %s) { _docID } }`,
		c.version.Name, doc.ID().String(), string(docJSON))
	result := c.client.ExecRequest(ctx, mutation)
	if len(result.GQL.Errors) > 0 {
		return result.GQL.Errors[0]
	}
	return nil
}

func (c *CollectionWrapper) Save(ctx context.Context, doc *client.Document, opts ...client.DocCreateOption) error {
	if doc.ID().String() == "" {
		return c.Create(ctx, doc, opts...)
	}
	return c.Update(ctx, doc)
}

func (c *CollectionWrapper) Delete(ctx context.Context, docID client.DocID) (bool, error) {
	mutation := fmt.Sprintf(`mutation { delete_%s(docID: "%s") { _docID } }`, c.version.Name, docID.String())
	result := c.client.ExecRequest(ctx, mutation)
	if len(result.GQL.Errors) > 0 {
		return false, result.GQL.Errors[0]
	}
	return true, nil
}

func (c *CollectionWrapper) Exists(ctx context.Context, docID client.DocID) (bool, error) {
	query := fmt.Sprintf(`{ %s(docID: "%s") { _docID } }`, c.version.Name, docID.String())
	result := c.client.ExecRequest(ctx, query)
	if len(result.GQL.Errors) > 0 {
		return false, nil
	}
	if data, ok := result.GQL.Data.(map[string]any); ok {
		if docs, ok := data[c.version.Name].([]any); ok {
			return len(docs) > 0, nil
		}
	}
	return false, nil
}

func (c *CollectionWrapper) UpdateWithFilter(ctx context.Context, filter any, updater string) (*client.UpdateResult, error) {
	filterJSON, err := json.Marshal(filter)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal filter: %w", err)
	}

	mutation := fmt.Sprintf(`mutation { update_%s(filter: %s, input: %s) { _docID } }`,
		c.version.Name, string(filterJSON), updater)
	result := c.client.ExecRequest(ctx, mutation)
	if len(result.GQL.Errors) > 0 {
		return nil, result.GQL.Errors[0]
	}

	// Extract results
	updateResult := &client.UpdateResult{}
	if data, ok := result.GQL.Data.(map[string]any); ok {
		if docs, ok := data["update_"+c.version.Name].([]any); ok {
			updateResult.Count = int64(len(docs))
			for _, d := range docs {
				if doc, ok := d.(map[string]any); ok {
					if id, ok := doc["_docID"].(string); ok {
						updateResult.DocIDs = append(updateResult.DocIDs, id)
					}
				}
			}
		}
	}
	return updateResult, nil
}

func (c *CollectionWrapper) DeleteWithFilter(ctx context.Context, filter any) (*client.DeleteResult, error) {
	filterJSON, err := json.Marshal(filter)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal filter: %w", err)
	}

	mutation := fmt.Sprintf(`mutation { delete_%s(filter: %s) { _docID } }`, c.version.Name, string(filterJSON))
	result := c.client.ExecRequest(ctx, mutation)
	if len(result.GQL.Errors) > 0 {
		return nil, result.GQL.Errors[0]
	}

	deleteResult := &client.DeleteResult{}
	if data, ok := result.GQL.Data.(map[string]any); ok {
		if docs, ok := data["delete_"+c.version.Name].([]any); ok {
			deleteResult.Count = int64(len(docs))
			for _, d := range docs {
				if doc, ok := d.(map[string]any); ok {
					if id, ok := doc["_docID"].(string); ok {
						deleteResult.DocIDs = append(deleteResult.DocIDs, id)
					}
				}
			}
		}
	}
	return deleteResult, nil
}

func (c *CollectionWrapper) Get(ctx context.Context, docID client.DocID, showDeleted bool) (*client.Document, error) {
	// Query the document by ID
	query := fmt.Sprintf(`{ %s(docID: "%s") { _docID } }`, c.version.Name, docID.String())
	result := c.client.ExecRequest(ctx, query)
	if len(result.GQL.Errors) > 0 {
		return nil, result.GQL.Errors[0]
	}

	// Parse the result to check if document exists
	data, ok := result.GQL.Data.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("document not found: %s", docID.String())
	}

	docs, ok := data[c.version.Name].([]any)
	if !ok || len(docs) == 0 {
		return nil, fmt.Errorf("document not found: %s", docID.String())
	}

	docData, ok := docs[0].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("document not found: %s", docID.String())
	}

	// Create a new document from the retrieved data
	doc, err := client.NewDocFromMap(docData, c.version)
	if err != nil {
		return nil, fmt.Errorf("failed to create document: %w", err)
	}

	return doc, nil
}

func (c *CollectionWrapper) GetAllDocIDs(ctx context.Context) (<-chan client.DocIDResult, error) {
	ch := make(chan client.DocIDResult)
	go func() {
		defer close(ch)
		query := fmt.Sprintf(`{ %s { _docID } }`, c.version.Name)
		result := c.client.ExecRequest(ctx, query)
		if len(result.GQL.Errors) > 0 {
			ch <- client.DocIDResult{Err: result.GQL.Errors[0]}
			return
		}
		if data, ok := result.GQL.Data.(map[string]any); ok {
			if docs, ok := data[c.version.Name].([]any); ok {
				for _, d := range docs {
					if doc, ok := d.(map[string]any); ok {
						if id, ok := doc["_docID"].(string); ok {
							docID, err := client.NewDocIDFromString(id)
							if err != nil {
								ch <- client.DocIDResult{Err: err}
								continue
							}
							ch <- client.DocIDResult{ID: docID}
						}
					}
				}
			}
		}
	}()
	return ch, nil
}

func (c *CollectionWrapper) CreateIndex(ctx context.Context, req client.IndexCreateRequest) (client.IndexDescription, error) {
	fields := make([]IndexField, len(req.Fields))
	for i, f := range req.Fields {
		fields[i] = IndexField{
			Name:       f.Name,
			Descending: f.Descending,
		}
	}

	index, err := c.client.node.CreateIndex(c.version.Name, req.Name, fields, req.Unique)
	if err != nil {
		return client.IndexDescription{}, err
	}

	resultFields := make([]client.IndexedFieldDescription, len(index.Fields))
	for i, f := range index.Fields {
		resultFields[i] = client.IndexedFieldDescription{
			Name:       f.Name,
			Descending: f.Descending,
		}
	}

	return client.IndexDescription{
		Name:   index.Name,
		ID:     index.ID,
		Fields: resultFields,
		Unique: index.Unique,
	}, nil
}

func (c *CollectionWrapper) DropIndex(ctx context.Context, indexName string) error {
	return c.client.node.DropIndex(c.version.Name, indexName)
}

func (c *CollectionWrapper) GetIndexes(ctx context.Context) ([]client.IndexDescription, error) {
	indexes, err := c.client.node.GetIndexes(c.version.Name)
	if err != nil {
		return nil, err
	}

	result := make([]client.IndexDescription, len(indexes))
	for i, idx := range indexes {
		fields := make([]client.IndexedFieldDescription, len(idx.Fields))
		for j, f := range idx.Fields {
			fields[j] = client.IndexedFieldDescription{
				Name:       f.Name,
				Descending: f.Descending,
			}
		}
		result[i] = client.IndexDescription{
			Name:   idx.Name,
			ID:     idx.ID,
			Fields: fields,
			Unique: idx.Unique,
		}
	}
	return result, nil
}

func (c *CollectionWrapper) CreateEncryptedIndex(ctx context.Context, desc client.EncryptedIndexDescription) (client.EncryptedIndexDescription, error) {
	return client.EncryptedIndexDescription{}, fmt.Errorf("encrypted indexes not yet implemented in FFI")
}

func (c *CollectionWrapper) DeleteEncryptedIndex(ctx context.Context, fieldName string) error {
	return fmt.Errorf("encrypted indexes not yet implemented in FFI")
}

func (c *CollectionWrapper) ListEncryptedIndexes(ctx context.Context) ([]client.EncryptedIndexDescription, error) {
	return nil, fmt.Errorf("encrypted indexes not yet implemented in FFI")
}
