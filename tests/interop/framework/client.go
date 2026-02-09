package framework

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Client wraps HTTP communication with a DefraDB node.
type Client struct {
	httpClient *http.Client
	baseURL    string
	authHeader string // optional Authorization header
}

// NewClient creates a new Client for the given base URL.
func NewClient(baseURL string) *Client {
	return &Client{
		httpClient: &http.Client{},
		baseURL:    baseURL,
	}
}

// WithIdentity returns a new Client that sends the identity's auth header with every request.
func (c *Client) WithIdentity(id *TestIdentity) *Client {
	return &Client{
		httpClient: c.httpClient,
		baseURL:    c.baseURL,
		authHeader: id.AuthHeader(),
	}
}

// setAuth sets the Authorization header on a request if configured.
func (c *Client) setAuth(req *http.Request) {
	if c.authHeader != "" {
		req.Header.Set("Authorization", c.authHeader)
	}
}

// P2PInfoResponse represents the response from GET /api/v0/p2p/info.
// Both Rust and Go now return array of full multiaddrs with peer ID embedded.
type P2PInfoResponse struct {
	ID        string
	Addresses []string
}

// extractPeerIDFromMultiaddr extracts the peer ID from a multiaddr string.
// Example: "/ip4/127.0.0.1/tcp/9182/p2p/12D3KooW..." -> "12D3KooW..."
func extractPeerIDFromMultiaddr(addr string) string {
	parts := strings.Split(addr, "/p2p/")
	if len(parts) == 2 {
		return parts[1]
	}
	return ""
}

// PeerInfo represents information about a connected peer.
type PeerInfo struct {
	ID      string  `json:"id"`
	Address *string `json:"address,omitempty"`
}

// ConnectPeerRequest represents a request to connect to a peer.
type ConnectPeerRequest struct {
	Address string `json:"address"`
}

// GraphQLRequest represents a GraphQL request body.
type GraphQLRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

// GraphQLResponse represents a GraphQL response.
type GraphQLResponse struct {
	Data   json.RawMessage `json:"data,omitempty"`
	Errors []GraphQLError  `json:"errors,omitempty"`
}

// GraphQLError represents a GraphQL error.
type GraphQLError struct {
	Message string `json:"message"`
}

// HealthCheck checks if the node is healthy.
func (c *Client) HealthCheck(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/health-check", nil)
	if err != nil {
		return fmt.Errorf("failed to create health check request: %w", err)
	}
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("health check request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("health check returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// P2PInfo retrieves P2P node information.
// Both Rust and Go now return array of multiaddrs with peer ID embedded.
func (c *Client) P2PInfo(ctx context.Context) (*P2PInfoResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v0/p2p/info", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create p2p info request: %w", err)
	}
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("p2p info request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("p2p info returned status %d: %s", resp.StatusCode, string(body))
	}

	// Response is array of full multiaddrs with peer ID embedded
	var addresses []string
	if err := json.NewDecoder(resp.Body).Decode(&addresses); err != nil {
		return nil, fmt.Errorf("failed to decode p2p info response: %w", err)
	}

	if len(addresses) == 0 {
		return nil, fmt.Errorf("no addresses returned from p2p/info")
	}

	// Extract peer ID from first address containing /p2p/
	var peerID string
	for _, addr := range addresses {
		if id := extractPeerIDFromMultiaddr(addr); id != "" {
			peerID = id
			break
		}
	}
	if peerID == "" {
		return nil, fmt.Errorf("no peer ID found in p2p/info response: %v", addresses)
	}

	return &P2PInfoResponse{
		ID:        peerID,
		Addresses: addresses,
	}, nil
}

// ConnectPeer connects to a peer via multiaddr.
// Both Rust and Go now support POST /api/v0/p2p/connect with ["..."]
func (c *Client) ConnectPeer(ctx context.Context, multiaddr string) error {
	body, _ := json.Marshal([]string{multiaddr})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v0/p2p/connect", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create connect peer request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("connect peer request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	respBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("connect peer returned status %d: %s", resp.StatusCode, string(respBody))
}

// ErrEndpointNotSupported indicates the endpoint doesn't exist on this node type.
var ErrEndpointNotSupported = fmt.Errorf("endpoint not supported")

// ListPeers returns a list of connected peers.
// Note: Go DefraDB doesn't have this endpoint (returns ErrEndpointNotSupported).
func (c *Client) ListPeers(ctx context.Context) ([]PeerInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v0/p2p/peers", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create list peers request: %w", err)
	}
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list peers request failed: %w", err)
	}
	defer resp.Body.Close()

	// Go DefraDB doesn't have this endpoint
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrEndpointNotSupported
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list peers returned status %d: %s", resp.StatusCode, string(body))
	}

	var peers []PeerInfo
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		return nil, fmt.Errorf("failed to decode list peers response: %w", err)
	}

	return peers, nil
}

// GraphQL executes a GraphQL query.
func (c *Client) GraphQL(ctx context.Context, query string, vars map[string]any) (*GraphQLResponse, error) {
	reqBody := GraphQLRequest{Query: query, Variables: vars}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal graphql request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v0/graphql", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create graphql request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("graphql request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("graphql returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var gqlResp GraphQLResponse
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return nil, fmt.Errorf("failed to decode graphql response: %w", err)
	}

	return &gqlResp, nil
}

// SetReplicatorRequest represents a request to set up replication.
type SetReplicatorRequest struct {
	Addresses   []string `json:"Addresses"`
	Collections []string `json:"Collections"`
}

// SetReplicator sets up replication for collections to peer addresses.
func (c *Client) SetReplicator(ctx context.Context, addresses []string, collections []string) error {
	reqBody := SetReplicatorRequest{Addresses: addresses, Collections: collections}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal set replicator request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v0/p2p/replicators", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create set replicator request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("set replicator request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	respBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("set replicator returned status %d: %s", resp.StatusCode, string(respBody))
}

// AddP2PCollections adds collections to P2P sync.
func (c *Client) AddP2PCollections(ctx context.Context, collectionIDs []string) error {
	body, err := json.Marshal(collectionIDs)
	if err != nil {
		return fmt.Errorf("failed to marshal add p2p collections request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v0/p2p/collections", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create add p2p collections request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("add p2p collections request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	respBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("add p2p collections returned status %d: %s", resp.StatusCode, string(respBody))
}

// SyncDocumentsRequest represents a request to sync documents.
type SyncDocumentsRequest struct {
	CollectionName string   `json:"collectionName"`
	DocIDs         []string `json:"docIDs"`
	Timeout        string   `json:"timeout,omitempty"`
}

// SyncDocuments synchronizes documents from the network.
func (c *Client) SyncDocuments(ctx context.Context, collectionName string, docIDs []string, timeout string) error {
	reqBody := SyncDocumentsRequest{
		CollectionName: collectionName,
		DocIDs:         docIDs,
		Timeout:        timeout,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal sync documents request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v0/p2p/documents/sync", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create sync documents request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sync documents request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	respBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("sync documents returned status %d: %s", resp.StatusCode, string(respBody))
}

// AddSchemaResponse represents a response from POST /api/v0/schema.
type AddSchemaResponse struct {
	Name         string `json:"Name"`
	VersionID    string `json:"VersionID"`
	CollectionID string `json:"CollectionID"`
}

// AddSchema adds a schema to the database via HTTP POST /api/v0/schema.
// The body should be GraphQL SDL defining the collection types.
func (c *Client) AddSchema(ctx context.Context, sdl string) ([]AddSchemaResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v0/schema", strings.NewReader(sdl))
	if err != nil {
		return nil, fmt.Errorf("failed to create add schema request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("add schema request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("add schema returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var schemas []AddSchemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&schemas); err != nil {
		return nil, fmt.Errorf("failed to decode add schema response: %w", err)
	}

	return schemas, nil
}

// PolicyInfo represents a policy returned from the API.
type PolicyInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

// AddPolicy adds an ACP policy to the node.
// POST /api/v0/acp/policy with text/plain body.
func (c *Client) AddPolicy(ctx context.Context, policyYAML string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v0/acp/policy", strings.NewReader(policyYAML))
	if err != nil {
		return "", fmt.Errorf("failed to create add policy request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("add policy request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("add policy returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		PolicyID string `json:"PolicyID"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode add policy response: %w", err)
	}

	return result.PolicyID, nil
}

// ListPolicies lists all ACP policies.
func (c *Client) ListPolicies(ctx context.Context) ([]PolicyInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v0/acp/policy", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create list policies request: %w", err)
	}
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list policies request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list policies returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var policies []PolicyInfo
	if err := json.NewDecoder(resp.Body).Decode(&policies); err != nil {
		return nil, fmt.Errorf("failed to decode list policies response: %w", err)
	}

	return policies, nil
}

// DocRelationshipRequest represents a request to add/delete a document relationship.
type DocRelationshipRequest struct {
	Collection string `json:"collection"`
	DocID      string `json:"docID"`
	Relation   string `json:"relation"`
	Actor      string `json:"actor"`
}

// AddDocRelationship adds a relationship to a document (e.g., granting a user read access).
func (c *Client) AddDocRelationship(ctx context.Context, collection, docID, relation, actor string) (bool, error) {
	reqBody := DocRelationshipRequest{
		Collection: collection,
		DocID:      docID,
		Relation:   relation,
		Actor:      actor,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return false, fmt.Errorf("failed to marshal doc relationship request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v0/acp/document/relationship", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("failed to create add doc relationship request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("add doc relationship request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("add doc relationship returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		ExistedAlready bool `json:"ExistedAlready"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode add doc relationship response: %w", err)
	}

	return !result.ExistedAlready, nil
}

// DeleteDocRelationship removes a relationship from a document.
func (c *Client) DeleteDocRelationship(ctx context.Context, collection, docID, relation, actor string) (bool, error) {
	reqBody := DocRelationshipRequest{
		Collection: collection,
		DocID:      docID,
		Relation:   relation,
		Actor:      actor,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return false, fmt.Errorf("failed to marshal doc relationship request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/api/v0/acp/document/relationship", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("failed to create delete doc relationship request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("delete doc relationship request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("delete doc relationship returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		RecordFound bool `json:"RecordFound"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode delete doc relationship response: %w", err)
	}

	return result.RecordFound, nil
}

// IndexField specifies a field and direction for an index.
type IndexField struct {
	Name      string `json:"Name"`
	Direction string `json:"Direction,omitempty"` // "ASC" or "DESC"
}

// CreateIndexRequest represents a request to create an index.
type CreateIndexRequest struct {
	Name   string       `json:"Name,omitempty"`
	Fields []IndexField `json:"Fields"`
	Unique bool         `json:"Unique,omitempty"`
}

// IndexInfo represents index information returned from the API.
type IndexInfo struct {
	Name   string       `json:"Name"`
	ID     int          `json:"ID"`
	Fields []IndexField `json:"Fields"`
	Unique bool         `json:"Unique"`
}

// CreateIndex creates an index on a collection.
func (c *Client) CreateIndex(ctx context.Context, collection string, fields []string, name string, unique bool) error {
	indexFields := make([]IndexField, len(fields))
	for i, f := range fields {
		indexFields[i] = IndexField{Name: f, Direction: "ASC"}
	}

	reqBody := CreateIndexRequest{
		Name:   name,
		Fields: indexFields,
		Unique: unique,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal create index request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v0/collections/%s/indexes", c.baseURL, collection)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create index request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("create index request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create index returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// ListIndexes lists all indexes on a collection.
func (c *Client) ListIndexes(ctx context.Context, collection string) ([]IndexInfo, error) {
	url := fmt.Sprintf("%s/api/v0/collections/%s/indexes", c.baseURL, collection)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create list indexes request: %w", err)
	}
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list indexes request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list indexes returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var indexes []IndexInfo
	if err := json.NewDecoder(resp.Body).Decode(&indexes); err != nil {
		return nil, fmt.Errorf("failed to decode list indexes response: %w", err)
	}

	return indexes, nil
}

// DropIndex drops an index from a collection.
func (c *Client) DropIndex(ctx context.Context, collection, indexName string) error {
	url := fmt.Sprintf("%s/api/v0/collections/%s/indexes/%s", c.baseURL, collection, indexName)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create drop index request: %w", err)
	}
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("drop index request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("drop index returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// Purge purges all data from the database.
func (c *Client) Purge(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v0/purge", nil)
	if err != nil {
		return fmt.Errorf("failed to create purge request: %w", err)
	}
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("purge request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("purge returned status %d: %s", resp.StatusCode, string(respBody))
	}

	// Go node restarts asynchronously after purge; wait for it to come back
	time.Sleep(500 * time.Millisecond)
	return WaitForReady(ctx, c, 30*time.Second)
}

// TruncateCollection truncates (removes all documents from) a collection.
func (c *Client) TruncateCollection(ctx context.Context, name string) error {
	url := fmt.Sprintf("%s/api/v0/collections/%s/truncate", c.baseURL, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create truncate request: %w", err)
	}
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("truncate request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("truncate returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// SubscriptionMessage represents a graphql-ws protocol message.
type SubscriptionMessage struct {
	Type    string          `json:"type"`
	ID      string          `json:"id,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// SubscriptionPayload represents the payload for a subscribe message.
type SubscriptionPayload struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

// SubscriptionData represents data received from a subscription.
type SubscriptionData struct {
	Data   json.RawMessage `json:"data,omitempty"`
	Errors []GraphQLError  `json:"errors,omitempty"`
}

// Subscription represents an active GraphQL subscription.
type Subscription struct {
	conn     *websocket.Conn
	id       string
	dataCh   chan SubscriptionData
	errCh    chan error
	closeCh  chan struct{}
	closeOnce sync.Once
}

// Data returns the channel that receives subscription data.
func (s *Subscription) Data() <-chan SubscriptionData {
	return s.dataCh
}

// Err returns the channel that receives errors.
func (s *Subscription) Err() <-chan error {
	return s.errCh
}

// Close closes the subscription.
func (s *Subscription) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		close(s.closeCh)

		// Send complete message
		completeMsg := SubscriptionMessage{
			Type: "complete",
			ID:   s.id,
		}
		if err := s.conn.WriteJSON(completeMsg); err != nil {
			closeErr = err
		}

		s.conn.Close()
	})
	return closeErr
}

// Subscribe opens a WebSocket subscription.
func (c *Client) Subscribe(ctx context.Context, query string, vars map[string]any) (*Subscription, error) {
	// Convert HTTP URL to WebSocket URL
	wsURL := strings.Replace(c.baseURL, "http://", "ws://", 1) + "/api/v0/graphql/ws"

	// Connect to WebSocket
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to websocket: %w", err)
	}

	// Send connection_init
	initMsg := SubscriptionMessage{Type: "connection_init"}
	if err := conn.WriteJSON(initMsg); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to send connection_init: %w", err)
	}

	// Wait for connection_ack
	var ackMsg SubscriptionMessage
	if err := conn.ReadJSON(&ackMsg); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to read connection_ack: %w", err)
	}
	if ackMsg.Type != "connection_ack" {
		conn.Close()
		return nil, fmt.Errorf("expected connection_ack, got %s", ackMsg.Type)
	}

	// Create subscription
	sub := &Subscription{
		conn:    conn,
		id:      "1",
		dataCh:  make(chan SubscriptionData, 10),
		errCh:   make(chan error, 1),
		closeCh: make(chan struct{}),
	}

	// Send subscribe message
	payload, _ := json.Marshal(SubscriptionPayload{Query: query, Variables: vars})
	subMsg := SubscriptionMessage{
		Type:    "subscribe",
		ID:      sub.id,
		Payload: payload,
	}
	if err := conn.WriteJSON(subMsg); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to send subscribe: %w", err)
	}

	// Start reading messages in background
	go sub.readLoop()

	return sub, nil
}

// readLoop reads messages from the WebSocket and dispatches them.
func (s *Subscription) readLoop() {
	defer close(s.dataCh)
	defer close(s.errCh)

	for {
		select {
		case <-s.closeCh:
			return
		default:
		}

		// Set read deadline to allow periodic close checks
		s.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

		var msg SubscriptionMessage
		if err := s.conn.ReadJSON(&msg); err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return
			}
			// Check if it's a timeout (expected, we'll retry)
			if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
				continue
			}
			select {
			case s.errCh <- err:
			default:
			}
			return
		}

		switch msg.Type {
		case "next":
			var data SubscriptionData
			if err := json.Unmarshal(msg.Payload, &data); err != nil {
				select {
				case s.errCh <- fmt.Errorf("failed to unmarshal data: %w", err):
				default:
				}
				continue
			}
			select {
			case s.dataCh <- data:
			case <-s.closeCh:
				return
			}
		case "error":
			var data SubscriptionData
			if err := json.Unmarshal(msg.Payload, &data); err == nil && len(data.Errors) > 0 {
				select {
				case s.errCh <- fmt.Errorf("subscription error: %s", data.Errors[0].Message):
				default:
				}
			}
		case "complete":
			return
		}
	}
}
