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
}

// NewClient creates a new Client for the given base URL.
func NewClient(baseURL string) *Client {
	return &Client{
		httpClient: &http.Client{},
		baseURL:    baseURL,
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
