package framework

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
type P2PInfoResponse struct {
	ID        string   `json:"id"`
	Addresses []string `json:"addresses"`
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

	var info P2PInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("failed to decode p2p info response: %w", err)
	}

	return &info, nil
}

// ConnectPeer connects to a peer via multiaddr.
func (c *Client) ConnectPeer(ctx context.Context, multiaddr string) error {
	reqBody := ConnectPeerRequest{Address: multiaddr}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal connect peer request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v0/p2p/peers", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create connect peer request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("connect peer request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("connect peer returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// ListPeers returns a list of connected peers.
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
