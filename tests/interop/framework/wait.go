package framework

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

const pollInterval = 100 * time.Millisecond

// WaitForReady polls the health endpoint until ready or timeout.
func WaitForReady(ctx context.Context, client *Client, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		err := client.HealthCheck(ctx)
		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
			// Continue polling
		}
	}

	return fmt.Errorf("node did not become ready within %v", timeout)
}

// WaitForP2PReady polls until P2P is fully initialized with a valid peer ID.
// This should be called after WaitForReady to ensure P2P operations will work.
func WaitForP2PReady(ctx context.Context, client *Client, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		info, err := client.P2PInfo(ctx)
		if err == nil && info.ID != "" && len(info.Addresses) > 0 {
			return nil
		}
		if err != nil {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
			// Continue polling
		}
	}

	if lastErr != nil {
		return fmt.Errorf("P2P did not become ready within %v: %w", timeout, lastErr)
	}
	return fmt.Errorf("P2P did not become ready within %v", timeout)
}

// WaitForPeerConnected polls until the expected peer appears in the connected peers list.
// Note: Go DefraDB doesn't support listing peers, so this returns nil immediately for Go nodes
// (assuming the connect call succeeded).
func WaitForPeerConnected(ctx context.Context, client *Client, expectedPeerID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		peers, err := client.ListPeers(ctx)
		if err == ErrEndpointNotSupported {
			// Go DefraDB doesn't support listing peers
			// If connect succeeded, we assume peer is connected
			return nil
		}
		if err == nil {
			for _, peer := range peers {
				if peer.ID == expectedPeerID {
					return nil
				}
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
			// Continue polling
		}
	}

	return fmt.Errorf("peer %s did not connect within %v", expectedPeerID, timeout)
}

// WaitForDocumentReplicated polls until a document with the expected docID appears in the collection.
// This replaces hardcoded sleeps for replication with deterministic polling.
func WaitForDocumentReplicated(ctx context.Context, client *Client, collectionName, docID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error

	// Query to find document by docID
	query := fmt.Sprintf(`query { %s(filter: {_docID: {_eq: "%s"}}) { _docID } }`, collectionName, docID)

	for time.Now().Before(deadline) {
		resp, err := client.GraphQL(ctx, query, nil)
		if err != nil {
			lastErr = err
		} else if len(resp.Errors) > 0 {
			lastErr = fmt.Errorf("GraphQL errors: %v", resp.Errors)
		} else {
			// Parse response to check if document exists
			found, err := checkDocumentInResponse(resp.Data, collectionName, docID)
			if err != nil {
				lastErr = err
			} else if found {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
			// Continue polling
		}
	}

	if lastErr != nil {
		return fmt.Errorf("document %s not replicated to %s within %v: %w", docID, collectionName, timeout, lastErr)
	}
	return fmt.Errorf("document %s not replicated to %s within %v", docID, collectionName, timeout)
}

// checkDocumentInResponse checks if the document with docID exists in the GraphQL response.
func checkDocumentInResponse(data json.RawMessage, collectionName, docID string) (bool, error) {
	if data == nil {
		return false, nil
	}

	// Parse the response dynamically
	var result map[string]json.RawMessage
	if err := json.Unmarshal(data, &result); err != nil {
		return false, fmt.Errorf("failed to parse response: %w", err)
	}

	collectionData, ok := result[collectionName]
	if !ok {
		return false, nil
	}

	// Parse as array of documents
	var docs []struct {
		DocID string `json:"_docID"`
	}
	if err := json.Unmarshal(collectionData, &docs); err != nil {
		return false, fmt.Errorf("failed to parse collection data: %w", err)
	}

	for _, doc := range docs {
		if doc.DocID == docID {
			return true, nil
		}
	}

	return false, nil
}
