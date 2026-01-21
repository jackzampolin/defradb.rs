package framework

import (
	"context"
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

// WaitForPeerConnected polls until the expected peer appears in the connected peers list.
func WaitForPeerConnected(ctx context.Context, client *Client, expectedPeerID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		peers, err := client.ListPeers(ctx)
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
