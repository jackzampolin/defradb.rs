package interop

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb.rs-interop/tests/interop/framework"
)

// dumpLogsOnFailure registers a cleanup function that dumps node logs if the test failed.
func dumpLogsOnFailure(t *testing.T, name string, node *framework.Node) {
	t.Cleanup(func() {
		if t.Failed() {
			logs, err := node.DumpLogsString()
			if err != nil {
				t.Logf("Failed to dump %s logs: %v", name, err)
				return
			}
			t.Logf("=== %s logs ===\n%s", name, logs)
		}
	})
}

// TestCrossRustToGoConnect tests that a Rust node can connect to a Go node.
func TestCrossRustToGoConnect(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Reserve ports for both nodes
	rustPorts, err := framework.ReserveNodePorts()
	require.NoError(t, err, "failed to reserve ports for Rust node")
	defer rustPorts.Release()

	goPorts, err := framework.ReserveNodePorts()
	require.NoError(t, err, "failed to reserve ports for Go node")
	defer goPorts.Release()

	// Start Go node first (server)
	goNode := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeGo,
		HTTPPort:     goPorts.HTTPPort,
		P2PPort:      goPorts.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Go node...")
	goPorts.Release()
	require.NoError(t, goNode.Start(ctx), "failed to start Go node")
	defer goNode.Stop()
	dumpLogsOnFailure(t, "go-node", goNode)

	t.Logf("Go node started with peer ID: %s", goNode.PeerID())

	// Start Rust node (client)
	rustNode := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     rustPorts.HTTPPort,
		P2PPort:      rustPorts.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Rust node...")
	rustPorts.Release()
	require.NoError(t, rustNode.Start(ctx), "failed to start Rust node")
	defer rustNode.Stop()
	dumpLogsOnFailure(t, "rust-node", rustNode)

	t.Logf("Rust node started with peer ID: %s", rustNode.PeerID())

	// Connect Rust node to Go node
	goMultiaddr := goNode.P2PMultiaddr()
	t.Logf("Go node multiaddr: %s", goMultiaddr)

	t.Log("Connecting Rust node to Go node...")
	rustClient := rustNode.Client()
	err = rustClient.ConnectPeer(ctx, goMultiaddr)
	require.NoError(t, err, "failed to connect Rust node to Go node")

	// Wait for connection to establish
	t.Log("Waiting for connection to establish...")
	goClient := goNode.Client()
	err = framework.WaitForPeerConnected(ctx, goClient, rustNode.PeerID(), 30*time.Second)
	require.NoError(t, err, "connection wait failed on Go node")

	err = framework.WaitForPeerConnected(ctx, rustClient, goNode.PeerID(), 30*time.Second)
	require.NoError(t, err, "Rust node did not see Go node connect")

	// Verify Rust node sees Go node
	peersRust, err := rustClient.ListPeers(ctx)
	require.NoError(t, err, "failed to list peers from Rust node")
	require.Len(t, peersRust, 1, "Rust node should see exactly 1 peer")
	require.Equal(t, goNode.PeerID(), peersRust[0].ID, "Rust node should see Go node")

	t.Log("Rust and Go nodes successfully connected!")
}

// TestCrossGoToRustConnect tests that a Go node can connect to a Rust node.
func TestCrossGoToRustConnect(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Reserve ports for both nodes
	rustPorts, err := framework.ReserveNodePorts()
	require.NoError(t, err, "failed to reserve ports for Rust node")
	defer rustPorts.Release()

	goPorts, err := framework.ReserveNodePorts()
	require.NoError(t, err, "failed to reserve ports for Go node")
	defer goPorts.Release()

	// Start Rust node first (server)
	rustNode := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     rustPorts.HTTPPort,
		P2PPort:      rustPorts.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Rust node...")
	rustPorts.Release()
	require.NoError(t, rustNode.Start(ctx), "failed to start Rust node")
	defer rustNode.Stop()
	dumpLogsOnFailure(t, "rust-node", rustNode)

	t.Logf("Rust node started with peer ID: %s", rustNode.PeerID())

	// Start Go node (client)
	goNode := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeGo,
		HTTPPort:     goPorts.HTTPPort,
		P2PPort:      goPorts.P2PPort,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Go node...")
	goPorts.Release()
	require.NoError(t, goNode.Start(ctx), "failed to start Go node")
	defer goNode.Stop()
	dumpLogsOnFailure(t, "go-node", goNode)

	t.Logf("Go node started with peer ID: %s", goNode.PeerID())

	// Connect Go node to Rust node
	rustMultiaddr := rustNode.P2PMultiaddr()
	t.Logf("Rust node multiaddr: %s", rustMultiaddr)

	t.Log("Connecting Go node to Rust node...")
	goClient := goNode.Client()
	err = goClient.ConnectPeer(ctx, rustMultiaddr)
	require.NoError(t, err, "failed to connect Go node to Rust node")

	// Wait for Rust node to see Go node as connected
	t.Log("Waiting for Rust node to see Go node...")
	rustClient := rustNode.Client()
	err = framework.WaitForPeerConnected(ctx, rustClient, goNode.PeerID(), 30*time.Second)
	require.NoError(t, err, "Rust node did not see Go node connect")

	// Wait for Go node
	t.Log("Waiting for Go node connection confirmation...")
	err = framework.WaitForPeerConnected(ctx, goClient, rustNode.PeerID(), 30*time.Second)
	require.NoError(t, err, "connection wait failed on Go node")

	// Verify Rust node sees Go node
	peersRust, err := rustClient.ListPeers(ctx)
	require.NoError(t, err, "failed to list peers from Rust node")
	require.Len(t, peersRust, 1, "Rust node should see exactly 1 peer")
	require.Equal(t, goNode.PeerID(), peersRust[0].ID, "Rust node should see Go node")

	t.Log("Go and Rust nodes successfully connected!")
}
