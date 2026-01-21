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

// TestConnectionTwoRustNodesConnect tests that two Rust nodes can discover
// and connect to each other via P2P.
func TestConnectionTwoRustNodesConnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Allocate ports for both nodes
	http1, p2p1, err := framework.AllocateNodePorts()
	require.NoError(t, err, "failed to allocate ports for node1")

	http2, p2p2, err := framework.AllocateNodePorts()
	require.NoError(t, err, "failed to allocate ports for node2")

	// Start node1
	node1 := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     http1,
		P2PPort:      p2p1,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting node1...")
	require.NoError(t, node1.Start(ctx), "failed to start node1")
	defer node1.Stop()
	dumpLogsOnFailure(t, "node1", node1)

	t.Logf("Node1 started with peer ID: %s", node1.PeerID())

	// Start node2
	node2 := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     http2,
		P2PPort:      p2p2,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting node2...")
	require.NoError(t, node2.Start(ctx), "failed to start node2")
	defer node2.Stop()
	dumpLogsOnFailure(t, "node2", node2)

	t.Logf("Node2 started with peer ID: %s", node2.PeerID())

	// Get node1's multiaddr for node2 to connect to
	node1Multiaddr := node1.P2PMultiaddr()
	t.Logf("Node1 multiaddr: %s", node1Multiaddr)

	// Connect node2 to node1
	t.Log("Connecting node2 to node1...")
	client2 := node2.Client()
	err = client2.ConnectPeer(ctx, node1Multiaddr)
	require.NoError(t, err, "failed to connect node2 to node1")

	// Wait for node1 to see node2 as connected
	t.Log("Waiting for node1 to see node2...")
	client1 := node1.Client()
	err = framework.WaitForPeerConnected(ctx, client1, node2.PeerID(), 30*time.Second)
	require.NoError(t, err, "node1 did not see node2 connect")

	// Wait for node2 to see node1 as connected
	t.Log("Waiting for node2 to see node1...")
	err = framework.WaitForPeerConnected(ctx, client2, node1.PeerID(), 30*time.Second)
	require.NoError(t, err, "node2 did not see node1 connect")

	// Verify both nodes see each other
	peers1, err := client1.ListPeers(ctx)
	require.NoError(t, err, "failed to list peers from node1")
	require.Len(t, peers1, 1, "node1 should see exactly 1 peer")
	require.Equal(t, node2.PeerID(), peers1[0].ID, "node1 should see node2")

	peers2, err := client2.ListPeers(ctx)
	require.NoError(t, err, "failed to list peers from node2")
	require.Len(t, peers2, 1, "node2 should see exactly 1 peer")
	require.Equal(t, node1.PeerID(), peers2[0].ID, "node2 should see node1")

	t.Log("Both nodes successfully connected and see each other!")
}

// setupConnectedRustNodes is a helper that starts two Rust nodes and connects them.
func setupConnectedRustNodes(t *testing.T, ctx context.Context) (node1, node2 *framework.Node, cleanup func()) {
	t.Helper()

	// Allocate ports
	http1, p2p1, err := framework.AllocateNodePorts()
	require.NoError(t, err)
	http2, p2p2, err := framework.AllocateNodePorts()
	require.NoError(t, err)

	// Create nodes
	node1 = framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     http1,
		P2PPort:      p2p1,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	node2 = framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     http2,
		P2PPort:      p2p2,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	// Start nodes
	require.NoError(t, node1.Start(ctx), "failed to start node1")
	dumpLogsOnFailure(t, "node1", node1)

	require.NoError(t, node2.Start(ctx), "failed to start node2")
	dumpLogsOnFailure(t, "node2", node2)

	// Connect node2 to node1
	err = node2.Client().ConnectPeer(ctx, node1.P2PMultiaddr())
	require.NoError(t, err, "failed to connect nodes")

	// Wait for bidirectional connection
	require.NoError(t, framework.WaitForPeerConnected(ctx, node1.Client(), node2.PeerID(), 30*time.Second))
	require.NoError(t, framework.WaitForPeerConnected(ctx, node2.Client(), node1.PeerID(), 30*time.Second))

	cleanup = func() {
		node2.Stop()
		node1.Stop()
	}

	return node1, node2, cleanup
}

// TestConnectionNodeInfo verifies the P2P info endpoint returns valid data.
func TestConnectionNodeInfo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	http1, p2p1, err := framework.AllocateNodePorts()
	require.NoError(t, err)

	node := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     http1,
		P2PPort:      p2p1,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	require.NoError(t, node.Start(ctx))
	defer node.Stop()
	dumpLogsOnFailure(t, "node", node)

	// Verify peer ID is set
	require.NotEmpty(t, node.PeerID(), "peer ID should not be empty")

	// Verify we can fetch P2P info
	client := node.Client()
	info, err := client.P2PInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, node.PeerID(), info.ID)
	require.NotEmpty(t, info.Addresses, "should have at least one listen address")

	t.Logf("Node peer ID: %s", info.ID)
	t.Logf("Node addresses: %v", info.Addresses)
}

// TestCrossRustToGoConnect tests that a Rust node can connect to a Go node.
func TestCrossRustToGoConnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Allocate ports for both nodes
	httpRust, p2pRust, err := framework.AllocateNodePorts()
	require.NoError(t, err, "failed to allocate ports for Rust node")

	httpGo, p2pGo, err := framework.AllocateNodePorts()
	require.NoError(t, err, "failed to allocate ports for Go node")

	// Start Go node first (server)
	goNode := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeGo,
		HTTPPort:     httpGo,
		P2PPort:      p2pGo,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Go node...")
	require.NoError(t, goNode.Start(ctx), "failed to start Go node")
	defer goNode.Stop()
	dumpLogsOnFailure(t, "go-node", goNode)

	t.Logf("Go node started with peer ID: %s", goNode.PeerID())

	// Start Rust node (client)
	rustNode := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     httpRust,
		P2PPort:      p2pRust,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Rust node...")
	require.NoError(t, rustNode.Start(ctx), "failed to start Rust node")
	defer rustNode.Stop()
	dumpLogsOnFailure(t, "rust-node", rustNode)

	t.Logf("Rust node started with peer ID: %s", rustNode.PeerID())

	// Get Go node's multiaddr for Rust node to connect to
	goMultiaddr := goNode.P2PMultiaddr()
	t.Logf("Go node multiaddr: %s", goMultiaddr)

	// Connect Rust node to Go node
	t.Log("Connecting Rust node to Go node...")
	rustClient := rustNode.Client()
	err = rustClient.ConnectPeer(ctx, goMultiaddr)
	require.NoError(t, err, "failed to connect Rust node to Go node")

	// Wait for connection to establish (Go doesn't support peer listing)
	t.Log("Waiting for connection to establish...")
	goClient := goNode.Client()
	err = framework.WaitForPeerConnected(ctx, goClient, rustNode.PeerID(), 30*time.Second)
	require.NoError(t, err, "connection wait failed on Go node")

	err = framework.WaitForPeerConnected(ctx, rustClient, goNode.PeerID(), 30*time.Second)
	require.NoError(t, err, "Rust node did not see Go node connect")

	// Verify Rust node sees Go node (Go doesn't support peer listing)
	peersRust, err := rustClient.ListPeers(ctx)
	require.NoError(t, err, "failed to list peers from Rust node")
	require.Len(t, peersRust, 1, "Rust node should see exactly 1 peer")
	require.Equal(t, goNode.PeerID(), peersRust[0].ID, "Rust node should see Go node")

	t.Log("Rust and Go nodes successfully connected!")
}

// TestCrossGoToRustConnect tests that a Go node can connect to a Rust node.
func TestCrossGoToRustConnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Allocate ports for both nodes
	httpRust, p2pRust, err := framework.AllocateNodePorts()
	require.NoError(t, err, "failed to allocate ports for Rust node")

	httpGo, p2pGo, err := framework.AllocateNodePorts()
	require.NoError(t, err, "failed to allocate ports for Go node")

	// Start Rust node first (server)
	rustNode := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeRust,
		HTTPPort:     httpRust,
		P2PPort:      p2pRust,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Rust node...")
	require.NoError(t, rustNode.Start(ctx), "failed to start Rust node")
	defer rustNode.Stop()
	dumpLogsOnFailure(t, "rust-node", rustNode)

	t.Logf("Rust node started with peer ID: %s", rustNode.PeerID())

	// Start Go node (client)
	goNode := framework.NewNode(framework.NodeConfig{
		Type:         framework.NodeTypeGo,
		HTTPPort:     httpGo,
		P2PPort:      p2pGo,
		Store:        "memory",
		NoEncryption: true,
		NoSigning:    true,
	})

	t.Log("Starting Go node...")
	require.NoError(t, goNode.Start(ctx), "failed to start Go node")
	defer goNode.Stop()
	dumpLogsOnFailure(t, "go-node", goNode)

	t.Logf("Go node started with peer ID: %s", goNode.PeerID())

	// Get Rust node's multiaddr for Go node to connect to
	rustMultiaddr := rustNode.P2PMultiaddr()
	t.Logf("Rust node multiaddr: %s", rustMultiaddr)

	// Connect Go node to Rust node
	t.Log("Connecting Go node to Rust node...")
	goClient := goNode.Client()
	err = goClient.ConnectPeer(ctx, rustMultiaddr)
	require.NoError(t, err, "failed to connect Go node to Rust node")

	// Wait for Rust node to see Go node as connected
	t.Log("Waiting for Rust node to see Go node...")
	rustClient := rustNode.Client()
	err = framework.WaitForPeerConnected(ctx, rustClient, goNode.PeerID(), 30*time.Second)
	require.NoError(t, err, "Rust node did not see Go node connect")

	// Wait for Go node (Go doesn't support peer listing, so this returns immediately)
	t.Log("Waiting for Go node connection confirmation...")
	err = framework.WaitForPeerConnected(ctx, goClient, rustNode.PeerID(), 30*time.Second)
	require.NoError(t, err, "connection wait failed on Go node")

	// Verify Rust node sees Go node (Go doesn't support peer listing)
	peersRust, err := rustClient.ListPeers(ctx)
	require.NoError(t, err, "failed to list peers from Rust node")
	require.Len(t, peersRust, 1, "Rust node should see exactly 1 peer")
	require.Equal(t, goNode.PeerID(), peersRust[0].ID, "Rust node should see Go node")

	t.Log("Go and Rust nodes successfully connected!")
}
