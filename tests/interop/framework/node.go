package framework

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// NodeType identifies the implementation type.
type NodeType string

const (
	NodeTypeRust NodeType = "rust"
	NodeTypeGo   NodeType = "go"
)

// NodeConfig holds configuration for starting a node.
type NodeConfig struct {
	Type         NodeType
	HTTPPort     int
	P2PPort      int
	Store        string // "memory", "redb", "badger"
	NoEncryption bool
	NoSigning    bool
}

// Node represents a running DefraDB node.
type Node struct {
	Config  NodeConfig
	cmd     *exec.Cmd
	tempDir string
	httpURL string
	peerID  string
	logFile *os.File
}

// NewNode creates a new Node with the given configuration.
func NewNode(cfg NodeConfig) *Node {
	return &Node{
		Config:  cfg,
		httpURL: fmt.Sprintf("http://127.0.0.1:%d", cfg.HTTPPort),
	}
}

// Start starts the node and waits for it to become ready.
func (n *Node) Start(ctx context.Context) error {
	// Create temp directory for node data
	tempDir, err := os.MkdirTemp("", "defra-interop-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	n.tempDir = tempDir

	// Build command based on node type
	switch n.Config.Type {
	case NodeTypeRust:
		if err := n.startRust(ctx); err != nil {
			n.cleanup()
			return err
		}
	case NodeTypeGo:
		return fmt.Errorf("Go node type not yet implemented")
	default:
		return fmt.Errorf("unknown node type: %s", n.Config.Type)
	}

	// Wait for the node to become ready
	client := n.Client()
	if err := WaitForReady(ctx, client, 30*time.Second); err != nil {
		n.Stop()
		return fmt.Errorf("node failed to become ready: %w", err)
	}

	// Fetch peer ID
	info, err := client.P2PInfo(ctx)
	if err != nil {
		n.Stop()
		return fmt.Errorf("failed to get peer info: %w", err)
	}
	n.peerID = info.ID

	return nil
}

// startRust starts the Rust defra binary.
func (n *Node) startRust(ctx context.Context) error {
	binary := n.rustBinaryPath()

	// Check if binary exists
	if _, err := os.Stat(binary); os.IsNotExist(err) {
		return fmt.Errorf("rust binary not found at %s (run 'make build-rust' first)", binary)
	}

	// Build command arguments
	args := []string{
		"start",
		"--url", fmt.Sprintf("127.0.0.1:%d", n.Config.HTTPPort),
		"--p2paddr", fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", n.Config.P2PPort),
		"--rootdir", n.tempDir,
		"--no-keyring", "true",
	}

	// Add store type
	store := n.Config.Store
	if store == "" {
		store = "memory"
	}
	args = append(args, "--store", store)

	// Add optional flags (boolean flags require explicit true/false values)
	if n.Config.NoEncryption {
		args = append(args, "--no-encryption", "true")
	}
	if n.Config.NoSigning {
		args = append(args, "--no-signing", "true")
	}

	n.cmd = exec.CommandContext(ctx, binary, args...)

	// Create log file in temp directory
	logPath := filepath.Join(n.tempDir, "node.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	n.logFile = logFile

	// Redirect stdout and stderr to log file
	n.cmd.Stdout = logFile
	n.cmd.Stderr = logFile

	// Start the process
	if err := n.cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("failed to start rust binary: %w", err)
	}

	return nil
}

// rustBinaryPath returns the path to the Rust defra binary.
func (n *Node) rustBinaryPath() string {
	// Check environment variable first
	if path := os.Getenv("DEFRA_RS_BINARY"); path != "" {
		return path
	}

	// Default to relative path from tests/interop directory
	return filepath.Join("..", "..", "target", "release", "defra")
}

// Stop stops the node and cleans up resources.
func (n *Node) Stop() error {
	var errs []error

	if n.cmd != nil && n.cmd.Process != nil {
		// Send SIGINT for graceful shutdown
		if err := n.cmd.Process.Signal(syscall.SIGINT); err != nil {
			errs = append(errs, fmt.Errorf("failed to send SIGINT: %w", err))
		}

		// Wait with timeout
		done := make(chan error, 1)
		go func() {
			done <- n.cmd.Wait()
		}()

		select {
		case <-done:
			// Process exited cleanly
		case <-time.After(5 * time.Second):
			// Force kill after timeout
			if err := n.cmd.Process.Kill(); err != nil {
				errs = append(errs, fmt.Errorf("failed to kill process: %w", err))
			}
		}
	}

	// Close log file
	if n.logFile != nil {
		n.logFile.Close()
		n.logFile = nil
	}

	// Clean up temp directory
	if err := n.cleanup(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("stop errors: %v", errs)
	}

	return nil
}

// cleanup removes the temp directory.
func (n *Node) cleanup() error {
	if n.tempDir != "" {
		if err := os.RemoveAll(n.tempDir); err != nil {
			return fmt.Errorf("failed to remove temp dir: %w", err)
		}
		n.tempDir = ""
	}
	return nil
}

// Client returns an HTTP client for this node.
func (n *Node) Client() *Client {
	return NewClient(n.httpURL)
}

// PeerID returns the node's peer ID.
func (n *Node) PeerID() string {
	return n.peerID
}

// P2PMultiaddr returns the full P2P multiaddr for connecting to this node.
func (n *Node) P2PMultiaddr() string {
	return fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", n.Config.P2PPort, n.peerID)
}

// HTTPURL returns the node's HTTP URL.
func (n *Node) HTTPURL() string {
	return n.httpURL
}

// LogPath returns the path to the node's log file.
func (n *Node) LogPath() string {
	if n.tempDir == "" {
		return ""
	}
	return filepath.Join(n.tempDir, "node.log")
}

// DumpLogs writes the node's logs to the given writer.
// Useful for debugging test failures.
func (n *Node) DumpLogs(w io.Writer) error {
	logPath := n.LogPath()
	if logPath == "" {
		return fmt.Errorf("no log file available")
	}

	// Sync log file to ensure all data is written
	if n.logFile != nil {
		n.logFile.Sync()
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		return fmt.Errorf("failed to read log file: %w", err)
	}

	_, err = w.Write(data)
	return err
}

// DumpLogsString returns the node's logs as a string.
func (n *Node) DumpLogsString() (string, error) {
	logPath := n.LogPath()
	if logPath == "" {
		return "", fmt.Errorf("no log file available")
	}

	// Sync log file to ensure all data is written
	if n.logFile != nil {
		n.logFile.Sync()
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		return "", fmt.Errorf("failed to read log file: %w", err)
	}

	return string(data), nil
}
