package framework

import (
	"fmt"
	"net"
)

// PortPair holds reserved HTTP and P2P ports with their listeners.
// The listeners prevent other processes from claiming the ports until Release() is called.
type PortPair struct {
	HTTPPort     int
	P2PPort      int
	httpListener net.Listener
	p2pListener  net.Listener
}

// Release closes the listeners, freeing the ports for use.
// Must be called before starting a node that will bind to these ports.
// Safe to call multiple times.
func (pp *PortPair) Release() error {
	var errs []error
	if pp.httpListener != nil {
		if err := pp.httpListener.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to release HTTP port: %w", err))
		}
		pp.httpListener = nil
	}
	if pp.p2pListener != nil {
		if err := pp.p2pListener.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to release P2P port: %w", err))
		}
		pp.p2pListener = nil
	}
	if len(errs) > 0 {
		return fmt.Errorf("release errors: %v", errs)
	}
	return nil
}

// ReserveNodePorts reserves HTTP and P2P ports for a node.
// The ports are held by listeners until Release() is called.
// This prevents race conditions in parallel tests where another test
// could claim a port between finding it free and actually binding to it.
//
// Usage:
//
//	ports, err := framework.ReserveNodePorts()
//	require.NoError(t, err)
//	defer ports.Release()
//
//	node := framework.NewNode(framework.NodeConfig{
//	    HTTPPort: ports.HTTPPort,
//	    P2PPort:  ports.P2PPort,
//	    ...
//	})
//	ports.Release() // Release before starting node
//	require.NoError(t, node.Start(ctx))
func ReserveNodePorts() (*PortPair, error) {
	// Reserve HTTP port
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("failed to reserve HTTP port: %w", err)
	}

	// Reserve P2P port
	p2pListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		httpListener.Close()
		return nil, fmt.Errorf("failed to reserve P2P port: %w", err)
	}

	return &PortPair{
		HTTPPort:     httpListener.Addr().(*net.TCPAddr).Port,
		P2PPort:      p2pListener.Addr().(*net.TCPAddr).Port,
		httpListener: httpListener,
		p2pListener:  p2pListener,
	}, nil
}

// AllocateNodePorts allocates HTTP and P2P ports for a node.
// DEPRECATED: Use ReserveNodePorts() instead for parallel-safe port allocation.
// This function has a race condition between finding a free port and binding to it.
func AllocateNodePorts() (httpPort, p2pPort int, err error) {
	httpPort, err = FindFreePort()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to allocate HTTP port: %w", err)
	}

	p2pPort, err = FindFreePort()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to allocate P2P port: %w", err)
	}

	return httpPort, p2pPort, nil
}

// FindFreePort finds an available TCP port on localhost.
// WARNING: This has a race condition - the port may be taken by the time you bind to it.
// Prefer using ReserveNodePorts() for parallel-safe allocation.
func FindFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("failed to find free port: %w", err)
	}
	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port, nil
}
