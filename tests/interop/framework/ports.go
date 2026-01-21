package framework

import (
	"fmt"
	"net"
)

// FindFreePort finds an available TCP port on localhost.
func FindFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("failed to find free port: %w", err)
	}
	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port, nil
}

// AllocateNodePorts allocates HTTP and P2P ports for a node.
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
