package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"golang.org/x/crypto/ssh"
)

type SSHTunnel struct {
	Local  *Endpoint
	Server *Endpoint
	Remote *Endpoint
	Config *ssh.ClientConfig

	mu       sync.Mutex
	listener net.Listener
	cancel   context.CancelFunc
	started  bool
	stopped  bool
	active   map[net.Conn]struct{}
	wg       sync.WaitGroup
	forward  func(context.Context, net.Conn)
}

type Endpoint struct {
	Host string
	Port int
}

func (endpoint *Endpoint) String() string {
	return net.JoinHostPort(endpoint.Host, fmt.Sprintf("%d", endpoint.Port))
}

func (tunnel *SSHTunnel) Start() error {
	tunnel.mu.Lock()
	defer tunnel.mu.Unlock()
	if tunnel.started {
		return fmt.Errorf("SSH tunnel has already been started")
	}
	if tunnel.stopped {
		return fmt.Errorf("SSH tunnel has already been stopped")
	}
	listener, err := net.Listen("tcp", tunnel.Local.String())
	if err != nil {
		return fmt.Errorf("listen for SSH tunnel connections: %w", err)
	}
	tcpAddress, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		_ = listener.Close()
		return fmt.Errorf("SSH tunnel listener returned unexpected address type %T", listener.Addr())
	}
	tunnel.Local.Host = tcpAddress.IP.String()
	if tunnel.Local.Host == "" || tunnel.Local.Host == "::" {
		tunnel.Local.Host = "127.0.0.1"
	}
	tunnel.Local.Port = tcpAddress.Port
	ctx, cancel := context.WithCancel(context.Background())
	tunnel.listener = listener
	tunnel.cancel = cancel
	tunnel.active = make(map[net.Conn]struct{})
	tunnel.started = true
	tunnel.wg.Add(1)
	go tunnel.acceptLoop(ctx, listener)
	log.Printf("SSH tunnel listening on %s for remote %s", tunnel.Local.String(), tunnel.Remote.String())
	return nil
}

func (tunnel *SSHTunnel) acceptLoop(ctx context.Context, listener net.Listener) {
	defer tunnel.wg.Done()
	for {
		localConn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("SSH tunnel accept failed: %v", err)
			continue
		}
		tunnel.mu.Lock()
		if tunnel.stopped {
			tunnel.mu.Unlock()
			_ = localConn.Close()
			continue
		}
		tunnel.active[localConn] = struct{}{}
		tunnel.wg.Add(1)
		forward := tunnel.forward
		tunnel.mu.Unlock()
		go func() {
			defer tunnel.wg.Done()
			defer func() {
				_ = localConn.Close()
				tunnel.mu.Lock()
				delete(tunnel.active, localConn)
				tunnel.mu.Unlock()
			}()
			if forward != nil {
				forward(ctx, localConn)
				return
			}
			tunnel.forwardConnection(ctx, localConn)
		}()
	}
}

func (tunnel *SSHTunnel) Stop() error {
	tunnel.mu.Lock()
	if tunnel.stopped {
		tunnel.mu.Unlock()
		tunnel.wg.Wait()
		return nil
	}
	tunnel.stopped = true
	cancel := tunnel.cancel
	listener := tunnel.listener
	connections := make([]net.Conn, 0, len(tunnel.active))
	for connection := range tunnel.active {
		connections = append(connections, connection)
	}
	tunnel.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	var result error
	if listener != nil {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			result = errors.Join(result, fmt.Errorf("close SSH tunnel listener: %w", err))
		}
	}
	for _, connection := range connections {
		if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			result = errors.Join(result, fmt.Errorf("close SSH tunnel connection: %w", err))
		}
	}
	tunnel.wg.Wait()
	return result
}

// Verify establishes a throwaway SSH session through the same auth and host
// verification settings as the tunnel, confirming the handshake and the remote
// dial before any real traffic depends on the listener. Without it those
// failures only close the local socket, surfacing to clients as a bare
// connection reset instead of the underlying SSH error.
func (tunnel *SSHTunnel) Verify(ctx context.Context) error {
	dialer := &net.Dialer{Timeout: tunnel.Config.Timeout}
	rawServerConn, err := dialer.DialContext(ctx, "tcp", tunnel.Server.String())
	if err != nil {
		return fmt.Errorf("connect to SSH server %s: %w", tunnel.Server.String(), err)
	}
	defer rawServerConn.Close()
	stopServerClose := context.AfterFunc(ctx, func() { _ = rawServerConn.Close() })
	defer stopServerClose()

	sshConn, channels, requests, err := ssh.NewClientConn(rawServerConn, tunnel.Server.String(), tunnel.Config)
	if err != nil {
		return fmt.Errorf("SSH handshake with %s: %w", tunnel.Server.String(), err)
	}
	client := ssh.NewClient(sshConn, channels, requests)
	defer client.Close()

	remoteConn, err := client.Dial("tcp", tunnel.Remote.String())
	if err != nil {
		return fmt.Errorf("SSH remote connection to %s: %w", tunnel.Remote.String(), err)
	}
	_ = remoteConn.Close()
	return nil
}

func (tunnel *SSHTunnel) forwardConnection(ctx context.Context, localConn net.Conn) {
	dialer := &net.Dialer{Timeout: tunnel.Config.Timeout}
	rawServerConn, err := dialer.DialContext(ctx, "tcp", tunnel.Server.String())
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("SSH server connection failed: %v", err)
		}
		return
	}
	defer rawServerConn.Close()
	stopServerClose := context.AfterFunc(ctx, func() { _ = rawServerConn.Close() })
	defer stopServerClose()

	sshConn, channels, requests, err := ssh.NewClientConn(rawServerConn, tunnel.Server.String(), tunnel.Config)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("SSH handshake failed: %v", err)
		}
		return
	}
	client := ssh.NewClient(sshConn, channels, requests)
	defer client.Close()
	remoteConn, err := client.Dial("tcp", tunnel.Remote.String())
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("SSH remote connection failed: %v", err)
		}
		return
	}
	defer remoteConn.Close()
	stopConnections := context.AfterFunc(ctx, func() {
		_ = localConn.Close()
		_ = remoteConn.Close()
	})
	defer stopConnections()

	var copies sync.WaitGroup
	copies.Add(2)
	go func() {
		defer copies.Done()
		_, _ = io.Copy(remoteConn, localConn)
		_ = remoteConn.Close()
	}()
	go func() {
		defer copies.Done()
		_, _ = io.Copy(localConn, remoteConn)
		_ = localConn.Close()
	}()
	copies.Wait()
}

// ListenerAddr returns the bound local address after Start.
func (tunnel *SSHTunnel) ListenerAddr() net.Addr {
	tunnel.mu.Lock()
	defer tunnel.mu.Unlock()
	if tunnel.listener == nil {
		return nil
	}
	return tunnel.listener.Addr()
}
