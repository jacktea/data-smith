package proxy

import (
	"context"
	"crypto/ed25519"
	cryptorand "crypto/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jacktea/data-smith/pkg/config"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

func testPublicKey(t *testing.T) ssh.PublicKey {
	t.Helper()
	public, _, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	key, err := ssh.NewPublicKey(public)
	if err != nil {
		t.Fatalf("SSH public key: %v", err)
	}
	return key
}

func TestCreateSSHTunnelRequiresHostVerification(t *testing.T) {
	proxyConfig := &config.SSHProxy{Host: "bastion.test", Port: 22, User: "user", Type: "pass", Pass: "placeholder"}
	_, _, err := CreateSSHTunnel(proxyConfig, &Endpoint{Host: "database.test", Port: 5432})
	if err == nil || !strings.Contains(err.Error(), "requires knownHostsPath or hostFingerprint") {
		t.Fatalf("got %v, want missing trust error", err)
	}
}

func TestPinnedHostFingerprintAcceptsOnlyPinnedKey(t *testing.T) {
	trusted := testPublicKey(t)
	untrusted := testPublicKey(t)
	callback, err := hostKeyCallback(&config.SSHProxy{HostFingerprint: ssh.FingerprintSHA256(trusted)})
	if err != nil {
		t.Fatalf("build callback: %v", err)
	}
	address := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 22}
	if err := callback("bastion.test", address, trusted); err != nil {
		t.Fatalf("trusted key rejected: %v", err)
	}
	if err := callback("bastion.test", address, untrusted); err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("untrusted key result: %v", err)
	}
}

func TestKnownHostsCallbackAcceptsMatchingEntryAndRejectsMismatch(t *testing.T) {
	trusted := testPublicKey(t)
	untrusted := testPublicKey(t)
	path := filepath.Join(t.TempDir(), "known_hosts")
	line := knownhosts.Line([]string{"[bastion.test]:22"}, trusted) + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatalf("write known_hosts: %v", err)
	}
	callback, err := hostKeyCallback(&config.SSHProxy{KnownHostsPath: path})
	if err != nil {
		t.Fatalf("build callback: %v", err)
	}
	address := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 22}
	if err := callback("bastion.test:22", address, trusted); err != nil {
		t.Fatalf("matching known_hosts entry rejected: %v", err)
	}
	if err := callback("bastion.test:22", address, untrusted); err == nil {
		t.Fatal("mismatched known_hosts key was accepted")
	}
}

func TestSSHTunnelStopClosesListenerAndAcceptedConnections(t *testing.T) {
	forwardStarted := make(chan struct{})
	forwardStopped := make(chan struct{})
	tunnel := &SSHTunnel{
		Local:  &Endpoint{Host: "127.0.0.1", Port: 0},
		Server: &Endpoint{Host: "127.0.0.1", Port: 1},
		Remote: &Endpoint{Host: "127.0.0.1", Port: 1},
		Config: &ssh.ClientConfig{Timeout: time.Second},
		forward: func(ctx context.Context, connection net.Conn) {
			close(forwardStarted)
			<-ctx.Done()
			close(forwardStopped)
		},
	}
	if err := tunnel.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if tunnel.Local.Port == 0 {
		t.Fatal("system-assigned port was not recorded")
	}
	client, err := net.Dial("tcp", tunnel.Local.String())
	if err != nil {
		t.Fatalf("dial tunnel: %v", err)
	}
	select {
	case <-forwardStarted:
	case <-time.After(time.Second):
		t.Fatal("accepted connection was not forwarded")
	}
	var stops sync.WaitGroup
	stopErrors := make(chan error, 8)
	for range 8 {
		stops.Add(1)
		go func() {
			defer stops.Done()
			stopErrors <- tunnel.Stop()
		}()
	}
	stops.Wait()
	close(stopErrors)
	for err := range stopErrors {
		if err != nil {
			t.Fatalf("concurrent stop: %v", err)
		}
	}
	if err := tunnel.Stop(); err != nil {
		t.Fatalf("repeated stop: %v", err)
	}
	select {
	case <-forwardStopped:
	case <-time.After(time.Second):
		t.Fatal("forwarding goroutine remained after Stop")
	}
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("accepted connection remained open after Stop")
	}
	_ = client.Close()
	if connection, err := net.DialTimeout("tcp", tunnel.Local.String(), 100*time.Millisecond); err == nil {
		_ = connection.Close()
		t.Fatal("listener accepted a connection after Stop")
	}
}
