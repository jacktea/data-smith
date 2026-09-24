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

func testSigner(t *testing.T) ssh.Signer {
	t.Helper()
	_, private, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(private)
	if err != nil {
		t.Fatalf("SSH signer: %v", err)
	}
	return signer
}

func TestCreateSSHTunnelRequiresHostVerification(t *testing.T) {
	proxyConfig := &config.SSHProxy{Host: "bastion.test", Port: 22, User: "user", Type: "pass", Pass: "placeholder"}
	_, _, err := CreateSSHTunnel(proxyConfig, &Endpoint{Host: "database.test", Port: 5432})
	if err == nil || !strings.Contains(err.Error(), "requires knownHostsPath or hostFingerprint") {
		t.Fatalf("got %v, want missing trust error", err)
	}
}

func TestCreateSSHTunnelAllowsExplicitInsecureFlag(t *testing.T) {
	proxyConfig := &config.SSHProxy{
		Host: "bastion.test", Port: 22, User: "user", Type: "pass", Pass: "placeholder",
		AllowInsecureHostKey: true,
	}
	tunnel, _, err := CreateSSHTunnel(proxyConfig, &Endpoint{Host: "database.test", Port: 5432})
	if err != nil {
		t.Fatalf("create with insecure flag: %v", err)
	}
	// 开关生效时回调接受任意主机公钥。
	if err := tunnel.Config.HostKeyCallback("bastion.test", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 22}, testPublicKey(t)); err != nil {
		t.Fatalf("insecure callback rejected a key: %v", err)
	}
	// 校验材料与开关同时提供时,仍按显式校验执行(更严格者优先)。
	strict := *proxyConfig
	strict.HostFingerprint = ssh.FingerprintSHA256(testPublicKey(t))
	tunnel, _, err = CreateSSHTunnel(&strict, &Endpoint{Host: "database.test", Port: 5432})
	if err != nil {
		t.Fatalf("create with fingerprint and flag: %v", err)
	}
	if err := tunnel.Config.HostKeyCallback("bastion.test", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 22}, testPublicKey(t)); err == nil {
		t.Fatal("fingerprint verification was skipped despite being configured")
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

// startTestSSHServer 仅供 Verify 测试:接受任意密码。返回监听地址。
func startTestSSHServer(t *testing.T, hostKey ssh.Signer, acceptRemote bool) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen test SSH server: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveTestSSHConn(conn, hostKey, acceptRemote)
		}
	}()
	return listener.Addr().String()
}

func serveTestSSHConn(conn net.Conn, hostKey ssh.Signer, acceptRemote bool) {
	serverConfig := &ssh.ServerConfig{
		PasswordCallback: func(ssh.ConnMetadata, []byte) (*ssh.Permissions, error) {
			return &ssh.Permissions{}, nil
		},
	}
	serverConfig.AddHostKey(hostKey)
	serverConn, channels, requests, err := ssh.NewServerConn(conn, serverConfig)
	if err != nil {
		return
	}
	defer serverConn.Close()
	go ssh.DiscardRequests(requests)
	for newChannel := range channels {
		if !acceptRemote {
			_ = newChannel.Reject(ssh.Prohibited, "remote dial disabled")
			continue
		}
		channel, channelRequests, err := newChannel.Accept()
		if err != nil {
			continue
		}
		go ssh.DiscardRequests(channelRequests)
		go func() {
			buffer := make([]byte, 1024)
			for {
				if _, err := channel.Read(buffer); err != nil {
					_ = channel.Close()
					return
				}
			}
		}()
	}
}

func verifyTestTunnel(t *testing.T, server *Endpoint, pinnedFingerprint string) *SSHTunnel {
	t.Helper()
	callback, err := hostKeyCallback(&config.SSHProxy{HostFingerprint: pinnedFingerprint})
	if err != nil {
		t.Fatalf("build host key callback: %v", err)
	}
	return &SSHTunnel{
		Local:  &Endpoint{Host: "127.0.0.1", Port: 0},
		Server: server,
		Remote: &Endpoint{Host: "database.test", Port: 5432},
		Config: &ssh.ClientConfig{
			User:            "user",
			Auth:            []ssh.AuthMethod{ssh.Password("password")},
			HostKeyCallback: callback,
			Timeout:         2 * time.Second,
		},
	}
}

func testEndpoint(addr string) *Endpoint {
	host, portText, _ := net.SplitHostPort(addr)
	port := 0
	for _, c := range portText {
		port = port*10 + int(c-'0')
	}
	return &Endpoint{Host: host, Port: port}
}

func TestVerifyDialsSSHEagerlyWithSpecificErrors(t *testing.T) {
	t.Run("connect refused", func(t *testing.T) {
		tunnel := verifyTestTunnel(t, &Endpoint{Host: "127.0.0.1", Port: 1}, ssh.FingerprintSHA256(testSigner(t).PublicKey()))
		err := tunnel.Verify(context.Background())
		if err == nil || !strings.Contains(err.Error(), "connect to SSH server") {
			t.Fatalf("got %v, want connect error", err)
		}
	})
	t.Run("fingerprint mismatch", func(t *testing.T) {
		serverKey := testSigner(t)
		unpinnedKey := testSigner(t)
		addr := startTestSSHServer(t, serverKey, true)
		tunnel := verifyTestTunnel(t, testEndpoint(addr), ssh.FingerprintSHA256(unpinnedKey.PublicKey()))
		err := tunnel.Verify(context.Background())
		if err == nil || !strings.Contains(err.Error(), "fingerprint mismatch") {
			t.Fatalf("got %v, want fingerprint mismatch", err)
		}
		if !strings.Contains(err.Error(), "got SHA256:") {
			t.Fatalf("mismatch error %v does not reveal received fingerprint", err)
		}
	})
	t.Run("remote dial rejected", func(t *testing.T) {
		serverKey := testSigner(t)
		addr := startTestSSHServer(t, serverKey, false)
		tunnel := verifyTestTunnel(t, testEndpoint(addr), ssh.FingerprintSHA256(serverKey.PublicKey()))
		err := tunnel.Verify(context.Background())
		if err == nil || !strings.Contains(err.Error(), "SSH remote connection") {
			t.Fatalf("got %v, want remote dial error", err)
		}
	})
	t.Run("success", func(t *testing.T) {
		serverKey := testSigner(t)
		addr := startTestSSHServer(t, serverKey, true)
		tunnel := verifyTestTunnel(t, testEndpoint(addr), ssh.FingerprintSHA256(serverKey.PublicKey()))
		if err := tunnel.Verify(context.Background()); err != nil {
			t.Fatalf("verify: %v", err)
		}
	})
}
