package proxy

import (
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/jacktea/data-smith/pkg/config"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

func CreateSSHTunnel(sshProxy *config.SSHProxy, remote *Endpoint) (*SSHTunnel, *Endpoint, error) {
	if err := validateProxy(sshProxy, remote); err != nil {
		return nil, nil, err
	}
	authMethods, err := authenticationMethods(sshProxy)
	if err != nil {
		return nil, nil, err
	}
	hostKeyCallback, err := hostKeyCallback(sshProxy)
	if err != nil {
		return nil, nil, err
	}
	sshConfig := &ssh.ClientConfig{
		User:            sshProxy.User,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCallback,
		Timeout:         30 * time.Second,
	}
	local := &Endpoint{Host: "127.0.0.1", Port: 0}
	tunnel := &SSHTunnel{
		Local:  local,
		Server: &Endpoint{Host: sshProxy.Host, Port: sshProxy.Port},
		Remote: &Endpoint{Host: remote.Host, Port: remote.Port},
		Config: sshConfig,
	}
	return tunnel, local, nil
}

func validateProxy(sshProxy *config.SSHProxy, remote *Endpoint) error {
	if sshProxy == nil {
		return fmt.Errorf("SSH proxy configuration is required")
	}
	if strings.TrimSpace(sshProxy.Host) == "" || sshProxy.Port <= 0 || sshProxy.Port > 65535 {
		return fmt.Errorf("SSH proxy host and valid port are required")
	}
	if strings.TrimSpace(sshProxy.User) == "" {
		return fmt.Errorf("SSH proxy user is required")
	}
	if remote == nil || strings.TrimSpace(remote.Host) == "" || remote.Port <= 0 || remote.Port > 65535 {
		return fmt.Errorf("SSH tunnel remote host and valid port are required")
	}
	if strings.TrimSpace(sshProxy.KnownHostsPath) == "" && strings.TrimSpace(sshProxy.HostFingerprint) == "" {
		return fmt.Errorf("SSH host verification requires knownHostsPath or hostFingerprint")
	}
	if sshProxy.KnownHostsPath != "" && sshProxy.HostFingerprint != "" {
		return fmt.Errorf("configure only one SSH host verification method: knownHostsPath or hostFingerprint")
	}
	return nil
}

func authenticationMethods(sshProxy *config.SSHProxy) ([]ssh.AuthMethod, error) {
	switch sshProxy.Type {
	case "pass":
		if sshProxy.Pass == "" {
			return nil, fmt.Errorf("SSH password authentication requires pass")
		}
		return []ssh.AuthMethod{ssh.Password(sshProxy.Pass)}, nil
	case "rsa":
		var privateKey []byte
		var err error
		if sshProxy.RsaKeyPath != "" {
			privateKey, err = os.ReadFile(sshProxy.RsaKeyPath)
			if err != nil {
				return nil, fmt.Errorf("read SSH private key file: %w", err)
			}
		} else if sshProxy.RsaKey != "" {
			privateKey = []byte(sshProxy.RsaKey)
		} else {
			return nil, fmt.Errorf("SSH RSA authentication requires rsaKey or rsaKeyPath")
		}
		var signer ssh.Signer
		if sshProxy.RsaKeyPassword != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(privateKey, []byte(sshProxy.RsaKeyPassword))
		} else {
			signer, err = ssh.ParsePrivateKey(privateKey)
		}
		if err != nil {
			return nil, fmt.Errorf("parse SSH private key: %w", err)
		}
		return []ssh.AuthMethod{ssh.PublicKeys(signer)}, nil
	default:
		return nil, fmt.Errorf("unsupported SSH authentication type %q; expected pass or rsa", sshProxy.Type)
	}
}

func hostKeyCallback(sshProxy *config.SSHProxy) (ssh.HostKeyCallback, error) {
	if sshProxy.KnownHostsPath != "" {
		callback, err := knownhosts.New(sshProxy.KnownHostsPath)
		if err != nil {
			return nil, fmt.Errorf("load SSH known_hosts file: %w", err)
		}
		return callback, nil
	}
	want := strings.TrimSpace(sshProxy.HostFingerprint)
	digest, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(want, "SHA256:"))
	if !strings.HasPrefix(want, "SHA256:") || err != nil || len(digest) != 32 {
		return nil, fmt.Errorf("SSH hostFingerprint must be an SHA256 fingerprint")
	}
	return func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		if ssh.FingerprintSHA256(key) != want {
			return fmt.Errorf("SSH host key fingerprint mismatch for %s", hostname)
		}
		return nil
	}, nil
}
