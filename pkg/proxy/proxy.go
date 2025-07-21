package proxy

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/jacktea/data-smith/pkg/config"

	"golang.org/x/crypto/ssh"
)

func CreateSSHTunnel(sshProxy *config.SSHProxy, remote *Endpoint) (*SSHTunnel, *Endpoint, error) {
	// 创建认证方法列表
	var authMethods []ssh.AuthMethod

	// 根据认证类型选择认证方法
	switch sshProxy.Type {
	case "pass":
		if sshProxy.Pass != "" {
			authMethods = append(authMethods, ssh.Password(sshProxy.Pass))
		}
	case "rsa":
		// 尝试从文件或内容加载私钥
		var privateKey []byte
		var err error

		if sshProxy.RsaKeyPath != "" {
			// 从文件加载私钥
			privateKey, err = os.ReadFile(sshProxy.RsaKeyPath)
			if err != nil {
				return nil, nil, fmt.Errorf("读取RSA私钥文件失败: %w", err)
			}
		} else if sshProxy.RsaKey != "" {
			// 使用私钥内容
			privateKey = []byte(sshProxy.RsaKey)
		} else {
			return nil, nil, fmt.Errorf("未提供RSA私钥")
		}

		// 解析私钥
		var signer ssh.Signer
		if sshProxy.RsaKeyPassword != "" {
			// 如果私钥有密码，使用密码解密
			signer, err = ssh.ParsePrivateKeyWithPassphrase(privateKey, []byte(sshProxy.RsaKeyPassword))
		} else {
			signer, err = ssh.ParsePrivateKey(privateKey)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("解析RSA私钥失败: %w", err)
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	default:
		return nil, nil, fmt.Errorf("不支持的认证类型: %s", sshProxy.Type)
	}

	// 创建SSH配置
	sshConfig := &ssh.ClientConfig{
		User:            sshProxy.User,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         30 * time.Second,
	}

	port := rand.Intn(1000) + 51000
	// 创建本地端点
	local := &Endpoint{Host: "127.0.0.1", Port: port}

	// 配置SSH隧道
	tunnel := &SSHTunnel{
		Local:  local,                                               // 本地监听端口
		Server: &Endpoint{Host: sshProxy.Host, Port: sshProxy.Port}, // SSH服务器
		Remote: &Endpoint{Host: remote.Host, Port: remote.Port},     // 目标数据库
		Config: sshConfig,
	}

	return tunnel, local, nil
}
