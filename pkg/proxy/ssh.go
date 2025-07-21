package proxy

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

type SSHTunnel struct {
	Local  *Endpoint
	Server *Endpoint
	Remote *Endpoint
	Config *ssh.ClientConfig
	done   chan struct{}
	mu     sync.Mutex
}

type Endpoint struct {
	Host string
	Port int
}

func (endpoint *Endpoint) String() string {
	return fmt.Sprintf("%s:%d", endpoint.Host, endpoint.Port)
}

// 创建SSH隧道
func (tunnel *SSHTunnel) Start() error {
	log.Printf("开始创建SSH隧道: 本地[%s] -> SSH服务器[%s] -> 远程[%s]",
		tunnel.Local.String(), tunnel.Server.String(), tunnel.Remote.String())

	listener, err := net.Listen("tcp", tunnel.Local.String())
	if err != nil {
		log.Printf("创建本地监听器失败: %v", err)
		return fmt.Errorf("创建本地监听器失败: %w", err)
	}
	log.Printf("成功创建本地监听器: %s", tunnel.Local.String())

	tunnel.done = make(chan struct{})

	go func() {
		defer func() {
			listener.Close()
			log.Printf("SSH隧道监听器已关闭: %s", tunnel.Local.String())
		}()

		log.Printf("开始监听本地连接: %s", tunnel.Local.String())
		for {
			select {
			case <-tunnel.done:
				log.Printf("收到停止信号，关闭SSH隧道")
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					select {
					case <-tunnel.done:
						return
					default:
						log.Printf("接受连接失败: %v", err)
						continue
					}
				}
				log.Printf("收到新的本地连接: %s", conn.RemoteAddr().String())
				go tunnel.forward(conn)
			}
		}
	}()

	return nil
}

func (tunnel *SSHTunnel) Stop() error {
	tunnel.mu.Lock()
	defer tunnel.mu.Unlock()

	log.Printf("正在停止SSH隧道...")
	if tunnel.done != nil {
		close(tunnel.done)
		log.Printf("SSH隧道已停止")
	}
	return nil
}

func (tunnel *SSHTunnel) forward(localConn net.Conn) {
	startTime := time.Now()
	log.Printf("开始转发连接: %s", localConn.RemoteAddr().String())

	// 连接到SSH服务器
	serverConn, err := ssh.Dial("tcp", tunnel.Server.String(), tunnel.Config)
	if err != nil {
		log.Printf("SSH连接失败: %v", err)
		localConn.Close()
		return
	}
	log.Printf("成功连接到SSH服务器: %s", tunnel.Server.String())

	// 通过SSH连接到远程数据库
	remoteConn, err := serverConn.Dial("tcp", tunnel.Remote.String())
	if err != nil {
		log.Printf("远程数据库连接失败: %v", err)
		serverConn.Close()
		localConn.Close()
		return
	}
	log.Printf("成功连接到远程数据库: %s", tunnel.Remote.String())

	// 记录连接建立时间
	log.Printf("连接转发完成，耗时: %v", time.Since(startTime))

	// 创建双向数据转发
	var wg sync.WaitGroup
	wg.Add(2)

	// 本地 -> 远程
	go func() {
		defer wg.Done()
		defer func() {
			remoteConn.Close()
			log.Printf("远程数据库连接已关闭: %s", tunnel.Remote.String())
		}()

		buf := make([]byte, 32*1024)
		for {
			select {
			case <-tunnel.done:
				log.Printf("收到停止信号，关闭本地->远程连接")
				return
			default:
				n, err := localConn.Read(buf)
				if err != nil {
					if err != io.EOF {
						log.Printf("本地->远程 读取数据失败: %v", err)
					}
					return
				}
				if n > 0 {
					_, err = remoteConn.Write(buf[:n])
					if err != nil {
						log.Printf("本地->远程 写入数据失败: %v", err)
						return
					}
				}
			}
		}
	}()

	// 远程 -> 本地
	go func() {
		defer wg.Done()
		defer func() {
			localConn.Close()
			log.Printf("本地连接已关闭: %s", localConn.RemoteAddr().String())
		}()

		buf := make([]byte, 32*1024)
		for {
			select {
			case <-tunnel.done:
				log.Printf("收到停止信号，关闭远程->本地连接")
				return
			default:
				n, err := remoteConn.Read(buf)
				if err != nil {
					if err != io.EOF {
						log.Printf("远程->本地 读取数据失败: %v", err)
					}
					return
				}
				if n > 0 {
					_, err = localConn.Write(buf[:n])
					if err != nil {
						log.Printf("远程->本地 写入数据失败: %v", err)
						return
					}
				}
			}
		}
	}()

	// 等待连接关闭
	wg.Wait()
	serverConn.Close()
	log.Printf("连接转发已结束: %s", localConn.RemoteAddr().String())
}
