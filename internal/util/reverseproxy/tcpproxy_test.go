package reverseproxy

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"runtime"
	"testing"
	"time"
)

func helloServer(host string, port int, stop chan struct{}) error {
	l, err := net.Listen("tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)))
	if err != nil {
		return err
	}
	defer l.Close()

	done := false
	go func() {
		<-stop
		done = true
		l.Close()
	}()

	for {
		if done {
			return nil
		}
		conn, err := l.Accept()
		if err != nil {
			if done {
				return nil
			}
			return err
		}
		conn.Write([]byte("Hello!\n"))
		conn.Close()
	}
}

func callServer(host string, port int) (string, error) {
	conn, err := net.Dial("tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	buf := bufio.NewReader(conn)
	return buf.ReadString('\n')
}

func TestProxyNormal(t *testing.T) {
	stopP := make(chan struct{}, 1)
	req := Request{
		LocalPort:  30390,
		RemoteIP:   "127.0.0.1",
		RemotePort: 30391,
		StopCh:     stopP,
		MaxRetry:   2,
	}

	if err := Proxy(req); err != nil {
		t.Errorf("unexpected error starting proxy: %s", err)
	}

	stopS := make(chan struct{}, 1)
	go func() {
		if err := helloServer("127.0.0.1", 30391, stopS); err != nil {
			t.Errorf("unexpected error running helloServer: %s", err)
		}
	}()

	<-time.After(time.Second)

	res, err := callServer("127.0.0.1", 30390)
	if err != nil {
		t.Errorf("unexpected error calling helloServer via proxy: %s", err)
	}

	if res != "Hello!\n" {
		t.Errorf("unexpected answer calling helloServer via proxy: %s", res)
	}

	stopP <- struct{}{}
	stopS <- struct{}{}
}

func TestProxyRefused(t *testing.T) {
	stopP := make(chan struct{}, 1)
	req := Request{
		LocalPort:  30490,
		RemoteIP:   "127.0.0.1",
		RemotePort: 30392,
		StopCh:     stopP,
		MaxRetry:   1,
	}

	if err := Proxy(req); err != nil {
		t.Errorf("unexpected error starting proxy: %s", err)
	}

	_, err := callServer("127.0.0.1", 30490)
	if err == nil {
		t.Errorf("expected error calling helloServer via proxy but didn't get any")
	}

	stopP <- struct{}{}
}

func TestProxyNotReady(t *testing.T) {
	stopP := make(chan struct{}, 1)
	req := Request{
		LocalPort:  30590,
		RemoteIP:   "127.0.0.1",
		RemotePort: 30393,
		StopCh:     stopP,
		MaxRetry:   2,
	}

	if err := Proxy(req); err != nil {
		t.Errorf("unexpected error starting proxy: %s", err)
	}

	<-time.After(time.Second)

	res, err := callServer("127.0.0.1", 30590)
	if err != io.EOF {
		t.Errorf("unexpected error calling helloServer via proxy: %s", err)
	}

	if res != "" {
		t.Errorf("unexpected answer calling helloServer via proxy: %s", res)
	}

	stopS := make(chan struct{}, 1)
	go func() {
		if err := helloServer("127.0.0.1", 30393, stopS); err != nil {
			t.Errorf("unexpected error running helloServer: %s", err)
		}
	}()

	<-time.After(time.Second)

	res, err = callServer("127.0.0.1", 30590)
	if err != nil {
		t.Errorf("unexpected error calling helloServer via proxy: %s", err)
	}

	if res != "Hello!\n" {
		t.Errorf("unexpected answer calling helloServer via proxy: %s", res)
	}

	stopP <- struct{}{}
	stopS <- struct{}{}
}

func TestProxyIPv6Normal(t *testing.T) {
	stopP := make(chan struct{}, 1)
	req := Request{
		LocalPort:  30690,
		RemoteIP:   "::1",
		RemotePort: 30691,
		StopCh:     stopP,
		MaxRetry:   2,
	}

	if err := Proxy(req); err != nil {
		t.Errorf("unexpected error starting proxy: %s", err)
	}

	stopS := make(chan struct{}, 1)
	go func() {
		if err := helloServer("::1", 30691, stopS); err != nil {
			t.Errorf("unexpected error running helloServer: %s", err)
		}
	}()

	<-time.After(time.Second)

	res, err := callServer("::1", 30690)
	if err != nil {
		t.Errorf("unexpected error calling helloServer via proxy: %s", err)
	}

	if res != "Hello!\n" {
		t.Errorf("unexpected answer calling helloServer via proxy: %s", res)
	}

	stopP <- struct{}{}
	stopS <- struct{}{}
}

func proxiedConn(t *testing.T) (client, upstream *net.TCPConn, done <-chan struct{}) {
	t.Helper()
	ul, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("unexpected error listening for upstream: %s", err)
	}
	defer ul.Close()
	pl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("unexpected error listening for proxy: %s", err)
	}
	defer pl.Close()

	c, err := net.Dial("tcp", pl.Addr().String())
	if err != nil {
		t.Fatalf("unexpected error dialing proxy: %s", err)
	}
	p, err := pl.Accept()
	if err != nil {
		t.Fatalf("unexpected error accepting client: %s", err)
	}
	ch := make(chan struct{})
	go func() {
		handleConnection(p, pl.Addr().String(), ul.Addr().String(), 1)
		close(ch)
	}()
	u, err := ul.Accept()
	if err != nil {
		t.Fatalf("unexpected error accepting proxied connection: %s", err)
	}
	t.Cleanup(func() {
		c.Close()
		u.Close()
	})
	return c.(*net.TCPConn), u.(*net.TCPConn), ch
}

func readAll(conn net.Conn) (string, error) {
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	b, err := io.ReadAll(conn)
	return string(b), err
}

func TestHandleConnectionClose(t *testing.T) {
	tests := []struct {
		name     string
		exchange func(t *testing.T, client, upstream *net.TCPConn)
	}{
		{
			name: "client close reaches upstream",
			exchange: func(t *testing.T, client, upstream *net.TCPConn) {
				client.Close()
				if _, err := readAll(upstream); err != nil {
					t.Errorf("expected upstream to read EOF after client close, but got: %s", err)
					return
				}
				upstream.Close()
			},
		},
		{
			name: "client half-close still gets the reply",
			exchange: func(t *testing.T, client, upstream *net.TCPConn) {
				client.Write([]byte("ping"))
				client.CloseWrite()
				if req, err := readAll(upstream); req != "ping" || err != nil {
					t.Errorf("expected upstream to read \"ping\" and EOF, but got: %q, %v", req, err)
					return
				}
				upstream.Write([]byte("pong"))
				upstream.Close()
				if res, err := readAll(client); res != "pong" || err != nil {
					t.Errorf("expected client to read \"pong\" and EOF, but got: %q, %v", res, err)
				}
			},
		},
		{
			name: "upstream close reaches client",
			exchange: func(t *testing.T, client, upstream *net.TCPConn) {
				upstream.Close()
				if _, err := readAll(client); err != nil {
					t.Errorf("expected client to read EOF after upstream close, but got: %s", err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			goroutines := runtime.NumGoroutine()
			client, upstream, done := proxiedConn(t)
			tt.exchange(t, client, upstream)

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatalf("expected handleConnection to return")
			}
			deadline := time.Now().Add(2 * time.Second)
			for runtime.NumGoroutine() > goroutines {
				if time.Now().After(deadline) {
					t.Fatalf("expected %d goroutines after the connection closed, but got %d", goroutines, runtime.NumGoroutine())
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}
