package dht

import (
	"net"
	"testing"
)

func TestWireCountsDialFailureAndBlacklistsEndpoint(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().(*net.TCPAddr)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	wire := NewWire(64, 1, 1)
	wire.handleRequest(Request{
		InfoHash: make([]byte, 20),
		IP:       addr.IP.String(),
		Port:     addr.Port,
	})

	if got := wire.Stats.DialAttempts.Load(); got != 1 {
		t.Fatalf("DialAttempts = %d, want 1", got)
	}
	if got := wire.Stats.DialFailed.Load(); got != 1 {
		t.Fatalf("DialFailed = %d, want 1", got)
	}
	if !wire.blackList.in(addr.IP.String(), addr.Port) {
		t.Fatal("failed endpoint was not blacklisted")
	}
}

func TestWireBlacklistsHandshakeFailureAcrossInfohashes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	addr := listener.Addr().(*net.TCPAddr)

	accepted := make(chan struct{})
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			close(accepted)
			_ = conn.Close()
		}
	}()

	wire := NewWire(64, 2, 1)
	wire.handleRequest(Request{
		InfoHash: make([]byte, 20),
		IP:       addr.IP.String(),
		Port:     addr.Port,
	})
	<-accepted

	if got := wire.Stats.DialOK.Load(); got != 1 {
		t.Fatalf("DialOK = %d, want 1", got)
	}
	if got := wire.Stats.HandshakeFailed.Load(); got != 1 {
		t.Fatalf("HandshakeFailed = %d, want 1", got)
	}

	secondHash := make([]byte, 20)
	secondHash[0] = 1
	wire.handleRequest(Request{
		InfoHash: secondHash,
		IP:       addr.IP.String(),
		Port:     addr.Port,
	})

	if got := wire.Stats.DialAttempts.Load(); got != 1 {
		t.Fatalf("DialAttempts after blacklisted request = %d, want 1", got)
	}
	if got := wire.Stats.Blacklisted.Load(); got != 1 {
		t.Fatalf("Blacklisted = %d, want 1", got)
	}
}
