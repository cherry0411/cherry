package dht

import (
	"net"
	"testing"
)

func TestRunReturnsListenErrorInsteadOfPanicking(t *testing.T) {
	listener, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	cfg := NewCrawlConfig()
	cfg.Address = listener.LocalAddr().String()
	d := New(cfg)
	if err := d.Run(); err == nil {
		t.Fatal("Run returned nil for an occupied listen address")
	}
}
