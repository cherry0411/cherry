package dht

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

func TestWireCountsPreDialQueueDrop(t *testing.T) {
	wire := NewWire(64, 1, 1)
	if !wire.RequestFromSource(make([]byte, 20), "127.0.0.1", 1, PeerSourceUnknown) {
		t.Fatal("first request was not admitted")
	}
	if wire.RequestFromSource(make([]byte, 20), "127.0.0.1", 2, PeerSourceUnknown) {
		t.Fatal("request was admitted into a full queue")
	}
	if got := wire.Stats.QueueDropped.Load(); got != 1 {
		t.Fatalf("QueueDropped = %d, want 1", got)
	}
	if got := wire.RequestDepth(); got != 1 {
		t.Fatalf("RequestDepth = %d, want 1", got)
	}
}

func TestWireConcurrentAdmissionResultMatchesQueueState(t *testing.T) {
	const (
		capacity = 64
		attempts = 512
	)
	wire := NewWire(64, capacity, 1)

	var admitted atomic.Int64
	var workers sync.WaitGroup
	workers.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func(port int) {
			defer workers.Done()
			if wire.RequestFromSource(make([]byte, 20), "127.0.0.1", port, PeerSourceGetPeers) {
				admitted.Add(1)
			}
		}(i + 1)
	}
	workers.Wait()

	if got := admitted.Load(); got != capacity {
		t.Fatalf("admitted = %d, want %d", got, capacity)
	}
	if got := wire.RequestDepth(); got != capacity {
		t.Fatalf("RequestDepth = %d, want %d", got, capacity)
	}
	if got := wire.Stats.QueueDropped.Load(); got != attempts-capacity {
		t.Fatalf("QueueDropped = %d, want %d", got, attempts-capacity)
	}
}

// TestWireFunnelAttributesDialBySource confirms the per-source funnel counter
// attributes a dial attempt to the request's PeerSource. This is the metric
// used to test whether announce_peer peers out-connect get_peers "values".
func TestWireFunnelAttributesDialBySource(t *testing.T) {
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
		Source:   PeerSourceAnnounce,
	})

	funnel := wire.FunnelBySource()
	if got := funnel[PeerSourceAnnounce].DialAttempts; got != 1 {
		t.Fatalf("announce DialAttempts = %d, want 1", got)
	}
	if got := funnel[PeerSourceGetPeers].DialAttempts; got != 0 {
		t.Fatalf("get_peers DialAttempts = %d, want 0", got)
	}
	// Global counter must still agree with the sum of per-source counters.
	if got := wire.Stats.DialAttempts.Load(); got != 1 {
		t.Fatalf("global DialAttempts = %d, want 1", got)
	}
}

// TestWireRequestFromSourceEnqueuesSource confirms the source tag survives the
// request channel, and that an out-of-range source is clamped (no panic / no
// out-of-bounds array write) in the dial accounting.
func TestWireRequestFromSourceEnqueuesSource(t *testing.T) {
	wire := NewWire(64, 4, 1)
	if !wire.RequestFromSource(make([]byte, 20), "127.0.0.1", 1, PeerSourceGetPeers) {
		t.Fatal("request was not admitted")
	}
	select {
	case r := <-wire.requests:
		if r.Source != PeerSourceGetPeers {
			t.Fatalf("enqueued Source = %d, want %d", r.Source, PeerSourceGetPeers)
		}
	default:
		t.Fatal("request was not enqueued")
	}

	// Out-of-range source must be clamped to Unknown by fetchMetadata, not panic.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().(*net.TCPAddr)
	_ = listener.Close()
	wire.handleRequest(Request{
		InfoHash: make([]byte, 20),
		IP:       addr.IP.String(),
		Port:     addr.Port,
		Source:   PeerSource(250), // out of range
	})
	if got := wire.FunnelBySource()[PeerSourceUnknown].DialAttempts; got != 1 {
		t.Fatalf("clamped-source DialAttempts on Unknown = %d, want 1", got)
	}
}

// TestWireFunnelQueuedAndBlacklistedBySource confirms the pre-dial funnel
// stages (queued, blacklisted) are attributed per source, so a starving
// funnel can be localized to supply loss rather than conversion.
func TestWireFunnelQueuedAndBlacklistedBySource(t *testing.T) {
	wire := NewWire(64, 4, 1)
	ip, port := "203.0.113.7", 6881
	wire.blackList.insert(ip, port)

	wire.handleRequest(Request{
		InfoHash: make([]byte, 20),
		IP:       ip,
		Port:     port,
		Source:   PeerSourceGetPeers,
	})

	funnel := wire.FunnelBySource()
	if got := funnel[PeerSourceGetPeers].Queued; got != 1 {
		t.Fatalf("get_peers Queued = %d, want 1", got)
	}
	if got := funnel[PeerSourceGetPeers].Blacklisted; got != 1 {
		t.Fatalf("get_peers Blacklisted = %d, want 1", got)
	}
	if got := funnel[PeerSourceGetPeers].DialAttempts; got != 0 {
		t.Fatalf("get_peers DialAttempts = %d, want 0 (blacklisted before dial)", got)
	}
}

// TestWireInflightDedupBySource confirms a second request for the same
// (infohash, peer) already in flight is counted as inflight-dedup, not as lost
// or blacklisted supply.
func TestWireInflightDedupBySource(t *testing.T) {
	wire := NewWire(64, 4, 4)
	infoHash := make([]byte, 20)
	key := genAddress("198.51.100.9", 6881)
	// Simulate an in-flight download by pre-occupying the dedup key.
	wire.queue.Set(string(infoHash) + ":" + key)

	wire.handleRequest(Request{
		InfoHash: infoHash,
		IP:       "198.51.100.9",
		Port:     6881,
		Source:   PeerSourceAnnounce,
	})

	funnel := wire.FunnelBySource()
	if got := funnel[PeerSourceAnnounce].InflightDeduped; got != 1 {
		t.Fatalf("announce InflightDeduped = %d, want 1", got)
	}
	if got := funnel[PeerSourceAnnounce].DialAttempts; got != 0 {
		t.Fatalf("announce DialAttempts = %d, want 0 (deduped before dial)", got)
	}
}

// TestBlacklistStatsExposeFullRejectAndSize confirms the blacklist reports its
// size, capacity, and silent full-rejects — the previously invisible black hole
// at Len>=maxSize.
func TestBlacklistStatsExposeFullRejectAndSize(t *testing.T) {
	wire := NewWire(2, 4, 1) // maxSize = 2
	wire.blackList.insert("1.1.1.1", 1)
	wire.blackList.insert("2.2.2.2", 2)
	wire.blackList.insert("3.3.3.3", 3) // rejected: full

	stats := wire.BlacklistStats()
	if stats.MaxSize != 2 {
		t.Fatalf("MaxSize = %d, want 2", stats.MaxSize)
	}
	if stats.Size != 2 {
		t.Fatalf("Size = %d, want 2", stats.Size)
	}
	if stats.InsertAccepted != 2 {
		t.Fatalf("InsertAccepted = %d, want 2", stats.InsertAccepted)
	}
	if stats.InsertRejected != 1 {
		t.Fatalf("InsertRejected = %d, want 1 (full-reject must be visible)", stats.InsertRejected)
	}
}

func TestDHTBlacklistStatsExposePerIdentityState(t *testing.T) {
	instance := &DHT{blackList: newBlackList(4)}
	instance.blackList.insert("1.1.1.1", 1)
	stats := instance.BlacklistStats()
	if stats.Size != 1 || stats.MaxSize != 4 || stats.InsertAccepted != 1 {
		t.Fatalf("unexpected DHT blacklist stats: %+v", stats)
	}
}

func TestBlacklistStatsCountLazyExpiryOnce(t *testing.T) {
	bl := newBlackList(4)
	bl.list.Set(bl.genKey("1.1.1.1", 1), &blockedItem{
		ip:         "1.1.1.1",
		port:       1,
		createTime: time.Now().Add(-bl.expiredAfter - time.Second),
	})

	if bl.in("1.1.1.1", 1) {
		t.Fatal("expired endpoint remained blacklisted")
	}
	stats := bl.stats()
	if stats.Size != 0 || stats.ExpiredEvicted != 1 {
		t.Fatalf("lazy-expiry stats = %+v, want size=0 expired=1", stats)
	}
	if bl.in("1.1.1.1", 1) {
		t.Fatal("deleted endpoint became blacklisted")
	}
	if got := bl.stats().ExpiredEvicted; got != 1 {
		t.Fatalf("ExpiredEvicted after second miss = %d, want 1", got)
	}
}
