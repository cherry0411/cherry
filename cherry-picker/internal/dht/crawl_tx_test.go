package dht

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func TestCrawlTransactionInfoHashRoundTrip(t *testing.T) {
	d := &DHT{}
	txID := d.crawlGenTxID()
	hash := "0123456789abcdefghij"
	if !d.rememberCrawlInfoHash(txID, hash) {
		t.Fatal("rememberCrawlInfoHash rejected a valid transaction")
	}
	got, ok := d.crawlInfoHash(txID)
	if !ok || string(got[:]) != hash {
		t.Fatalf("crawlInfoHash = %q, %v; want %q, true", string(got[:]), ok, hash)
	}
}

func TestFollowCrawlGetPeersResponseSendsOneBoundedQuery(t *testing.T) {
	receiver, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	remote, err := newNode("abcdefghijklmnopqrst", "udp4", receiver.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	d := &DHT{
		Config:    &Config{Network: "udp4"},
		conn:      sender,
		blackList: newBlackList(16),
	}
	d.node, err = newNode("zyxwvutsrqponmlkjihg", "udp4", sender.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	infoHash := "0123456789abcdefghij"
	txID := d.crawlGenTxID()
	if !d.rememberCrawlInfoHashWithFollowups(txID, infoHash, 1) {
		t.Fatal("failed to remember iterative transaction")
	}
	if !followCrawlGetPeersResponse(d, txID, remote.compactInfo) {
		t.Fatal("expected one follow-up query")
	}
	if got := d.stats.followupsSent.Load(); got != 1 {
		t.Fatalf("followups sent = %d, want 1", got)
	}

	if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 512)
	n, _, err := receiver.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}
	pkt, ok := parseCrawlPacket(buf[:n])
	if !ok || pkt.q != getPeersType || pkt.infoHash != infoHash {
		t.Fatalf("follow-up packet = %#v, ok=%v", pkt, ok)
	}
	entry, ok := d.crawlTransaction(pkt.t)
	if !ok || entry.followups != 0 {
		t.Fatalf("follow-up generation = %#v, ok=%v", entry, ok)
	}
	if string(entry.nodeID[:]) != remote.id.RawString() {
		t.Fatalf("follow-up node ID = %x, want %x", entry.nodeID, remote.id.data)
	}
}

func TestCrawlNodeIsCloser(t *testing.T) {
	var target, current [20]byte
	current[19] = 0x10

	closer := make([]byte, 20)
	closer[19] = 0x08
	farther := make([]byte, 20)
	farther[19] = 0x20
	equal := make([]byte, 20)
	equal[19] = 0x10

	if !crawlNodeIsCloser(string(closer), current, target) {
		t.Fatal("closer node was rejected")
	}
	if crawlNodeIsCloser(string(farther), current, target) {
		t.Fatal("farther node was accepted")
	}
	if crawlNodeIsCloser(string(equal), current, target) {
		t.Fatal("equal-distance node was accepted")
	}
	if crawlNodeIsCloser("short", current, target) {
		t.Fatal("malformed node ID was accepted")
	}
}

func TestCrawlTransactionRejectsOverwrittenGeneration(t *testing.T) {
	d := &DHT{}
	oldTxID := d.crawlGenTxID()
	if !d.rememberCrawlInfoHash(oldTxID, "0123456789abcdefghij") {
		t.Fatal("failed to remember old transaction")
	}
	oldCounter, _ := crawlTxCounter(oldTxID)
	d.crawlTxCtr.Store(oldCounter + crawlTxRingSize - 1)
	newTxID := d.crawlGenTxID() // same low ring index, different generation
	if !d.rememberCrawlInfoHash(newTxID, "abcdefghij0123456789") {
		t.Fatal("failed to remember new transaction")
	}
	if _, ok := d.crawlInfoHash(oldTxID); ok {
		t.Fatal("overwritten transaction generation must not resolve")
	}
	got, ok := d.crawlInfoHash(newTxID)
	if !ok || string(got[:]) != "abcdefghij0123456789" {
		t.Fatalf("new transaction = %q, %v", string(got[:]), ok)
	}
}

func TestCrawlTransactionConcurrentAccess(t *testing.T) {
	d := &DHT{}
	const workers = 256
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			txID := d.crawlGenTxID()
			hash := fmt.Sprintf("%020d", i)
			if !d.rememberCrawlInfoHash(txID, hash) {
				t.Errorf("worker %d: remember failed", i)
				return
			}
			got, ok := d.crawlInfoHash(txID)
			if !ok || string(got[:]) != hash {
				t.Errorf("worker %d: got %q, %v", i, string(got[:]), ok)
			}
		}(i)
	}
	wg.Wait()
}
