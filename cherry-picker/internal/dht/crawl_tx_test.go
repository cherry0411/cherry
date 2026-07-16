package dht

import (
	"fmt"
	"sync"
	"testing"
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

func TestCrawlTransactionRejectsOverwrittenGeneration(t *testing.T) {
	d := &DHT{}
	oldTxID := d.crawlGenTxID()
	if !d.rememberCrawlInfoHash(oldTxID, "0123456789abcdefghij") {
		t.Fatal("failed to remember old transaction")
	}
	oldCounter, _ := crawlTxCounter(oldTxID)
	d.crawlTxCtr.Store(oldCounter + (1 << 16) - 1)
	newTxID := d.crawlGenTxID() // same low 16-bit ring index
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
