package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Peer struct {
	InfoHash string `json:"info_hash"`
	IP       string `json:"ip"`
	Port     int    `json:"port"`
}

type Broker struct {
	mu     sync.Mutex
	queue  []Peer
	seen   map[string]time.Time
	ttl    time.Duration
	pushed uint64
	pulled uint64
	active int
}

func NewBroker() *Broker {
	b := &Broker{
		queue: make([]Peer, 0, 16384),
		seen:  make(map[string]time.Time, 100000),
		ttl:   5 * time.Minute,
	}
	go b.cleanup()
	return b
}

func (b *Broker) cleanup() {
	for range time.Tick(30 * time.Second) {
		b.mu.Lock()
		now := time.Now()
		for k, t := range b.seen {
			if now.Sub(t) > b.ttl {
				delete(b.seen, k)
			}
		}
		b.mu.Unlock()
	}
}

func (b *Broker) Push(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	var peers []Peer
	if err := json.NewDecoder(r.Body).Decode(&peers); err != nil {
		http.Error(w, "bad json", 400)
		return
	}
	b.mu.Lock()
	added := 0
	for _, p := range peers {
		if p.InfoHash == "" || p.IP == "" || p.Port <= 0 {
			continue
		}
		key := p.InfoHash + ":" + p.IP
		if _, ok := b.seen[key]; ok {
			continue
		}
		b.seen[key] = time.Now()
		b.queue = append(b.queue, p)
		added++
	}
	if len(b.queue) > 65536 {
		b.queue = b.queue[len(b.queue)-32768:]
	}
	b.pushed += uint64(added)
	b.mu.Unlock()
	w.WriteHeader(200)
}

func (b *Broker) Pull(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", 405)
		return
	}
	b.mu.Lock()
	n := min(64, len(b.queue))
	if n == 0 {
		b.mu.Unlock()
		w.Write([]byte("[]"))
		return
	}
	batch := make([]Peer, n)
	copy(batch, b.queue[:n])
	b.queue = b.queue[n:]
	b.pulled += uint64(n)
	b.active = len(b.queue)
	b.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(batch)
}

func (b *Broker) Stats(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	s := map[string]interface{}{
		"queue":  len(b.queue),
		"pushed": b.pushed,
		"pulled": b.pulled,
		"seen":   len(b.seen),
	}
	b.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s)
}

func main() {
	addr := ":9800"
	if a := os.Getenv("BROKER_ADDR"); a != "" {
		addr = a
	}
	broker := NewBroker()
	http.HandleFunc("/push", broker.Push)
	http.HandleFunc("/pull", broker.Pull)
	http.HandleFunc("/stats", broker.Stats)
	log.Printf("Broker listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func init() {
	// unused import guard
	_ = strings.TrimSpace
}
