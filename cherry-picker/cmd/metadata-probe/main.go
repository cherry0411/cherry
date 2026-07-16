// metadata-probe verifies the peer-wire metadata path against known peers.
// It is intentionally small so operators can separate DHT reachability from
// TCP/ut_metadata failures when validating a deployment.
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cherry-picker/internal/dht"
)

func main() {
	hashFlag := flag.String("hash", "", "40-character torrent info hash")
	peersFlag := flag.String("peers", "", "optional comma-separated IPv4 peer addresses (host:port)")
	listenFlag := flag.String("listen", ":20003", "DHT UDP listen address")
	primeFlag := flag.String("prime-nodes", "", "optional comma-separated DHT bootstrap addresses")
	timeoutFlag := flag.Duration("timeout", 30*time.Second, "overall probe timeout")
	flag.Parse()

	infoHash, err := hex.DecodeString(strings.TrimSpace(*hashFlag))
	if err != nil || len(infoHash) != 20 {
		fmt.Fprintln(os.Stderr, "-hash must be a 40-character hexadecimal info hash")
		os.Exit(2)
	}

	wire := dht.NewWire(4096, 1024, 64)
	go wire.Run()
	var queued atomic.Int64
	for _, address := range strings.Split(*peersFlag, ",") {
		host, portText, err := net.SplitHostPort(strings.TrimSpace(address))
		if err != nil {
			continue
		}
		port, err := net.LookupPort("tcp", portText)
		if err != nil {
			continue
		}
		wire.Request(infoHash, host, port)
		queued.Add(1)
	}

	var crawler *dht.DHT
	var sampleMu sync.Mutex
	peerSamples := make([]string, 0, 10)
	if queued.Load() == 0 {
		cfg := dht.NewCrawlConfig()
		cfg.Address = *listenFlag
		cfg.MaxNodes = 5000
		cfg.RefreshNodeNum = 256
		cfg.PacketWorkerLimit = 8
		cfg.PacketReadWorkers = 2
		if nodes := splitCSV(*primeFlag); len(nodes) > 0 {
			cfg.PrimeNodes = nodes
		}
		cfg.OnGetPeers = func(string, string, int) {}
		cfg.OnGetPeersResponse = func(_ string, peer *dht.Peer) {
			sampleMu.Lock()
			if len(peerSamples) < cap(peerSamples) {
				peerSamples = append(peerSamples, net.JoinHostPort(peer.IP.String(), strconv.Itoa(peer.Port)))
			}
			sampleMu.Unlock()
			wire.Request(infoHash, peer.IP.String(), peer.Port)
			queued.Add(1)
		}
		cfg.OnAnnouncePeer = func(string, string, int) {}
		crawler = dht.New(cfg)
		go func() {
			if err := crawler.Run(); err != nil {
				fmt.Fprintf(os.Stderr, "DHT failed: %v\n", err)
			}
		}()
	}

	timer := time.NewTimer(*timeoutFlag)
	defer timer.Stop()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case response := <-wire.Response():
			fmt.Printf("metadata_ok hash=%s peer=%s:%d bytes=%d queued=%d dial=%d connected=%d dial_failed=%d handshake=%d handshake_failed=%d downloaded=%d download_failed=%d\n",
				*hashFlag, response.IP, response.Port, len(response.MetadataInfo), queued.Load(),
				wire.Stats.DialAttempts.Load(), wire.Stats.DialOK.Load(),
				wire.Stats.DialFailed.Load(), wire.Stats.HandshakeOK.Load(), wire.Stats.HandshakeFailed.Load(),
				wire.Stats.DownloadOK.Load(), wire.Stats.DownloadFailed.Load())
			return
		case <-ticker.C:
			if crawler != nil {
				_ = crawler.GetPeers(*hashFlag)
			}
		case <-timer.C:
			packets := dht.PacketStats{}
			if crawler != nil {
				packets = crawler.PacketStats()
			}
			sampleMu.Lock()
			samples := strings.Join(peerSamples, ",")
			sampleMu.Unlock()
			fmt.Printf("metadata_timeout hash=%s queued=%d dht_ready=%v dht_recv=%d dht_handled=%d dht_sent_bytes=%d dial=%d connected=%d dial_failed=%d handshake=%d handshake_failed=%d downloaded=%d download_failed=%d blacklisted=%d sample_peers=%s\n",
				*hashFlag, queued.Load(), crawler != nil && crawler.Ready, packets.Received, packets.Handled, packets.BytesSent,
				wire.Stats.DialAttempts.Load(), wire.Stats.DialOK.Load(),
				wire.Stats.DialFailed.Load(), wire.Stats.HandshakeOK.Load(), wire.Stats.HandshakeFailed.Load(),
				wire.Stats.DownloadOK.Load(), wire.Stats.DownloadFailed.Load(), wire.Stats.Blacklisted.Load(), samples)
			os.Exit(1)
		}
	}
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			if _, port, err := net.SplitHostPort(part); err == nil {
				if _, err = strconv.Atoi(port); err == nil {
					result = append(result, part)
				}
			}
		}
	}
	return result
}
