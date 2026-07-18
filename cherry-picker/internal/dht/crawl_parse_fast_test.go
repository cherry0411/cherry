package dht

import "testing"

func TestParseCrawlPacketSampleInfohashesResponse(t *testing.T) {
	id := "0123456789abcdefghij"
	hashes := "abcdefghijklmnopqrst" + "uvwxyz0123456789ABCD"
	data := []byte("d1:rd2:id20:" + id +
		"8:intervali300e5:nodes0:3:numi2e7:samples40:" + hashes +
		"e1:t4:txid1:y1:re")

	pkt, ok := parseCrawlPacket(data)
	if !ok {
		t.Fatal("sample_infohashes response did not parse")
	}
	if pkt.y != "r" || pkt.id != id || !pkt.hasSamples {
		t.Fatalf("unexpected response fields: %#v", pkt)
	}
	if pkt.samples != hashes || pkt.interval != 300 || pkt.num != 2 {
		t.Fatalf("sample fields = %q interval=%d num=%d", pkt.samples, pkt.interval, pkt.num)
	}
}

func TestParseCrawlPacketEmptySampleSet(t *testing.T) {
	data := []byte("d1:rd2:id20:0123456789abcdefghij8:intervali60e3:numi0e7:samples0:e1:t4:txid1:y1:re")
	pkt, ok := parseCrawlPacket(data)
	if !ok || !pkt.hasSamples || pkt.samples != "" || pkt.num != 0 {
		t.Fatalf("empty sample response = %#v, ok=%v", pkt, ok)
	}
}
