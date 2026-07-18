package dht

import (
	"net"
	"testing"
)

func TestCompareDistanceToTarget(t *testing.T) {
	target := newBitmapFromString(string([]byte{0x00, 0x00}))
	closer := newBitmapFromString(string([]byte{0x00, 0x01}))
	farther := newBitmapFromString(string([]byte{0x00, 0x10}))

	if got := compareDistanceToTarget(closer, farther, target); got >= 0 {
		t.Fatalf("compareDistanceToTarget(closer, farther) = %d, want < 0", got)
	}
	if got := compareDistanceToTarget(farther, closer, target); got <= 0 {
		t.Fatalf("compareDistanceToTarget(farther, closer) = %d, want > 0", got)
	}
}

func TestNodeCompactInfoCached(t *testing.T) {
	no, err := newNode(string(make([]byte, 20)), "udp4", "127.0.0.1:6881")
	if err != nil {
		t.Fatalf("newNode() error = %v", err)
	}
	if got := len(no.CompactNodeInfo()); got != 26 {
		t.Fatalf("len(CompactNodeInfo()) = %d, want 26", got)
	}
}

func TestNewNodeFromCompactInfoAvoidsAddressResolution(t *testing.T) {
	id := string([]byte("01234567890123456789"))
	compact := id + compactIPPortInfo(net.IPv4(203, 0, 113, 7), 51413)

	no, err := newNodeFromCompactInfo(compact, "udp4")
	if err != nil {
		t.Fatalf("newNodeFromCompactInfo() error = %v", err)
	}
	if got := no.id.RawString(); got != id {
		t.Fatalf("node id = %q, want %q", got, id)
	}
	if got := no.addr.String(); got != "203.0.113.7:51413" {
		t.Fatalf("node address = %q, want 203.0.113.7:51413", got)
	}
	if no.compactInfo != compact {
		t.Fatal("compact node info was not preserved")
	}
}

func TestGetTopKNodesReturnsTrueClosestNodes(t *testing.T) {
	target := newBitmapFromString(string(make([]byte, 20)))
	distances := []byte{9, 1, 7, 3, 8, 2, 6, 4, 5}
	nodes := make([]*node, 0, len(distances))
	for _, distance := range distances {
		id := make([]byte, 20)
		id[len(id)-1] = distance
		nodes = append(nodes, &node{id: newBitmapFromString(string(id))})
	}

	got := getTopKNodes(nodes, target, 3)
	if len(got) != 3 {
		t.Fatalf("len(getTopKNodes()) = %d, want 3", len(got))
	}
	want := []byte{1, 2, 3}
	for i, no := range got {
		if distance := no.id.data[len(no.id.data)-1]; distance != want[i] {
			t.Fatalf("result[%d] distance = %d, want %d", i, distance, want[i])
		}
	}
}

func BenchmarkGetTopKNodes1000(b *testing.B) {
	target := newBitmapFromString(string(make([]byte, 20)))
	nodes := make([]*node, 1000)
	for i := range nodes {
		id := make([]byte, 20)
		id[len(id)-2] = byte(i >> 8)
		id[len(id)-1] = byte(i)
		nodes[i] = &node{id: newBitmapFromString(string(id))}
	}

	b.ReportAllocs()
	for b.Loop() {
		_ = getTopKNodes(nodes, target, 32)
	}
}
