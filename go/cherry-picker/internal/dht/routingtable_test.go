package dht

import "testing"

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
