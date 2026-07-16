package dht

import (
	"errors"
	"testing"
)

func TestGetPeersLimitRejectsInvalidInfoHashLength(t *testing.T) {
	d := &DHT{
		Ready: true,
		Config: &Config{
			OnGetPeersResponse: func(string, *Peer) {},
		},
	}
	for _, infoHash := range []string{"short", "012345678901234567890"} {
		if err := d.GetPeersLimit(infoHash, 1); !errors.Is(err, ErrInvalidInfoHash) {
			t.Fatalf("GetPeersLimit(%q) error = %v, want ErrInvalidInfoHash", infoHash, err)
		}
	}
}
