package heat

import (
	"net/netip"
	"testing"
	"time"
)

var testMasterSecret = []byte("0123456789abcdef0123456789abcdef")

func TestActorIdentityPrivacyAndDaySemantics(t *testing.T) {
	identity, err := newActorIdentity(testMasterSecret, "8.8.4.4,2001:4860:4860::/48", []netip.Addr{netip.MustParseAddr("1.1.1.1")})
	if err != nil {
		t.Fatal(err)
	}
	hash := string(make([]byte, 20))
	day1 := time.Date(2026, 7, 18, 23, 59, 0, 0, time.UTC)
	day2 := day1.Add(2 * time.Minute)

	v4a, ok := identity.observation(hash, "8.8.8.8", day1)
	if !ok {
		t.Fatal("public IPv4 rejected")
	}
	v4b, ok := identity.observation(hash, "::ffff:8.8.8.8", day1)
	if !ok || v4a.Actor != v4b.Actor {
		t.Fatal("IPv4-mapped form did not canonicalize to IPv4")
	}
	v4NextDay, _ := identity.observation(hash, "8.8.8.8", day2)
	if v4NextDay.Actor == v4a.Actor || v4NextDay.Day == v4a.Day {
		t.Fatal("UTC-day key did not rotate")
	}

	v6a, ok := identity.observation(hash, "2606:4700:4700:1234::1", day1)
	if !ok {
		t.Fatal("public IPv6 rejected")
	}
	v6b, ok := identity.observation(hash, "2606:4700:4700:1234:ffff::2", day1)
	if !ok || v6a.Actor != v6b.Actor {
		t.Fatal("IPv6 addresses in one /64 must share the actor")
	}
	v6c, _ := identity.observation(hash, "2606:4700:4700:1235::1", day1)
	if v6c.Actor == v6a.Actor {
		t.Fatal("different IPv6 /64 unexpectedly shared actor")
	}
}

func TestActorIdentityRejectsNonPublicLocalAndCrawlerAddresses(t *testing.T) {
	identity, err := newActorIdentity(testMasterSecret, "8.8.4.0/24,2606:4700:4700::/48", []netip.Addr{netip.MustParseAddr("1.1.1.1")})
	if err != nil {
		t.Fatal(err)
	}
	hash := string(make([]byte, 20))
	now := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	rejected := []string{
		"", "127.0.0.1", "10.0.0.1", "100.64.0.1", "169.254.1.1", "192.0.2.1",
		"198.18.0.1", "224.0.0.1", "1.1.1.1", "8.8.4.4", "::1", "fc00::1",
		"fe80::1", "2001:db8::1", "2606:4700:4700::1111",
	}
	for _, address := range rejected {
		if _, ok := identity.observation(hash, address, now); ok {
			t.Errorf("address %q was not excluded", address)
		}
	}
	if _, ok := identity.observation("short", "8.8.8.8", now); ok {
		t.Fatal("invalid infohash accepted")
	}
}

func TestParsePrefixesFailsClosed(t *testing.T) {
	if _, err := parsePrefixes("8.8.8.8,not-an-address"); err == nil {
		t.Fatal("invalid known-crawler entry accepted")
	}
}

func BenchmarkActorIdentityObservation(b *testing.B) {
	identity, err := newActorIdentity(testMasterSecret, "", []netip.Addr{})
	if err != nil {
		b.Fatal(err)
	}
	hash := string(make([]byte, 20))
	now := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := identity.observation(hash, "8.8.8.8", now); !ok {
			b.Fatal("unexpected rejection")
		}
	}
}
