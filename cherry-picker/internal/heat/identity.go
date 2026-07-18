package heat

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const dayKeyDomain = "cherry/heat/day/v1\x00"

var reservedPrefixes = mustPrefixes(
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8",
	"169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24",
	"192.168.0.0/16", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24",
	"224.0.0.0/4", "240.0.0.0/4",
	"::/128", "::1/128", "100::/64", "2001:2::/48", "2001:db8::/32",
	"2001:10::/28", "fc00::/7", "fe80::/10", "ff00::/8",
)

type dayHMAC struct {
	day  uint32
	key  [sha256.Size]byte
	pool sync.Pool
}

type actorHMACState struct {
	mac       hash.Hash
	canonical [9]byte
	digest    [sha256.Size]byte
}

type actorIdentity struct {
	master   [sha256.Size]byte
	excluded []netip.Prefix
	current  atomic.Pointer[dayHMAC]
	mu       sync.Mutex
}

func newActorIdentity(masterSecret []byte, knownCrawlerRanges string, local []netip.Addr) (*actorIdentity, error) {
	if len(masterSecret) < 32 {
		return nil, errors.New("heat: master secret must contain at least 32 bytes")
	}
	i := &actorIdentity{master: sha256.Sum256(masterSecret)}
	i.excluded = append(i.excluded, reservedPrefixes...)
	known, err := parsePrefixes(knownCrawlerRanges)
	if err != nil {
		return nil, fmt.Errorf("heat: parse known crawler addresses: %w", err)
	}
	i.excluded = append(i.excluded, known...)
	for _, addr := range local {
		addr = addr.Unmap()
		if !addr.IsValid() {
			continue
		}
		bits := 128
		if addr.Is4() {
			bits = 32
		}
		i.excluded = append(i.excluded, netip.PrefixFrom(addr, bits))
	}
	return i, nil
}

func localInterfaceAddresses() ([]netip.Addr, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	result := make([]netip.Addr, 0, len(addrs))
	for _, raw := range addrs {
		text := raw.String()
		if idx := strings.LastIndexByte(text, '/'); idx >= 0 {
			text = text[:idx]
		}
		if addr, err := netip.ParseAddr(text); err == nil {
			result = append(result, addr.Unmap())
		}
	}
	return result, nil
}

func (i *actorIdentity) observation(infoHash, rawIP string, now time.Time) (Observation, bool) {
	if len(infoHash) != 20 {
		return Observation{}, false
	}
	addr, err := netip.ParseAddr(strings.TrimSpace(rawIP))
	if err != nil {
		return Observation{}, false
	}
	addr = addr.Unmap()
	if !i.isPublic(addr) {
		return Observation{}, false
	}
	day, ok := utcDay(now)
	if !ok {
		return Observation{}, false
	}
	h := i.hasher(day)
	state := h.pool.Get().(*actorHMACState)
	var actorBytes []byte
	if addr.Is4() {
		state.canonical[0] = 4
		v4 := addr.As4()
		copy(state.canonical[1:5], v4[:])
		actorBytes = state.canonical[:5]
	} else if addr.Is6() {
		state.canonical[0] = 6
		v6 := addr.As16()
		copy(state.canonical[1:9], v6[:8])
		actorBytes = state.canonical[:9]
	} else {
		h.pool.Put(state)
		return Observation{}, false
	}

	state.mac.Reset()
	_, _ = state.mac.Write(actorBytes)
	state.mac.Sum(state.digest[:0])
	actor := binary.BigEndian.Uint64(state.digest[:8])
	h.pool.Put(state)

	var obs Observation
	obs.Day = day
	copy(obs.InfoHash[:], infoHash)
	obs.Actor = actor
	return obs, true
}

func (i *actorIdentity) isPublic(addr netip.Addr) bool {
	if !addr.IsValid() || !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.IsLoopback() ||
		addr.IsLinkLocalUnicast() || addr.IsMulticast() || addr.IsUnspecified() {
		return false
	}
	for _, prefix := range i.excluded {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

func (i *actorIdentity) hasher(day uint32) *dayHMAC {
	if current := i.current.Load(); current != nil && current.day == day {
		return current
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if current := i.current.Load(); current != nil && current.day == day {
		return current
	}
	var raw [len(dayKeyDomain) + 4]byte
	copy(raw[:], dayKeyDomain)
	binary.BigEndian.PutUint32(raw[len(dayKeyDomain):], day)
	derive := hmac.New(sha256.New, i.master[:])
	_, _ = derive.Write(raw[:])
	next := &dayHMAC{day: day}
	derive.Sum(next.key[:0])
	key := next.key
	next.pool.New = func() any {
		return &actorHMACState{mac: hmac.New(sha256.New, key[:])}
	}
	i.current.Store(next)
	return next
}

func utcDay(now time.Time) (uint32, bool) {
	seconds := now.UTC().Unix()
	if seconds < 0 {
		return 0, false
	}
	day := uint64(seconds / 86_400)
	if day > uint64(^uint32(0)) {
		return 0, false
	}
	return uint32(day), true
}

func parsePrefixes(value string) ([]netip.Prefix, error) {
	var result []netip.Prefix
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if prefix, err := netip.ParsePrefix(part); err == nil {
			result = append(result, prefix.Masked())
			continue
		}
		addr, err := netip.ParseAddr(part)
		if err != nil {
			return nil, fmt.Errorf("%q is neither an IP address nor CIDR", part)
		}
		addr = addr.Unmap()
		bits := 128
		if addr.Is4() {
			bits = 32
		}
		result = append(result, netip.PrefixFrom(addr, bits))
	}
	return result, nil
}

func mustPrefixes(values ...string) []netip.Prefix {
	result := make([]netip.Prefix, len(values))
	for idx, value := range values {
		result[idx] = netip.MustParsePrefix(value)
	}
	return result
}
