package heat

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

const (
	wireVersion          = byte(1)
	wireHeaderSize       = 9
	MaxWireBytes         = 64 << 20
	MaxWireGroups        = 1_000_000
	MaxWireActorsPerHash = 10_000_000
)

var wireMagic = [4]byte{'C', 'H', 'H', 'T'}

// Observation is the only heat identity retained after the inbound callback.
// It cannot represent an IP address, port, node ID, region or metadata body.
type Observation struct {
	Day      uint32
	InfoHash [20]byte
	Actor    uint64
}

type WireGroup struct {
	InfoHash [20]byte
	Actors   []uint64
}

type WireBatch struct {
	Day    uint32
	Groups []WireGroup
}

// BuildWireBatch sorts by raw authority infohash and actor, removing exact
// duplicates within this delivery batch. Cross-batch idempotence belongs to
// the receiver's daily exact set.
func BuildWireBatch(day uint32, observations []Observation) (WireBatch, error) {
	if day == 0 || len(observations) == 0 {
		return WireBatch{}, errors.New("heat: wire batch requires a day and observations")
	}
	rows := append([]Observation(nil), observations...)
	for _, row := range rows {
		if row.Day != day {
			return WireBatch{}, errors.New("heat: wire batch mixes UTC days")
		}
	}
	slices.SortFunc(rows, compareObservation)
	groups := make([]WireGroup, 0, min(len(rows), MaxWireGroups))
	// One backing store avoids one allocation per hash group. Its full capacity
	// is fixed before any group slice is published, so later appends cannot move
	// the storage underneath earlier slices.
	actors := make([]uint64, 0, len(rows))
	for _, row := range rows {
		if len(groups) == 0 || groups[len(groups)-1].InfoHash != row.InfoHash {
			if len(groups) >= MaxWireGroups {
				return WireBatch{}, errors.New("heat: wire group limit exceeded")
			}
			actors = append(actors, row.Actor)
			groups = append(groups, WireGroup{InfoHash: row.InfoHash, Actors: actors[len(actors)-1:]})
			continue
		}
		group := &groups[len(groups)-1]
		if group.Actors[len(group.Actors)-1] == row.Actor {
			continue
		}
		if len(group.Actors) >= MaxWireActorsPerHash {
			return WireBatch{}, errors.New("heat: actor count limit exceeded")
		}
		start := len(actors) - len(group.Actors)
		actors = append(actors, row.Actor)
		group.Actors = actors[start:]
	}
	return WireBatch{Day: day, Groups: groups}, nil
}

func compareObservation(a, b Observation) int {
	if cmp := bytes.Compare(a.InfoHash[:], b.InfoHash[:]); cmp != 0 {
		return cmp
	}
	if a.Actor < b.Actor {
		return -1
	}
	if a.Actor > b.Actor {
		return 1
	}
	return 0
}

func EncodeWire(batch WireBatch) ([]byte, error) {
	if err := validateWireBatch(batch); err != nil {
		return nil, err
	}
	capacity := int64(wireHeaderSize + binary.MaxVarintLen64)
	capacity += int64(len(batch.Groups)) * 21
	for _, group := range batch.Groups {
		capacity += int64(len(group.Actors)) * 8
	}
	if capacity > MaxWireBytes {
		return nil, errors.New("heat: encoded wire batch exceeds 64 MiB")
	}
	out := make([]byte, 0, int(capacity))
	out = append(out, wireMagic[:]...)
	out = append(out, wireVersion)
	var day [4]byte
	binary.BigEndian.PutUint32(day[:], batch.Day)
	out = append(out, day[:]...)
	out = binary.AppendUvarint(out, uint64(len(batch.Groups)))
	for _, group := range batch.Groups {
		out = append(out, group.InfoHash[:]...)
		out = binary.AppendUvarint(out, uint64(len(group.Actors)))
		for _, actor := range group.Actors {
			out = binary.BigEndian.AppendUint64(out, actor)
		}
	}
	if len(out) > MaxWireBytes {
		return nil, errors.New("heat: encoded wire batch exceeds 64 MiB")
	}
	return out, nil
}

func DecodeWire(data []byte) (WireBatch, error) {
	if len(data) > MaxWireBytes {
		return WireBatch{}, errors.New("heat: wire body exceeds 64 MiB")
	}
	if len(data) < wireHeaderSize || !bytes.Equal(data[:4], wireMagic[:]) {
		return WireBatch{}, errors.New("heat: invalid CHHT magic/header")
	}
	if data[4] != wireVersion {
		return WireBatch{}, fmt.Errorf("heat: unsupported wire version %d", data[4])
	}
	batch := WireBatch{Day: binary.BigEndian.Uint32(data[5:9])}
	if batch.Day == 0 {
		return WireBatch{}, errors.New("heat: invalid UTC day")
	}
	offset := wireHeaderSize
	groupCount, n, err := readCanonicalUvarint(data[offset:])
	if err != nil || groupCount == 0 || groupCount > MaxWireGroups {
		return WireBatch{}, errors.New("heat: invalid group count")
	}
	offset += n
	batch.Groups = make([]WireGroup, 0, int(groupCount))
	var previousHash [20]byte
	for groupIdx := uint64(0); groupIdx < groupCount; groupIdx++ {
		if len(data)-offset < 20 {
			return WireBatch{}, errors.New("heat: truncated infohash")
		}
		var group WireGroup
		copy(group.InfoHash[:], data[offset:offset+20])
		offset += 20
		if groupIdx > 0 && bytes.Compare(previousHash[:], group.InfoHash[:]) >= 0 {
			return WireBatch{}, errors.New("heat: infohash groups are not strictly sorted")
		}
		previousHash = group.InfoHash
		actorCount, used, err := readCanonicalUvarint(data[offset:])
		if err != nil || actorCount == 0 || actorCount > MaxWireActorsPerHash {
			return WireBatch{}, errors.New("heat: invalid actor count")
		}
		offset += used
		if actorCount > uint64((len(data)-offset)/8) {
			return WireBatch{}, errors.New("heat: truncated actor list")
		}
		group.Actors = make([]uint64, int(actorCount))
		for actorIdx := range group.Actors {
			actor := binary.BigEndian.Uint64(data[offset : offset+8])
			offset += 8
			if actorIdx > 0 && actor <= group.Actors[actorIdx-1] {
				return WireBatch{}, errors.New("heat: actors are not strictly sorted and unique")
			}
			group.Actors[actorIdx] = actor
		}
		batch.Groups = append(batch.Groups, group)
	}
	if offset != len(data) {
		return WireBatch{}, errors.New("heat: trailing wire bytes")
	}
	return batch, nil
}

func validateWireBatch(batch WireBatch) error {
	if batch.Day == 0 || len(batch.Groups) == 0 || len(batch.Groups) > MaxWireGroups {
		return errors.New("heat: invalid wire batch bounds")
	}
	for groupIdx, group := range batch.Groups {
		if groupIdx > 0 && bytes.Compare(batch.Groups[groupIdx-1].InfoHash[:], group.InfoHash[:]) >= 0 {
			return errors.New("heat: infohash groups must be strictly sorted")
		}
		if len(group.Actors) == 0 || len(group.Actors) > MaxWireActorsPerHash {
			return errors.New("heat: invalid actor count")
		}
		for actorIdx := 1; actorIdx < len(group.Actors); actorIdx++ {
			if group.Actors[actorIdx] <= group.Actors[actorIdx-1] {
				return errors.New("heat: actors must be strictly sorted and unique")
			}
		}
	}
	return nil
}

func readCanonicalUvarint(data []byte) (uint64, int, error) {
	value, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, errors.New("invalid uvarint")
	}
	var canonical [binary.MaxVarintLen64]byte
	if binary.PutUvarint(canonical[:], value) != n || !bytes.Equal(canonical[:n], data[:n]) {
		return 0, 0, errors.New("non-canonical uvarint")
	}
	return value, n, nil
}
