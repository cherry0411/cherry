package heat

import (
	"bytes"
	cryptorand "crypto/rand"
	"encoding/binary"
	"math/rand/v2"
	"reflect"
	"testing"
)

func TestWireCanonicalSortDedupeAndRoundTrip(t *testing.T) {
	var low, high [20]byte
	low[19] = 1
	high[0] = 1
	observations := []Observation{
		{Day: 20_654, InfoHash: high, Actor: 9},
		{Day: 20_654, InfoHash: low, Actor: 3},
		{Day: 20_654, InfoHash: low, Actor: 1},
		{Day: 20_654, InfoHash: low, Actor: 3},
	}
	batch, err := BuildWireBatch(20_654, observations)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Groups) != 2 || batch.Groups[0].InfoHash != low ||
		!reflect.DeepEqual(batch.Groups[0].Actors, []uint64{1, 3}) {
		t.Fatalf("non-canonical batch: %+v", batch)
	}
	payload, err := EncodeWire(batch)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeWire(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, batch) {
		t.Fatalf("round trip mismatch\n got=%+v\nwant=%+v", decoded, batch)
	}
}

func TestDecodeWireRejectsNonCanonicalAndBoundaryViolations(t *testing.T) {
	base := append([]byte{'C', 'H', 'H', 'T', 1, 0, 0, 0, 1}, 1)
	base = append(base, make([]byte, 20)...)
	base = append(base, 1)
	base = binary.BigEndian.AppendUint64(base, 1)
	cases := map[string][]byte{
		"trailing":            append(append([]byte(nil), base...), 0),
		"noncanonical groups": append(append([]byte(nil), base[:9]...), append([]byte{0x81, 0}, base[10:]...)...),
		"zero actor count":    append(append([]byte(nil), base[:30]...), 0),
		"unsupported version": append([]byte(nil), base...),
	}
	cases["unsupported version"][4] = 2
	for name, data := range cases {
		if _, err := DecodeWire(data); err == nil {
			t.Errorf("%s accepted", name)
		}
	}

	duplicateActor := append([]byte{'C', 'H', 'H', 'T', 1, 0, 0, 0, 1, 1}, make([]byte, 20)...)
	duplicateActor = append(duplicateActor, 2)
	duplicateActor = binary.BigEndian.AppendUint64(duplicateActor, 7)
	duplicateActor = binary.BigEndian.AppendUint64(duplicateActor, 7)
	if _, err := DecodeWire(duplicateActor); err == nil {
		t.Fatal("duplicate actor accepted")
	}
}

func TestWireRandomizedCanonicalRoundTrips(t *testing.T) {
	for iteration := 0; iteration < 500; iteration++ {
		rows := make([]Observation, 1+rand.IntN(300))
		for idx := range rows {
			rows[idx].Day = 20_654
			_, _ = cryptorand.Read(rows[idx].InfoHash[:])
			rows[idx].Actor = rand.Uint64()
			if idx > 0 && idx%7 == 0 {
				rows[idx] = rows[idx-1]
			}
		}
		batch, err := BuildWireBatch(20_654, rows)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := EncodeWire(batch)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeWire(payload)
		if err != nil || !reflect.DeepEqual(decoded, batch) {
			t.Fatalf("iteration %d failed: %v", iteration, err)
		}
	}
}

func FuzzDecodeWireNeverPanics(f *testing.F) {
	f.Add([]byte("CHHT"))
	f.Add(bytes.Repeat([]byte{0xff}, 128))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeWire(data)
	})
}

func BenchmarkBuildAndEncodeWire(b *testing.B) {
	rows := make([]Observation, 4096)
	for idx := range rows {
		rows[idx].Day = 20_654
		binary.BigEndian.PutUint64(rows[idx].InfoHash[12:], uint64(idx/4))
		rows[idx].Actor = uint64(idx)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		batch, err := BuildWireBatch(20_654, rows)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := EncodeWire(batch); err != nil {
			b.Fatal(err)
		}
	}
}
