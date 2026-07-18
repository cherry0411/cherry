package heat

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func spoolObservation(index int) Observation {
	var obs Observation
	obs.Day = 20_654
	obs.InfoHash[0] = byte(index >> 8)
	obs.InfoHash[1] = byte(index)
	obs.Actor = uint64(index + 1)
	return obs
}

func TestSpoolTornTailRecoveryReceiptAndMonotonicSequence(t *testing.T) {
	dir := t.TempDir()
	sp, err := openHeatSpool(spoolOptions{dir: dir, maxBytes: 1 << 20, segmentBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	epoch := sp.epoch
	if err := sp.appendDurable([]Observation{spoolObservation(0), spoolObservation(1), spoolObservation(2)}); err != nil {
		t.Fatal(err)
	}
	activePath := sp.segments[len(sp.segments)-1].path
	if err := sp.close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(activePath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("torn-tail")); err != nil {
		t.Fatal(err)
	}
	_ = file.Sync()
	_ = file.Close()

	sp, err = openHeatSpool(spoolOptions{dir: dir, maxBytes: 1 << 20, segmentBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.close()
	batch, err := sp.readBatch(10)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Epoch != epoch || batch.StartSequence != 1 || batch.EndSequence != 3 || len(batch.Observations) != 3 {
		t.Fatalf("recovered batch = %+v", batch)
	}
	if err := sp.commit(batch); err != nil {
		t.Fatal(err)
	}
	if err := sp.appendDurable([]Observation{spoolObservation(3)}); err != nil {
		t.Fatal(err)
	}
	next, err := sp.readBatch(10)
	if err != nil {
		t.Fatal(err)
	}
	if next.Epoch != epoch || next.StartSequence != 4 || next.EndSequence != 4 {
		t.Fatalf("sequence was reused after compaction: %+v", next)
	}
}

func TestSpoolContinuousProduceAndAckReclaimsSealedPrefixes(t *testing.T) {
	dir := t.TempDir()
	const maxBytes = int64(16 << 10)
	sp, err := openHeatSpool(spoolOptions{dir: dir, maxBytes: maxBytes, segmentBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.close()
	seed := make([]Observation, 40)
	for idx := range seed {
		seed[idx] = spoolObservation(idx)
	}
	if err := sp.appendDurable(seed); err != nil {
		t.Fatal(err)
	}
	wantSequence := uint64(1)
	for cycle := 0; cycle < 500; cycle++ {
		appendRows := make([]Observation, 10)
		for idx := range appendRows {
			appendRows[idx] = spoolObservation(40 + cycle*10 + idx)
		}
		if err := sp.appendDurable(appendRows); err != nil {
			t.Fatalf("cycle %d append hit historical-capacity leak: %v", cycle, err)
		}
		batch, err := sp.readBatch(10)
		if err != nil {
			t.Fatal(err)
		}
		if batch.StartSequence != wantSequence {
			t.Fatalf("cycle %d start sequence=%d want=%d", cycle, batch.StartSequence, wantSequence)
		}
		wantSequence = batch.EndSequence + 1
		if err := sp.commit(batch); err != nil {
			t.Fatalf("cycle %d commit: %v", cycle, err)
		}
	}
	used, _, records := sp.snapshot()
	if used > maxBytes {
		t.Fatalf("spool exceeded hard capacity: bytes=%d max=%d", used, maxBytes)
	}
	if used >= maxBytes/2 {
		t.Fatalf("acknowledged historical prefixes were not reclaimed: bytes=%d", used)
	}
	if records != 40 {
		t.Fatalf("steady backlog=%d want=40", records)
	}
}

func TestSpoolReadBatchStopsAtUTCDateBoundary(t *testing.T) {
	sp, err := openHeatSpool(spoolOptions{dir: t.TempDir(), maxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.close()
	rows := []Observation{spoolObservation(0), spoolObservation(1), spoolObservation(2)}
	rows[2].Day++
	if err := sp.appendDurable(rows); err != nil {
		t.Fatal(err)
	}
	first, err := sp.readBatch(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Observations) != 2 || first.StartSequence != 1 || first.EndSequence != 2 ||
		first.Observations[0].Day != first.Observations[1].Day {
		t.Fatalf("first date-bounded batch=%+v", first)
	}
	if err := sp.commit(first); err != nil {
		t.Fatal(err)
	}
	second, err := sp.readBatch(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Observations) != 1 || second.StartSequence != 3 || second.EndSequence != 3 ||
		second.Observations[0].Day != rows[2].Day {
		t.Fatalf("second date-bounded batch=%+v", second)
	}
}

func TestSpoolFailsOnInteriorCorruption(t *testing.T) {
	dir := t.TempDir()
	sp, err := openHeatSpool(spoolOptions{dir: dir, maxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.appendDurable([]Observation{spoolObservation(0), spoolObservation(1)}); err != nil {
		t.Fatal(err)
	}
	path := sp.segments[0].path
	if err := sp.close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, spoolHeaderSize+8); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if reopened, err := openHeatSpool(spoolOptions{dir: dir, maxBytes: 1 << 20}); err == nil {
		reopened.close()
		t.Fatal("interior corruption was silently recovered")
	} else if !errors.Is(err, ErrCorruptSpool) {
		t.Fatalf("wrong corruption error: %v", err)
	}
}

func TestSpoolCapacityCountsCurrentDiskNotHistoricalACKs(t *testing.T) {
	dir := t.TempDir()
	sp, err := openHeatSpool(spoolOptions{dir: dir, maxBytes: 512, segmentBytes: 160})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.close()
	rows := make([]Observation, 20)
	for idx := range rows {
		rows[idx] = spoolObservation(idx)
	}
	if err := sp.appendDurable(rows); !errors.Is(err, ErrAtCapacity) {
		t.Fatalf("oversized append error=%v", err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "heat-*.spool"))
	if err != nil || len(files) != 1 {
		t.Fatalf("capacity failure created partial segments: %v %v", files, err)
	}
}

func TestSpoolCapacityReservesCrashSafeSuccessorHeader(t *testing.T) {
	const maxBytes = int64(4 * (spoolHeaderSize + spoolFrameSize))
	sp, err := openHeatSpool(spoolOptions{dir: t.TempDir(), maxBytes: maxBytes, segmentBytes: maxBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.close()
	rows := make([]Observation, 0, 8)
	for idx := 0; ; idx++ {
		if err := sp.appendDurable([]Observation{spoolObservation(idx)}); err != nil {
			if !errors.Is(err, ErrAtCapacity) {
				t.Fatal(err)
			}
			break
		}
		rows = append(rows, spoolObservation(idx))
	}
	used, _, _ := sp.snapshot()
	if used > maxBytes-spoolHeaderSize {
		t.Fatalf("successor header was not reserved: used=%d max=%d", used, maxBytes)
	}
	batch, err := sp.readBatch(len(rows))
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.commit(batch); err != nil {
		t.Fatalf("reserved header did not permit crash-safe final commit: %v", err)
	}
	used, _, _ = sp.snapshot()
	if used > maxBytes {
		t.Fatalf("commit exceeded hard capacity: used=%d max=%d", used, maxBytes)
	}
}
