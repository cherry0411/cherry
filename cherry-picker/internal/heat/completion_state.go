package heat

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const completionStateVersion = 1

type completionDayState struct {
	StartSequence uint64 `json:"startSequence"`
	NextSequence  uint64 `json:"nextSequence"`
	Closed        bool   `json:"closed"`
	Dirty         bool   `json:"dirty"`
	Acknowledged  bool   `json:"acknowledged"`
}

type completionStateFile struct {
	Version   int                            `json:"version"`
	CrawlerID string                         `json:"crawlerId"`
	Epoch     uint64                         `json:"epoch"`
	Running   bool                           `json:"running"`
	ActiveDay uint32                         `json:"activeDay"`
	Days      map[string]*completionDayState `json:"days"`
}

// completionTracker is the crawler's fail-closed evidence ledger. The spool
// cursor remains the durable receipt head; this file only records UTC boundary
// anchors and whether the local admission path was lossless. It is rewritten
// once per boundary (and on exceptional loss), never once per observation.
type completionTracker struct {
	mu   sync.Mutex
	path string
	data completionStateFile
}

func openCompletionTracker(
	dir, crawlerID string,
	epoch uint64,
	today uint32,
	nextSequence uint64,
) (*completionTracker, error) {
	path := filepath.Join(dir, "heat.completion.json")
	data, exists, err := readCompletionState(path)
	if err != nil {
		return nil, err
	}
	if !exists {
		data = completionStateFile{
			Version: completionStateVersion, CrawlerID: crawlerID, Epoch: epoch,
			Running: true, ActiveDay: today, Days: map[string]*completionDayState{},
		}
		// The first partial UTC day has no proof that the crawler was present
		// since midnight. A full uninterrupted following day can be complete.
		data.Days[dayKey(today)] = &completionDayState{
			StartSequence: nextSequence, NextSequence: nextSequence, Dirty: true,
		}
		tracker := &completionTracker{path: path, data: data}
		if err := tracker.persistLocked(); err != nil {
			return nil, err
		}
		return tracker, nil
	}
	if data.Version != completionStateVersion || data.CrawlerID != crawlerID || data.Epoch != epoch ||
		data.ActiveDay == 0 || data.Days == nil || data.ActiveDay > today {
		return nil, fmt.Errorf("%w: invalid completion state identity", ErrCorruptSpool)
	}
	active, ok := data.Days[dayKey(data.ActiveDay)]
	if !ok || active.Closed || active.StartSequence == 0 {
		return nil, fmt.Errorf("%w: invalid active completion day", ErrCorruptSpool)
	}
	// Any process boundary inside a UTC day is partial, including a graceful
	// restart: callbacks could not be observed while the process was absent.
	active.Dirty = true
	if data.ActiveDay < today {
		active.Closed = true
		active.NextSequence = nextSequence
		data.ActiveDay = today
		data.Days[dayKey(today)] = &completionDayState{
			StartSequence: nextSequence, NextSequence: nextSequence, Dirty: true,
		}
	}
	data.Running = true
	tracker := &completionTracker{path: path, data: data}
	if err := tracker.persistLocked(); err != nil {
		return nil, err
	}
	return tracker, nil
}

func readCompletionState(path string) (completionStateFile, bool, error) {
	contents, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return completionStateFile{}, false, nil
	}
	if err != nil {
		return completionStateFile{}, false, fmt.Errorf("heat: read completion state: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var state completionStateFile
	if err := decoder.Decode(&state); err != nil {
		return completionStateFile{}, false, fmt.Errorf("%w: decode completion state: %v", ErrCorruptSpool, err)
	}
	if err := decoder.Decode(&struct{}{}); err == nil {
		return completionStateFile{}, false, fmt.Errorf("%w: multiple completion state values", ErrCorruptSpool)
	} else if !errors.Is(err, io.EOF) {
		return completionStateFile{}, false, fmt.Errorf("%w: trailing completion state data", ErrCorruptSpool)
	}
	for key, day := range state.Days {
		parsed, ok := parseDayKey(key)
		if !ok || parsed == 0 || day == nil || day.StartSequence == 0 || day.NextSequence < day.StartSequence ||
			(!day.Closed && parsed != state.ActiveDay) || (day.Acknowledged && (!day.Closed || day.Dirty)) {
			return completionStateFile{}, false, fmt.Errorf("%w: invalid completion day state", ErrCorruptSpool)
		}
	}
	return state, true, nil
}

func (t *completionTracker) activeDay() uint32 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.data.ActiveDay
}

func (t *completionTracker) markDirty(day uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if value := t.data.Days[dayKey(day)]; value != nil && !value.Acknowledged {
		value.Dirty = true
	}
}

func (t *completionTracker) markDirtyDurable(day uint32) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	value := t.data.Days[dayKey(day)]
	if value == nil || value.Acknowledged {
		return nil
	}
	value.Dirty = true
	return t.persistLocked()
}

func (t *completionTracker) closeThrough(day, nextDay uint32, nextSequence uint64, cleanBoundary bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.data.ActiveDay != day || nextDay <= day || nextSequence == 0 {
		return fmt.Errorf("%w: invalid completion day transition", ErrCorruptSpool)
	}
	active := t.data.Days[dayKey(day)]
	if active == nil || active.Closed || nextSequence < active.StartSequence {
		return fmt.Errorf("%w: invalid completion boundary", ErrCorruptSpool)
	}
	active.Closed = true
	active.NextSequence = nextSequence
	if !cleanBoundary {
		active.Dirty = true
	}
	// A clock jump skips an unobserved interval. Only a direct UTC rollover
	// creates a clean new active day.
	t.data.ActiveDay = nextDay
	t.data.Days[dayKey(nextDay)] = &completionDayState{
		StartSequence: nextSequence, NextSequence: nextSequence, Dirty: nextDay != day+1,
	}
	t.data.Running = true
	return t.persistLocked()
}

func (t *completionTracker) ready(cursorSequence uint64) []completionRequest {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]completionRequest, 0, 2)
	keys := make([]string, 0, len(t.data.Days))
	for key := range t.data.Days {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		day, ok := parseDayKey(key)
		value := t.data.Days[key]
		if !ok || value == nil || !value.Closed || value.Dirty || value.Acknowledged ||
			cursorSequence < value.NextSequence {
			continue
		}
		result = append(result, completionRequest{
			Crawler: t.data.CrawlerID, Day: formatObservationDay(day), DayNumber: day, Epoch: t.data.Epoch,
			StartSequence: value.StartSequence, NextSequence: value.NextSequence, Clean: true,
		})
	}
	return result
}

func (t *completionTracker) acknowledge(day uint32) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	value := t.data.Days[dayKey(day)]
	if value == nil || !value.Closed || value.Dirty {
		return fmt.Errorf("%w: acknowledge invalid completion day", ErrCorruptSpool)
	}
	value.Acknowledged = true
	return t.persistLocked()
}

func (t *completionTracker) stopDirty() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if active := t.data.Days[dayKey(t.data.ActiveDay)]; active != nil && !active.Acknowledged {
		active.Dirty = true
	}
	t.data.Running = false
	return t.persistLocked()
}

func (t *completionTracker) persistLocked() error {
	body, err := json.Marshal(t.data)
	if err != nil {
		return fmt.Errorf("heat: encode completion state: %w", err)
	}
	tmp := t.path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("heat: create completion state temp: %w", err)
	}
	if _, err = file.Write(body); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("heat: persist completion state: %w", err)
	}
	if err := replaceFile(tmp, t.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("heat: replace completion state: %w", err)
	}
	if err := fsyncDir(filepath.Dir(t.path)); err != nil {
		return fmt.Errorf("heat: sync completion state directory: %w", err)
	}
	return nil
}

func dayKey(day uint32) string { return fmt.Sprintf("%010d", day) }

func parseDayKey(value string) (uint32, bool) {
	if len(value) != 10 {
		return 0, false
	}
	var day uint64
	for _, current := range []byte(value) {
		if current < '0' || current > '9' {
			return 0, false
		}
		day = day*10 + uint64(current-'0')
		if day > uint64(^uint32(0)) {
			return 0, false
		}
	}
	return uint32(day), true
}
