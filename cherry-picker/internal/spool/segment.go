// Package spool implements a single-writer, crash-safe, append-only durable
// spool for typed, normalized crawler metadata.
package spool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Frame v1: magic(4) | version(2) | flags(2) | length(4) | crc32c(4) |
// payload(length). CRC32C covers the complete typed JSON payload.
const (
	recordMagic     uint32 = 0x43485231 // "CHR1"
	frameVersion    uint16 = 1
	headerSize             = 16
	maxRecordLength        = 4 << 20
	segmentPrefix          = "seg_"
	segmentSuffix          = ".spool"
	lockName               = "spool.lock"
	cursorName             = "cursor.json"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrRecordTooLarge = errors.New("spool: record exceeds maximum length")
	ErrCorruption     = errors.New("spool: fatal segment corruption")
	errStopScan       = errors.New("spool: stop segment scan")
)

func encodeFrame(payload []byte) []byte {
	buf := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], recordMagic)
	binary.BigEndian.PutUint16(buf[4:6], frameVersion)
	binary.BigEndian.PutUint16(buf[6:8], 0)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[12:16], crc32.Checksum(payload, castagnoli))
	copy(buf[headerSize:], payload)
	return buf
}

type scanResult struct {
	ValidOffset int64
	FileSize    int64
	Records     uint64
	TornTail    bool
	Stopped     bool
}

// recordVisitor consumes one bounded payload before the scanner reuses its
// working memory. Returning errStopScan is a successful early stop.
type recordVisitor func(segmentID, int64, int64, []byte) error

// scanSegment streams a segment one record at a time. Its peak frame memory is
// maxRecordLength, independent of the segment size.
//
// Only two byte patterns are recoverable as a torn tail:
//   - fewer than headerSize bytes remain;
//   - a complete, valid header declares a record end beyond the observed EOF.
//
// A complete header with bad magic/version/flags/length or a complete frame
// with bad CRC is always fatal corruption. There is deliberately no resync.
func scanSegment(path string, id segmentID, visit recordVisitor) (scanResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return scanResult{}, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return scanResult{}, err
	}
	res := scanResult{FileSize: info.Size()}
	var header [headerSize]byte

	for res.ValidOffset < res.FileSize {
		remaining := res.FileSize - res.ValidOffset
		if remaining < headerSize {
			res.TornTail = true
			return res, nil
		}
		if _, err := io.ReadFull(f, header[:]); err != nil {
			return res, fmt.Errorf("spool: read header at %d in %s: %w", res.ValidOffset, filepath.Base(path), err)
		}

		magic := binary.BigEndian.Uint32(header[0:4])
		version := binary.BigEndian.Uint16(header[4:6])
		flags := binary.BigEndian.Uint16(header[6:8])
		length := binary.BigEndian.Uint32(header[8:12])
		wantCRC := binary.BigEndian.Uint32(header[12:16])
		if magic != recordMagic || version != frameVersion || flags != 0 || length == 0 || length > maxRecordLength {
			return res, fmt.Errorf("%w: invalid header at %d in %s (magic=%08x version=%d flags=%d length=%d)",
				ErrCorruption, res.ValidOffset, filepath.Base(path), magic, version, flags, length)
		}

		recordStart := res.ValidOffset
		recordEnd := recordStart + headerSize + int64(length)
		if recordEnd > res.FileSize {
			res.TornTail = true
			return res, nil
		}
		payload := make([]byte, int(length))
		if _, err := io.ReadFull(f, payload); err != nil {
			return res, fmt.Errorf("spool: read payload at %d in %s: %w", recordStart, filepath.Base(path), err)
		}
		if got := crc32.Checksum(payload, castagnoli); got != wantCRC {
			return res, fmt.Errorf("%w: crc mismatch at %d in %s", ErrCorruption, recordStart, filepath.Base(path))
		}

		res.ValidOffset = recordEnd
		res.Records++
		if visit != nil {
			if err := visit(id, recordStart, recordEnd, payload); err != nil {
				if errors.Is(err, errStopScan) {
					res.Stopped = true
					return res, nil
				}
				return res, err
			}
		}
	}
	return res, nil
}

type segmentID uint64

func segmentName(id segmentID) string {
	return fmt.Sprintf("%s%020d%s", segmentPrefix, uint64(id), segmentSuffix)
}

func parseSegmentName(name string) (segmentID, bool) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	mid := name[len(segmentPrefix) : len(name)-len(segmentSuffix)]
	if len(mid) != 20 {
		return 0, false
	}
	v, err := strconv.ParseUint(mid, 10, 64)
	if err != nil || v == 0 {
		return 0, false
	}
	return segmentID(v), true
}

func listSegments(dir string) ([]segmentID, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("spool: list segments: %w", err)
	}
	ids := make([]segmentID, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		id, ok := parseSegmentName(e.Name())
		if ok {
			ids = append(ids, id)
		} else if strings.HasPrefix(e.Name(), segmentPrefix) && strings.HasSuffix(e.Name(), segmentSuffix) {
			return nil, fmt.Errorf("%w: malformed segment name %q", ErrCorruption, e.Name())
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for i := 1; i < len(ids); i++ {
		if ids[i] != ids[i-1]+1 {
			return nil, fmt.Errorf("%w: segment id gap %d -> %d", ErrCorruption, ids[i-1], ids[i])
		}
	}
	return ids, nil
}

func segmentPath(dir string, id segmentID) string {
	return filepath.Join(dir, segmentName(id))
}
