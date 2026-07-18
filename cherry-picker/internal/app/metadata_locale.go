package app

import (
	"sync/atomic"
	"unicode/utf8"

	"cherry-picker/internal/pipeline"
)

// metadataLocaleSignals are deliberately coarse script signals, not a
// language detector. In particular, Han-only Japanese and Korean metadata can
// satisfy chineseProxy, so the metric must only be used as a regional proxy.
type metadataLocaleSignals struct {
	han          bool
	kana         bool
	hangul       bool
	chineseProxy bool
}

type metadataLocaleCounters struct {
	classified   atomic.Uint64
	han          atomic.Uint64
	kana         atomic.Uint64
	hangul       atomic.Uint64
	chineseProxy atomic.Uint64
}

type metadataLocaleSnapshot struct {
	classified   uint64
	han          uint64
	kana         uint64
	hangul       uint64
	chineseProxy uint64
}

// observe classifies one already-normalized metadata value and updates the
// cumulative counters. It neither retains metadata strings nor changes the
// exported metadata event.
func (c *metadataLocaleCounters) observe(metadata *pipeline.Metadata) metadataLocaleSignals {
	if metadata == nil {
		return metadataLocaleSignals{}
	}
	signals := classifyMetadataLocale(metadata)
	c.classified.Add(1)
	if signals.han {
		c.han.Add(1)
	}
	if signals.kana {
		c.kana.Add(1)
	}
	if signals.hangul {
		c.hangul.Add(1)
	}
	if signals.chineseProxy {
		c.chineseProxy.Add(1)
	}
	return signals
}

func (c *metadataLocaleCounters) snapshot() metadataLocaleSnapshot {
	return metadataLocaleSnapshot{
		classified:   c.classified.Load(),
		han:          c.han.Load(),
		kana:         c.kana.Load(),
		hangul:       c.hangul.Load(),
		chineseProxy: c.chineseProxy.Load(),
	}
}

func (s metadataLocaleSnapshot) subtract(previous metadataLocaleSnapshot) metadataLocaleSnapshot {
	return metadataLocaleSnapshot{
		classified:   s.classified - previous.classified,
		han:          s.han - previous.han,
		kana:         s.kana - previous.kana,
		hangul:       s.hangul - previous.hangul,
		chineseProxy: s.chineseProxy - previous.chineseProxy,
	}
}

func (s metadataLocaleSnapshot) addWorkerStats(stats map[string]uint64) {
	stats["metadata_locale_classified"] = s.classified
	stats["metadata_name_path_han"] = s.han
	stats["metadata_name_path_kana"] = s.kana
	stats["metadata_name_path_hangul"] = s.hangul
	stats["metadata_chinese_proxy"] = s.chineseProxy
}

// classifyMetadataLocale scans the normalized name and canonical file paths
// without regexes or temporary strings. chineseProxy means Han is present and
// Kana/Hangul are absent; it is intentionally not named "Chinese" because
// Han-only Japanese names and Korean Hanja are indistinguishable here.
func classifyMetadataLocale(metadata *pipeline.Metadata) metadataLocaleSignals {
	var signals metadataLocaleSignals
	if metadata == nil {
		return signals
	}

	scanLocaleString(metadata.Name, &signals)
	for i := range metadata.Files {
		file := &metadata.Files[i]
		if file.PathText != "" {
			scanLocaleString(file.PathText, &signals)
		} else {
			// normalizeMetadata always creates PathText. Keep this fallback for
			// programmatically constructed Metadata values and old producers.
			for _, component := range file.Path {
				scanLocaleString(component, &signals)
				if signals.han && signals.kana && signals.hangul {
					break
				}
			}
		}
		if signals.han && signals.kana && signals.hangul {
			break
		}
	}
	signals.chineseProxy = signals.han && !signals.kana && !signals.hangul
	return signals
}

func scanLocaleString(value string, signals *metadataLocaleSignals) {
	for offset := 0; offset < len(value); {
		if value[offset] < utf8.RuneSelf {
			offset++
			continue
		}
		r, size := utf8.DecodeRuneInString(value[offset:])
		offset += size
		switch {
		case isHanRune(r):
			signals.han = true
		case isKanaRune(r):
			signals.kana = true
		case isHangulRune(r):
			signals.hangul = true
		}
		if signals.han && signals.kana && signals.hangul {
			return
		}
	}
}

func isHanRune(r rune) bool {
	return (r >= 0x3400 && r <= 0x4DBF) ||
		(r >= 0x4E00 && r <= 0x9FFF) ||
		(r >= 0xF900 && r <= 0xFAFF) ||
		(r >= 0x20000 && r <= 0x2EE5F) ||
		(r >= 0x2F800 && r <= 0x2FA1F) ||
		(r >= 0x30000 && r <= 0x323AF)
}

func isKanaRune(r rune) bool {
	return (r >= 0x3031 && r <= 0x3035) ||
		(r >= 0x3041 && r <= 0x309F) ||
		(r >= 0x30A1 && r <= 0x30FA) ||
		r == 0x30FC ||
		(r >= 0x30FD && r <= 0x30FF) ||
		(r >= 0x31F0 && r <= 0x31FF) ||
		(r >= 0xFF66 && r <= 0xFF9D) ||
		(r >= 0x1B000 && r <= 0x1B16F)
}

func isHangulRune(r rune) bool {
	return (r >= 0x1100 && r <= 0x11FF) ||
		(r >= 0x3130 && r <= 0x318F) ||
		(r >= 0xA960 && r <= 0xA97F) ||
		(r >= 0xAC00 && r <= 0xD7AF) ||
		(r >= 0xD7B0 && r <= 0xD7FF) ||
		(r >= 0xFFA0 && r <= 0xFFDC)
}
