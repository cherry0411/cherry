package filter

import (
	"path/filepath"
	"strings"

	"cherry-picker/internal/pipeline"
)

// TooManyFiles rejects metadata whose FileCount exceeds maxCount.
// Intended as a hard cap against data-dump or auto-generated torrents with an
// unreasonable number of files.
func TooManyFiles(maxCount int) Rule {
	return func(m *pipeline.Metadata) Reason {
		if m.FileCount > maxCount {
			return ReasonTooManyFiles
		}
		return ReasonPass
	}
}

// NonChineseHighFileCount rejects metadata with FileCount > threshold when
// none of the file paths or the torrent name contain Chinese characters.
// This targets large non-Chinese content dumps that are unlikely to be
// relevant to a Chinese-audience search engine.
func NonChineseHighFileCount(threshold int) Rule {
	return func(m *pipeline.Metadata) Reason {
		if m.FileCount > threshold && !hasChineseInMetadata(m) {
			return ReasonNonChinese
		}
		return ReasonPass
	}
}

// NumericOnlyFileNames rejects metadata with FileCount > threshold when every
// file's base name (extension stripped) consists solely of ASCII digits.
// Purely-numeric filenames are a hallmark of auto-numbered or machine-generated
// archives that provide no value to users.
func NumericOnlyFileNames(threshold int) Rule {
	return func(m *pipeline.Metadata) Reason {
		if m.FileCount > threshold && allNumericFileNames(m) {
			return ReasonNumericFileNames
		}
		return ReasonPass
	}
}

// ---------- internal helpers ----------

// hasChineseInMetadata reports whether m contains any CJK codepoint in the
// torrent name or any file path component.
func hasChineseInMetadata(m *pipeline.Metadata) bool {
	if containsChinese(m.Name) {
		return true
	}
	for _, f := range m.Files {
		if containsChinese(f.PathText) {
			return true
		}
		for _, p := range f.Path {
			if containsChinese(p) {
				return true
			}
		}
	}
	return false
}

// allNumericFileNames reports whether every file in m has a purely-numeric
// base name (ASCII digits only, extension stripped). Returns false for empty
// file lists so as not to trigger on single-file torrents.
func allNumericFileNames(m *pipeline.Metadata) bool {
	if len(m.Files) == 0 {
		return false
	}
	for _, f := range m.Files {
		if !isASCIIDigits(fileBaseName(f)) {
			return false
		}
	}
	return true
}

// fileBaseName returns the final path component of a file with its extension
// stripped and whitespace trimmed.
func fileBaseName(f pipeline.MetadataFile) string {
	var name string
	switch {
	case f.PathText != "":
		name = filepath.Base(f.PathText)
	case len(f.Path) > 0:
		name = f.Path[len(f.Path)-1]
	}
	name = strings.TrimSpace(name)
	if ext := filepath.Ext(name); ext != "" {
		name = name[:len(name)-len(ext)]
	}
	return strings.TrimSpace(name)
}

// containsChinese reports whether s contains at least one CJK codepoint.
func containsChinese(s string) bool {
	for _, r := range s {
		if isCJK(r) {
			return true
		}
	}
	return false
}

// isCJK reports whether r falls within a common CJK Unicode block.
func isCJK(r rune) bool {
	return (r >= 0x4E00 && r <= 0x9FFF) || // CJK Unified Ideographs
		(r >= 0x3400 && r <= 0x4DBF) || // CJK Extension A
		(r >= 0x20000 && r <= 0x2A6DF) || // CJK Extension B
		(r >= 0xF900 && r <= 0xFAFF) || // CJK Compatibility Ideographs
		(r >= 0x2E80 && r <= 0x2EFF) || // CJK Radicals Supplement
		(r >= 0x3000 && r <= 0x303F) // CJK Symbols and Punctuation
}

// isASCIIDigits reports whether s is non-empty and every byte is an ASCII digit.
func isASCIIDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
