// Package buildinfo exposes reproducible source and configuration identifiers.
package buildinfo

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"runtime"
	"runtime/debug"
	"strings"
)

// These values are intentionally variables so release/benchmark builds can
// stamp them with -ldflags. Go's VCS build settings are used as a fallback.
var (
	GitCommit  = "unknown"
	GitDirty   = "unknown"
	SourceHash = "unknown"
	BuildTime  = "unknown"
)

type Info struct {
	GitCommit  string `json:"git_commit"`
	GitDirty   string `json:"git_dirty"`
	SourceHash string `json:"source_hash"`
	ConfigHash string `json:"config_hash"`
	BuildTime  string `json:"build_time"`
	GoVersion  string `json:"go_version"`
}

// Current returns build identifiers, filling unstamped commit/dirty values
// from the metadata embedded automatically by `go build` when available.
func Current(configHash string) Info {
	commit := GitCommit
	dirty := GitDirty
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range bi.Settings {
			switch setting.Key {
			case "vcs.revision":
				if commit == "unknown" && setting.Value != "" {
					commit = setting.Value
				}
			case "vcs.modified":
				if dirty == "unknown" && setting.Value != "" {
					dirty = setting.Value
				}
			}
		}
	}
	return Info{
		GitCommit:  commit,
		GitDirty:   dirty,
		SourceHash: SourceHash,
		ConfigHash: configHash,
		BuildTime:  BuildTime,
		GoVersion:  runtime.Version(),
	}
}

// Fingerprint returns a stable SHA-256 identifier for an effective config.
// The serialized config is never logged, so credentials remain private.
func Fingerprint(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func Short(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 12 {
		return value[:12]
	}
	return value
}
