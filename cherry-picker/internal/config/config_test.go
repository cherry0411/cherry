package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadFromFileAppliesRoleDefaults(t *testing.T) {
	t.Setenv("CHERRY_PICKER_CONFIG", filepath.Join("..", "..", "configs", "metadata.json"))

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Role != "metadata" {
		t.Fatalf("Role = %q, want metadata", cfg.Role)
	}
	if cfg.Discovery.EmitPeerEvents {
		t.Fatal("metadata role should disable peer events")
	}
	if !cfg.Metadata.Enabled {
		t.Fatal("metadata role should enable metadata workers")
	}
	if cfg.Exporter.RetryBackoff != time.Second {
		t.Fatalf("RetryBackoff = %v, want 1s", cfg.Exporter.RetryBackoff)
	}
	if cfg.Exporter.HTTPRetries != 5 {
		t.Fatalf("HTTPRetries = %d, want 5", cfg.Exporter.HTTPRetries)
	}
	if _, err := os.Stat(filepath.Join("..", "..", "configs", "metadata.json")); err != nil {
		t.Fatalf("expected metadata config fixture: %v", err)
	}
}
