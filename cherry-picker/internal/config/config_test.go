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

func TestLoad2C4GMetadataProfile(t *testing.T) {
	t.Setenv("CHERRY_PICKER_CONFIG", filepath.Join("..", "..", "configs", "metadata-2c4g.json"))

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Role != "metadata" || cfg.Discovery.Instances != 96 ||
		cfg.Discovery.LookupNodes != 2 || cfg.Discovery.LookupDHTs != 2 ||
		cfg.Discovery.LookupRate != 300 || cfg.Discovery.LookupWorkers != 2 ||
		cfg.Discovery.LookupFollowups != 8 || !cfg.Discovery.LookupSpread ||
		cfg.Discovery.SampleInfohashes || cfg.Metadata.WorkerQueueSize != 1024 {
		t.Fatalf("unexpected 2C4G profile: %+v", cfg)
	}
}

func TestLoadPrimeNodesFromEnvironment(t *testing.T) {
	t.Setenv("CHERRY_PICKER_CONFIG", "")
	t.Setenv("CHERRY_PICKER_DHT_PRIME_NODES", "87.98.162.88:6881,212.129.33.59:6881")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := cfg.Discovery.PrimeNodes; got != "87.98.162.88:6881,212.129.33.59:6881" {
		t.Fatalf("PrimeNodes = %q", got)
	}
}

func TestLoadDHTInstancesFromEnvironment(t *testing.T) {
	t.Setenv("CHERRY_PICKER_CONFIG", "")
	t.Setenv("CHERRY_PICKER_DHT_INSTANCES", "96")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Discovery.Instances != 96 {
		t.Fatalf("Instances = %d, want 96", cfg.Discovery.Instances)
	}
}

func TestLoadActiveLookupFromEnvironment(t *testing.T) {
	t.Setenv("CHERRY_PICKER_CONFIG", "")
	t.Setenv("CHERRY_PICKER_DHT_ACTIVE_LOOKUP", "false")
	t.Setenv("CHERRY_PICKER_DHT_LOOKUP_NODES", "48")
	t.Setenv("CHERRY_PICKER_DHT_LOOKUP_DHTS", "2")
	t.Setenv("CHERRY_PICKER_DHT_LOOKUP_QUEUE", "4096")
	t.Setenv("CHERRY_PICKER_DHT_LOOKUP_RATE", "75")
	t.Setenv("CHERRY_PICKER_DHT_LOOKUP_WORKERS", "3")
	t.Setenv("CHERRY_PICKER_DHT_LOOKUP_FOLLOWUPS", "9")
	t.Setenv("CHERRY_PICKER_DHT_LOOKUP_SPREAD", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Discovery.ActiveLookup || cfg.Discovery.LookupNodes != 48 ||
		cfg.Discovery.LookupDHTs != 2 || cfg.Discovery.LookupQueue != 4096 ||
		cfg.Discovery.LookupRate != 75 || cfg.Discovery.LookupWorkers != 3 ||
		cfg.Discovery.LookupFollowups != 8 || !cfg.Discovery.LookupSpread {
		t.Fatalf("unexpected lookup config: %+v", cfg.Discovery)
	}
}

func TestLoadDurableIdentityAndStoragePolicyFromEnvironment(t *testing.T) {
	t.Setenv("CHERRY_PICKER_CONFIG", "")
	t.Setenv("CHERRY_PICKER_CRAWLER_ID", "jp-1")
	t.Setenv("CHERRY_PICKER_ORACLE_URL", " https://oracle.example/ ")
	t.Setenv("CHERRY_PICKER_ORACLE_API_KEY", " oracle-secret ")
	t.Setenv("CHERRY_PICKER_POLICY_SUMMARY_FILES", "2500")
	t.Setenv("CHERRY_PICKER_POLICY_SUMMARY_PATH_BYTES", "123456")
	t.Setenv("CHERRY_PICKER_POLICY_HASH_ONLY_FILES", "5000")
	t.Setenv("CHERRY_PICKER_POLICY_REJECT_FILES", "6000")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Exporter.CrawlerID != "jp-1" {
		t.Fatalf("durable identity = %+v", cfg.Exporter)
	}
	if cfg.Exporter.OracleEndpoint != "https://oracle.example/" ||
		cfg.Exporter.OracleAPIKey != "oracle-secret" {
		t.Fatalf("oracle config = %+v", cfg.Exporter)
	}
	if cfg.Filter.SummaryAboveFiles != 2500 || cfg.Filter.SummaryAbovePathBytes != 123456 ||
		cfg.Filter.HashOnlyAboveFiles != 5000 || cfg.Filter.RejectAboveFiles != 6000 {
		t.Fatalf("storage policy = %+v", cfg.Filter)
	}
}

func TestLoadOracleConfigFromJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crawler.json")
	if err := os.WriteFile(path, []byte(`{
  "role": "combined",
  "exporter": {
    "kind": "http",
    "http_endpoint": "https://storage.example/api/v1/torrents/batch",
    "api_key": "storage-secret",
    "oracle_endpoint": "https://oracle.example",
    "oracle_api_key": "oracle-secret"
  }
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CHERRY_PICKER_CONFIG", path)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Exporter.HTTPEndpoint != "https://storage.example/api/v1/torrents/batch" ||
		cfg.Exporter.APIKey != "storage-secret" ||
		cfg.Exporter.OracleEndpoint != "https://oracle.example" ||
		cfg.Exporter.OracleAPIKey != "oracle-secret" {
		t.Fatalf("exporter = %+v", cfg.Exporter)
	}
}

func TestLoadDefaultStoragePolicyIsConservative(t *testing.T) {
	t.Setenv("CHERRY_PICKER_CONFIG", "")
	t.Setenv("CHERRY_PICKER_SPOOL_DIR", "spool-test")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Filter.SummaryAboveFiles != 2000 || cfg.Filter.SummaryAbovePathBytes != 512<<10 ||
		cfg.Filter.HashOnlyAboveFiles != 0 || cfg.Filter.RejectAboveFiles != 0 {
		t.Fatalf("unexpected default storage policy: %+v", cfg.Filter)
	}
	if cfg.Exporter.SpoolMaxBytes != 4<<30 {
		t.Fatalf("default bounded spool bytes=%d", cfg.Exporter.SpoolMaxBytes)
	}
}
