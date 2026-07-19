package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadFromFileRejectsUnknownField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crawler.json")
	if err := os.WriteFile(path, []byte(`{
  "role": "metadata",
  "exporter": {
    "kind": "http",
    "spool_dri": "/var/lib/cherry-picker/metadata-spool"
  }
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CHERRY_PICKER_CONFIG", path)

	_, err := Load()
	if err == nil {
		t.Fatal("Load() accepted an unknown JSON field")
	}
	if !strings.Contains(err.Error(), `unknown field "spool_dri"`) {
		t.Fatalf("Load() error = %q, want unknown field name", err)
	}
}

func TestLoadFromFileRejectsTrailingJSONValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crawler.json")
	if err := os.WriteFile(path, []byte(`{"role":"metadata"} {"role":"combined"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CHERRY_PICKER_CONFIG", path)

	_, err := Load()
	if err == nil {
		t.Fatal("Load() accepted a trailing JSON value")
	}
	if !strings.Contains(err.Error(), "expected exactly one JSON value") {
		t.Fatalf("Load() error = %q, want trailing-value error", err)
	}
}

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
		cfg.Discovery.SampleInfohashes || cfg.Metadata.WorkerQueueSize != 1024 ||
		cfg.Metadata.WorkerInitial != 1024 || cfg.Metadata.WorkerMin != 128 ||
		cfg.Metadata.WorkerMax != 1024 {
		t.Fatalf("unexpected 2C4G profile: %+v", cfg)
	}
}

func TestLoadFixedMetadataWorkersFromJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crawler.json")
	if err := os.WriteFile(path, []byte(`{
  "auto_tune": false,
  "metadata": {
    "enabled": true,
    "worker_initial": 384,
    "worker_min": 384,
    "worker_max": 384
  }
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CHERRY_PICKER_CONFIG", path)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.AutoTune || cfg.Metadata.WorkerInitial != 384 || cfg.Metadata.WorkerMin != 384 ||
		cfg.Metadata.WorkerMax != 384 || cfg.Metadata.WorkerQueueSize != 384 {
		t.Fatalf("unexpected fixed worker config: auto_tune=%v metadata=%+v", cfg.AutoTune, cfg.Metadata)
	}
}

func TestLoadRetryObserverDefaultsDisabledAndParsesJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crawler.json")
	if err := os.WriteFile(path, []byte(`{
  "metadata": {
    "retry_observer_enabled": true,
    "retry_observer_sample_denominator": 32,
    "retry_observer_window": "31m",
    "retry_observer_capacity": 65536
  }
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CHERRY_PICKER_CONFIG", path)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Metadata.RetryObserverEnabled || cfg.Metadata.RetryObserverSampleDenominator != 32 ||
		cfg.Metadata.RetryObserverWindow != 31*time.Minute || cfg.Metadata.RetryObserverCapacity != 65_536 {
		t.Fatalf("unexpected retry observer config: %+v", cfg.Metadata)
	}

	t.Setenv("CHERRY_PICKER_CONFIG", "")
	t.Setenv("CHERRY_PICKER_RETRY_OBSERVER_ENABLED", "false")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Metadata.RetryObserverEnabled || cfg.Metadata.RetryObserverSampleDenominator != 64 ||
		cfg.Metadata.RetryObserverWindow != 31*time.Minute || cfg.Metadata.RetryObserverCapacity != 131_072 {
		t.Fatalf("unexpected retry observer defaults: %+v", cfg.Metadata)
	}
}

func TestLoadFixedMetadataWorkersFromEnvironment(t *testing.T) {
	t.Setenv("CHERRY_PICKER_CONFIG", "")
	t.Setenv("CHERRY_PICKER_AUTO_TUNE", "false")
	t.Setenv("CHERRY_PICKER_METADATA_WORKERS_INITIAL", "320")
	t.Setenv("CHERRY_PICKER_METADATA_WORKERS_MIN", "320")
	t.Setenv("CHERRY_PICKER_METADATA_WORKERS_MAX", "320")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.AutoTune || cfg.Metadata.WorkerInitial != 320 || cfg.Metadata.WorkerMin != 320 ||
		cfg.Metadata.WorkerMax != 320 || cfg.Metadata.WorkerQueueSize != 320 {
		t.Fatalf("unexpected fixed worker env config: auto_tune=%v metadata=%+v", cfg.AutoTune, cfg.Metadata)
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

func TestLoadHeatConfigDefaultsDisabledAndUsesSecretFilePaths(t *testing.T) {
	t.Setenv("CHERRY_PICKER_CONFIG", "")
	t.Setenv("CHERRY_PICKER_HEAT_ENABLED", "true")
	t.Setenv("CHERRY_PICKER_HEAT_ENDPOINT", "https://storage.example/api/v1/heat/ingest")
	t.Setenv("CHERRY_PICKER_HEAT_CRAWLER_ID", "jp-1")
	t.Setenv("CHERRY_PICKER_HEAT_MASTER_SECRET_FILE", "/run/secrets/heat-master")
	t.Setenv("CHERRY_PICKER_HEAT_HMAC_SECRET_FILE", "/run/secrets/heat-hmac")
	t.Setenv("CHERRY_PICKER_HEAT_SPOOL_DIR", "/var/lib/cherry-picker/heat")
	t.Setenv("CHERRY_PICKER_HEAT_KNOWN_CRAWLERS", "1.1.1.1,2001:db8::/32")
	t.Setenv("CHERRY_PICKER_HEAT_SHADOW_BLOOM_ENABLED", "true")
	t.Setenv("CHERRY_PICKER_HEAT_SHADOW_BLOOM_DROP_PROBABLE_DUPLICATES", "true")
	t.Setenv("CHERRY_PICKER_HEAT_SHADOW_BLOOM_CAPACITY", "123456")
	t.Setenv("CHERRY_PICKER_HEAT_SHADOW_BLOOM_FP_PPM", "250")
	t.Setenv("CHERRY_PICKER_HEAT_SHADOW_SAMPLE_CAPACITY", "321")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Heat.Enabled || cfg.Heat.CrawlerID != "jp-1" ||
		cfg.Heat.MasterSecretFile != "/run/secrets/heat-master" ||
		cfg.Heat.HMACSecretFile != "/run/secrets/heat-hmac" || cfg.Heat.QueueCapacity != 65_536 ||
		cfg.Heat.BatchSize != 4_096 || cfg.Heat.SpoolMaxBytes != 512<<20 ||
		!cfg.Heat.ShadowBloomEnabled || !cfg.Heat.ShadowBloomDropProbableDuplicates ||
		cfg.Heat.ShadowBloomCapacity != 123456 ||
		cfg.Heat.ShadowBloomFalsePositive != 250 || cfg.Heat.ShadowBloomSampleCapacity != 321 {
		t.Fatalf("unexpected heat config: %+v", cfg.Heat)
	}
}

func TestLoadHeatShadowBloomDefaultsDisabledAndBounded(t *testing.T) {
	t.Setenv("CHERRY_PICKER_CONFIG", "")
	t.Setenv("CHERRY_PICKER_HEAT_SHADOW_BLOOM_ENABLED", "")
	t.Setenv("CHERRY_PICKER_HEAT_SHADOW_BLOOM_DROP_PROBABLE_DUPLICATES", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Heat.ShadowBloomEnabled || cfg.Heat.ShadowBloomDropProbableDuplicates ||
		cfg.Heat.ShadowBloomCapacity != 1_000_000 ||
		cfg.Heat.ShadowBloomFalsePositive != 1_000 || cfg.Heat.ShadowBloomSampleCapacity != 4_096 {
		t.Fatalf("unexpected shadow defaults: %+v", cfg.Heat)
	}
}

func TestLoadHeatShadowBloomHardDropEnablesBloom(t *testing.T) {
	t.Setenv("CHERRY_PICKER_CONFIG", "")
	t.Setenv("CHERRY_PICKER_HEAT_SHADOW_BLOOM_ENABLED", "")
	t.Setenv("CHERRY_PICKER_HEAT_SHADOW_BLOOM_DROP_PROBABLE_DUPLICATES", "true")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Heat.ShadowBloomEnabled || !cfg.Heat.ShadowBloomDropProbableDuplicates {
		t.Fatalf("hard-drop mode did not enable Bloom: %+v", cfg.Heat)
	}
}

func TestLoadHeatConfigFromJSONContainsPathsNotSecretValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crawler.json")
	if err := os.WriteFile(path, []byte(`{
  "role": "combined",
  "heat": {
    "enabled": true,
    "endpoint": "https://storage.example/api/v1/heat/ingest",
    "crawler_id": "jp-1",
    "master_secret_file": "/run/secrets/heat-master",
    "hmac_secret_file": "/run/secrets/heat-hmac",
    "spool_dir": "/var/lib/cherry-picker/heat",
		"queue_capacity": 100,
		"batch_size": 200,
		"flush_interval": "10ms",
		"shadow_bloom_enabled": true,
		"shadow_bloom_drop_probable_duplicates": true,
		"shadow_bloom_capacity": 654321,
		"shadow_bloom_fp_ppm": 500,
		"shadow_bloom_sample_capacity": 432
  }
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CHERRY_PICKER_CONFIG", path)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Heat.Enabled || cfg.Heat.HMACSecretFile != "/run/secrets/heat-hmac" ||
		cfg.Heat.QueueCapacity != 100 || cfg.Heat.BatchSize != 100 ||
		cfg.Heat.FlushInterval != 10*time.Millisecond || !cfg.Heat.ShadowBloomEnabled ||
		!cfg.Heat.ShadowBloomDropProbableDuplicates ||
		cfg.Heat.ShadowBloomCapacity != 654321 || cfg.Heat.ShadowBloomFalsePositive != 500 ||
		cfg.Heat.ShadowBloomSampleCapacity != 432 {
		t.Fatalf("unexpected JSON heat config: %+v", cfg.Heat)
	}
}
