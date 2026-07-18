package app

import (
	"testing"

	"cherry-picker/internal/pipeline"
)

func TestClassifyMetadataLocale(t *testing.T) {
	tests := []struct {
		name     string
		metadata *pipeline.Metadata
		want     metadataLocaleSignals
	}{
		{
			name:     "nil",
			metadata: nil,
			want:     metadataLocaleSignals{},
		},
		{
			name:     "ascii",
			metadata: &pipeline.Metadata{Name: "ubuntu release"},
			want:     metadataLocaleSignals{},
		},
		{
			name:     "simplified Chinese name",
			metadata: &pipeline.Metadata{Name: "中文电影合集"},
			want:     metadataLocaleSignals{han: true, chineseProxy: true},
		},
		{
			name:     "supplementary Han path",
			metadata: &pipeline.Metadata{Name: "release", Files: []pipeline.MetadataFile{{PathText: "disk/𠀀.txt"}}},
			want:     metadataLocaleSignals{han: true, chineseProxy: true},
		},
		{
			name:     "Japanese with Kanji and Katakana",
			metadata: &pipeline.Metadata{Name: "東京アニメ"},
			want:     metadataLocaleSignals{han: true, kana: true},
		},
		{
			name:     "Japanese Hiragana only",
			metadata: &pipeline.Metadata{Name: "ひらがな"},
			want:     metadataLocaleSignals{kana: true},
		},
		{
			name:     "Korean Hangul",
			metadata: &pipeline.Metadata{Name: "한국 영화"},
			want:     metadataLocaleSignals{hangul: true},
		},
		{
			name:     "Korean with Hanja",
			metadata: &pipeline.Metadata{Name: "韓國 영화"},
			want:     metadataLocaleSignals{han: true, hangul: true},
		},
		{
			name: "path component fallback",
			metadata: &pipeline.Metadata{Name: "release", Files: []pipeline.MetadataFile{{
				Path: []string{"東京", "ドラマ.mkv"},
			}}},
			want: metadataLocaleSignals{han: true, kana: true},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyMetadataLocale(test.metadata); got != test.want {
				t.Fatalf("classifyMetadataLocale() = %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestMetadataLocaleStatsChain(t *testing.T) {
	var counters metadataLocaleCounters
	counters.observe(nil)
	counters.observe(&pipeline.Metadata{Name: "中文"})
	counters.observe(&pipeline.Metadata{Name: "東京アニメ"})
	counters.observe(&pipeline.Metadata{Name: "한국 영화"})
	counters.observe(&pipeline.Metadata{Name: "plain ascii"})

	snapshot := counters.snapshot()
	want := metadataLocaleSnapshot{
		classified:   4,
		han:          2,
		kana:         1,
		hangul:       1,
		chineseProxy: 1,
	}
	if snapshot != want {
		t.Fatalf("snapshot = %+v, want %+v", snapshot, want)
	}

	workerStats := make(map[string]uint64)
	snapshot.addWorkerStats(workerStats)
	wantWorkerStats := map[string]uint64{
		"metadata_locale_classified": 4,
		"metadata_name_path_han":     2,
		"metadata_name_path_kana":    1,
		"metadata_name_path_hangul":  1,
		"metadata_chinese_proxy":     1,
	}
	for key, value := range wantWorkerStats {
		if got := workerStats[key]; got != value {
			t.Errorf("workerStats[%q] = %d, want %d", key, got, value)
		}
	}

	previous := metadataLocaleSnapshot{classified: 2, han: 1, kana: 1}
	wantDelta := metadataLocaleSnapshot{classified: 2, han: 1, hangul: 1, chineseProxy: 1}
	if delta := snapshot.subtract(previous); delta != wantDelta {
		t.Fatalf("delta = %+v, want %+v", delta, wantDelta)
	}
}

func BenchmarkClassifyMetadataLocaleASCII(b *testing.B) {
	metadata := &pipeline.Metadata{
		Name: "ubuntu-linux-release",
		Files: []pipeline.MetadataFile{
			{PathText: "ubuntu/docs/readme.txt"},
			{PathText: "ubuntu/images/disk-amd64.iso"},
		},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = classifyMetadataLocale(metadata)
	}
}

func BenchmarkObserveMetadataLocaleChinese(b *testing.B) {
	metadata := &pipeline.Metadata{
		Name: "中文电影合集",
		Files: []pipeline.MetadataFile{
			{PathText: "电影/正片.mkv"},
			{PathText: "电影/字幕.srt"},
		},
	}
	var counters metadataLocaleCounters
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		counters.observe(metadata)
	}
}
