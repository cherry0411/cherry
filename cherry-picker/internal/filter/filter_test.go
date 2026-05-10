package filter_test

import (
	"testing"

	"cherry-picker/internal/filter"
	"cherry-picker/internal/pipeline"
)

// ---------- TooManyFiles ----------

func TestTooManyFiles_Reject(t *testing.T) {
	rule := filter.TooManyFiles(10_000)
	m := &pipeline.Metadata{FileCount: 10_001}
	if r := rule(m); r != filter.ReasonTooManyFiles {
		t.Errorf("expected %q, got %q", filter.ReasonTooManyFiles, r)
	}
}

func TestTooManyFiles_PassAtThreshold(t *testing.T) {
	rule := filter.TooManyFiles(10_000)
	m := &pipeline.Metadata{FileCount: 10_000}
	if r := rule(m); r != filter.ReasonPass {
		t.Errorf("expected pass at exact threshold, got %q", r)
	}
}

func TestTooManyFiles_PassBelow(t *testing.T) {
	rule := filter.TooManyFiles(10_000)
	m := &pipeline.Metadata{FileCount: 500}
	if r := rule(m); r != filter.ReasonPass {
		t.Errorf("expected pass, got %q", r)
	}
}

// ---------- NonChineseHighFileCount ----------

func TestNonChinese_RejectNoChineseHighCount(t *testing.T) {
	rule := filter.NonChineseHighFileCount(1000)
	m := &pipeline.Metadata{
		Name:      "pack",
		FileCount: 1001,
		Files:     makeFiles(1001, "file.txt"),
	}
	if r := rule(m); r != filter.ReasonNonChinese {
		t.Errorf("expected %q, got %q", filter.ReasonNonChinese, r)
	}
}

func TestNonChinese_PassWithChineseName(t *testing.T) {
	rule := filter.NonChineseHighFileCount(1000)
	m := &pipeline.Metadata{
		Name:      "中文包",
		FileCount: 1001,
		Files:     makeFiles(1001, "file.txt"),
	}
	if r := rule(m); r != filter.ReasonPass {
		t.Errorf("expected pass when torrent name has Chinese, got %q", r)
	}
}

func TestNonChinese_PassWithChineseFilePath(t *testing.T) {
	rule := filter.NonChineseHighFileCount(1000)
	m := &pipeline.Metadata{
		Name:      "pack",
		FileCount: 1001,
		Files: []pipeline.MetadataFile{
			{Path: []string{"第一集.mp4"}, PathText: "第一集.mp4"},
		},
	}
	if r := rule(m); r != filter.ReasonPass {
		t.Errorf("expected pass when a file path has Chinese, got %q", r)
	}
}

func TestNonChinese_PassAtThreshold(t *testing.T) {
	rule := filter.NonChineseHighFileCount(1000)
	m := &pipeline.Metadata{
		Name:      "pack",
		FileCount: 1000,
		Files:     makeFiles(1000, "file.txt"),
	}
	if r := rule(m); r != filter.ReasonPass {
		t.Errorf("expected pass at exact threshold, got %q", r)
	}
}

// ---------- NumericOnlyFileNames ----------

func TestNumeric_RejectAllNumeric(t *testing.T) {
	rule := filter.NumericOnlyFileNames(100)
	m := &pipeline.Metadata{
		FileCount: 101,
		Files: []pipeline.MetadataFile{
			{Path: []string{"001.jpg"}, PathText: "001.jpg"},
			{Path: []string{"002.jpg"}, PathText: "002.jpg"},
			{Path: []string{"003.jpg"}, PathText: "003.jpg"},
		},
	}
	if r := rule(m); r != filter.ReasonNumericFileNames {
		t.Errorf("expected %q, got %q", filter.ReasonNumericFileNames, r)
	}
}

func TestNumeric_PassWhenOneNonNumeric(t *testing.T) {
	rule := filter.NumericOnlyFileNames(100)
	m := &pipeline.Metadata{
		FileCount: 101,
		Files: []pipeline.MetadataFile{
			{Path: []string{"001.jpg"}, PathText: "001.jpg"},
			{Path: []string{"readme.txt"}, PathText: "readme.txt"},
		},
	}
	if r := rule(m); r != filter.ReasonPass {
		t.Errorf("expected pass when a file has non-numeric name, got %q", r)
	}
}

func TestNumeric_PassAtThreshold(t *testing.T) {
	rule := filter.NumericOnlyFileNames(100)
	m := &pipeline.Metadata{
		FileCount: 100,
		Files:     makeFiles(100, "001.jpg"),
	}
	if r := rule(m); r != filter.ReasonPass {
		t.Errorf("expected pass at exact threshold, got %q", r)
	}
}

func TestNumeric_PassWithNoFiles(t *testing.T) {
	rule := filter.NumericOnlyFileNames(100)
	m := &pipeline.Metadata{FileCount: 101, Files: nil}
	if r := rule(m); r != filter.ReasonPass {
		t.Errorf("expected pass for empty file list, got %q", r)
	}
}

// ---------- Chain ----------

func TestChain_FirstRuleWins(t *testing.T) {
	c := filter.NewChain()
	c.Add("too_many", filter.TooManyFiles(10_000))
	c.Add("non_cn", filter.NonChineseHighFileCount(1000))

	// Both rules would fire, but TooManyFiles comes first.
	m := &pipeline.Metadata{Name: "x", FileCount: 20_000, Files: makeFiles(20_000, "f.txt")}
	if r := c.Apply(m); r != filter.ReasonTooManyFiles {
		t.Errorf("expected first rule to win, got %q", r)
	}
}

func TestChain_PassThroughAllRules(t *testing.T) {
	c := filter.NewChain()
	c.Add("too_many", filter.TooManyFiles(10_000))
	c.Add("non_cn", filter.NonChineseHighFileCount(1000))
	c.Add("numeric", filter.NumericOnlyFileNames(100))

	m := &pipeline.Metadata{Name: "中文包", FileCount: 50, Files: makeFiles(50, "doc.pdf")}
	if r := c.Apply(m); r != filter.ReasonPass {
		t.Errorf("expected pass, got %q", r)
	}
}

func TestChain_NilMetadata(t *testing.T) {
	c := filter.NewChain()
	c.Add("too_many", filter.TooManyFiles(10_000))
	if r := c.Apply(nil); r != filter.ReasonPass {
		t.Errorf("expected pass for nil metadata, got %q", r)
	}
}

func TestChain_EmptyChain(t *testing.T) {
	c := filter.NewChain()
	m := &pipeline.Metadata{FileCount: 999_999}
	if r := c.Apply(m); r != filter.ReasonPass {
		t.Errorf("empty chain should always pass, got %q", r)
	}
}

// ---------- helper ----------

func makeFiles(n int, pathText string) []pipeline.MetadataFile {
	files := make([]pipeline.MetadataFile, n)
	for i := range files {
		files[i] = pipeline.MetadataFile{
			Path:     []string{pathText},
			PathText: pathText,
		}
	}
	return files
}
