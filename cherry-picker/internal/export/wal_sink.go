package export

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"cherry-picker/internal/pipeline"
)

// walSink 包装另一个 Sink，当写入失败时将 batch 追加到本地 WAL 文件。
// 后台 goroutine 每30秒扫描 WAL 目录，重放失败的 batch。
// WAL 文件按小时轮转，超过24小时的文件自动删除。
type walSink struct {
	inner  Sink
	walDir string
	mu     sync.Mutex
	done   chan struct{}
}

type walEntry struct {
	Events []pipeline.Event `json:"events"`
}

func newWalSink(inner Sink, walDir string) (*walSink, error) {
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: failed to create dir %s: %w", walDir, err)
	}
	s := &walSink{
		inner:  inner,
		walDir: walDir,
		done:   make(chan struct{}),
	}
	go s.replayLoop()
	return s, nil
}

func (w *walSink) WriteBatch(ctx context.Context, batch []pipeline.Event) error {
	if err := w.inner.WriteBatch(ctx, batch); err != nil {
		// 写入失败：序列化到 WAL 文件，不向上报错（避免 BatchExporter 重置 batch）
		if walErr := w.appendToWAL(batch); walErr != nil {
			log.Printf("wal: failed to write fallback: %v (original: %v)", walErr, err)
		} else {
			log.Printf("wal: %d events queued for retry (original error: %v)", len(batch), err)
		}
		return nil
	}
	return nil
}

func (w *walSink) Close(ctx context.Context) error {
	close(w.done)
	return w.inner.Close(ctx)
}

// appendToWAL 将 batch 追加到当前小时的 WAL 文件（加锁，线程安全）。
func (w *walSink) appendToWAL(batch []pipeline.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	filename := fmt.Sprintf("wal_%s.jsonl", time.Now().UTC().Format("2006010215"))
	f, err := os.OpenFile(filepath.Join(w.walDir, filename), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	data, err := json.Marshal(walEntry{Events: batch})
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = f.Write(data)
	return err
}

// replayLoop 每30秒扫描 WAL 目录，重放失败的 batch，超过24小时的文件删除。
func (w *walSink) replayLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
			w.replayAll()
		}
	}
}

func (w *walSink) replayAll() {
	files, err := filepath.Glob(filepath.Join(w.walDir, "wal_*.jsonl"))
	if err != nil {
		return
	}
	for _, path := range files {
		w.replayFile(path)
	}
}

func (w *walSink) replayFile(path string) {
	info, err := os.Stat(path)
	if err != nil {
		return
	}
	// 超过24小时的 WAL 文件直接删除（数据已过期，意义不大）
	if time.Since(info.ModTime()) > 24*time.Hour {
		os.Remove(path)
		log.Printf("wal: expired file deleted: %s", filepath.Base(path))
		return
	}

	// 尝试重放：逐行读取，整文件成功后删除
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	var batches [][]pipeline.Event
	scanner := bufio.NewScanner(f)
	// 单行最大 8MB（防止超大 batch 导致 scanner buffer 溢出）
	scanner.Buffer(make([]byte, 8*1024*1024), 8*1024*1024)
	for scanner.Scan() {
		var entry walEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}
		if len(entry.Events) > 0 {
			batches = append(batches, entry.Events)
		}
	}

	if len(batches) == 0 {
		os.Remove(path)
		return
	}

	// 重放所有 batch，任何失败都停止（等下次重试）
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	allOK := true
	total := 0
	for _, batch := range batches {
		if err := w.inner.WriteBatch(ctx, batch); err != nil {
			log.Printf("wal: replay failed for %s: %v", filepath.Base(path), err)
			allOK = false
			break
		}
		total += len(batch)
	}

	if allOK {
		f.Close()
		os.Remove(path)
		log.Printf("wal: replayed %d events from %s", total, filepath.Base(path))
	}
}
