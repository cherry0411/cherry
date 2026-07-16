package export

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
		// 写入失败：序列化到 WAL。只有 WAL 持久化成功才确认该 batch，
		// 否则把错误返回给 BatchExporter，让它保留 batch 并施加背压。
		if walErr := w.appendToWAL(batch); walErr != nil {
			log.Printf("wal: failed to write fallback: %v (original: %v)", walErr, err)
			return fmt.Errorf("wal fallback failed: %v (original: %w)", walErr, err)
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
	// 先在 append 锁内把活跃 WAL 原子重命名成快照。后续 append 会创建
	// 新文件，因此慢速 HTTP 重放不会阻塞 crawler，也不会删除并发追加的数据。
	activeFiles, err := filepath.Glob(filepath.Join(w.walDir, "wal_*.jsonl"))
	if err != nil {
		return
	}
	snapshots, _ := filepath.Glob(filepath.Join(w.walDir, "replay_*.jsonl"))
	for _, path := range activeFiles {
		if snapshot, snapshotErr := w.snapshotWAL(path); snapshotErr != nil {
			log.Printf("wal: failed to snapshot %s: %v", filepath.Base(path), snapshotErr)
		} else if snapshot != "" {
			snapshots = append(snapshots, snapshot)
		}
	}
	for _, path := range snapshots {
		w.replayFile(path)
	}
}

func (w *walSink) snapshotWAL(path string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	snapshot := filepath.Join(w.walDir, fmt.Sprintf(
		"replay_%d_%s", time.Now().UnixNano(), filepath.Base(path)))
	if err := os.Rename(path, snapshot); err != nil {
		return "", err
	}
	return snapshot, nil
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

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	total := 0
	decoder := json.NewDecoder(f)
	for {
		var entry walEntry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			// 保留文件，避免把超大或部分写入的 batch 当作空文件删除。
			log.Printf("wal: decode failed for %s: %v", filepath.Base(path), err)
			return
		}
		if len(entry.Events) == 0 {
			continue
		}
		if err := w.inner.WriteBatch(ctx, entry.Events); err != nil {
			log.Printf("wal: replay failed for %s: %v", filepath.Base(path), err)
			return
		}
		total += len(entry.Events)
	}

	if total == 0 {
		os.Remove(path)
		return
	}
	f.Close()
	os.Remove(path)
	log.Printf("wal: replayed %d events from %s", total, filepath.Base(path))
}
