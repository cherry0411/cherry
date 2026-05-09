package pipeline

import "time"

type EventType string

const (
	EventPeerDiscovered  EventType = "peer_discovered"
	EventMetadataFetched EventType = "metadata_fetched"
	EventWorkerStats     EventType = "worker_stats"
)

type Event struct {
	Type       EventType         `json:"type"`
	Timestamp  time.Time         `json:"timestamp"`
	InstanceID string            `json:"instance_id"`
	Source     string            `json:"source,omitempty"`
	InfoHash   string            `json:"info_hash,omitempty"`
	IP         string            `json:"ip,omitempty"`
	Port       int               `json:"port,omitempty"`
	Metadata   *Metadata         `json:"metadata,omitempty"`
	Stats      map[string]uint64 `json:"stats,omitempty"`
	Error      string            `json:"error,omitempty"`
}

type Metadata struct {
	Name        string         `json:"name,omitempty"`
	PieceLength int            `json:"piece_length,omitempty"`
	Length      int64          `json:"length,omitempty"`
	FileCount   int            `json:"file_count,omitempty"`
	Private     bool           `json:"private,omitempty"`
	Files       []MetadataFile `json:"files,omitempty"`
}

type MetadataFile struct {
	Path     []string `json:"path"`
	PathText string   `json:"path_text,omitempty"`
	Length   int64    `json:"length"`
}
