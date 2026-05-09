-- Initial database schema for Cherry magnet link search engine
-- Requires: PostgreSQL 16+, pg_trgm extension

-- Extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Main torrent table
CREATE TABLE IF NOT EXISTS torrents (
    info_hash       VARCHAR(40) NOT NULL,
    name            TEXT        NOT NULL,
    piece_length    INT         NOT NULL,
    total_length    BIGINT      NOT NULL,
    file_count      INT         NOT NULL,
    is_private      BOOLEAN     NOT NULL DEFAULT FALSE,
    source          VARCHAR(32),
    peer_count      INT         NOT NULL DEFAULT 0,
    peer_updated_at TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_torrents PRIMARY KEY (info_hash)
);

CREATE INDEX IF NOT EXISTS idx_torrents_name_trgm
    ON torrents USING GIN (name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_torrents_created
    ON torrents (created_at);

CREATE INDEX IF NOT EXISTS idx_torrents_peer_count
    ON torrents (peer_count);

-- Torrent files table
CREATE TABLE IF NOT EXISTS torrent_files (
    info_hash       VARCHAR(40) NOT NULL,
    path_text       TEXT        NOT NULL,
    length          BIGINT      NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_torrent_files_info_hash
    ON torrent_files (info_hash);

CREATE INDEX IF NOT EXISTS idx_torrent_files_path
    ON torrent_files USING GIN (path_text gin_trgm_ops);


-- Torrent request table
CREATE TABLE IF NOT EXISTS torrent_requests (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    info_hash       VARCHAR(40) NOT NULL,
    status          VARCHAR(16) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_torrent_requests_hash
    ON torrent_requests (info_hash);

CREATE INDEX IF NOT EXISTS idx_torrent_requests_status
    ON torrent_requests (status);
