-- Initial database schema for Cherry magnet link search engine
-- Requires: PostgreSQL 16+, pg_trgm extension

-- Extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Main torrent table (expect ~80M rows)
CREATE TABLE IF NOT EXISTS torrents (
    id              BIGINT GENERATED ALWAYS AS IDENTITY,
    info_hash       CHAR(40)    NOT NULL,
    name            TEXT        NOT NULL,
    piece_length    INT         NOT NULL DEFAULT 0,
    total_length    BIGINT      NOT NULL DEFAULT 0,
    file_count      INT         NOT NULL DEFAULT 0,
    is_private      BOOLEAN     NOT NULL DEFAULT FALSE,
    source          VARCHAR(32),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_torrents PRIMARY KEY (id),
    CONSTRAINT uq_torrents_info_hash UNIQUE (info_hash)
);

-- Trigrams index for fuzzy search on name
CREATE INDEX IF NOT EXISTS idx_torrents_name_trgm
    ON torrents USING GIN (name gin_trgm_ops);

-- Sort by creation time
CREATE INDEX IF NOT EXISTS idx_torrents_created
    ON torrents (created_at DESC);

-- Full-text search index for Chinese word segmentation
-- Requires zhparser extension: CREATE EXTENSION zhparser;
-- Uncomment after installing zhparser:
-- CREATE TEXT SEARCH CONFIGURATION chinese (PARSER = zhparser);
-- ALTER TEXT SEARCH CONFIGURATION chinese ADD MAPPING FOR n,v,a,i,e,l WITH simple;
-- CREATE INDEX IF NOT EXISTS idx_torrents_name_fts
--     ON torrents USING GIN (to_tsvector('chinese', name));

-- Torrent files table (expect ~1B rows, partitioned)
CREATE TABLE IF NOT EXISTS torrent_files (
    id              BIGINT GENERATED ALWAYS AS IDENTITY,
    torrent_id      BIGINT      NOT NULL,
    path_text       TEXT        NOT NULL,
    length          BIGINT      NOT NULL DEFAULT 0,

    CONSTRAINT pk_torrent_files PRIMARY KEY (id, torrent_id),
    CONSTRAINT fk_torrent_files_torrent FOREIGN KEY (torrent_id)
        REFERENCES torrents(id) ON DELETE CASCADE
) PARTITION BY HASH (torrent_id);

-- 16 hash partitions for torrent_files
CREATE TABLE IF NOT EXISTS torrent_files_p0  PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 0);
CREATE TABLE IF NOT EXISTS torrent_files_p1  PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 1);
CREATE TABLE IF NOT EXISTS torrent_files_p2  PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 2);
CREATE TABLE IF NOT EXISTS torrent_files_p3  PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 3);
CREATE TABLE IF NOT EXISTS torrent_files_p4  PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 4);
CREATE TABLE IF NOT EXISTS torrent_files_p5  PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 5);
CREATE TABLE IF NOT EXISTS torrent_files_p6  PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 6);
CREATE TABLE IF NOT EXISTS torrent_files_p7  PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 7);
CREATE TABLE IF NOT EXISTS torrent_files_p8  PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 8);
CREATE TABLE IF NOT EXISTS torrent_files_p9  PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 9);
CREATE TABLE IF NOT EXISTS torrent_files_p10 PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 10);
CREATE TABLE IF NOT EXISTS torrent_files_p11 PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 11);
CREATE TABLE IF NOT EXISTS torrent_files_p12 PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 12);
CREATE TABLE IF NOT EXISTS torrent_files_p13 PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 13);
CREATE TABLE IF NOT EXISTS torrent_files_p14 PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 14);
CREATE TABLE IF NOT EXISTS torrent_files_p15 PARTITION OF torrent_files FOR VALUES WITH (MODULUS 16, REMAINDER 15);

-- Trigrams index for file path search
CREATE INDEX IF NOT EXISTS idx_torrent_files_path
    ON torrent_files USING GIN (path_text gin_trgm_ops);

-- Full-text search for file paths (Chinese)
-- Uncomment after installing zhparser:
-- CREATE INDEX IF NOT EXISTS idx_torrent_files_path_fts
--     ON torrent_files USING GIN (to_tsvector('chinese', path_text));
