#!/usr/bin/env bash
# scripts/import-remote.sh
# Usage: ./scripts/import-remote.sh <path/to/torrents.csv.gz> <path/to/torrent_files.csv.gz>
# Merges a remote Postgres export into the local cherry database.
# Deduplication is handled by ON CONFLICT DO NOTHING on info_hash.
#
# Dependencies: psql, gzip
# Environment variables (optional, defaults shown):
#   PG_HOST     localhost
#   PG_PORT     5432
#   PG_DB       cherry
#   PG_USER     postgres
#   PG_PASSWORD cindy131120

set -euo pipefail

TORRENTS_GZ="${1:-}"
FILES_GZ="${2:-}"

if [[ -z "$TORRENTS_GZ" || -z "$FILES_GZ" ]]; then
  echo "Usage: $0 <torrents.csv.gz> <torrent_files.csv.gz>"
  exit 1
fi

if [[ ! -f "$TORRENTS_GZ" ]]; then
  echo "Error: torrents file not found: $TORRENTS_GZ"
  exit 1
fi

if [[ ! -f "$FILES_GZ" ]]; then
  echo "Error: torrent_files file not found: $FILES_GZ"
  exit 1
fi

export PGHOST="${PG_HOST:-localhost}"
export PGPORT="${PG_PORT:-5432}"
export PGDATABASE="${PG_DB:-cherry}"
export PGUSER="${PG_USER:-postgres}"
export PGPASSWORD="${PG_PASSWORD:-cindy131120}"

psql_cmd() {
  psql -v ON_ERROR_STOP=1 "$@"
}

echo "=== Step 1: Create temp tables ==="
psql_cmd <<'SQL'
CREATE TEMP TABLE _tmp_torrents (
    info_hash    VARCHAR(40)      NOT NULL,
    name         TEXT             NOT NULL,
    piece_length INTEGER          NOT NULL DEFAULT 0,
    total_length BIGINT           NOT NULL DEFAULT 0,
    file_count   INTEGER          NOT NULL DEFAULT 0,
    is_private   BOOLEAN          NOT NULL DEFAULT false,
    source       VARCHAR(32),
    peer_count   INTEGER          NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE TEMP TABLE _tmp_files (
    info_hash  VARCHAR(40) NOT NULL,
    path_text  TEXT        NOT NULL,
    length     BIGINT      NOT NULL DEFAULT 0
);
SQL

echo "=== Step 2: COPY torrents into temp table ==="
gunzip -c "$TORRENTS_GZ" | psql_cmd -c "\copy _tmp_torrents (info_hash, name, piece_length, total_length, file_count, is_private, source, peer_count, created_at) FROM STDIN WITH CSV"

echo "=== Step 3: COPY torrent_files into temp table ==="
gunzip -c "$FILES_GZ" | psql_cmd -c "\copy _tmp_files (info_hash, path_text, length) FROM STDIN WITH CSV"

echo "=== Step 4: Count incoming data ==="
psql_cmd <<'SQL'
SELECT
    (SELECT COUNT(*) FROM _tmp_torrents) AS remote_torrents,
    (SELECT COUNT(*) FROM _tmp_files)    AS remote_files;
SQL

echo "=== Step 5: Insert new torrents (skip duplicates by info_hash) ==="
psql_cmd <<'SQL'
WITH inserted AS (
    INSERT INTO torrents (info_hash, name, piece_length, total_length, file_count, is_private, source, peer_count, created_at)
    SELECT info_hash, name, piece_length, total_length, file_count, is_private, source, peer_count, created_at
    FROM _tmp_torrents
    ON CONFLICT (info_hash) DO NOTHING
    RETURNING info_hash
)
SELECT COUNT(*) AS newly_inserted_torrents FROM inserted;
SQL

echo "=== Step 6: Insert files for torrents that had no files before this import ==="
# torrent_files has no unique constraint, so we only insert for hashes that
# did not already have file rows — this prevents duplicating files for
# pre-existing torrents.
psql_cmd <<'SQL'
INSERT INTO torrent_files (info_hash, path_text, length)
SELECT f.info_hash, f.path_text, f.length
FROM _tmp_files f
WHERE NOT EXISTS (
    SELECT 1 FROM torrent_files tf WHERE tf.info_hash = f.info_hash
)
AND EXISTS (
    SELECT 1 FROM torrents t WHERE t.info_hash = f.info_hash
);

SELECT COUNT(*) AS inserted_files
FROM (
    SELECT DISTINCT f.info_hash
    FROM _tmp_files f
    WHERE NOT EXISTS (
        SELECT 1 FROM torrent_files tf
        WHERE tf.info_hash = f.info_hash
    )
) sub;
SQL

echo "=== Step 7: Final counts ==="
psql_cmd <<'SQL'
SELECT
    (SELECT COUNT(*) FROM torrents)      AS total_torrents,
    (SELECT COUNT(*) FROM torrent_files) AS total_files;
SQL

echo "=== Done. Temp tables dropped automatically at end of session. ==="
