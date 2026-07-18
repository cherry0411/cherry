#!/usr/bin/env sh
# Merge a compact Cherry catalog export into the current PostgreSQL schema.
#
# Usage:
#   ./scripts/import-remote.sh compact-torrents.copy.gz
#
# The gzip-compressed PostgreSQL binary COPY stream has exactly these columns:
#   info_hash,name,total_length,file_count,created_at,detail_payload
#
# Produce it from another current Cherry database with:
#   psql -X -q ... -c "\copy (SELECT t.info_hash,t.name,t.total_length,t.file_count,t.created_at,d.payload FROM torrents t JOIN torrent_details d ON d.torrent_id=t.id ORDER BY t.id) TO STDOUT WITH (FORMAT binary)" | gzip > compact-torrents.copy.gz
#
# This script deliberately does not accept the old torrents/torrent_files
# exports: those tables and the old wide catalog fields no longer exist.
# Dependencies: psql, gzip. Authentication uses standard libpq PG* variables;
# PG_HOST/PG_PORT/PG_DB/PG_USER/PG_PASSWORD remain accepted as aliases.

set -eu

ARCHIVE="${1:-}"
if [ -z "$ARCHIVE" ]; then
  echo "Usage: $0 <compact-torrents.copy.gz>" >&2
  exit 2
fi
if [ ! -f "$ARCHIVE" ]; then
  echo "Error: compact export not found: $ARCHIVE" >&2
  exit 2
fi
command -v psql >/dev/null || { echo "Error: psql is required" >&2; exit 2; }
command -v gzip >/dev/null || { echo "Error: gzip is required" >&2; exit 2; }
gzip -t -- "$ARCHIVE"

export PGHOST="${PGHOST:-${PG_HOST:-localhost}}"
export PGPORT="${PGPORT:-${PG_PORT:-5432}}"
export PGDATABASE="${PGDATABASE:-${PG_DB:-cherry}}"
export PGUSER="${PGUSER:-${PG_USER:-postgres}}"
if [ -n "${PG_PASSWORD:-}" ] && [ -z "${PGPASSWORD:-}" ]; then
  export PGPASSWORD="$PG_PASSWORD"
fi

psql_cmd() {
  psql -X -v ON_ERROR_STOP=1 "$@"
}

# COPY and the merge use separate psql sessions, so a uniquely named UNLOGGED
# staging table is used instead of a session-local temp table. The EXIT trap
# removes it after success or failure; authoritative writes happen in one SQL
# transaction.
STAGE="_cherry_compact_import_$$_$(date +%s)"
cleanup() {
  psql_cmd -qAtc "DROP TABLE IF EXISTS public.\"$STAGE\"" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "Creating compact import staging table..."
psql_cmd -q <<SQL
DO \$preflight\$
BEGIN
    IF to_regclass('public.torrent_details') IS NULL OR
       NOT EXISTS (
           SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'torrents'
              AND column_name = 'id') THEN
        RAISE EXCEPTION 'target is not the current compact Cherry schema';
    END IF;
END
\$preflight\$;

CREATE UNLOGGED TABLE public."$STAGE" (
    info_hash       bytea       NOT NULL,
    name            text        NOT NULL,
    total_length    bigint      NOT NULL,
    file_count      integer     NOT NULL,
    created_at      timestamptz NOT NULL,
    detail_payload  bytea       NOT NULL
);
SQL

echo "Loading compressed binary COPY stream..."
gzip -dc -- "$ARCHIVE" | psql_cmd -q -c "\copy public.\"$STAGE\" (info_hash,name,total_length,file_count,created_at,detail_payload) FROM STDIN WITH (FORMAT binary)"

echo "Validating and merging in one transaction..."
psql_cmd <<SQL
BEGIN;

DO \$validation\$
BEGIN
    IF EXISTS (
        SELECT 1 FROM public."$STAGE"
         WHERE octet_length(info_hash) <> 20) THEN
        RAISE EXCEPTION 'compact export contains an invalid info hash';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public."$STAGE"
         WHERE total_length < 0 OR file_count <= 0 OR file_count > 10000) THEN
        RAISE EXCEPTION 'compact export contains invalid catalog bounds';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public."$STAGE"
         WHERE octet_length(detail_payload) NOT BETWEEN 3 AND 67108864
            OR get_byte(detail_payload, 0) <> 1) THEN
        RAISE EXCEPTION 'compact export contains an unsupported detail payload';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public."$STAGE"
         GROUP BY info_hash HAVING count(*) > 1) THEN
        RAISE EXCEPTION 'compact export contains duplicate info hashes';
    END IF;
END
\$validation\$;

CREATE TEMP TABLE _inserted_ids (
    torrent_id bigint PRIMARY KEY,
    info_hash bytea UNIQUE NOT NULL
) ON COMMIT DROP;

WITH inserted AS (
    INSERT INTO torrents (info_hash, name, total_length, file_count, created_at)
    SELECT info_hash, name, total_length, file_count, created_at
      FROM public."$STAGE"
     ORDER BY info_hash
    ON CONFLICT (info_hash) DO NOTHING
    RETURNING id, info_hash
)
INSERT INTO _inserted_ids (torrent_id, info_hash)
SELECT id, info_hash FROM inserted;

INSERT INTO torrent_details (torrent_id, payload)
SELECT inserted.torrent_id, staged.detail_payload
  FROM _inserted_ids AS inserted
  JOIN public."$STAGE" AS staged
    ON staged.info_hash = inserted.info_hash;

-- A catalog row is authoritative over an older processed-only decision.
DELETE FROM metadata_decisions AS decision
 USING public."$STAGE" AS staged
 WHERE decision.info_hash = staged.info_hash
   AND EXISTS (
       SELECT 1 FROM torrents AS torrent
        WHERE torrent.info_hash = decision.info_hash);

-- Enqueue only newly inserted metadata. Existing rows are unchanged and do
-- not need a new projection generation.
INSERT INTO search_outbox (
    torrent_id, generation, enqueued_at, available_at, lease_owner,
    lease_until, attempt_count, last_error, updated_at)
SELECT torrent_id, 1, NOW(), NOW(), NULL, NULL, 0, NULL, NOW()
  FROM _inserted_ids
ON CONFLICT (torrent_id) DO UPDATE
    SET generation = search_outbox.generation + 1,
        enqueued_at = NOW(),
        available_at = NOW(),
        lease_owner = NULL,
        lease_until = NULL,
        attempt_count = 0,
        last_error = NULL,
        updated_at = NOW();

SELECT (SELECT count(*) FROM public."$STAGE") AS incoming,
       (SELECT count(*) FROM _inserted_ids) AS inserted;
COMMIT;
SQL

echo "Final compact catalog counts:"
psql_cmd -c "SELECT (SELECT count(*) FROM torrents) AS torrents, (SELECT count(*) FROM torrent_details) AS details, (SELECT count(*) FROM search_outbox) AS search_backlog"
echo "Done. Meilisearch projection will be delivered by the durable outbox worker."
