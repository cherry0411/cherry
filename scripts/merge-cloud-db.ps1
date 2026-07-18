# This helper merged the pre-compact torrents/torrent_files schema and cannot
# preserve the current surrogate IDs, compact detail payloads, or durable
# search outbox by restoring a data-only dump. Keep the filename as a safe
# guard for old operator notes, but never attempt the legacy merge.

$ErrorActionPreference = "Stop"

throw @"
scripts/merge-cloud-db.ps1 is retired for the compact Cherry schema.

Export the source database as the six-column binary COPY stream documented in:
  scripts/import-remote.sh

Then run that script against the target database. It remaps surrogate IDs,
copies torrent_details atomically, deduplicates by the 20-byte info hash, and
enqueues newly inserted rows through the durable Meilisearch outbox.
"@
