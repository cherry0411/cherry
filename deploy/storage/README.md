# Dedicated storage/search deployment

This bundle is the conservative 2C4G starting point for PostgreSQL-authoritative, zero-raw
metadata storage and rebuildable Meilisearch projection. It is an experimental
baseline, not a final sizing or tuning declaration.

## Security and data flow

- PostgreSQL and Meilisearch exist only on Docker's internal network.
- The API binds only to storage-host loopback (`127.0.0.1:5070`).
- Each crawler opens a persistent SSH local forward and exports to its own
  `127.0.0.1:15070`; the API key and normalized metadata do not cross the
  public Internet in plaintext.
- The storage firewall permits SSH only. Public search is added later behind
  HTTPS, with crawler ingest and outbox administration still separately
  authenticated.
- Meili is a projection. PostgreSQL plus crawler durable receipts are the
  authority; no raw bencode, `pieces`, or raw metadata backup is permitted.

```text
SG/JP crawler durable spool
  -> localhost:15070
  -> encrypted SSH forward
  -> storage localhost:5070
  -> PostgreSQL commit + receipt + compact search_outbox marker
  -> asynchronous Meili task (delete marker only after task succeeded)

SG/JP identity-reduced heat spool
  -> POST /api/v1/heat/batches (canonical CHHT v2 + UTC hour + HMAC)
  -> /srv/cherry/api/heat/heat-YYYY-MM-DD.sqlite3 (WAL, synchronous FULL)
  -> disposable /srv/cherry/api/heat/heat-rolling-24h.sqlite3
  -> hourly exact-unique Meili partial PUT (heat24h, previous complete hour)
  -> 64 immutable compressed PostgreSQL frames after the UTC grace window
  -> resumable daily Meili partial PUT (heat3d/7d/15d only)
```

The heat body can represent only `info_hash`, UTC hour and a stable rolling actor
fingerprint; raw IPs, ports, node IDs and raw metadata are not accepted. The
storage API re-HMACs the actor with a storage-only daily key before writing the
daily file. The stable rolling token exists only in the disposable rolling DB
for at most 24 hours; backup and restore verification fail if that DB appears.
The rolling authority has independent fail-closed watermarks: 5 GiB maximum by
default and 2 GiB minimum filesystem free space. Ingest over either watermark
returns HTTP 507 without advancing the crawler spool; metadata ingest remains
independent.
`Heat__ExpectedCrawlerIds` is a completeness gate: a day missing either SG or
JP is sealed `partial`. Its observations are excluded, projection advances it
as explicitly unknown, and each search reports the actual complete-day count
for the selected window.

## Bootstrap

Clone the repository at `/opt/cherry`. Before running the bootstrap, configure
an off-host rclone destination or select the exact audited no-backup opt-out
documented below. The exposed remote must be `type = crypt`; its
underlying remote must live in another provider account, region, or failure
domain. A second directory on the same server does not satisfy the gate.

```bash
sudo install -d -m 0700 /etc/rclone
sudo touch /etc/rclone/cherry.conf
sudo chmod 0600 /etc/rclone/cherry.conf
sudo rclone --config /etc/rclone/cherry.conf config
sudo install -m 0600 /opt/cherry/deploy/storage/backup.env.example \
  /etc/cherry-backup.env
sudoedit /etc/cherry-backup.env
```

Create a provider remote first and a crypt remote over it. The rclone config
contains the crypt decryption key: store the exact file in a password manager
or other independent secret escrow outside both the storage host and backup
provider account. Merely backing it up through itself is unrecoverable. Only
after verifying the escrowed copy, bind an acknowledgement to the exact local
configuration (the marker contains a digest, not a secret):

```bash
sudo sh -c 'sha256sum /etc/rclone/cherry.conf | awk "{print \$1}" \
  > /etc/cherry-rclone-key-escrowed'
sudo chmod 0600 /etc/cherry-rclone-key-escrowed
```

Changing either remote or crypt password invalidates this marker and closes
the authority gate until the new config is independently escrowed. Do not
commit or print either configuration. Then run:

```bash
sudo REPO_DIR=/opt/cherry bash /opt/cherry/scripts/setup-storage-server.sh
```

The script creates `/opt/cherry/deploy/storage/.env` with mode `0600`, random
PostgreSQL/API/Meili credentials, and two independent crawler transport keys.
It separately creates the raw 32-byte mode-`0600` files
`/etc/cherry-secrets/heat-hmac-sg`, `heat-hmac-jp`, and `heat-storage-daily`.
Generate `heat-actor-master` outside storage and install a byte-identical copy on both crawlers so the
same actor merges across regions. Install only `heat-hmac-sg` on SG and only
`heat-hmac-jp` on JP; sharing them would let one crawler forge the other's
daily completion proof. The actor key is never created or configured on the backend,
while its environment contains the Base64 encodings of both transport keys.
The script initializes the
coverage start to the current UTC date and the crawler IDs to
`sg-crawler-01`/`jp-crawler-01`; change those IDs before production ingest if
the crawler configuration uses different stable values. Do not copy the whole
environment file into logs, Git, or a crawler host. Verify only the actor
master's SHA-256 matches across SG/JP; the transport-key hashes must differ.
Never substitute the Base64 text or metadata API key for a raw crawler secret.
The setup fails closed if an actor master is found on storage. Backups contain
only an allowlisted `.env` and `cherry-backup.env`, never the raw
`cherry-secrets` directory. The `.env` retains the storage-only daily re-HMAC
key so an in-progress daily SQLite source resumes without double counting; in
the absence of the crawler master and rolling DB, it cannot link daily
pseudonyms back to stable actors or source IPs.
Validate without printing secrets:

```bash
cd /opt/cherry
sudo docker compose --env-file deploy/storage/.env \
  -f deploy/storage/compose.yml ps
curl --fail --silent http://127.0.0.1:5070/health
sudo cat /var/lib/cherry-backup/offsite-ready
sudo systemctl list-timers 'cherry-storage-*'
```

The setup exits non-zero and stops the API if the crypt config lacks a matching
escrow acknowledgement or if the remote cannot be written and read back. Do
not install crawler tunnel keys, and therefore do not let crawlers delete
acknowledged spool ranges, until `offsite-ready` exists. That marker means an
immutable base was uploaded and its manifest was read back; it does **not**
claim an application or PITR restore was completed. A later timer failure must
page an operator through the host's normal systemd monitoring; inspect it with
`systemctl --failed` and `journalctl -u 'cherry-storage-*'`.

The first API start applies PostgreSQL migrations and seeds one coalesced
search outbox marker per existing torrent. A crawler must not delete a local
spool segment until the durable endpoint returns its strict committed receipt.

## Per-crawler SSH tunnel

On each crawler, create a key used only for this forward:

```bash
sudo install -d -m 0700 /var/lib/cherry-picker/tunnel
sudo ssh-keygen -q -t ed25519 -N '' \
  -f /var/lib/cherry-picker/tunnel/storage_ed25519
sudo cat /var/lib/cherry-picker/tunnel/storage_ed25519.pub
```

Append the public key on storage as one line (use a distinct key per region):

```text
no-agent-forwarding,no-X11-forwarding,no-pty,permitopen="127.0.0.1:5070" ssh-ed25519 AAAA... crawler-sg
```

After verifying the storage host key out of band, install
`cherry-storage-tunnel@.service` on the crawler and create
`/etc/cherry-storage-tunnel/sg` or `jp` from the example environment file.
The exporter URL becomes
`http://127.0.0.1:15070/api/v1/torrents/batch`; the experiment oracle remains
an independent loopback endpoint. The heat exporter uses
`http://127.0.0.1:15070/api/v1/heat/batches` and must send its durable epoch,
start/end sequence, payload SHA-256 and HMAC headers. It may delete a spool
range only after HTTP 200; a lost response must replay identical bytes and
headers. An authenticated canonical batch for a closed day receives HTTP 410
with `code=day_closed` and a complete matching negative receipt; the crawler
may advance only after all receipt fields match, while recording the rejected
records/batch as explicit coverage loss.

## Resource and measurement gates

The initial 2C4G caps deliberately leave OS page-cache and failure headroom:

- PostgreSQL: 1280 MiB container, 384 MiB shared buffers, synchronous durable commit.
- Meili: 1152 MiB container, 640 MiB indexing limit, one indexing thread.
- API/dedupe/outbox/heat accumulator: 576 MiB container.

The 3008 MiB combined hard ceiling leaves roughly 652 MiB of this host's measured
3659.9 MiB `MemTotal` for the kernel, Docker, SSH, page cache, and short-lived
work. The ceilings are not
an invitation to fill every cgroup; real RSS/page-cache and pressure-stall data
decide subsequent reallocations.

CPU limits are scheduling ceilings and may overlap; memory limits are hard
guards. Change only one parameter per recorded experiment. Before declaring a
new baseline, report PostgreSQL ACK p95/WAL bytes per retained metadata,
outbox depth/oldest age/recovery slope, Meili task p95/docs per second/RSS,
index bytes per document, and query quality. Meili compaction requires free
temporary space near the index size, so do not fill the data disk past the
pre-registered high-water mark.

PostgreSQL backup/PITR and off-host retention are hard-gated by default.
Meili snapshots are intentionally omitted because the index can be rebuilt
from PostgreSQL; duplicating it wastes disk.

For the explicitly accepted short-lived experiment where all data may be lost,
run bootstrap with
`CHERRY_ALLOW_UNBACKED_AUTHORITY=I_ACCEPT_DATA_LOSS`. The exact opt-out disables
base-backup, restore-drill, and WAL-upload timers, starts PostgreSQL with
`archive_mode=off`, and writes
`/var/lib/cherry-backup/UNBACKED_AUTHORITY`. PostgreSQL and heat are then single
copies: loss of this host means starting collection from zero. No archive or
backup package is produced. Any other value fails closed.

## Backup and PITR policy

PostgreSQL archives each completed WAL segment locally as gzip level 1. A
low-priority five-minute timer uploads immutable WAL files and SHA-256
sidecars, verifies the remote logical sizes, and only then removes local files
older than the configured 24-hour cushion. `archive_timeout=300s` bounds the
idle-traffic archive delay without storing padded 16 MiB files off host.

The weekly base job performs these operations in this order:

1. Stop only the API, archive the entire heat directory, and restart the API.
2. Take an online, checksummed PostgreSQL 17 base backup at a bounded rate.
3. Package the deployment credentials into the crypt-only backup.
4. SHA-256 every artifact, upload to a new immutable directory, and read the
   manifest back before advancing `LATEST` and the local readiness marker.

The heat-before-PostgreSQL order is intentional. If sealing races the two
snapshots, recovery retains either the later PostgreSQL frame or the earlier
SQLite source; the opposite order can lose both. Crawler spools bridge the
brief API stop because a request without a strict committed receipt is replayed.

Six weekly bases are retained by default. Remote WAL is retained for 50 days,
and is never aged out when the last successful base is more than eight days
old. Never add an underlying provider lifecycle rule that expires the crypt
root independently; it can invalidate the oldest retained base. Provider
object versioning is useful but does not replace this policy.

Every month, `cherry-storage-backup-verify.timer` downloads the latest base,
checks every SHA-256, parses the heat/secret archives, expands PostgreSQL, runs
the matching PostgreSQL 17 `pg_verifybackup`, and downloads one remote
WAL/sidecar pair for SHA-256 and gzip validation. Its small drill report is
written back off host. This is an integrity drill, not proof of application
semantics or WAL replay; perform the isolated recovery below at least
quarterly and after a PostgreSQL/image/backup-format change.

Useful manual commands:

```bash
sudo systemctl start cherry-storage-backup.service
sudo systemctl start cherry-storage-wal-upload.service
sudo systemctl start cherry-storage-backup-verify.service
sudo journalctl -u cherry-storage-backup.service -n 100 --no-pager
```

## Isolated restore drill

Use a disposable host with enough free space for the compressed backup, the
expanded PostgreSQL cluster, and WAL. Copy `/etc/rclone/cherry.conf` and a
minimal `/etc/cherry-backup.env` there through a secure channel. Do not attach
crawler tunnels or expose the API.

```bash
sudo -i
source /etc/cherry-backup.env
root="${CHERRY_BACKUP_REMOTE%/}/${CHERRY_BACKUP_HOST_ID}"
latest="$(rclone --config "$CHERRY_RCLONE_CONFIG" cat "$root/base/LATEST")"
mkdir -m 0700 -p /srv/cherry-restore/download /srv/cherry-restore/pgdata
rclone --config "$CHERRY_RCLONE_CONFIG" copy \
  "$root/base/$latest" /srv/cherry-restore/download
cd /srv/cherry-restore/download
sha256sum -c SHA256SUMS
tar --zstd -xpf postgres.tar.zst -C /srv/cherry-restore/pgdata
docker run --rm --network none --entrypoint pg_verifybackup \
  -v /srv/cherry-restore/pgdata:/restore:ro \
  postgres:17.10-alpine@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193 \
  /restore
```

For a PITR drill, download the WAL directory and verify every sidecar before
starting PostgreSQL:

```bash
mkdir -m 0700 -p /srv/cherry-restore/wal
rclone --config "$CHERRY_RCLONE_CONFIG" copy \
  "$root/wal" /srv/cherry-restore/wal
cd /srv/cherry-restore/wal
for sum in *.gz.sha256; do sha256sum -c "$sum"; done
```

Put the expanded cluster at the compose `PGDATA` location, mount the verified
WAL directory at `/var/lib/postgresql/restore-wal`, and add the following to
`postgresql.auto.conf`. Set the UTC target to a time covered by the downloaded
WAL and create an empty `recovery.signal`:

```text
restore_command = 'cd /var/lib/postgresql/restore-wal && sha256sum -c %f.gz.sha256 >/dev/null && gzip -dc %f.gz > %p'
recovery_target_time = 'YYYY-MM-DD HH:MM:SS+00'
recovery_target_timeline = 'latest'
recovery_target_action = 'pause'
```

Start PostgreSQL only. Confirm the log reaches the target without a missing
WAL/checksum error, then inspect catalog row counts, the latest heat manifest,
outbox depth, and several known searches. Record backup ID, target time,
recovery duration, checks, and result outside the restored host. Promotion is
a separate explicit decision. Destroy the drill host and its plaintext
expanded secrets afterward.

## Heat recovery semantics

PostgreSQL frames are authoritative only for sealed days. Unsealed/today and
late-grace yesterday live under `${CHERRY_DATA_ROOT}/api/heat`; back up the
SQLite database together with its `-wal` and `-shm` files. The supplied job
stops the API before archiving that directory. Never copy only the main
`.sqlite3` file while the service is live.

After restoring PostgreSQL and the API heat directory, start PostgreSQL first,
then the API, then resume both crawler tunnels. Crawler spools must retain every
range lacking a confirmed HTTP 200 and replay it unchanged. Exact
`(hash_id,actor)` sets and authenticated receipts make replay idempotent.

Meilisearch is disposable, but an empty index is not treated as a valid empty
search result. At startup the API applies settings, compares Meili's document
count with PostgreSQL, and automatically starts coordinated metadata plus heat
recovery only when PostgreSQL is non-empty and Meili is provably empty. A
non-empty partial index is never reset automatically. Any startup check or
recovery failure aborts startup rather than silently serving zero results.

`Heat__IndexGeneration` fences PostgreSQL projection state; it does not migrate
or clear physical Meili documents. First heat-v2 bootstrap therefore requires
empty PostgreSQL, Meili, and API heat directories and writes
`${CHERRY_DATA_ROOT}/.heat-v2-empty-bootstrap`; setup fails closed if data exists
without that marker. A future non-empty upgrade must coordinate ingest pause,
run destructive recovery (physical delete/create), replay the full catalog and
retained daily plus rolling heat, and verify metadata counts, daily watermark,
pending tasks, `rollingProjectedThroughUtc`, and
`rollingRebuildRequired=false` before resuming. Changing only the generation
can leave old heat fields permanently stale and is forbidden.

For an operator-requested clean recovery, run this from the repository root:

```bash
CHERRY_API_KEY='...' node scripts/sync-meilisearch.js --recover-empty-index
```

This destructive path pauses both projection workers in the single API process,
waits for Meili index deletion, creation, and settings tasks, verifies that the
new physical index has zero documents, then atomically queues all PostgreSQL
metadata and marks heat for full replay. The heat replay targets the latest
retained sealed day and needs only its retained 16-day source window; garbage
collection of the original `CoverageStartDay` history does not prevent recovery.
The script waits for the metadata outbox and heat projection, then verifies that
Meili and PostgreSQL document counts match. The current deployment intentionally
runs one API replica; add a distributed recovery lock before scaling that service
horizontally.

The recovery status is incomplete while `rollingRebuildRequired=true`, even if
the daily watermark and metadata outbox are finished. Rolling coverage remains
reported as zero/unknown until authenticated per-crawler hourly closure proofs
are implemented; a numeric 24h projection must not be presented as complete
coverage merely because the storage process stayed up.

A routine metadata refresh remains non-destructive:

```bash
CHERRY_API_KEY='...' node scripts/sync-meilisearch.js
```

Do not manually advance a heat watermark or delete a partial day. Missing or
unsealed retained-window input intentionally halts projection; a sealed partial
day advances with reduced coverage and contributes no fabricated zero.
