# Dedicated storage/search deployment

This bundle is the 4C8G starting point for PostgreSQL-authoritative, zero-raw
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
```

## Bootstrap

Clone the repository at `/opt/cherry`, then run:

```bash
sudo REPO_DIR=/opt/cherry bash /opt/cherry/scripts/setup-storage-server.sh
```

The script creates `/opt/cherry/deploy/storage/.env` with mode `0600` and
random PostgreSQL/API/Meili credentials. Do not copy it into logs, Git, or a
crawler host. Validate the deployment without printing secrets:

```bash
cd /opt/cherry
sudo docker compose --env-file deploy/storage/.env \
  -f deploy/storage/compose.yml ps
curl --fail --silent http://127.0.0.1:5070/health
```

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
an independent loopback endpoint.

## Resource and measurement gates

The initial caps deliberately leave OS page-cache and failure headroom:

- PostgreSQL: 2.5 GiB container, 768 MiB shared buffers, synchronous durable commit.
- Meili: 2.75 GiB container, 1.75 GiB indexing limit, one indexing thread.
- API/dedupe/outbox: 1.25 GiB container.

The 6.5 GiB combined hard ceiling leaves roughly 1.5 GiB of an 8 GiB host for
the kernel, Docker, SSH, and short-lived operational work. The ceilings are not
an invitation to fill every cgroup; real RSS/page-cache and pressure-stall data
decide subsequent reallocations.

CPU limits are scheduling ceilings and may overlap; memory limits are hard
guards. Change only one parameter per recorded experiment. Before declaring a
new baseline, report PostgreSQL ACK p95/WAL bytes per retained metadata,
outbox depth/oldest age/recovery slope, Meili task p95/docs per second/RSS,
index bytes per document, and query quality. Meili compaction requires free
temporary space near the index size, so do not fill the data disk past the
pre-registered high-water mark.

PostgreSQL backup/PITR and off-host retention must be configured before this is
treated as the sole durable copy. Meili snapshots are optional because the
index can be rebuilt from PostgreSQL; duplicating Meili by default wastes disk.
