# Migration Guide: Single Server → Multi Server

## Current State (Single Server)

```
┌───────────── One Cloud VM ─────────────┐
│  Docker Compose                         │
│  ┌───────┐ ┌────┐ ┌──────┐ ┌────────┐ │
│  │postgres│ │api │ │nginx │ │crawler │ │
│  └───────┘ └────┘ └──────┘ └────────┘ │
└────────────────────────────────────────┘
```

## Step 1: Extract PostgreSQL (Recommended First)

Move PostgreSQL to a managed cloud DB — no maintenance, auto-backups, easy scaling.

- **Alibaba Cloud**: ApsaraDB RDS PostgreSQL (~¥300/mo)
- **AWS**: RDS PostgreSQL (~$15/mo for db.t4g.micro)
- **Or**: Keep self-hosted, but on a dedicated small VM

After migration, just change the connection string:
```
Host=your-db-host.rds.aliyuncs.com;Port=5432;Database=cherry;...
```

## Step 2: Run Multiple Crawler Instances

Add more crawler instances on different servers for wider DHT coverage:

```yaml
# On server-2 (Germany), server-3 (Singapore), etc.
# Use docker compose with ONLY the crawler service
services:
  crawler:
    build: ./go/cherry-picker
    environment:
      CHERRY_PICKER_INSTANCE_ID: crawler-de-01
      CHERRY_PICKER_EXPORTER_URL: http://main-api:5070/api/v1/torrents/batch
      # ... same config
```

## Step 3: Scale API Horizontally

Put the API behind a load balancer (nginx, HAProxy, or cloud LB):

```
          ┌── nginx-lb:5070 ──┐
          │                    │
    ┌── api-1 ──┐      ┌── api-2 ──┐
    │           │      │           │
    └── postgres(shared) ──────────┘
```

Remove the CuckooFilter singleton (move to Redis):
```csharp
// Replace in-memory CuckooFilter with Redis-based dedup
services.AddStackExchangeRedisCache(...)
```

## Step 4: CDN for Frontend

Move static files to CDN + edge caching:
- Cloudflare Pages / Vercel (free tier)
- Or AliCloud OSS + CDN

## Key Principle

All services communicate via **DNS hostnames**, not hardcoded IPs. The docker-compose already uses service names (`postgres`, `api`). In production, replace these with real DNS:

| Docker Name | Production Equivalent |
|-------------|---------------------|
| `postgres` | `cherry-db.internal.example.com` |
| `api:5070` | `cherry-api.internal.example.com` |
| `frontend` | Cloudflare Pages / CDN |

Just change environment variables — no code changes needed.
