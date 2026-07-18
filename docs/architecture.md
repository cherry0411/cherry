# Cherry DHT 搜索引擎 — 详细架构设计

> 注意：本文保留了早期架构背景，其中关于 lock-free CuckooFilter、GZip 快照和“内存 Channel 即 ACK”的描述已不再代表当前正确性边界。去重权威、durable ACK、升级步骤和存储演进请以 [`storage-architecture.md`](./storage-architecture.md) 为准。

## 第一章：整体架构

### 系统定位

Cherry 是一个 DHT 磁力链接搜索引擎：从 BitTorrent DHT 网络实时抓取 torrent 元数据，提供全文搜索和浏览。

### 设计优先级

1. **成本优先**：整个后端服务运行在单台 2C4G 云服务器上，爬虫独立部署
2. **体验次之**：搜索低延迟，新种子尽快可搜
3. **扩展性/稳定性**：自愈、自动化，最小化人工干预

### 技术栈（全部保留，不引入新中间件）

| 组件 | 技术 | 说明 |
|------|------|------|
| 爬虫 | Go 1.25，BEP-5/BEP-9 | 独立部署，可多实例 |
| 后端 | .NET 10 ASP.NET Core Minimal API | Channel + IHostedService 流式消费 |
| 数据库 | PostgreSQL 17（pgvector 镜像） | BINARY COPY 批量写入，pg_trgm fallback |
| 全文搜索 | Meilisearch | 百万文档约 300MB 内存，中文 unicode 分词 |
| 去重 | CuckooFilter（内存+GZip持久化） | 1亿容量约 200MB，lock-free CAS |
| 前端 | Vue 3 + Vue Router（CDN） | 零构建工具，nginx 直接 serve |
| 部署 | Docker Compose | 单机，无 k8s |

**明确不引入**：Kafka、RabbitMQ、Redis、Elasticsearch（2C4G 内存预算不允许）

---

## 第二章：数据流

### 主数据流（端到端）

```
DHT网络 (BEP-5/BEP-9)
  → Go爬虫（可多实例，独立网络）
      ↓ 失败时写本地 WAL 文件，后台重放
  → POST /api/v1/torrents/batch（X-API-Key 认证）
      ↓ Channel 超 80% 返回 429，爬虫退避 30s
  → IngestService Channel（10万容量）
      → CuckooFilter 去重（1亿容量，0.0015% 误报率）
      → 批次5000，PostgreSQL BINARY COPY（临时表+ON CONFLICT）
      → [异步] MeiliSyncService Channel（5万容量）→ Meilisearch（2s内可搜）
      → 内存原子计数器 +N
  → Vue SPA 搜索展示
```

### Peer Count 反馈回路

```
爬虫看到重复 infohash → 不重复下载元数据 → 累计 peerCounts map
每60秒 POST /api/v1/torrents/peers → 更新 PG peer_count 字段
→ [异步] MeiliSyncService 推送部分更新（infoHash + peerCount）
→ Meilisearch 按 peerCount 排序更新
```

### 统计数据路径

```
内存原子计数器（每次 BulkInsert 成功后 Interlocked.Add）
+ PG reltuples 估算（启动时初始化，每小时精确刷新）
→ GET /api/v1/stats → O(1) 读内存，无 DB 查询
```

---

## 第三章：爬虫→API 可靠传输

### 3.1 认证（零成本）

静态 API Key via HTTP Header，只保护写入接口：

```
爬虫: X-API-Key: ${CHERRY_API_KEY}
API: 检查 /batch 和 /peers 端点的 X-API-Key header
配置: ApiKey: ${API_KEY} 在 appsettings / docker-compose env
空值 = 不验证（向后兼容）
```

### 3.2 爬虫侧 WAL 本地缓冲

**问题**：网络不稳定时（丢包/超时），失败的 batch 直接丢弃。

**方案**：基于文件的 WAL，失败写文件，后台重放。

```
文件格式：每行一个 JSON batch（JSONL 格式）
文件轮转：按小时，wal_2026010215.jsonl
重放频率：每 30s 扫描一次 WAL 目录
过期清理：超过 24 小时的文件自动删除
容量估算：最坏情况 ~20MB/小时（100% 失败率下）
```

配置：`CHERRY_PICKER_WAL_DIR=/data/wal`（空 = 不启用 WAL）

### 3.3 API 侧背压控制（防止雪崩）

**问题**：Channel 满时 `WriteAsync` 阻塞，爬虫 HTTP 请求超时后重试，导致雪崩。

**方案**：Channel 超 80% 时立即返回 429，爬虫触发退避。

```csharp
// IngestService.SubmitBatchAsync
if (_channel.Reader.Count > 80_000)  // 100k 容量的 80%
    return new BatchIngestResponse(Backpressure: true);

// TorrentEndpoints.IngestBatchAsync
if (result.Backpressure)
    return Results.StatusCode(429);
```

```go
// httpSink.WriteBatch
if response.StatusCode == 429 {
    time.Sleep(30 * time.Second)  // 退避，不计入重试次数
    continue
}
```

### 3.4 幂等性（现有三层）

| 层次 | 机制 | 特点 |
|------|------|------|
| 第一层 | CuckooFilter | 纳秒级，内存，0.0015% 误报率 |
| 第二层 | BatchSet 批内去重 | 同批次去重 |
| 第三层 | `ON CONFLICT DO NOTHING` | 数据库唯一约束兜底 |

---

## 第四章：数据入库管道

### 4.1 批量写入（已是最优）

`BulkInsertTorrentsAsync` 使用 PostgreSQL BINARY COPY + 临时表 + ON CONFLICT，是 PG 批量写入最优方案，不需要改变。

### 4.2 Meilisearch 实时增量推送

**问题**：现有方案依赖手动脚本全量同步，延迟高。

**方案**：新增 `MeiliSyncService`（IHostedService），BulkInsert 后异步推送。

```
MeiliSyncService:
  - 内部 Channel<TorrentMeiliDoc>（容量 5万，满时 DropOldest）
  - PushLoop：积累500条或等2秒，批量推送到 Meilisearch
  - 失败时重新入队，等5秒重试（不阻塞主路径）

IngestService 改造：
  var (inserted, hashToIdMap) = await repo.BulkInsertTorrentsAsync(...)
  if (inserted > 0)
  {
      _counter.Add(inserted);
      _meiliSync.Enqueue(newTorrents);  // 非 async，TryWrite 即返回
  }
```

**Meilisearch 文档结构**（精简，不存文件列表）：

```json
{
  "infoHash": "...",
  "name": "...",
  "totalLength": 1234567890,
  "fileCount": 3,
  "isPrivate": false,
  "peerCount": 42,
  "createdAt": 1746662400000
}
```

不存文件列表原因：文件列表会使 Meili 内存翻倍，fileType 过滤通过 PG 二次查询实现。

### 4.3 索引初始化（启动时幂等）

```csharp
// Program.cs 启动时调用一次
await meiliClient.EnsureIndexConfiguredAsync();

// 配置：
searchableAttributes: ["name"]
sortableAttributes: ["peerCount", "totalLength", "fileCount", "createdAt"]
filterableAttributes: ["fileCount", "totalLength", "isPrivate", "peerCount"]
rankingRules: ["words", "exactness", "proximity", "sort"]
typoTolerance: { minWordSizeForTypos: { oneTypo: 5, twoTypos: 8 } }
```

---

## 第五章：搜索服务

### 5.1 双引擎查询策略

```
搜索请求
  ├── 有 fileType 参数 → Meilisearch 多取候选（pageSize×5）
  │       → PG JOIN torrent_files 过滤扩展名 → 取前 pageSize
  │
  └── 无 fileType 参数 → Meilisearch 直接搜索（快速路径）
          → 按 infoHash 从 PG 批量取完整数据
          → 按 Meilisearch 原始顺序返回（保留相关性）

fallback（Meilisearch 不可用）：
  CJK 查询 → EF.Functions.ILike(t.Name, $"%{query}%")
  英文查询 → TrigramsSimilarityDistance < 0.7（原为0.95，太松）
```

### 5.2 CJK 查询优化

```csharp
matchingStrategy = isCjk ? "all" : "last"  // 已实现，保持
typoTolerance = isCjk ? { enabled: false } : { enabled: true }  // 新增：中文不需要 typo
```

### 5.3 OutputCache（内存上限）

```csharp
builder.Services.AddOutputCache(options =>
{
    options.SizeLimit = 100 * 1024 * 1024;  // 100MB 上限
});
// /stats: 10s, /search: 15s, /recent: 30s, /{infoHash}: 60s, /check: 5s
```

---

## 第六章：统计和监控

### 6.1 三层计数器（避免 COUNT(*) 全表扫描）

| 层次 | 机制 | 精度 | 延迟 |
|------|------|------|------|
| 内存 | `Interlocked.Add`，写入时更新 | 精确 | O(1) |
| 估算 | `SELECT reltuples FROM pg_class` | 误差 0.1-1% | ~1ms |
| 精确 | `COUNT(*)`，每小时 cron 刷新 | 100% | 数百ms |

`/stats` 接口直接读内存计数器，无 DB 查询。

### 6.2 低成本监控

不引入 Prometheus/Grafana，用以下方案：

- **结构化日志**：每批处理打印 `accepted/inserted/queue_depth`
- **`/health/detailed` 端点**：返回 total_torrents、today_new、dedup_fill_pct、ingest_queue_depth
- **healthcheck.sh**：curl 该端点 + jq 判断阈值，超出时告警

---

## 第七章：部署架构

### 7.1 2C4G 资源分配

| 服务 | CPU | 内存 | 说明 |
|------|-----|------|------|
| PostgreSQL | 1.0C | 800MB | shared_buffers=200MB, max_connections=50 |
| ASP.NET API | 0.5C | 700MB | 200MB CuckooFilter + 300MB Channel + 200MB 运行时 |
| Meilisearch | 0.3C | 500MB | 千万文档约 300MB 索引 |
| Nginx | 0.1C | 30MB | 纯静态文件 |
| 系统/Docker | — | 200MB | 系统预留 |
| **合计** | **≈2C** | **≈2.2GB** | 4GB 的 55%，安全 |

爬虫在独立服务器运行，不计入主机资源。

### 7.2 Docker Compose 关键配置

```yaml
postgres:
  command: postgres
    -c shared_buffers=200MB
    -c work_mem=4MB
    -c max_connections=50
    -c effective_cache_size=600MB
    -c log_min_duration_statement=2000
  deploy:
    resources:
      limits: { cpus: "1.0", memory: 800M }

api:
  environment:
    ApiKey: ${API_KEY:-}          # 空 = 不验证
    DOTNET_GCHeapHardLimit: 600000000  # GC 堆上限 600MB
  deploy:
    resources:
      limits: { cpus: "0.5", memory: 700M }

meilisearch:
  image: getmeili/meilisearch:v1.9
  deploy:
    resources:
      limits: { cpus: "0.3", memory: 500M }
```

### 7.3 Volume 策略

所有 volume 绑定到宿主机 `/opt/cherry/data/`（非匿名 volume），方便备份和迁移。

**备份策略**：
- PostgreSQL：每日凌晨3点 pg_dump + gzip，保留7天
- Meilisearch：**不需要单独备份**。损坏后清空重启，API 启动时检测 Meili 文档数为0则触发一次性全量同步
- CuckooFilter：随 PG 备份，或删除后重启自愈（PG 的 ON CONFLICT 兜底）

### 7.4 Nginx 配置

```nginx
# /api/v1/torrents/batch 和 /peers 只允许内网 + 已认证请求
# 所有其他 API 公开
# SPA 路由：try_files $uri /index.html
# 静态文件：expires 1d, Cache-Control: public, immutable
```

---

## 第八章：实施路线图

按收益/风险比排序：

| 步骤 | 改动范围 | 收益 | 风险 |
|------|---------|------|------|
| 1 | `CounterService` + `StatsRefreshService`，`GetStatsAsync` 读内存 | 统计 O(1) | 低 |
| 2 | `MeiliSyncService` + `IngestService` 触发推送 | 新种子 2s 内可搜 | 中 |
| 3 | Program.cs API Key 中间件 + docker env | 安全 | 低 |
| 4 | IngestService 429 背压 + 爬虫处理 429 | 稳定性 | 低 |
| 5 | 爬虫 WAL sink（~100行 Go） | 网络抖动数据不丢 | 低 |
| 6 | fileType 过滤修正 + PG trigram 阈值 0.95→0.7 | 搜索质量 | 低 |

---

## 附录：关键文件索引

| 文件 | 说明 |
|------|------|
| `backend/src/Cherry.Api/Program.cs` | DI 注册、中间件、API Key 验证 |
| `backend/src/Cherry.Api/Endpoints/TorrentEndpoints.cs` | HTTP 端点，含 429 返回 |
| `backend/src/Cherry.Application/Services/IngestService.cs` | 核心消费管道，背压检测 |
| `backend/src/Cherry.Application/Services/MeiliSyncService.cs` | [待创建] Meili 实时同步 |
| `backend/src/Cherry.Application/Services/CounterService.cs` | [待创建] 内存原子计数器 |
| `backend/src/Cherry.Infrastructure/Repositories/TorrentRepository.cs` | BINARY COPY，搜索双引擎 |
| `backend/src/Cherry.Infrastructure/Search/MeiliSearchClient.cs` | Meilisearch HTTP 客户端 |
| `go/cherry-picker/internal/export/exporter.go` | httpSink，含 WAL 包装和 429 处理 |
| `go/cherry-picker/internal/export/wal_sink.go` | [待创建] WAL 本地缓冲 |
| `scripts/sync-meilisearch.js` | 全量同步脚本（启动兜底用） |
