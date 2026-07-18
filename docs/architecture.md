# Cherry 当前运行架构

本文只描述仓库当前可运行实现，不保存早期方案。存储取舍、容量基准和后续实验见
[`storage-architecture.md`](./storage-architecture.md)，热度协议的推导与边界见
[`heat-storage-design.md`](./heat-storage-design.md)，2C4G 存储机部署步骤见
[`../deploy/storage/README.md`](../deploy/storage/README.md)。

## 1. 系统边界

Cherry 由三类节点组成：

- 一个或多个 Go 1.25 `cherry-picker`，从 BitTorrent DHT/BEP-9 获取 normalized
  metadata，并独立采集经过身份缩减的 `get_peers` 活跃度。
- 一个 .NET 10 API，负责协议校验、事务提交、精确去重状态、热度日结和异步搜索投影。
- PostgreSQL 17 是 metadata 与 sealed heat 的权威存储；Meilisearch 1.45.1 只是可重建的
  title/heat 搜索投影。Vue 前端只调用 API，不直连数据库或 Meili。

生产存储机不需要 Redis、Kafka、RabbitMQ、Elasticsearch 或 pgvector。未封日的精确
actor-day 集合使用本地 SQLite；封日后压成 PostgreSQL frame，避免为每次 sighting
制造 PG 行/WAL。Meili 损坏不等于数据丢失。

## 2. Metadata 正确性链路

```text
DHT metadata
  -> crawler 过滤/规范化
  -> 本地 typed durable spool（stable crawler/epoch/sequence）
  -> SSH tunnel
  -> POST /api/v1/torrents/batch/durable + X-API-Key
  -> 单个 PostgreSQL 事务
       torrents + torrent_details + durable_batch_receipts
       + metadata_decisions + search_outbox
  -> 严格 committed receipt
  -> crawler 才能推进 spool cursor
  -> SearchOutboxWorker partial PUT Meili，并等待 task succeeded 后删 marker
```

生产链路的 ACK 边界是 PostgreSQL commit，不是 API 内存 Channel，也不是 Meili 接收请求。
同一 `(crawler_id, epoch, sequence range, payload digest)` 可安全重放；提交成功但 HTTP
响应丢失时不会重复写权威 metadata。`/api/v1/torrents/batch` 仍是兼容端点，不具有上述
receipt 语义，不能替代 durable 生产路径。

Crawler 在本地 spool durable 之前不得宣称数据已保存；中央端返回完整匹配的 committed
receipt 之前不得删除记录。磁盘满、断网和 5xx 必须形成背压并保留待重放数据。

## 3. PostgreSQL 当前结构

### 3.1 Catalog

`torrents` 是极窄目录表：

| 字段 | 类型 | 含义 |
|---|---|---|
| `id` | `bigint identity` | PG、Meili 和 heat 共用的紧凑主键 |
| `info_hash` | `bytea(20)` | 唯一 BTIH；API 边界才转 40 位 hex |
| `name` | `text` | 搜索标题 |
| `total_length` | `bigint` | 总字节数 |
| `file_count` | `integer` | 文件数 |
| `created_at` | `timestamptz` | 首次入库时间 |

`torrent_details(torrent_id, payload)` 每个 torrent 一行。`payload` 是 version 1 紧凑二进制：
文件路径按排序后的 UTF-8 公共前缀压缩，数字使用 uvarint，并附扩展名汇总；PostgreSQL
列再使用 LZ4 TOAST。详情读取时才解码。当前没有 `torrent_files` 行表，也不保存 raw
bencode、raw metadata、`pieces` 或可重建 `.torrent` 的字节。

`metadata_decisions(info_hash, decision_code)` 只保存没有 catalog 行的永久 processed
结果。概率 Cuckoo filter 在进程启动时从 `torrents + metadata_decisions` 精确重建；它只
是负查询快速路径，所有 positive 都回 PG 确认，因此不是去重权威。

已删除并禁止重新投影的 catalog 字段包括：`piece_length`、`is_private`、`source`、
`region`、`policy_id`、`retained_level`、`needs_refetch` 和累计 `peer_count`。

### 3.2 交付状态

- `durable_batch_receipts` 保存每个 crawler epoch 的最后提交序列、digest 和计数。
- `search_outbox` 每个 torrent 只保留一个可合并 marker；generation fencing 防止旧
  Meili task ACK 新一代更新。
- `torrent_requests` 只服务用户提交的待抓取 hash，不属于 metadata 存档。

导入当前 compact catalog 时使用 [`../scripts/import-remote.sh`](../scripts/import-remote.sh)。
脚本只接受 `torrents + torrent_details` 的当前紧凑导出，并在同一事务内建立新的 surrogate
ID、详情和 outbox marker；旧 `torrent_files.csv`/宽表导出不兼容。

## 4. Meilisearch 薄投影

索引 UID 固定为 `torrents`，primary key 固定为数值 `id`。metadata worker 使用 partial
`PUT /indexes/torrents/documents` 写入：

```json
{"id": 42, "name": "example", "firstSeen": 1784304000000}
```

heat worker 对同一文档做独立 partial PUT：

```json
{"id": 42, "heat1d": 3, "heat7d": 17, "heat15d": 31, "heat30d": 66}
```

两条链路都不发送对方字段，所以 metadata 重建不会清空 heat，heat 投影也不会复制标题。
当前 settings 是：

- searchable: `name`
- sortable: `firstSeen`, `heat1d`, `heat7d`, `heat15d`, `heat30d`
- filterable: 空
- ranking: `words`, `typo`, `proximity`, `attribute`, `exactness`, `sort`

搜索请求先由 Meili 按所选 1d/7d/15d/30d heat 和 `firstSeen` 排序，只返回 `id` 与四个
heat 值；API 再按 `id` 批量取 PG catalog 并恢复 Meili 顺序。详情页按 hex info hash 查
PG 并按需解码 compact detail。Meili 请求非 2xx、超时或返回畸形响应时，搜索 API 明确
返回 HTTP 503，不把基础设施故障伪装成“0 条结果”，也不伪装成等价的 PG 全文 fallback。

全量 metadata 重投必须调用受 API key 保护的 `POST /api/v1/search/outbox/rebuild`，或运行
[`../scripts/sync-meilisearch.js`](../scripts/sync-meilisearch.js)。该脚本不删除索引、不直连
PG/Meili，并可等待 durable outbox 清空。

Meili 丢卷后的干净恢复使用同一脚本的 `--recover-empty-index`。API 先暂停 metadata/heat
投影，删除并重建物理索引、确认文档数为 0，再在同一 PG 事务中重投全部 metadata 并
请求 full heat replay。启动时只在“PG 非空且 Meili 确认为空”时自动走该恢复；非空的部分
索引不会被武断清空。full heat replay 以最新 retained sealed day 为目标，只依赖保留的
31 日窗口，不依赖已被 GC 的 CoverageStart 历史。

## 5. 近期热度链路

```text
inbound get_peers observation
  -> crawler 排除自身/已知 crawler/metadata fetch 等自反馈
  -> (info_hash, daily keyed actor fingerprint)
  -> 独立 CHHT v1 durable spool + HMAC/sequence/digest
  -> POST /api/v1/heat/batches
  -> 当日 SQLite exact actor-day set（WAL + synchronous FULL）
  -> crawler writer barrier + contiguous receipt cursor
  -> POST /api/v1/heat/completions（crawler 专属 HMAC、幂等）
  -> UTC grace window 后按 64 shard 封成 immutable PG frames
  -> manifest 标注 complete/partial coverage
  -> HeatProjectionWorker 逐日、逐 shard、可恢复地 partial PUT Meili
```

热度表示窗口内经去重的 actor-day observations 之和，不冒充唯一用户或唯一 peer。
`Heat__ExpectedCrawlerIds` 决定每日覆盖完整性；缺少区域的日子以 partial 明示，不能静默
补零。每个 expected crawler 必须配置独立 `Heat__CrawlerSecrets__{id}`；只有显式 completion
与其 start/next 之间单 epoch receipt 链完全连续、且 crawler 本地没有 queue/drop/restart
损失时才是 complete。其余情况一律 partial。搜索响应返回所选窗口的 `heatAsOfDay` 与实际
complete coverage days。

PG 中 sealed heat 使用 `heat_day_manifests` 和 `heat_day_frames`；投影进度使用
`heat_projection_watermarks` 与 `heat_projection_tasks`。首次构建或 index generation
变化时做全量恢复，之后仅重算日窗口边界受影响的 ID。Redis 不在正确性链路中，也不
需要保存长期 IP/node/hash 关系。

## 6. 安全与部署

[`../deploy/storage/compose.yml`](../deploy/storage/compose.yml) 是当前 2C4G 起始配置：

| 服务 | 内存硬限制 | CPU ceiling | 网络 |
|---|---:|---:|---|
| PostgreSQL 17.10 | 1408 MiB | 1.25 | Docker internal |
| Meilisearch 1.45.1 | 1408 MiB | 1.0 | Docker internal |
| API | 640 MiB | 0.75 | 仅 `127.0.0.1:5070` |

三者 CPU ceiling 可以重叠调度；内存总硬限制为 3.375 GiB，给 kernel、Docker、SSH 和
page cache 留约 640 MiB。Meili 限一个 indexing thread/768 MiB indexing arena。上述是
保守基线，不是无需 benchmark 的最终参数。

PG、Meili 和 API 都不直接暴露公网。Crawler 用受限 SSH key 建 persistent local forward，
metadata API key 与 CHHT HMAC secret 分离。Meili master key、PG 密码和 API key 只存在
mode 0600 的部署环境文件中；脚本没有默认密码。

## 7. 运维不变量

- PostgreSQL + crawler 未 ACK spool 是 metadata authority；Meili 随时可删后重建。
- 未封日 heat 的备份必须同时覆盖 SQLite 主文件、WAL 和 SHM，或停 API 后复制。
- 对 metadata/heat/outbox 的成功必须以 durable commit 或 Meili task `succeeded` 为界，
  不能以 HTTP request 已发送为界。
- 直接 schema 导入必须生成 search outbox marker；禁止直接向 Meili 灌旧 fat document。
- 新索引必须保持 numeric `id` primary key 和当前薄字段契约；任何 generation 切换必须
  让 heat projection 从权威 frames 恢复。
- PostgreSQL 需要独立备份/PITR；Meili snapshot 可选，不能代替 PG 备份。
- 每次资源、schema、codec、ranking 或 crawler 参数变更都要保留 benchmark、推理和回滚
  记录；历史证据不应被“清理”为当前文档。

## 8. 关键实现入口

| 路径 | 职责 |
|---|---|
| `cherry-picker/internal/export/` | metadata spool、durable batch、receipt 重放 |
| `cherry-picker/internal/heat/` | actor-day 缩减、CHHT spool/export |
| `backend/src/Cherry.Infrastructure/Repositories/DurableIngestService.cs` | metadata 原子提交 |
| `backend/src/Cherry.Infrastructure/Storage/TorrentDetailCodec.cs` | compact detail 编解码 |
| `backend/src/Cherry.Infrastructure/Search/SearchOutboxWorker.cs` | durable metadata projection |
| `backend/src/Cherry.Infrastructure/Heat/` | heat 累积、封日、frame 与投影 |
| `backend/src/Cherry.Infrastructure/Search/MeiliSearchClient.cs` | thin index 契约 |
| `deploy/storage/compose.yml` | 2C4G storage/search baseline |
