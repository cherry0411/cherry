# Cherry metadata 存储与检索架构

> 本文保留设计取舍和迁移背景；已落地的当前契约以
> `docs/architecture.md` 和 `docs/storage-search-optimization-ledger.md` 为准。

本文定义 metadata、近期热度、全文检索和全局去重的目标架构，以及从 1M 真实数据开始的验证路线。设计目标按优先级为：数据不丢且可恢复、中文搜索质量、单位 metadata 的磁盘与内存成本、持续写入吞吐、运维复杂度。

## 1. 边界与基本原则

当前 crawler 输出的是 **normalized metadata**：

- `info_hash`
- `name`
- `total_length`
- `file_count`
- `files(path_text, length)`

解析时已经丢弃原始 BEP-9 info dictionary 中的 `pieces` 等字段。因此，即使完整保存当前事件，也**不能重建原始 `.torrent` 文件**，更不是原始 metadata 字节的无损归档。

产品目标已经明确：只提供磁力链接搜索和有限文件详情，不要求重新解析、
取证或重建 `.torrent`。因此永久 raw retention 固定为 0；原始 info dictionary
只在 wire 内存中完成 SHA-1 校验和一次受限解析，随后立即释放，不进入 spool、
中央 archive、PostgreSQL、Meilisearch 或备份。

全文索引不是权威数据库，概率过滤器也不是 exact oracle。各层职责必须分离：

| 层 | 职责 | 是否权威 |
|---|---|---|
| PostgreSQL catalog | hash 唯一性、摘要、首见时间、outbox | 是 |
| 压缩 metadata detail | 有界 normalized 文件详情（无 raw） | 是 |
| Meilisearch | 全文召回、过滤、粗粒度热度排序 | 否，可重建 |
| heat store | 近期热度状态 | 是，但可容忍少量观测损失需另行定义 |
| Cuckoo/Bloom/Xor filter | negative fast-path | 否 |

## 2. 目标架构

```text
crawler A / crawler B
  -> 本地持久 WAL 或压缩 spool
  -> HTTPS 幂等批次
  -> ingest service
       -> PostgreSQL transaction
            -> compact catalog / exact unique key
            -> one-row compact detail
            -> search outbox
            -> heat batch receipt
       -> compact detail writer -> prefix/varint payload -> PostgreSQL LZ4 TOAST
       -> outbox worker -> Meilisearch -> wait task succeeded -> advance cursor
       -> heat aggregator -> EWMA hot set -> quantized Meili updates
```

首版采用 **精简 PostgreSQL + 每资源一条不可变压缩 detail + Meilisearch 瘦索引**。
详情量超过单机 PostgreSQL 的实测门槛后，保持相同编码再迁移到不可变对象 frame；
现在不提前支付独立 archive 服务和双写一致性的复杂度。暂不引入 ClickHouse。

两台 crawler 不应同时承载正式存储。跨区域 metadata 流量远小于 DHT 流量，中心存储地域应优先考虑 NVMe 性能、可靠性、备份成本和用户搜索延迟，而不是 crawler 所在地。

### 2.1 当前实现如何承接目标架构

当前实现已经形成可独立部署的 P0 过渡底座；正式启用仍以故障回归、迁移核对和部署配置审计为门槛，不需要等待 archive/Meili/heat 全部完成：

| 链路 | 当前实现可保留部分 | 下一道正确性边界 |
|---|---|---|
| crawler → API | typed zero-raw segment spool、CRC、持久 epoch/sequence、group fsync、同批重试和严格 ACK 校验 | 在双 crawler 上做固定 treatment 的吞吐/背压 ABBA 后部署 |
| API ingest | `/batch/durable` 在一个 PG transaction 提交 metadata/decision 和连续 receipt；响应丢失可重放最后批次 | crawler API key 绑定允许的 crawler identity；多 key 轮换 |
| exact dedup | `torrents + metadata_decisions`、20-byte decision key、同 hash advisory lock；旧 `rejected_hashes` 只服务旧接口 | 清理旧接口/表前先完成兼容迁移和对账 |
| metadata storage | compact catalog + 单行压缩 detail；wire 使用 `normalized/summary/hash_only/reject` 四个闭合 encoding | 在真实 corpus 上持续验证 codec CPU、TOAST/WAL 和随机详情延迟 |
| search | 查询走 Meili，PG 按 Meili ID 排名回表；同事务 outbox 只在 Meili task `succeeded` 后 ACK，支持全量重建 | 上线后持续量化搜索质量、backlog 和重建速率 |
| heat | CHHT receipt → 每 UTC 日 SQLite 精确 actor 去重 → 64 个 PG 压缩日帧 → Meili 1/7/15/30 天投影 | 上线后验证真实磁盘 fsync/day-seal 尾延迟 |

部署顺序必须让“尽快不丢数据”和“极限压缩”解耦：先让两台 crawler 的四动作 zero-raw record 安全落入当前 PG，再用固定 corpus 选择 archive 格式和继续收紧 policy。不能为了等待最终 schema 继续让已抓到的 metadata 只停留在进程内存。

### 2.2 Crawler durable spool 与幂等中央 ingest

旧 `walSink` 仍只属于 legacy HTTP 路径。生产 durable 模式在 metadata 被标记
为 remote-known 之前，把 policy 裁决后的 typed record 写入单写者、长度前缀、
CRC32C segment spool，并满足：

- record 在 crawler 标记为可丢弃前先进入 spool；当前最多 128 条或 25 ms group commit，整批 fsync 后才完成 producer Submit，避免每条同步写拖慢 2C4G crawler。
- 每个 record/batch 带持久化的 `crawler_id + epoch + sequence`。中央 receipt 以 `(crawler_id, epoch)` 串行锁定并保存最后一个连续批次的 start/end/checksum/ACK；metadata、decision 和 receipt 在同一 PG transaction 提交。
- 中央响应成功后，crawler 以原子 cursor/manifest 标记已确认；只能删除完全 ACK 的 sealed segment。不得按年龄静默删除，达到磁盘高水位时对 metadata downloader 施加背压并报警。
- CRC、长度上限和截断尾恢复 fail-safe：未发布整批通过 durable intent 全量回滚，完整 ACK 才推进删除 cursor；可证明的 torn tail 截断，已提交区 CRC/结构损坏则 fail closed 并保留现场，不猜测跳过字节边界。
- replay 允许 at-least-once。metadata 由 info hash 幂等；heat 使用独立 receipt，不能因重放翻倍。
- benchmark sink/oracle 与 production spool 指标分开。性能实验仍以全局 persistent oracle 计数，spool 作为固定 treatment 单独做 AB/BA，不能把重放量计作新 metadata。

wire 继续使用闭合 typed JSON，checksum 覆盖 HTTP body 中 `events` 的精确 JSON
字节；spool 内部没有 `json.RawMessage` 逃生口，也没有 raw bencode、`pieces` 或
piece hash 字段。取得 corpus 后再比较 binary/CBOR 与 zstd frame，避免把 durable
正确性和 archive 压缩实验捆绑。

`durable_batch_receipts` 保存 `(crawler_id, epoch)`、最后批次 start/end、
`payload_sha256`、accepted/duplicate counts 和 committed_at。首次 start 必须为 1，
后续严格连续；最后批次以相同 identity/checksum 重试时返回已保存结果，gap、
overlap 或 checksum 冲突返回 `409`。durable route 在 API key 未配置时 fail closed，
但当前还是一个共享 key；按 crawler identity 绑定 key 是上线公网后的下一安全边界。

`DurableIngestService` 不走旧 `IngestService` 的内存 Channel：它对单个最多
5,000-event 批次做严格结构/字节预算验证，以排序后的 per-infohash advisory lock
串行化新旧写路径，并在**同一事务内**用 `INSERT ... RETURNING` 计算
accepted/duplicate、处理 torrent/decision 排他状态并更新 receipt。Meili 队列只在
commit 后收到可重建索引任务，不参与权威 ACK。

## 3. P0 正确性阻断

以下问题修复并通过故障注入前，现有 backend 不能作为生产 oracle 或无损存储。仓库中的 P0 exact-authority 改动已经覆盖 Cuckoo、metadata ACK 和 rejected hash；Meili durable outbox 仍是独立后续项。

### 3.1 CuckooFilter 不是可靠 oracle

当前实现存在三类语义问题：

- `Add()` 未先检查 fingerprint 是否已存在，重复 hash 会继续占槽，而不是返回 duplicate。
- `ComputeHash()` 独立生成 `i1/i2`，但踢出时用 `idx XOR hash(fingerprint)` 推导 alternate bucket。被移动的 fingerprint 可能落入 `MightContain()` 永远不检查的 bucket，从而产生 false negative。
- ingest 曾把 filter 的 `Add(false)` 直接当作 exact duplicate，16-bit fingerprint collision 会误丢 metadata。

处理原则：

- PostgreSQL 的 20 字节 hash 唯一键是 exact authority。
- ingest 依赖 `ON CONFLICT`/唯一约束完成最终去重，API 以 `RETURNING info_hash` 计算 accepted/duplicate。
- 为消除“DB 已 commit、filter 尚未更新”的并发窗口，候选 hash 在事务前只做**正向 warm**；事务回滚最多留下 harmless false positive，任何 positive 仍必须查询 DB。若 filter 无法表示新 hash，则立即禁用 fast-path。
- 概率过滤器判定“不存在”时可以跳过 DB 查询；判定“可能存在”时仍需精确确认，不能直接丢弃 metadata。
- 为过滤器补充重复插入、接近满载、连续 eviction、并发保存/加载和无 false-negative 的性质测试。

当前 100M filter 固定分配约 `200,000,000` bytes（190.73 MiB）；独立 rejected filter 已从生产 authority 移除。保存快照还会短暂复制约 191 MiB。快照不具备和 PostgreSQL commit 一致的 watermark，因此进程启动时一律先旁路 filter，从 exact store 后台全量重建；重建完成前 `/check` 直接查询 DB。

### 3.2 HTTP 成功不等于 durable ACK

旧 API 在 metadata 只进入内存 Channel 后即返回成功，PG commit 在后台发生；API 在两者之间崩溃会丢失已确认数据。当前实现仍使用内存 Channel 合并多个请求以保留约 5k 行的批量效率，但每个请求由 `TaskCompletionSource` 等待共享 PG transaction commit，只有 commit 后才返回 accepted；响应丢失后的重试由 hash 唯一键幂等吸收。

必须满足：

1. crawler 发送前先把 batch 持久化，或由服务端先写 durable WAL。
2. API 只有在 PG commit 或服务端 WAL `fdatasync` 成功后才能 ACK。
3. retry 使用稳定 `crawler_id + epoch + sequence`；metadata 以 hash 天然幂等，热度计数必须通过 batch receipt 防止重试翻倍。
4. 进程被 `kill -9`、磁盘写满、连接中断和响应丢失后，最终 exact count 必须不丢不重。

### 3.2.1 Cuckoo/rejected 快照升级步骤

1. 先备份 PostgreSQL，并应用 `20260718090000_AddRejectedHashes`；新表以 `bytea(20)` 主键紧凑保存 exact rejected hashes。
2. 停止旧 API 后部署新版本。新版本会明确记录日志并忽略旧 `cuckoo.dat`，从 `torrents + rejected_hashes` 重建；`/health` 的 `processed_hash_fast_path_ready=false` 表示安全 DB bypass，不是服务故障。
3. 旧 `rejected.dat` 只有 fingerprint，无法反演原 hash；新版本记录警告后丢弃。后果只是部分旧 rejected hash 被重新抓取，不会误丢 metadata。
4. 等待日志出现 exact replay complete，并确认 `/health` fast-path ready。若容量不足或重建失败，ready 会保持 false，查询仍由 PostgreSQL 精确回答。
5. 重建完成后会用带 magic/version/SHA-256/长度校验的新格式覆盖 `cuckoo.dat`。损坏或截断快照在直接加载模式下必须 fail-fast；生产启动策略仍不信任快照而是重建。
6. 不得把旧 API 二进制直接回滚到新快照上：旧实现会静默空启动并错误使用空 filter。回滚必须同时禁用旧 filter 查询路径，或删除快照并使用已修复版本。

运行时必须保证所有 `torrents` 和 `rejected_hashes` 的在线写入都经过当前 repository，使其在 DB commit 前先把 candidate 正向 warm 到同一个 live filter。离线导入、人工 SQL 或其他绕过 repository 的写入会破坏“filter negative 可安全跳过 DB”的不变量；执行这类维护前必须禁用 fast-path，完成后重启 API 并等待 exact replay 重新进入 ready。

自动迁移现在遇到表冲突会 fail-fast，不再把全部 pending migration 伪装成已执行。历史上手工建表的实例必须先核对 schema，再显式建立正确的 EF baseline；禁止通过补写 `__EFMigrationsHistory` 跳过 `rejected_hashes` 等真实 DDL。

### 3.3 Meilisearch 持久 outbox（已实现）

旧 Meili 队列只把 HTTP `202` 当成功，没有等待异步 task 的最终状态，因此已被替换。

必须在写 catalog 的同一 PG transaction 中写 search outbox。worker 批量提交后，只有当 Meilisearch task 变为 `succeeded` 才推进持久 cursor；失败应退避重试并暴露 backlog。Meili 全部删除后必须能只依赖 catalog/archive 重建。

outbox 首版按 `torrent_id` 每资源一行，只保存 generation、lease、attempt 和可用时间；
文档从 compact catalog/heat 派生，不重复保存 title。worker 崩溃在 Meili succeeded
与 PG advance 之间时允许按相同主键/version 重投。监控至少包含 oldest age、rows、
retry count、Meili task latency 和 dead-letter count。

### 3.4 旧 `peer_count` 不是近期热度

当前值实际是 crawler observation 的累计次数：20 秒聚合上报，失败直接丢；仅手工调用接口才会对 7 天未更新项减半。该值既不是当前 peer 数，也不是去重后的 unique peer，名称和排序语义均不准确，迁移时不得把它当历史热度导入。

目标语义是同一资源内、同一网络 actor 在 1/7/15/30 天窗口中只计一次；跨区、重放、
端口变化和 crawler 重启都不能重复增热。

## 4. 数据模型

### 4.1 Compact catalog

建议字段如下，具体类型由 1M corpus 实测确认：

```text
torrents
  id                 bigint primary key
  info_hash          bytea(20) unique not null
  name               text not null
  total_length       bigint
  file_count         integer
  first_seen_at      timestamptz not null

torrent_details
  torrent_id         bigint primary key references torrents(id)
  payload            bytea not null  # versioned prefix/varint, LZ4 TOAST

metadata_decisions
  info_hash          bytea(20) primary key
  decision_code      smallint not null
```

原则：

- 不再用 40 字节 hex 作为内部主键；API 边界再编码为 hex。
- metadata 基本不可变，不为它维护高频 `updated_at`。
- 不在长期 PG 主库保存每个文件一行，也不为名称和每条路径同时维护 trigram GIN；全文职责交给 Meili。
- 首见时间使用 crawler event time；不再为不可变 metadata 保存 `updated_at`。
- 已明确删除 `piece_length/is_private/source/region/policy_id/retained_level/needs_refetch`；
  这也明确放弃 policy 放宽后的自动重抓和 summary→full 升级状态。
- 近似按时间追加的数据可评估 BRIN，避免大 B-tree。

### 4.2 Compact immutable detail

不要每条 metadata 建一个对象，也不要为了读取一条详情解压整个大文件。

首版格式：

- 每个 torrent 一条 `bytea`，不再为每个文件支付 PG heap tuple/index/FK 成本。
- payload 使用版本号、varint、UTF-8 和排序路径前缀压缩；full 保存完整有界文件，
  summary 只保存扩展名/数量/大小统计，不再生成代表文件或 aliases，也不保存 retention 标记。
- 完全相同的 path/length 为兼容旧数据可重复；其余同 path length 必须非降序。codec、
  数据库 constraint 和迁移 backfill 统一采用 64 MiB payload 硬上限，坏数据 fail closed。
- 列启用 PostgreSQL `COMPRESSION lz4`；小详情避免外部 frame 的随机读放大，大详情由
  TOAST 分块。
- 当真实 corpus 证明对象 frame 能显著降低总成本时，再复用相同 record 编码迁移到
  256KiB–1MiB 独立压缩 frame。迁移必须 shadow-read 校验后才删 PG detail。

压缩级别、frame 大小和封存周期必须同时衡量 bytes/metadata、压缩 CPU、随机详情 P95 和重建吞吐。

### 4.3 Zero-raw 与版本化 metadata policy

永久 raw retention 为 **0%**。`pieces` 是高熵 SHA-1 列表，通常难以压缩，且对
搜索和当前详情体验没有价值。crawler 在内存中验证
`SHA1(raw_info) == info_hash` 后，使用有界解析器直接产出 policy 所需的 compact
normalized record；raw bytes 随即释放。pre-send spool 也只写 policy 已裁决的
normalized/summary/hash-only record，从源头避免 raw 的跨区传输、archive、备份和
compaction 成本。

detail record 只允许版本化的 normalized 文件/汇总结构。删除 `pieces` 后的内容不是
raw，其 SHA-1 也不等于原 infohash；实现中不保留这种“伪 raw”中间格式。catalog
不记录 full/summary 等 retention 状态，也不支持日后自动重抓升级，这是为最低存储
成本接受的明确取舍。

metadata filter 仍是本地可配置、可 shadow 的模块，但中央不持久化 policy ID、来源、
区域或 refetch 状态。wire 的 hash-only/reject 使用闭集数值 decision code，数据库不
保存自由文本 reason。主动作是：

| 动作 | 保留内容 | 典型条件 | 搜索行为 |
|---|---|---|---|
| `full` | title、scalars、完整文件列表 | 正常规模且质量可接受 | 只索引 title |
| `summary` | title、scalars、扩展名/大小汇总；不保存代表文件/aliases | 如 `files > 2000` 或路径总字节超限 | 只索引 title |
| `hash_only` | hash + 数值 decision code | 高成本低价值 | 不索引、永久阻止重复抓取 |
| `reject` | hash + 数值 decision code | 无效/恶意/明确不收录 | 不索引、永久阻止重复抓取 |

`files > 2000` 首选 `summary` 而不是直接 `reject`：下载成本已经发生，保留 title 和标量通常只需很少空间，也能维持标题搜索召回。是否进一步降为 `hash_only/reject` 必须由真实语料证明这些记录占用大量 bytes 且几乎没有搜索价值。

边缘预检只能在下载前利用 hash 做 exact `/check`/短租约，无法仅凭 info hash 预知 file count。下载后应使用有深度、条目数和字符串长度上限的流式 bencode inspector，尽早决定 `full/summary`，避免为几万条路径构造完整对象。两区域可选用 30–120 秒的批量 claim lease 减少同时抓同一 hash；租约失效只造成重复抓取，不能阻止最终入库。

`rejected_hashes` 与旧 `metadata_decisions` 合并为
`metadata_decisions(info_hash bytea(20), decision_code smallint)`；兼容接口写同一 authority，
对账后删除重复表。decision 是永久 processed 状态，不再有 `needs_refetch`。

### 4.4 生产持久化与实验 oracle 双通道

优化期不能在“保存新 metadata”和“保持基准隔离”之间二选一。crawler 使用两个
语义不同的 endpoint：

1. `exporter_endpoint` 是生产 authority。所有经过 policy 的记录先写本地 durable
   spool，再写 PostgreSQL；它不随实验 arm 切换。
2. `oracle_endpoint` 是实验控制面，只传输 20-byte hash 和 typed action。每个 arm
   查询相同 frozen baseline，并只写自己的 append-only overlay。它不保存 title、
   files 或任何 raw bytes，也不承担生产恢复职责。

因此中央 PostgreSQL 可以在实验期间持续增长，却不会耗尽后续 arm 的 frozen
oracle。实验结束后用各区/各 arm overlay 计算 `full+summary` 的 exact union、
intersection 和 exclusive；生产 authority 依靠 durable receipt 独立去重。两个通道
不得共享 ACK：生产提交成功才允许删除 spool segment，oracle 暂时不可用只影响实验
证据完整性并触发该窗口无效，不能导致已抓 metadata 丢失或重新传 raw。

## 5. Meilisearch 瘦索引与中文搜索

Meili 文档只保存搜索和排序必需字段：

```json
{
  "id": 123,
  "name": "原始标题",
  "firstSeen": 1780000000,
  "heat1d": 3,
  "heat7d": 19,
  "heat15d": 27,
  "heat30d": 41
}
```

优化顺序：

1. `title` 是第一 searchable attribute。
2. 不保存 aliases、文件名副本、拼音或首字母字段；当前接受 filename-only 查询无召回，
   换取最小文档。以后只有真实查询集证明质量收益高于存储/CPU成本才另开版本。
3. 只保留四个实际可选的 sortable heat 字段；发现时间是独立 filter/sort，不能与
   `heatWindow` 混为一个参数。
4. ranking 先考虑 `words / typo / proximity / attribute / exactness`，所选热度只作
   非空查询 tie-breaker；空查询的 hot-list 才把所选热度放第一。
5. 是否保留 prefix search 由搜索质量/索引体积实验决定。Meili prefix 只匹配查询最后一个词的开头，不提供任意子串匹配。
6. raw bencode、`pieces`、完整路径、policy 诊断字段和未被产品使用的 scalar 一律不进 Meili。

Meilisearch 当前对中文使用 jieba-based segmentation 和 kvariant normalization，但不会自动提供汉字到拼音的召回。官方建议大规模多语言数据按语言拆索引；torrent 标题常混合中文、日文和 Latin，因此应比较“单索引 localized attributes”与“中文/其他双索引”，不能直接假定拆分一定更好。

参考：

- [Meilisearch language support](https://www.meilisearch.com/docs/resources/help/language)
- [Handling multilingual datasets](https://www.meilisearch.com/docs/capabilities/indexing/how_to/handle_multilingual_data)
- [Configure searchable attributes](https://www.meilisearch.com/docs/capabilities/full_text_search/how_to/configure_searchable_attributes)
- [Configure granular filters](https://www.meilisearch.com/docs/capabilities/filtering_sorting_faceting/how_to/configure_granular_filters)
- [Configure prefix search](https://www.meilisearch.com/docs/capabilities/full_text_search/how_to/configure_prefix_search)

## 6. 近期热度

首版不保存无限累计计数、EWMA 或全量 30 天 actor last-seen。热度定义为 UTC
calendar-day 的唯一网络 actor-days：同一资源、同一公网 IPv4（IPv6 按 /64）在同一
天内无论端口、node ID、两区域、crawler 重启和 HTTP 重放只贡献一次；跨日再次出现
可再次贡献一次，以表达持续活跃。

热路径不写 PostgreSQL，也不长期保存 actor 行。中心端按 UTC 日维护一个临时 SQLite
authority：

```sql
CREATE TABLE hashes(id INTEGER PRIMARY KEY, info_hash BLOB UNIQUE NOT NULL);
CREATE TABLE seen(
  hash_id INTEGER NOT NULL,
  actor BLOB NOT NULL,
  PRIMARY KEY(hash_id, actor)
) WITHOUT ROWID;
```

grace 结束后，SQLite 按 hash 聚合、批量映射 catalog ID，只把不可变的 64-shard
daily frame 写入 PostgreSQL。每帧按 `id & 63` 分片，按 `id >> 6` 有序编码
`delta-id,count` uvarint，并带 codec version、entry count、coverage、checksum 和 sealed
manifest。PostgreSQL 不建 `heat_actor_day`、`torrent_heat_daily` 或长期
`torrent_heat` 行表。

- actor 是 crawler 内存中用两区共享 master secret 派生当日 key 后计算的 HMAC64；
  原始 IP、端口和 node ID 不出 crawler、不落 spool/PG/备份。IPv4 用完整地址，IPv6
  用 /64；port/node churn 不能刷同一资源的热度，NAT 合并带来的保守低估必须在 UI
  语义中承认。v1 不跨日比较 actor，因此 key 可按 UTC 日轮换，只为迟到窗口保留前一日 key。
- 主信号只接收入站 `get_peers`。主动 lookup response、metadata fetch、用户 pending
  lookup 和 crawler 自己的流量全部排除，避免优化参数形成自反馈。
- `announce_peer` 必须先把当前固定 token 改为 source-IP-bound rolling HMAC token，
  在此之前只能 shadow，不能作为做种/供给热度。
- 每日 SQLite 只保留今天和尚未 seal 的昨天；`FULL` commit 才 ACK，重复 batch 用
  `INSERT OR IGNORE` 精确幂等。daily frame 与 manifest 持久且校验成功后才删除 actor
  identity。未 sealed 或 coverage 不完整的日子不能被静默当成零流量日。
- 四个窗口是 daily actor-day 的 1/7/15/30 日和，不冒充窗口内唯一用户/peer。严格
  cross-day unique 如确有产品价值，再以 1% exact last-seen oracle 比较 sparse HLL。
- 两区域的 CHHT batch 可重放；actor-day 主键使“中心已提交但响应丢失”不会重复计数。
  尚未入 catalog 的 hash 先保留在临时 SQLite，seal 时仍不可搜索才丢弃。
- Meili 只投影已 sealed 日历日。若上一成功日为 `D-1`，可能变化的 ID 严格来自
  `D ∪ D-1 ∪ D-7 ∪ D-15 ∪ D-30`；只为这些 ID 扫描当前 30 帧计算四个绝对值并
  partial PUT，四元组未变化则跳过。漏投多天必须逐日补齐；首次/新索引重建必须从
  全部 base docs 和当前 30 日 union 开始。
- daily frame 的 GC 由 Meili `projected_through` 水位控制，不能在投影故障时机械限制
  为 31 天。metadata outbox 与 heat 都使用 partial PUT；PG catalog join 证明 ID 存在
  后允许 heat stub，metadata 后到不得覆盖已有 heat。
- exact-day 状态投影超过 5GiB、heat WAL 超 metadata WAL 25%、API 增加超过 10 个 CPU
  百分点或 ACK p99 超 250ms 时，先启用已验证的 CRC-framed append-log fallback。
  Redis 不属于默认依赖；只有产品硬性要求可查询的当日实时热度时，才以受限 Bloom
  作为可丢弃缓存，最终日值仍以 SQLite 精确收敛为准。

## 7. 1M corpus 基准矩阵

在定最终 schema 和购买大规格服务器前，先保留 1M 条真实 normalized metadata。corpus 必须固定、可校验、可重复导入，并统计：

- title UTF-8 bytes 的 P50/P90/P99/max
- file count 的 P50/P90/P99/max
- path 总字节和 basename 去重后的字节分布
- 中文/日文/Latin/混合标题比例
- metadata duplicate、rejected、无效项比例
- 每小时活跃 hash 数和 sightings 分布

### 7.1 存储矩阵

| 编号 | Catalog | Files | 目的 |
|---|---|---|---|
| D0 | 当前 PG schema | 每文件一行 + GIN | 空间/吞吐基线 |
| D1 | compact PG | zstd frame archive | 推荐方案 |
| D2 | SQLite `BLOB(20)`/WITHOUT ROWID 候选 | 同一 archive | 只比较极限空间，不预设迁移 |

记录 catalog heap/index、normalized archive、WAL、备份和 compaction 临时空间的独立字节数，不用单一“数据库大小”掩盖写放大。

另外对同一 1M corpus 重放 policy 矩阵：

| 编号 | Policy | 要回答的问题 |
|---|---|---|
| P0 | 全部 `full` | bytes 与搜索质量上界 |
| P1 | `files > 2000 -> summary` | 极端文件列表节省多少，文件名召回下降多少 |
| P2 | path 总字节/嵌套深度/异常比例触发 `summary` | 是否比单一 file-count 阈值更稳健 |
| P3 | 明确噪声 `hash_only/reject` | 额外节省是否值得 title 召回损失 |

每个方案必须报告 action 占比、各 action 的 bytes/metadata、超大 torrent 占总 archive/Meili bytes 的比例、边缘解析 CPU/allocations、spool 写放大、压缩吞吐，以及相对 P0 的 Recall@20/nDCG@10。只报告总体平均值会掩盖 `files > 2000` 长尾。

### 7.2 搜索矩阵

| 编号 | Search fields | 目的 |
|---|---|---|
| S0 | title | 最瘦基线 |
| S1 | title + 4 heat fields | 最小热度排序成本/质量 |
| S2 | relevance-first + selected heat tie-break | 防止热度压过精确结果 |
| S3 | localized single index vs zh/other indexes | 中文分词与混合标题 |

建立 200–500 条中文真实查询 judgment set，覆盖：精确标题、标题中间词、简繁体、
季集编号和噪声发布名。filename-only 查询保留为已知零召回质量哨兵，但当前明确不为
aliases 支付索引成本。报告 Recall@20、nDCG@10、MRR、zero-result rate，并保存每次
实验的 index settings 与 Meili 精确版本。

### 7.3 验收门槛

在目标服务器上至少满足：

- live ingest 持续不低于 250 metadata/s，并连续运行 6 小时无无界 backlog。
- 开启 pre-send durable spool 后，相同保留 cohort 的全局新 metadata/s 相对当前 champion 下降不超过 2%；2C4G 上 crawler CPU 增量不超过 5 个百分点、RSS 增量不超过 128 MiB，spool group-commit P99 不超过 250 ms。
- `kill -9` 发生在 download→spool、spool→HTTP、PG commit→HTTP response 三个窗口时，重启后最终 exact hash 集与故障前已 durable 的 spool 集完全一致；允许重复投递，不允许缺失。
- 10M 全量 catalog + Meili rebuild 不超过 12 小时。
- 20 QPS 混合中文查询下 P95 <= 100 ms、P99 <= 250 ms。
- 候选方案相对质量最佳方案的 nDCG@10 下降不超过 2%；不能只因节省空间接受更差搜索。
- policy 候选必须同时报告 bytes saved 和搜索质量；`files > 2000 -> summary` 若不能节省至少 20% 的 files/archive bytes，或使 Recall@20 下降超过 1 个百分点，则不应仅凭直觉上线。
- 正常 RAM < 70%，bulk build 峰值 < 85%，无 swap storm/OOM。
- kill -9、断网、重复投递和响应丢失后，crawler receipt、catalog、archive manifest、outbox 和 Meili 最终计数可对账。
- 完成一次从空机恢复：PG/catalog、archive manifest、heat 和 Meili 重建均通过 checksum/抽样详情校验。
- 监控 Meili `databaseSize` 与 `usedDatabaseSize`；碎片达到约 30% 再 compact，并预留约一份索引的临时磁盘。

Meili 自建版不会自动 compact，且 compaction 可能需要约等于索引大小的临时空间：[Compact an index](https://www.meilisearch.com/docs/capabilities/indexing/how_to/compact_an_index)。索引内存必须通过 `MEILI_MAX_INDEXING_MEMORY` 显式限制；默认上限约为可用内存的 2/3：[Configuration reference](https://www.meilisearch.com/docs/resources/self_hosting/configuration/reference)。

### 7.4 故障注入矩阵

| 注入点 | 必须观察到的结果 |
|---|---|
| download 完成、spool sync 前 `kill -9` | 该条不计入 durable set；不得损坏此前已 sync record |
| spool sync 后、HTTP 前 `kill -9` | 重启从 cursor 重放，最终 PG exact set 包含该 hash |
| PG commit 后、HTTP response 前断线 | 同一 receipt 重试返回已保存结果，不重复 heat，不丢 metadata |
| archive append 后、catalog transaction 前崩溃 | 只允许产生可回收 orphan record，不允许 catalog 指向未 durable bytes |
| Meili task succeeded 后、outbox advance 前崩溃 | 重启可幂等重投同一主键，最终 outbox 清空且文档只保留一个版本 |
| crawler/central 磁盘写满 | 不返回成功、不按年龄删 spool；背压下载并暴露 remaining-bytes/ETA 告警 |
| spool/segment 尾截断或单 record CRC 损坏 | 恢复到最后完整边界；损坏项 quarantine，后续 segment 仍可处理 |
| crawler filter 配置中途切换 | 新记录按本地新配置裁决；旧 hash 的永久 processed decision 不自动 refetch |
| Meili 全删、PG/API 重启 | 仅凭 catalog/archive/outbox 重建相同 searchable set，并通过 judgment smoke test |

## 8. 容量预算与服务器触发点

仓库目前没有真实 corpus，以下仅是购买前的规划区间。假设 title 90 bytes、平均 8 个文件、路径平均 90 bytes：

| 层 | bytes/metadata | 1M | 10M | 100M |
|---|---:|---:|---:|---:|
| 当前 PG 全文件表 + GIN | 2.5–7 KB | 2.5–7 GB | 25–70 GB | 250–700 GB |
| zstd normalized archive | 0.32–0.60 KB | 0.32–0.60 GB | 3.2–6 GB | 32–60 GB |
| compact PG catalog | 0.3–0.7 KB | 0.3–0.7 GB | 3–7 GB | 30–70 GB |
| Meili title 瘦索引 | 0.4–1.5 KB | 0.4–1.5 GB | 4–15 GB | 40–150 GB |
| Meili aliases + pinyin | 0.8–3 KB | 0.8–3 GB | 8–30 GB | 80–300 GB |

上述不含 WAL、vacuum、page cache、备份、对象副本和 Meili compaction headroom。最终采购必须用 1M/10M 实测的 bytes/metadata 外推。

| 数据规模/用途 | 建议最低规格 | 说明 |
|---|---|---|
| <= 1M disposable POC | 2C4G，50–100GB NVMe | 只用于测量，不承担双 crawler 正式留存 |
| 1–10M 正式起步 | 4C8G，100–200GB NVMe + object storage | 两台 crawler 持续留存前应准备独立存储机 |
| 10–30M | 8C16G，300–500GB NVMe + object storage | 由 10M 重建/QPS 实测决定是否拆 Meili |
| 约 100M | 先按 8C32G、约 1TB NVMe + object storage 预算 | 再按实测收缩，禁止沿用“1M≈300MB”未经验证估算 |

容量不是唯一扩容触发器。出现任一情况即应拆分/扩容：

- Meili bulk indexing 使 durable ingest backlog 持续增长。
- 正常查询 working set 无法保留在 page cache，P95 连续超门槛。
- compaction 或 rebuild 无法在维护窗口内完成。
- 本地磁盘超过 60%，无法同时容纳 live index、compact 临时副本和恢复文件。
- PG checkpoint/WAL 写入与 Meili index 写入持续争抢 NVMe。

## 9. 备份、安全与运维

- Meili Docker image 必须 pin 精确版本，设置 master key，不对公网暴露 7700。
- PG、archive writer 和管理端口只在私网/loopback；公开面只保留 HTTPS ingest/search，crawler 使用非空 API key。
- Meili snapshot 适合同版本快速恢复，dump 用于跨版本迁移但需要完整重建索引：[Backing up Meilisearch data](https://www.meilisearch.com/docs/resources/self_hosting/data_backup/overview)。
- PostgreSQL 到大规模后使用 base backup + WAL/PITR，不只依赖每日 `pg_dump`：[PostgreSQL continuous archiving and PITR](https://www.postgresql.org/docs/17/continuous-archiving.html)。
- archive segment 和 manifest 复制到异机对象存储，定期抽取随机 record 做 checksum 和详情解码。
- 每次 schema、tokenizer、dictionary、ranking 或压缩格式变更都记录版本，并以 shadow index/双写方式验证后切换。

## 10. 实施顺序

1. **P0 exact bridge（当前改动）**：应用 `AddRejectedHashes`，部署 commit-before-ACK API，确认 exact replay ready/bypass；先把 normalized metadata 写入当前 PG schema。该阶段不等待 policy/archive。
2. **P0 delivery**：crawler pre-send spool + stable sequence；PG 增加 `ingest_receipts`，并把每个请求的 receipt 与 metadata 同事务提交。先做 kill-9/断网矩阵，再让两台生产 crawler 切中央 HTTPS endpoint。
3. **P0 search durability**：增加 key-only `search_outbox`；worker 提交后轮询 Meili task，幂等重试。当前内存 `MeiliIndexQueue` 只在过渡期保留，不能作为最终链路。
4. **Corpus/policy shadow**：保留 1M 条 lossless normalized corpus，不保留 raw；对 `full/summary/hash_only/reject` 只记录 shadow decision，不改变线上保留，建立查询 judgment set。
5. **Compact detail migration（已实现，部署待办）**：`CompactCatalog` 后追加可逆的
   `CompactTorrentDetails`；先校验并编码全部 legacy 行，再建 LZ4 `torrent_details`，
   最后删除旧行表。部署前按实际行数估算锁时长与临时磁盘；fresh 存储机直接建立
   最终形状，不创建 archive/pointer 层。
6. **Thin Meili shadow index**：跑 D0–D2、P0–P3、S0–S4，选择空间/质量 Pareto 点；用新 index UID 全量构建、查询对比后原子切换 alias，不原地赌博式改 settings。
7. **Heat**：sighting batch 使用独立 receipt，lazy-decay state 定期 checkpoint；只把跨档 hash 更新到 Meili，禁止每次 sighting 写 PG+Meili。
8. **Soak/扩容**：双 crawler 做 6 小时、24 小时和 7 天 soak；到 1M、10M 分别重新实测 bytes/metadata、backlog、重建时间和查询质量，再决定拆 Meili、扩大 NVMe 或增加 ClickHouse。
