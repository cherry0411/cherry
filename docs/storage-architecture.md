# Cherry metadata 存储与检索架构

本文定义 metadata、近期热度、全文检索和全局去重的目标架构，以及从 1M 真实数据开始的验证路线。设计目标按优先级为：数据不丢且可恢复、中文搜索质量、单位 metadata 的磁盘与内存成本、持续写入吞吐、运维复杂度。

## 1. 边界与基本原则

当前 crawler 输出的是 **normalized metadata**：

- `info_hash`
- `name`
- `piece_length`
- `total_length`
- `file_count`
- `private`
- `files(path_text, length)`

解析时已经丢弃原始 BEP-9 info dictionary 中的 `pieces` 等字段。因此，即使完整保存当前事件，也**不能重建原始 `.torrent` 文件**，更不是原始 metadata 字节的无损归档。

在进入实现前必须明确产品目标：

1. 若只要求磁力链接搜索和文件详情，保存 normalized metadata 即可。
2. 若要求重建 `.torrent` 或保留取证级原始数据，crawler 必须另外输出原始 info dictionary；该数据量和安全边界需单独评估。

全文索引不是权威数据库，概率过滤器也不是 exact oracle。各层职责必须分离：

| 层 | 职责 | 是否权威 |
|---|---|---|
| PostgreSQL catalog | hash 唯一性、摘要、归档位置、首见时间、outbox | 是 |
| 压缩 metadata archive | 完整 normalized metadata 和文件列表 | 是 |
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
            -> archive manifest or sealed-frame pointer
            -> search outbox
            -> heat batch receipt
       -> archive writer -> immutable zstd frames -> local NVMe / object storage
       -> outbox worker -> Meilisearch -> wait task succeeded -> advance cursor
       -> heat aggregator -> EWMA hot set -> quantized Meili updates
```

长期推荐 **精简 PostgreSQL + 不可变压缩 archive + Meilisearch 瘦索引**。暂不引入 ClickHouse；只有在产品需要任意时间窗口趋势、长期原始 sighting 分析，且热度事件达到高基数/十亿级后再增加。

两台 crawler 不应同时承载正式存储。跨区域 metadata 流量远小于 DHT 流量，中心存储地域应优先考虑 NVMe 性能、可靠性、备份成本和用户搜索延迟，而不是 crawler 所在地。

## 3. P0 正确性阻断

以下问题修复并通过故障注入前，现有 backend 不能作为生产 oracle 或无损存储。

### 3.1 CuckooFilter 不是可靠 oracle

当前实现存在三类语义问题：

- `Add()` 未先检查 fingerprint 是否已存在，重复 hash 会继续占槽，而不是返回 duplicate。
- `ComputeHash()` 独立生成 `i1/i2`，但踢出时用 `idx XOR hash(fingerprint)` 推导 alternate bucket。被移动的 fingerprint 可能落入 `MightContain()` 永远不检查的 bucket，从而产生 false negative。
- ingest 在数据库 commit 前先写 filter；数据库失败会污染 filter 状态。

处理原则：

- PostgreSQL 的 20 字节 hash 唯一键是 exact authority。
- ingest 依赖 `ON CONFLICT`/唯一约束完成最终去重；只有 commit 成功后才更新概率过滤器。
- 概率过滤器判定“不存在”时可以跳过 DB 查询；判定“可能存在”时仍需精确确认，不能直接丢弃 metadata。
- 为过滤器补充重复插入、接近满载、连续 eviction、并发保存/加载和无 false-negative 的性质测试。

当前 100M filter 固定分配 `200,000,008` bytes（190.73 MiB），rejected filter 另占 19.07 MiB；保存快照还会短暂复制约 191 MiB。是否继续使用 Cuckoo 应由实际容量和性质测试决定。

### 3.2 HTTP 成功不等于 durable ACK

当前 API 在 metadata 只进入内存 Channel 后即返回成功，PG commit 在后台发生；API 在两者之间崩溃会丢失已确认数据。crawler fallback WAL 只在 HTTP 失败时写入，append 也没有 durability barrier。

必须满足：

1. crawler 发送前先把 batch 持久化，或由服务端先写 durable WAL。
2. API 只有在 PG commit 或服务端 WAL `fdatasync` 成功后才能 ACK。
3. retry 使用稳定 `crawler_id + epoch + sequence`；metadata 以 hash 天然幂等，热度计数必须通过 batch receipt 防止重试翻倍。
4. 进程被 `kill -9`、磁盘写满、连接中断和响应丢失后，最终 exact count 必须不丢不重。

### 3.3 Meilisearch 同步缺少持久 outbox

当前 Meili 队列只把 HTTP `202` 当成功，没有等待异步 task 的最终状态；失败重试耗尽后直接丢批次。peer count 更新也不会回写 Meili。

必须在写 catalog 的同一 PG transaction 中写 search outbox。worker 批量提交后，只有当 Meilisearch task 变为 `succeeded` 才推进持久 cursor；失败应退避重试并暴露 backlog。Meili 全部删除后必须能只依赖 catalog/archive 重建。

### 3.4 当前 `peer_count` 不是近期热度

当前值实际是 crawler observation 的累计次数：20 秒聚合上报，失败直接丢；仅手工调用接口才会对 7 天未更新项减半。该值既不是当前 peer 数，也不是去重后的 unique peer，名称和排序语义均不准确。

应统一称为 `sightings`，先定义热度的可接受损失、跨区权重和半衰期，再实现 EWMA。

## 4. 数据模型

### 4.1 Compact catalog

建议字段如下，具体类型由 1M corpus 实测确认：

```text
torrents
  id                 bigint primary key
  info_hash          bytea(20) unique not null
  name               text not null
  piece_length       integer
  total_length       bigint
  file_count         integer
  flags              smallint
  first_seen_at      timestamptz or epoch integer
  source_region      smallint
  archive_object     bigint / uuid
  archive_frame_off  bigint
  archive_frame_len  integer
  archive_record     integer
  archive_checksum   integer / bytea
```

原则：

- 不再用 40 字节 hex 作为内部主键；API 边界再编码为 hex。
- metadata 基本不可变，不为它维护高频 `updated_at`。
- 不在长期 PG 主库保存每个文件一行，也不为名称和每条路径同时维护 trigram GIN；全文职责交给 Meili。
- 首见时间使用 crawler event time，并记录接收时间用于诊断延迟。
- 近似按时间追加的数据可评估 BRIN，避免大 B-tree。

### 4.2 Immutable compressed archive

不要每条 metadata 建一个对象，也不要为了读取一条详情解压整个大文件。

建议格式：

- 64–256 MiB 大对象，由多个独立 zstd frame 组成。
- frame 初始候选为 256 KiB、512 KiB、1 MiB，使用 corpus 决定。
- record 使用版本号、20 字节 hash、varint、UTF-8 字符串和排序后路径的前缀压缩。
- catalog 保存 object、frame range 和 record index；详情请求只需 range-read 并解压一个 frame。
- 每个 frame 和 manifest 带 checksum；segment 封存后只读。
- 本地 NVMe 作为写入/近期读取层，封存后复制到 S3/COS 等对象存储；对象存储是异机灾备，不在应用层实现分布式对象服务。

压缩级别、frame 大小和封存周期必须同时衡量 bytes/metadata、压缩 CPU、随机详情 P95 和重建吞吐。

## 5. Meilisearch 瘦索引与中文搜索

Meili 文档只保存搜索和排序必需字段：

```json
{
  "id": 123,
  "title": "原始标题",
  "aliases": "有界且去重的关键文件名",
  "pinyin": "可选全拼",
  "initials": "可选拼音首字母",
  "heat": 37,
  "firstSeen": 1780000000
}
```

优化顺序：

1. `title` 是第一 searchable attribute。
2. `aliases` 只取能增加召回的 basename，去重并设置总字节硬上限，不能复制完整文件列表。
3. 拼音字段由 ingest 离线生成；CJK 查询优先只搜原文，Latin 查询再扩到拼音，避免污染中文相关性。
4. 只保留产品真实使用的 sortable/filterable attributes；优先使用 granular filters。
5. ranking 先考虑 `words / typo / proximity / attribute / exactness`，量化热度只作 tie-breaker，不能把 `sort` 放在相关性之前。
6. 是否保留 prefix search 由搜索质量/索引体积实验决定。Meili prefix 只匹配查询最后一个词的开头，不提供任意子串匹配。
7. 自定义 dictionary/synonyms 会触发全量重建，必须版本化且低频发布。

Meilisearch 当前对中文使用 jieba-based segmentation 和 kvariant normalization，但不会自动提供汉字到拼音的召回。官方建议大规模多语言数据按语言拆索引；torrent 标题常混合中文、日文和 Latin，因此应比较“单索引 localized attributes”与“中文/其他双索引”，不能直接假定拆分一定更好。

参考：

- [Meilisearch language support](https://www.meilisearch.com/docs/resources/help/language)
- [Handling multilingual datasets](https://www.meilisearch.com/docs/capabilities/indexing/how_to/handle_multilingual_data)
- [Configure searchable attributes](https://www.meilisearch.com/docs/capabilities/full_text_search/how_to/configure_searchable_attributes)
- [Configure granular filters](https://www.meilisearch.com/docs/capabilities/filtering_sorting_faceting/how_to/configure_granular_filters)
- [Configure prefix search](https://www.meilisearch.com/docs/capabilities/full_text_search/how_to/configure_prefix_search)

## 6. 近期热度

不保存无限累计计数，也不为每次 sighting 更新 PG 和 Meili。每个近期活跃 hash 维护 lazy-decay 状态：

```text
score(now) = old_score * exp(-(now - last_seen) / tau) + new_sightings
```

建议：

- 先比较 6 小时、24 小时、7 天三个半衰期，或同时保存少量多尺度分数。
- hot store 只保留最近 30–90 天活跃 hash；冷项在读取时自然衰减，无需全表定时 UPDATE。
- 可按 region 保存小型分量，便于评估日本/其他区域的资源差异。
- Meili 只存 0–255 或更粗的 `heat` 档位，每 15–60 分钟批量更新跨档文档。
- 只在需要任意窗口趋势和长期原始 sighting 分析时引入 ClickHouse；搜索排序本身不需要它。

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

记录 raw corpus、catalog heap/index、archive、WAL、备份和 compaction 临时空间的独立字节数，不用单一“数据库大小”掩盖写放大。

### 7.2 搜索矩阵

| 编号 | Search fields | 目的 |
|---|---|---|
| S0 | title | 最瘦基线 |
| S1 | title + bounded aliases | 文件名召回收益 |
| S2 | S1 + full pinyin | 拼音查询收益/空间成本 |
| S3 | S2 + initials | 缩写查询收益/空间成本 |
| S4 | localized single index vs zh/other indexes | 中文分词与混合标题 |

建立 200–500 条中文真实查询 judgment set，覆盖：精确标题、标题中间词、简繁体、拼音、首字母、季集编号、文件名命中和噪声发布名。报告 Recall@20、nDCG@10、MRR、zero-result rate，并保存每次实验的 index settings 与 Meili 精确版本。

### 7.3 验收门槛

在目标服务器上至少满足：

- live ingest 持续不低于 250 metadata/s，并连续运行 6 小时无无界 backlog。
- 10M 全量 catalog + Meili rebuild 不超过 12 小时。
- 20 QPS 混合中文查询下 P95 <= 100 ms、P99 <= 250 ms。
- 候选方案相对质量最佳方案的 nDCG@10 下降不超过 2%；不能只因节省空间接受更差搜索。
- 正常 RAM < 70%，bulk build 峰值 < 85%，无 swap storm/OOM。
- kill -9、断网、重复投递和响应丢失后，crawler receipt、catalog、archive manifest、outbox 和 Meili 最终计数可对账。
- 完成一次从空机恢复：PG/catalog、archive manifest、heat 和 Meili 重建均通过 checksum/抽样详情校验。
- 监控 Meili `databaseSize` 与 `usedDatabaseSize`；碎片达到约 30% 再 compact，并预留约一份索引的临时磁盘。

Meili 自建版不会自动 compact，且 compaction 可能需要约等于索引大小的临时空间：[Compact an index](https://www.meilisearch.com/docs/capabilities/indexing/how_to/compact_an_index)。索引内存必须通过 `MEILI_MAX_INDEXING_MEMORY` 显式限制；默认上限约为可用内存的 2/3：[Configuration reference](https://www.meilisearch.com/docs/resources/self_hosting/configuration/reference)。

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

1. 修复 exact oracle、durable ACK、idempotent receipt 和 Meili outbox 的 P0 正确性。
2. 建立 lossless 1M normalized corpus 与查询 judgment set。
3. 跑完 D0–D2、S0–S4 矩阵，选择质量/空间 Pareto 最优点。
4. 在独立 4C8G 存储机部署 compact catalog、archive、Meili 和 EWMA heat；crawler 继续只负责抓取。
5. 用双 crawler 做 6 小时、24 小时和 7 天 soak，验证恢复、backlog、写放大和热度稳定性。
6. 到 10M 时重新外推 100M，再决定是否升级服务器、拆 Meili 或增加 ClickHouse。
