# Cherry 全局持续优化框架

本文是跨任务、暂停和上下文压缩后的恢复入口。系统仍处于优化迭代期，
不存在 champion 或自动进入稳定性测试的候选；每个已验证改进只是下一轮基线。
只有当优化空间按本文标准完成审计后，才向用户报告残余方向，由用户决定是否
进入最终稳定性测试。

## 1. 目标函数与调度不变量

主目标不是本地下载数、进程重启峰值或带宽占用，而是固定资源下：

```text
两区 globally-new、可搜索 metadata / 时间
= union(SG, JP)
× 新 hash 发现率
× 未知且未被另一区重复处理的比例
× 找到可用 peer 的概率
× 正确 admission × connect × extension handshake × metadata 校验
× (full + summary) 保留率
× durable commit 成功率
```

`hash_only/reject` 只计去重和成本，不能进入“可搜索 metadata”主指标；否则更
激进丢弃会伪造吞吐提升。SG/JP 独立 oracle 的 `new` 也不能相加，必须报告
SG-exclusive、JP-exclusive、intersection 和 exact union/time。

调度遵循以下不变量：

1. 本地实现、存储/搜索和两区 crawler 实验是独立流水线；一个方向等待时，
   不暂停其他安全且不冲突的方向。
2. 远端在实验块之间只允许短暂的可解释切换/健康检查；本地收尾不能成为两台
   crawler 长期空闲的理由。
3. 每项实验先写卡片：机制、唯一变量、状态周期、证伪、硬门、证据、回滚。
   失败、中性、取消和回滚与胜出结果同样保留。
4. 每完成一项，按“可挽回损失上界 × 不确定性 ÷ 成本与风险”重排队列，
   不机械执行固定列表，也不因局部胜出停止探索。
5. 6–12 小时 soak 不属于搜索环节，未经用户授权不得启动。

## 2. 当前高置信盲点

### 2.1 状态机衰减

- `peer_ttl`、`metadata_ttl` 当前是死配置；热路径实际使用容量型 LRU，形成随
  流量变化、不可直接比较的隐式时间窗。
- `queueMetadataRequest` 在 remote-known、暂停和 wire 真正接纳前预留
  `metadataRequestSeen`；队列满、暂停和失败不撤销，失败机会会被长期抑制。
- active lookup 的 `live|hash` 也只有一次机会，直到容量淘汰。
- wire blacklist 固定 131,072 项、TTL 15 分钟；满后拒绝新坏节点，可能让失败
  endpoint 重新消耗 worker。
- AutoTune 的目标是阶段成功率/CPU，不是 absolute global-new；极端情况下可每
  30 秒把 active workers 乘 0.75。常规 30 秒窗口必须记录 worker 和 queue depth。
- 96 个 DHT 实例各有独立 blacklist，但当前只聚合了 wire blacklist。

因此自动重启、换端口或换 node ID 不能直接当优化。它们同时清理多种状态并改变
路由暖态，只能在 global union oracle 下作为独立 treatment。

### 2.2 两区与持久化

- 两区 loopback overlays 的和会重复计算 intersection；必须离线/在线 exact union。
- remote `/check` 与昂贵 wire fetch 同时发生，通常来不及阻止首个重复下载。
- backend outage 或 spool 高水位时，如果仍烧掉 seen/frontier，恢复后会永久损失
  当前发现机会；背压必须传播到 admission，而不是只阻塞最终 exporter。
- durable ACK 必须早于 Meili；Meili 只能通过可重建 outbox 异步推进。
- 当前 Meili 内存队列在进程崩溃、重试耗尽或异步 task 失败时可能永久漏索引，
  需要 PG transactional outbox、task-succeeded 游标和全量 rebuild。

### 2.3 协议与基础设施覆盖

- IPv4/IPv6 是独立 DHT；IPv6 需独立路由和实验（[BEP 32](https://www.bittorrent.org/beps/bep_0032.html)）。
- v2 使用 SHA-256/file tree；当前 SHA-1/v1 parser 可能漏 v2-only metadata
  （[BEP 52](https://www.bittorrent.org/beps/bep_0052.html)）。
- BEP 51 是索引器的合法 discovery 面，但必须尊重返回 interval 并分配显式容量
  （[BEP 51](https://www.bittorrent.org/beps/bep_0051.html)）。
- TCP-only 可能缺失 uTP peer；先做小样本 probe，再决定完整实现
  （[BEP 29](https://www.bittorrent.org/beps/bep_0029.html)）。
- 200 Mbit/s 不是已证实瓶颈；云侧 PPS、新建连接率、单 NIC queue、softnet、
  conntrack 或 ephemeral ports 可能更早饱和。
- socket 请求 8 MiB 不等于内核实际给到 8 MiB，必须记录实际 buffer。

## 3. 实验队列

队列是动态优先级，不是封闭范围。

### P0 — 先消除测量盲区

| ID | 单一改动 | 证伪/输出 | 回滚 |
|---|---|---|---|
| O1 | 每个 LRU 增加 hit/miss/insert/evict/oldest-age | 衰减是否对应容量窗口 | 关闭遥测 |
| O2 | 每 30s 记录 active wire workers、request/response depth、admission drop | worker/队列是否是中介 | 关闭遥测 |
| O3 | 聚合 96 个 DHT blacklist | DHT ban 是否抑制发现 | 关闭遥测 |
| O4 | SG/JP 分时间桶 exact union/intersection/exclusive | 得到真实两区边际覆盖 | 保留旧 overlay |
| O5 | 拆 full/summary/hash_only/reject | 防止保留策略虚增吞吐 | 保留旧总数作诊断 |
| O6 | softnet/nstat/sockstat/PPS/SYN retrans/TIME_WAIT/实际 socket buffer | 定位 OS/云边界 | 移除采样 |
| O7 | shadow 统计 v2、metadata size 和失败原因 | 决定协议投资上界 | 关闭 shadow |

### P1 — 正确性与衰减根因

1. wire admission 返回是否真正入队；只在成功 admission 后保留 request key，
   pause/queue-full 时撤销。
2. 冻结 AutoTune，测 workers `128/256/512/768/1024` 的 absolute metadata 曲线。
3. active lookup 改为“新 hash 优先 + 失败 hash 低权重 cooldown retry”，一次只改
   cooldown。
4. endpoint dedupe 改为结果感知：durable success 永久 known；failure 按原因冷却；
   未接纳不留痕。
5. blacklist 容量、TTL、失败分类三个实验严格分开；先由工作集上界选择容量，
   不能把 `131072→524288`、`15m→5m` 和策略修改打包。

### P2 — Peer 调度和两区协同

6. announce/get_peers 分队列，按每 worker-second 的 metadata 成功率测试权重。
7. 以 infohash 为调度单位：primary peer 失败后再 hedge，取消无收益的同 hash 并发。
8. 中央短 lease/claim 只用于昂贵 fetch，首区失败释放/转交；metadata 上传仍走
   独立 durable spool。以 intersection 节省是否覆盖 RTT/等待为证伪。
9. backend 高延迟/断网时暂停消耗 frontier；恢复后核对 durable set 和未处理队列。

### P3 — 发现面响应曲线

10. lookup rate `300→600→900`。
11. lookup DHT redundancy `2→1`，固定初始 query budget，测 breadth vs redundancy。
12. followups `8→4→2` 的 global-new/query 拐点。
13. BEP 51 显式获得 `5%/10%/20%` 容量份额。
14. identities `96→64→128`，看新增身份的 exclusive hash 而非包数。
15. refresh nodes `32→16→8`；只有出现 burst/drop 才测试 phase stagger。
16. routing max-nodes 与节点质量分开；先 shadow 响应率、RTT、peer yield、prefix/ASN
    多样性和 BEP42，不直接硬拒绝。
17. transaction counter 随机 origin、端口轮换、node-ID 轮换三项分开。

### P4 — Wire 与协议面

18. dial timeout `250/500/750ms`。
19. total deadline `5/10/15s`，主看 metadata/worker-second。
20. 按 `metadata_size` shadow 预算，只有可搜索 metadata/byte 上升才收紧。
21. v2 shadow 有实质补集后，SHA-256 验证和 bounded file-tree parser 分开实现。
22. 先确认全局 IPv6 route，再做独立 IPv6 DHT。
23. 小样本 uTP probe 有显著 TCP-exclusive 补集后才实现完整 transport。
24. 拨号前过滤 RFC1918、loopback、link-local、multicast/bogon endpoint。

### P5 — OS、云网络、存储与搜索

以下只由 P0 指标触发，不能凭经验捆绑：

- UDP 实际 rmem/wmem 曲线；conntrack 有压才测 NOTRACK；qdisc burst 有损才测
  pacing/phase stagger/fq；单 RX queue 且 softnet 有压才测 RPS。
- pprof 证明 syscall 占比高才投入 `recvmmsg/sendmmsg`。
- SYN success 饱和而内核无 drop 时，把云厂商 PPS/新建连接率列为外部上限。
- null sink 与 durable spool 做固定 treatment，量化 policy/fsync/HTTP 吞吐税。
- PG、archive、Meili 分别报告 bytes/metadata、WAL/write amplification、ACK p99、
  backlog 和 rebuild time；搜索同时报告 Recall@20/nDCG@10。
- HTTP 压缩/binary 只在 wire/spool/HTTP 被实测为瓶颈后进行。

## 4. 时间设计

- 不用一个固定时长覆盖所有机制。窗口至少覆盖自适应预热、该机制预计状态周期
  的 1.5–2 倍，以及足以收窄置信区间的事件数。
- lookup 等方向 screen 可用 10m warm-up + 5m measurement；cache/blacklist/cooldown
  必须单臂观察 20–30m，覆盖容量和 TTL 稳态。
- per-hash queue/cooldown/source priority 优先建设进程内 deterministic interleaving：
  同一端口、身份、路由表和时间内按 hash 分 A/B，减少重启预热。
- lookup rate、identity、OS 等全局旋钮使用 sequential cross-over；两台服务器可
  同时做反序验证，结果按 region 分层并计算跨区 union。

## 5. “探索队列已穷尽”的操作性标准

开放网络无法证明数学意义的绝无可能；提交给用户决策前，至少满足：

1. 90–95% 的漏斗损失有实测归因和可挽回上界。
2. 所有候选处于保留、证伪、风险过高或外部条件缺失之一，并保留证据。
3. lookup、并发、身份、深度、TTL、timeout、refresh 均画出响应曲线和拐点。
4. IPv4/IPv6、v1/v2、TCP/uTP、BEP51 与合法补充源均已测或以实测条件判定不可用。
5. 两区使用 exact union，存储只以 full+summary 计主指标。
6. 每个未实施候选的完美修复乐观上界都低于约定最小收益（默认可先用全局可搜索
   metadata/hour 的 1%），或不足以覆盖稳定性、安全和存储代价。
7. 再做一次独立反向审计，找不到上界高于门槛且未测试的变量。

达到这些条件后只报告探索证据和残余不确定性，不自动启动稳定性测试。
