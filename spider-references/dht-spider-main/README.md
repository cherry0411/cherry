# dht-spider

Rust 实现的 BitTorrent DHT（BEP‑5）与爬虫，并内置元数据下载（BEP‑9/10）与 PeX（BEP‑11/ut_pex）。

现在只支持一种用法：cargo run。所有能力均已整合进主程序并以 JSONL 输出。

## 特性概览

- 模式：
  - Standard：严格遵循 DHT 协议
  - Crawl：偏向嗅探 infohash（促使对端 announce_peer）
- KRPC：ping / find_node / get_peers / announce_peer（入站与递归）
- 路由：K‑Bucket 分裂/候选、XOR 距离邻居选择、ping‑then‑replace 维护
- 事务：超时指数退避重试、黑名单；token 校验
- 元数据：集成 Wire（BEP‑9/10）下载 .torrent metadata
- PeX：对等交换（BEP‑11/ut_pex）发现更多 peers
  - 扩展握手声明 ut_pex
  - 解析 `added`（紧凑 IPv4）中的 peers，并：
    1) 统一输出为 `type=peer` 的 JSON 行
    2) 自动加入抓取队列尝试下载对应 infohash 的 metadata

## 快速开始

构建与运行：

```zsh
cargo build --release
cargo run
```

运行时行为：

- 默认模式：Crawl
- 默认监听：UDP 0.0.0.0:6881
- 输出格式：JSONL（每行一个事件）

示例输出（部分）：

```json
{"type":"peer","ip":"203.0.113.7","port":51413,"info_hash":"a1b2...c3d4"}
{"type":"metadata","infohash":"a1b2...c3d4","name":"SomePack","files":[{"path":["dir","file1.mkv"],"length":12345}]}
{"type":"node","id":"a1b2c3...","ip":"162.83.157.130","port":6881}
```

提示：若端口被占用，启动会输出错误并退出，例如：

```json
{"level":"error","event":"startup","error":"IO error: Address already in use (os error 98)"}
```

## 配置与默认值

本项目遵循“零参数可运行”，开箱即用。默认参数（可能随版本调整）包括：

- 路由与桶：k=8，kbucket_size=8
- 监听：UDP 0.0.0.0:6881
- 引导节点（prime_nodes）：包含多组官方/社区节点（示例见下），例如：
  - router.bittorrent.com:6881、dht.transmissionbt.com:6881、router.utorrent.com:6881、router.bitcomet.com:6881
  - dht.aelitis.com:6881、dht.libtorrent.org:25401、router.bittorrentcloud.com:6881、dht.anaconda.com:6881
  - dht.vuze.com:6881、dht.transmissionbt.net:6881、router.silotis.us:6881、router.ktorrent.com:6881、router.tribler.org:6881
  - router.bittorrent.jp:6881、router.cn.utorrent.com:6881、router.bittorrent.ru:6881、router.bittorrent.kr:6881
  - 实际列表可能随版本更新进行调整
- 周期与过期：kbucket_expired_after≈15 分钟、node_expired_after≈15 分钟、check_kbucket_period≈30 秒
- token 过期：≈600 秒
- 最大节点：≈5000；黑名单最大：≈65536
- 运行模式：默认 Crawl（偏向触发对端 announce）
- 刷新与重试：refresh_node_num≈8、try_times≈2

维护语义：

- 对过期/失活节点：先 ping，超时才替换；无全局“定期裁剪”。
- Crawl 模式：入站 get_peers 返回空 nodes + token，促使对端 announce_peer。

## 行为说明

- KRPC 处理、递归查找与 announce 流程遵循 BEP 规范；announce_peer 会复用 get_peers 获取的 token。
- 路由表与邻居选择（XOR 距离）；节点按 last_active 排序。
- 默认 prime 节点、compact IPv4 地址编码。

## 输出格式（统一 JSON 行）

- Peer（来自 announce_peer / get_peers 的 values / PeX）：
  {"type":"peer","ip":"<ip>","port":<port>,"info_hash":"<hex>"}
- Metadata（torrent 信息，单/多文件统一）：
  - 始终输出 files 数组；单文件时 files=[{"path":[name],"length":...}]
  - 示例： {"type":"metadata","infohash":"<hex>","name":"...","files":[{"path":[...],"length":...},...]}
- DHT 节点（解析自 nodes）：
  {"type":"node","id":"<20字节hex>","ip":"<ip>","port":<port>}

说明：
- 当前实现已支持 PeX 的 IPv4 `added` 列表；如需 IPv6，可后续扩展 `added6` 的解析。
- 私有种通常禁用 PeX；程序会自然尊重对端能力。