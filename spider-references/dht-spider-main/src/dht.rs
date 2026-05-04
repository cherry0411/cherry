use crate::types::{Mode, DhtError, decode_info_hash, Peer};
use crate::token::TokenManager;
use crate::transaction::{TransactionManager, Transaction};
use crate::bencode::{BVal, dict, bytes};
use crate::peers::PeersManager;
use crate::blacklist::BlackList;
use crate::routing::RoutingTable;
use crate::command::DhtCommand;
use tokio::net::UdpSocket;
use std::sync::Arc;
use tokio::time::{interval, Duration};
use crate::util::{random_id20};
use std::time::Instant;

#[derive(Clone)]
pub struct Config {
    pub k: usize,
    pub kbucket_size: usize,
    pub network: String,
    pub address: String,
    pub prime_nodes: Vec<String>,
    pub kbucket_expired_after: Duration,
    pub node_expired_after: Duration,
    pub check_kbucket_period: Duration,
    pub token_expired_after: Duration,
    pub max_nodes: usize,
    pub blocked_ips: Vec<String>,
    pub blacklist_max_size: usize,
    pub mode: Mode,
    pub refresh_node_num: usize,
    pub try_times: u8,
}

impl Default for Config { fn default() -> Self { Self{ k:8, kbucket_size:8, network:"udp4".into(), address:":6881".into(), prime_nodes: vec![
    "router.bittorrent.com:6881".into(),
    "dht.transmissionbt.com:6881".into(),
    "router.utorrent.com:6881".into(),
    "router.bitcomet.com:6881".into(),
    // 海外稳定节点
    "dht.aelitis.com:6881".into(),
    "dht.libtorrent.org:25401".into(),
    "router.bittorrentcloud.com:6881".into(),
    "dht.anaconda.com:6881".into(),
    // 欧洲社区节点
    "dht.vuze.com:6881".into(),
    "dht.transmissionbt.net:6881".into(),
    "router.silotis.us:6881".into(),
    "router.ktorrent.com:6881".into(),
    "router.tribler.org:6881".into(),
    // 亚洲/俄罗斯节点
    "router.bittorrent.jp:6881".into(),
    "router.cn.utorrent.com:6881".into(),
    "router.bittorrent.ru:6881".into(),
    "router.bittorrent.kr:6881".into(),
], kbucket_expired_after: Duration::from_secs(60*15), node_expired_after: Duration::from_secs(60*15), check_kbucket_period: Duration::from_secs(30), token_expired_after: Duration::from_secs(600), max_nodes: 5000, blocked_ips: vec![], blacklist_max_size: 65536, mode: Mode::Standard, refresh_node_num: 8, try_times: 2 } } }

pub struct DhtCallbacks {
    pub on_get_peers: Option<Arc<dyn Fn(String, String, u16) + Send + Sync>>,
    pub on_get_peers_response: Option<Arc<dyn Fn(String, &Peer) + Send + Sync>>,
    pub on_announce_peer: Option<Arc<dyn Fn(String, String, u16) + Send + Sync>>,
    pub on_node: Option<Arc<dyn Fn(String, String, u16) + Send + Sync>>,
}
impl Default for DhtCallbacks { fn default() -> Self { Self{ on_get_peers: None, on_get_peers_response: None, on_announce_peer: None, on_node: None } } }

pub struct Dht { pub cfg: Config, pub peers: PeersManager, pub blacklist: BlackList, pub routing: RoutingTable, pub ready: bool, pub callbacks: DhtCallbacks, socket: Arc<UdpSocket>, self_id: [u8;20], tokens: TokenManager, trans: TransactionManager }

impl Dht {
    pub async fn new(cfg: Config) -> Result<Self, DhtError> { let socket=UdpSocket::bind(&cfg.address).await?; Ok(Self{ cfg: cfg.clone(), peers: PeersManager::new(cfg.k), blacklist: BlackList::new(cfg.blacklist_max_size), routing: RoutingTable::new(cfg.kbucket_size), ready: false, callbacks: DhtCallbacks::default(), socket: Arc::new(socket), self_id: random_id20(), tokens: TokenManager::new(cfg.token_expired_after), trans: TransactionManager::new(u64::MAX/2) }) }
    pub async fn run(&mut self) -> Result<(), DhtError> { self.ready=true; let mut tick=interval(self.cfg.check_kbucket_period); let mut buf=[0u8; 8192]; loop { tokio::select! { _ = tick.tick() => { if self.routing.len()==0 { self.join().await; } else {
                        // timeout & retry
                        let (to_retry, to_remove) = self.trans.scan_timeouts(Duration::from_secs(10), self.cfg.try_times);
                        for tr in to_retry { let t = tr.lock().unwrap(); let tid = t.trans_id_bytes.clone(); match t.qtype.as_str() { "get_peers" => { if let Some(ih) = t.info_hash { let a = crate::bencode::dict(vec![("id".into(), bytes(&self.select_id_for(Some(&ih)))), ("info_hash".into(), bytes(&ih))]); let buf = crate::krpc::make_query(&tid, "get_peers", a); let _ = self.socket.try_send_to(&buf, t.addr); drop(t); self.trans.mark_retry(&tid); } }, "find_node" => { if let Some(target) = t.target { let a = crate::bencode::dict(vec![("id".into(), bytes(&self.select_id_for(None))), ("target".into(), bytes(&target))]); let buf = crate::krpc::make_query(&tid, "find_node", a); let _ = self.socket.try_send_to(&buf, t.addr); drop(t); self.trans.mark_retry(&tid); } }, _=>{} } }
                        for tid in to_remove {
                            if let Some(tr) = self.trans.get_by_trans_id(&tid) { let addr = tr.lock().unwrap().addr; self.blacklist.insert(&addr.ip().to_string(), Some(addr.port())); }
                            self.trans.finish_by_trans_id(&tid);
                        }
                        if self.trans.len()==0 { // perform fresh
                            let (tasks, clear) = self.routing.fresh_plan(Instant::now(), self.cfg.kbucket_expired_after, self.cfg.refresh_node_num, matches!(self.cfg.mode, Mode::Crawl));
                            for (addr, target) in tasks { let tid = self.trans.gen_id(); let (tx,_rx)=tokio::sync::oneshot::channel::<()>(); let trans = crate::transaction::Transaction{ trans_id_bytes: tid.clone(), addr, qtype: "find_node".into(), responder: Some(tx), info_hash: None, target: Some(target), announce_port: None, announce_implied_port: false, created_at: Instant::now(), retries: 0 }; self.trans.insert(trans); let a = crate::bencode::dict(vec![("id".into(), bytes(&self.select_id_for(None))), ("target".into(), bytes(&target))]); let buf = crate::krpc::make_query(&tid, "find_node", a); let _ = self.socket.try_send_to(&buf, addr); }
                            if matches!(self.cfg.mode, Mode::Crawl) { for addr_str in clear { self.routing.remove_by_addr(&addr_str); } }
                        }
                        // periodic clean
                        self.tokens.clear_expired();
                        self.blacklist.clear_expired();
                    } }, res = self.socket.recv_from(&mut buf) => { match res { Ok((n, addr)) => { let data = &buf[..n]; self.handle_packet(data, addr).await; }, Err(_)=>{} } } } }
    }
    fn select_id_for(&self, target: Option<&[u8;20]>) -> [u8;20] { if matches!(self.cfg.mode, Mode::Standard) || target.is_none() { self.self_id } else { let mut out=self.self_id; if let Some(t)=target { out[..15].copy_from_slice(&t[..15]); } out } }
    async fn handle_packet(&mut self, data: &[u8], addr: std::net::SocketAddr) {
        if self.blacklist.contains(&addr.ip().to_string(), Some(addr.port())) { return; }
        let Ok(val) = crate::krpc::decode_value(data) else { return; };
    let Ok((t, y, msg)) = crate::krpc::parse_message(&val) else { return; };
        match y {
            "q" => { self.handle_request(t, msg, addr).await; },
            "r" => { self.handle_response(t, msg, addr).await; },
            "e" => { // finish related transaction on error
                self.trans.finish_by_trans_id(t);
            },
            _=>{}
        }
    }
    async fn send_to(&self, addr: std::net::SocketAddr, buf: &[u8]) { let _ = self.socket.send_to(buf, addr).await; }
    async fn handle_request(&mut self, t: &[u8], msg: &std::collections::BTreeMap<String, BVal>, addr: std::net::SocketAddr) {
        // { "q": <string>, "a": <dict>, "t": <bytes> }
        use BVal::*;
    let Some(BVal::Bytes(qb)) = msg.get("q") else { return; };
        let Ok(q) = std::str::from_utf8(qb) else { return; };
    let Some(BVal::Dict(a)) = msg.get("a") else { return; };
        // id validation
        let Some(Bytes(idb)) = a.get("id") else { let buf=crate::krpc::make_error(t, 203, "invalid id"); self.send_to(addr, &buf).await; return; };
        if idb.len()!=20 { let buf=crate::krpc::make_error(t, 203, "invalid id"); self.send_to(addr, &buf).await; return; }
        let mut remote_id = [0u8;20]; remote_id.copy_from_slice(idb);
        if q == "ping" {
            let id = self.select_id_for(Some(&remote_id));
            let r = dict(vec![ ("id".into(), bytes(&id)) ]);
            let buf = crate::krpc::make_response(t, r);
            self.send_to(addr, &buf).await; return;
        }
        else if q == "find_node" {
            // only in Standard mode
            if matches!(self.cfg.mode, Mode::Standard) {
                let Some(Bytes(targetb)) = a.get("target") else { let buf=crate::krpc::make_error(t, 203, "invalid target"); self.send_to(addr, &buf).await; return; };
                if targetb.len()!=20 { let buf=crate::krpc::make_error(t, 203, "invalid target"); self.send_to(addr, &buf).await; return; }
                let mut target=[0u8;20]; target.copy_from_slice(targetb);
                let nodes_bytes = self.routing.get_neighbors_compact(&target, self.cfg.k);
                let id = self.select_id_for(Some(&target));
                let r = dict(vec![ ("id".into(), bytes(&id)), ("nodes".into(), bytes(nodes_bytes)) ]);
                let buf = crate::krpc::make_response(t, r);
                self.send_to(addr, &buf).await;
            }
        }
        else if q == "get_peers" {
            let Some(Bytes(info_hashb)) = a.get("info_hash") else { let buf=crate::krpc::make_error(t, 203, "invalid info_hash"); self.send_to(addr, &buf).await; return; };
            if info_hashb.len()!=20 { let buf=crate::krpc::make_error(t, 203, "invalid info_hash"); self.send_to(addr, &buf).await; return; }
            let mut ih=[0u8;20]; ih.copy_from_slice(info_hashb);
            let token = self.tokens.token(addr.ip());
            if matches!(self.cfg.mode, Mode::Crawl) {
                let r = dict(vec![ ("id".into(), bytes(&self.select_id_for(Some(&ih)))), ("token".into(), bytes(token.as_bytes())), ("nodes".into(), bytes(Vec::new())) ]);
                let buf = crate::krpc::make_response(t, r); self.send_to(addr, &buf).await;
            } else {
                let peers = self.peers.get(ih, self.cfg.k);
                if !peers.is_empty() {
                    let mut vals = Vec::with_capacity(peers.len());
                    for p in peers { if let Some(comp)=crate::util::encode_compact_ip_port(p.ip, p.port) { vals.push(BVal::Bytes(comp.to_vec())); } }
                    let r = dict(vec![ ("id".into(), bytes(&self.select_id_for(Some(&ih)))), ("values".into(), BVal::List(vals)), ("token".into(), bytes(token.as_bytes())) ]);
                    let buf = crate::krpc::make_response(t, r); self.send_to(addr, &buf).await;
                } else {
                    let nodes_bytes = self.routing.get_neighbors_compact(&ih, self.cfg.k);
                    let r = dict(vec![ ("id".into(), bytes(&self.select_id_for(Some(&ih)))), ("token".into(), bytes(token.as_bytes())), ("nodes".into(), bytes(nodes_bytes)) ]);
                    let buf = crate::krpc::make_response(t, r); self.send_to(addr, &buf).await;
                }
            }
            if let Some(cb) = &self.callbacks.on_get_peers { cb(hex::encode(&ih), addr.ip().to_string(), addr.port()); }
        }
        else if q == "announce_peer" {
            // required keys: info_hash, port, token; optional implied_port
            let Some(Bytes(info_hashb)) = a.get("info_hash") else { return; };
            let Some(vport) = a.get("port") else { return; };
            let Some(Bytes(tokenb)) = a.get("token") else { return; };
            let port = match vport { Int(n) => *n as i64, _ => { return; } } as i32;
            let mut portu = if port<0 { 0 } else { port as u16 };
            if let Some(Int(implied)) = a.get("implied_port") { if *implied != 0 { portu = addr.port(); } }
            let valid = self.tokens.check(addr.ip(), &String::from_utf8_lossy(tokenb));
            if !valid { return; }
            if info_hashb.len()!=20 { return; }
            if matches!(self.cfg.mode, Mode::Standard) {
                // insert peer
                let mut ih=[0u8;20]; ih.copy_from_slice(info_hashb);
                self.peers.insert(ih, Peer{ ip: addr.ip(), port: portu, token: Some(String::from_utf8_lossy(tokenb).into_owned())});
                // response
                let r = dict(vec![ ("id".into(), bytes(&self.select_id_for(Some(&remote_id)))) ]);
                let buf = crate::krpc::make_response(t, r); self.send_to(addr, &buf).await;
            }
            if let Some(cb) = &self.callbacks.on_announce_peer { let ihs = hex::encode(info_hashb); cb(ihs, addr.ip().to_string(), portu); }
        }
        // insert/update routing table for the remote node
        if self.routing.len() < self.cfg.max_nodes { let node = crate::types::Node{ id: crate::types::NodeId(remote_id), addr, last_active: std::time::Instant::now() }; self.routing.insert(node); }
    }

    async fn handle_response(&mut self, t: &[u8], msg: &std::collections::BTreeMap<String, BVal>, addr: std::net::SocketAddr) {
        use BVal::*;
    let Some(Dict(r)) = msg.get("r") else { return; };
    let Some(Bytes(idb)) = r.get("id") else { return; };
    if idb.len()!=20 { return; }
    let mut remote_id = [0u8;20]; remote_id.copy_from_slice(idb);
        // Check transaction mapping
        let trans_opt = self.trans.get_by_trans_id(t);
        let mut trans_info_hash: Option<[u8;20]> = None;
        let mut is_find_node = false;
        let mut announce_after: Option<(std::net::SocketAddr, [u8;20], u16, bool)> = None;
        if let Some(tarc) = trans_opt.as_ref() {
            let tlock = tarc.lock().unwrap();
            // Ensure the response is from the expected address
            if tlock.addr != addr { return; }
            trans_info_hash = tlock.info_hash;
            if tlock.qtype == "find_node" { is_find_node = true; }
            if tlock.qtype == "get_peers" { if let (Some(ih), Some(port)) = (tlock.info_hash, tlock.announce_port) { announce_after = Some((tlock.addr, ih, port, tlock.announce_implied_port)); } }
        }
        if let Some(Bytes(values_nodes)) = r.get("nodes") { // find_node or get_peers fallback
            // insert nodes and continue querying
            for (id, ip, port) in crate::util::decode_compact_nodes(values_nodes) {
                // honor blacklist and max_nodes
                if self.blacklist.contains(&ip.to_string(), Some(port)) { continue; }
                if self.routing.len() >= self.cfg.max_nodes { continue; }
                let node = crate::types::Node{ id: crate::types::NodeId(id), addr: std::net::SocketAddr::new(ip, port), last_active: std::time::Instant::now() };
                self.routing.insert(node);
                // emit node callback (minimal info)
                if let Some(cb) = &self.callbacks.on_node { let id_hex = hex::encode(id); cb(id_hex, ip.to_string(), port); }
                // If this was a get_peers transaction, continue querying newly discovered nodes
                if let Some(ih) = trans_info_hash {
                    // build and send a get_peers to this node if not already queried
                    if self.trans.get_by_index("get_peers", &format!("{}:{}", ip, port)).is_none() {
                        let tid = self.trans.gen_id();
                        let (tx, _rx) = tokio::sync::oneshot::channel::<()>();
                        let trans = Transaction{ trans_id_bytes: tid.clone(), addr: std::net::SocketAddr::new(ip, port), qtype: "get_peers".into(), responder: Some(tx), info_hash: Some(ih), target: None, announce_port: None, announce_implied_port: false, created_at: Instant::now(), retries: 0 };
                        self.trans.insert(trans);
                        let a = crate::bencode::dict(vec![ ("id".into(), bytes(&self.select_id_for(Some(&ih)))), ("info_hash".into(), bytes(&ih)) ]);
                        let buf = crate::krpc::make_query(&tid, "get_peers", a);
                        let _ = self.socket.try_send_to(&buf, std::net::SocketAddr::new(ip, port));
                    }
                }
                // If this was a find_node transaction, recursively continue searching towards original target
                if is_find_node {
                    if let Some(tarc2) = trans_opt.as_ref() {
                        let target = tarc2.lock().unwrap().target;
                        if let Some(tgt) = target {
                            // send find_node to newly discovered node if not yet queried
                            if self.trans.get_by_index("find_node", &format!("{}:{}", ip, port)).is_none() {
                                let tid = self.trans.gen_id();
                                let (tx,_rx)=tokio::sync::oneshot::channel::<()>();
                                let trans = Transaction{ trans_id_bytes: tid.clone(), addr: std::net::SocketAddr::new(ip, port), qtype: "find_node".into(), responder: Some(tx), info_hash: None, target: Some(tgt), announce_port: None, announce_implied_port: false, created_at: Instant::now(), retries: 0 };
                                self.trans.insert(trans);
                                let a = crate::bencode::dict(vec![("id".into(), bytes(&self.select_id_for(None))), ("target".into(), bytes(&tgt))]);
                                let buf = crate::krpc::make_query(&tid, "find_node", a);
                                let _ = self.socket.try_send_to(&buf, std::net::SocketAddr::new(ip, port));
                            }
                        }
                    }
                }
            }
        }
            if let Some(List(vals)) = r.get("values") {
            // peers from get_peers
            if let Some(ih) = trans_info_hash {
                for v in vals {
                    if let Bytes(b)=v { if b.len()==6 { if let Some((ip,port))=crate::util::decode_compact_ip_port(b) {
                        let peer = Peer{ ip, port, token: None };
                        self.peers.insert(ih, peer.clone());
                        if let Some(cb) = &self.callbacks.on_get_peers_response { let ihs = hex::encode(&ih); cb(ihs, &Peer{ ip, port, token: None }); }
                    } } }
                }
                // transaction completed
                self.trans.finish_by_trans_id(t);
            }
        }
        // If token is present and we scheduled an announce, send announce_peer now
        if let Some((target_addr, ih, port, implied)) = announce_after {
            if let Some(Bytes(tokenb)) = r.get("token") {
                // build announce_peer query
                let tid = self.trans.gen_id();
                let (tx,_rx)=tokio::sync::oneshot::channel::<()>();
                let trans = Transaction{ trans_id_bytes: tid.clone(), addr: target_addr, qtype: "announce_peer".into(), responder: Some(tx), info_hash: Some(ih), target: None, announce_port: None, announce_implied_port: false, created_at: Instant::now(), retries: 0 };
                self.trans.insert(trans);
                let mut a_pairs = vec![ ("id".into(), bytes(&self.select_id_for(Some(&remote_id)))), ("info_hash".into(), bytes(&ih)), ("port".into(), crate::bencode::int(port as i64)), ("token".into(), bytes(tokenb.clone())) ];
                if implied { a_pairs.push(("implied_port".into(), crate::bencode::int(1))); }
                let a = crate::bencode::dict(a_pairs);
                let buf = crate::krpc::make_query(&tid, "announce_peer", a);
                let _ = self.socket.try_send_to(&buf, target_addr);
            }
            // finish original get_peers transaction if not already
            self.trans.finish_by_trans_id(t);
        }
        // For pure find_node response without values, finish transaction to avoid leak
        if is_find_node && trans_info_hash.is_none() { self.trans.finish_by_trans_id(t); }
    }

    async fn join(&self) {
        // send find_node to prime nodes with our id as target
        for host in &self.cfg.prime_nodes {
            let mut sent = false;
            if let Ok(mut addrs) = tokio::net::lookup_host(host).await {
                while let Some(sockaddr) = addrs.next() {
                    let tid = self.trans.gen_id();
                    let (tx,_rx)=tokio::sync::oneshot::channel::<()>();
                    let trans = crate::transaction::Transaction{ trans_id_bytes: tid.clone(), addr: sockaddr, qtype: "find_node".into(), responder: Some(tx), info_hash: None, target: Some(self.self_id), announce_port: None, announce_implied_port: false, created_at: Instant::now(), retries: 0 };
                    self.trans.insert(trans);
                    let a = crate::bencode::dict(vec![ ("id".into(), bytes(&self.select_id_for(None))), ("target".into(), bytes(&self.self_id)) ]);
                    let buf = crate::krpc::make_query(&tid, "find_node", a);
                    let _ = self.socket.send_to(&buf, sockaddr).await;
                    sent = true;
                }
            }
            if !sent {
                // try parse as socket addr fallback
                if let Ok(sockaddr) = host.parse::<std::net::SocketAddr>() {
                    let tid = self.trans.gen_id();
                    let (tx,_rx)=tokio::sync::oneshot::channel::<()>();
                    let trans = crate::transaction::Transaction{ trans_id_bytes: tid.clone(), addr: sockaddr, qtype: "find_node".into(), responder: Some(tx), info_hash: None, target: Some(self.self_id), announce_port: None, announce_implied_port: false, created_at: Instant::now(), retries: 0 };
                    self.trans.insert(trans);
                    let a = crate::bencode::dict(vec![ ("id".into(), bytes(&self.select_id_for(None))), ("target".into(), bytes(&self.self_id)) ]);
                    let buf = crate::krpc::make_query(&tid, "find_node", a);
                    let _ = self.socket.send_to(&buf, sockaddr).await;
                }
            }
        }
    }
    pub fn get_peers(&self, info_hash: &str) -> Result<(), DhtError> {
        if !self.ready { return Err(DhtError::NotReady); }
        if self.callbacks.on_get_peers_response.is_none() { return Err(DhtError::GetPeersResponseNotSet); }
        let ih = decode_info_hash(info_hash)?;
        // send to neighbors sorted by XOR distance to ih (all nodes)
        for n in self.routing.neighbors_by_target(&ih, self.routing.len()) {
            let tid = self.trans.gen_id();
            let (tx, _rx) = tokio::sync::oneshot::channel::<()>();
            let trans = Transaction{ trans_id_bytes: tid.clone(), addr: n.addr, qtype: "get_peers".into(), responder: Some(tx), info_hash: Some(ih), target: None, announce_port: None, announce_implied_port: false, created_at: Instant::now(), retries: 0 };
            self.trans.insert(trans);
            let a = crate::bencode::dict(vec![ ("id".into(), bytes(&self.select_id_for(Some(&ih)))), ("info_hash".into(), bytes(&ih)) ]);
            let buf = crate::krpc::make_query(&tid, "get_peers", a);
            let _ = self.socket.try_send_to(&buf, n.addr);
        }
        Ok(())
    }

    pub fn start(self) -> DhtHandle {
        use tokio::sync::mpsc;
        let (tx, mut rx) = mpsc::channel::<DhtCommand>(1024);
        tokio::spawn(async move {
            let mut d = self;
            let mut tick=interval(d.cfg.check_kbucket_period);
            let socket = d.socket.clone();
            let mut buf=[0u8;8192];
            d.ready = true;
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        if d.routing.len()==0 { d.join().await; } else {
                            let (to_retry, to_remove) = d.trans.scan_timeouts(Duration::from_secs(10), d.cfg.try_times);
                            for tr in to_retry { let t = tr.lock().unwrap(); let tid = t.trans_id_bytes.clone(); match t.qtype.as_str() { "get_peers" => { if let Some(ih) = t.info_hash { let a = crate::bencode::dict(vec![("id".into(), bytes(&d.select_id_for(Some(&ih)))), ("info_hash".into(), bytes(&ih))]); let buf = crate::krpc::make_query(&tid, "get_peers", a); let _ = socket.try_send_to(&buf, t.addr); drop(t); d.trans.mark_retry(&tid); } }, "find_node" => { if let Some(target) = t.target { let a = crate::bencode::dict(vec![("id".into(), bytes(&d.select_id_for(None))), ("target".into(), bytes(&target))]); let buf = crate::krpc::make_query(&tid, "find_node", a); let _ = socket.try_send_to(&buf, t.addr); drop(t); d.trans.mark_retry(&tid); } }, _=>{} } }
                            for tid in to_remove { if let Some(tr) = d.trans.get_by_trans_id(&tid) { let tlock = tr.lock().unwrap(); let addr = tlock.addr; let qtype = tlock.qtype.clone(); drop(tlock); if qtype == "ping" { d.routing.remove_by_addr(&addr.to_string()); } else { d.blacklist.insert(&addr.ip().to_string(), Some(addr.port())); } } d.trans.finish_by_trans_id(&tid); }
                            if d.trans.len()==0 { let (tasks, clear) = d.routing.fresh_plan(Instant::now(), d.cfg.kbucket_expired_after, d.cfg.refresh_node_num, matches!(d.cfg.mode, Mode::Crawl)); for (addr, target) in tasks { let tid = d.trans.gen_id(); let (tx1,_rx1)=tokio::sync::oneshot::channel::<()>(); let trans = crate::transaction::Transaction{ trans_id_bytes: tid.clone(), addr, qtype: "find_node".into(), responder: Some(tx1), info_hash: None, target: Some(target), announce_port: None, announce_implied_port: false, created_at: Instant::now(), retries: 0 }; d.trans.insert(trans); let a = crate::bencode::dict(vec![("id".into(), bytes(&d.select_id_for(None))), ("target".into(), bytes(&target))]); let buf = crate::krpc::make_query(&tid, "find_node", a); let _ = socket.try_send_to(&buf, addr); } if matches!(d.cfg.mode, Mode::Crawl) { for addr_str in clear { d.routing.remove_by_addr(&addr_str); } } }
                            // Ping expired nodes to validate liveness
                            let expired = d.routing.collect_expired_nodes(Instant::now(), d.cfg.node_expired_after);
                            for addr in expired { if d.trans.get_by_index("ping", &addr.to_string()).is_some() { continue; } let tid = d.trans.gen_id(); let (tx,_rx)=tokio::sync::oneshot::channel::<()>(); let trans = crate::transaction::Transaction{ trans_id_bytes: tid.clone(), addr, qtype: "ping".into(), responder: Some(tx), info_hash: None, target: None, announce_port: None, announce_implied_port: false, created_at: Instant::now(), retries: 0 }; d.trans.insert(trans); let a = crate::bencode::dict(vec![("id".into(), bytes(&d.select_id_for(None)))]); let buf = crate::krpc::make_query(&tid, "ping", a); let _ = socket.try_send_to(&buf, addr); }
                            d.tokens.clear_expired();
                            d.blacklist.clear_expired();
                        }
                    },
                    Some(cmd) = rx.recv() => {
                        match cmd {
                            DhtCommand::GetPeers{info_hash} => {
                                if let Ok(ih) = crate::types::decode_info_hash(&info_hash) {
                                    let neighbors = d.routing.neighbors_by_target(&ih, d.routing.len());
                                    for n in neighbors {
                                        // avoid duplicate in-flight to same addr
                                        if d.trans.get_by_index("get_peers", &n.addr.to_string()).is_some() { continue; }
                                        let tid = d.trans.gen_id();
                                        let (tx,_rx)=tokio::sync::oneshot::channel::<()>();
                                        let trans = crate::transaction::Transaction{ trans_id_bytes: tid.clone(), addr: n.addr, qtype: "get_peers".into(), responder: Some(tx), info_hash: Some(ih), target: None, announce_port: None, announce_implied_port: false, created_at: Instant::now(), retries: 0 };
                                        d.trans.insert(trans);
                                        let a = crate::bencode::dict(vec![("id".into(), bytes(&d.select_id_for(Some(&ih)))), ("info_hash".into(), bytes(&ih))]);
                                        let buf = crate::krpc::make_query(&tid, "get_peers", a);
                                        let _ = socket.send_to(&buf, n.addr).await;
                                    }
                                }
                            },
                            DhtCommand::AnnouncePeer{info_hash, port} => {
                                if let Ok(ih) = crate::types::decode_info_hash(&info_hash) {
                                    // choose a set of neighbor nodes to announce to (all current neighbors)
                                    for n in d.routing.neighbors_by_target(&ih, d.routing.len()) {
                                        // First send get_peers to obtain token (if not already querying this addr)
                                        if d.trans.get_by_index("get_peers", &n.addr.to_string()).is_none() {
                                            let tid = d.trans.gen_id();
                                            let (tx,_rx)=tokio::sync::oneshot::channel::<()>();
                                            let trans = crate::transaction::Transaction{ trans_id_bytes: tid.clone(), addr: n.addr, qtype: "get_peers".into(), responder: Some(tx), info_hash: Some(ih), target: None, announce_port: Some(port), announce_implied_port: false, created_at: Instant::now(), retries: 0 };
                                            d.trans.insert(trans);
                                            let a = crate::bencode::dict(vec![("id".into(), bytes(&d.select_id_for(Some(&ih)))), ("info_hash".into(), bytes(&ih))]);
                                            let buf = crate::krpc::make_query(&tid, "get_peers", a);
                                            let _ = socket.try_send_to(&buf, n.addr);
                                        }
                                    }
                                }
                            },
                            DhtCommand::Shutdown => { break; }
                        }
                    },
                    res = socket.recv_from(&mut buf) => { match res { Ok((n, addr)) => { let data=&buf[..n]; d.handle_packet(data, addr).await; }, Err(_)=>{} } }
                }
            }
        });
        DhtHandle{ tx }
    }
}

pub struct DhtHandle { tx: tokio::sync::mpsc::Sender<DhtCommand> }
impl DhtHandle {
    pub async fn get_peers(&self, info_hash: &str) { let _ = self.tx.send(DhtCommand::GetPeers{ info_hash: info_hash.to_string() }).await; }
    pub async fn announce_peer(&self, info_hash: &str, port: u16) { let _ = self.tx.send(DhtCommand::AnnouncePeer{ info_hash: info_hash.to_string(), port }).await; }
    pub async fn shutdown(&self){ let _ = self.tx.send(DhtCommand::Shutdown).await; }
}
