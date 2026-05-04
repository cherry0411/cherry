//! Peer wire (BEP-9/10) metadata downloader
use crate::blacklist::BlackList;
use crate::bencode::{self as be, BVal};
use dashmap::DashMap;
use sha1::{Digest, Sha1};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{timeout, Duration};

const REQUEST: i64 = 0;
const DATA: i64 = 1;
const _REJECT: i64 = 2;

const BLOCK: usize = 16384;
const MAX_METADATA_SIZE: usize = BLOCK * 1000;
const EXTENDED: u8 = 20;
const HANDSHAKE: u8 = 0;

// 68-byte handshake: pstrlen + pstr + reserved + info_hash + peer_id
const HANDSHAKE_PREFIX: [u8; 28] = [
    19, b'B', b'i', b't', b'T', b'o', b'r', b'r', b'e', b'n', b't', b' ', b'p', b'r', b'o', b't', b'o', b'c', b'o', b'l',
    0, 0, 0, 0, 0, 16, 0, 1, // reserved with extension + DHT bits
];

#[derive(Clone, Debug)]
pub struct Request {
    pub info_hash: [u8; 20],
    pub ip: String,
    pub port: u16,
}

#[derive(Clone, Debug)]
pub struct Response {
    pub request: Request,
    pub metadata_info: Arc<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct PeerEvent {
    pub info_hash: [u8; 20],
    pub ip: String,
    pub port: u16,
}

pub struct Wire {
    blacklist: Arc<BlackList>,
    queue: DashMap<String, ()>,
    resp_tx: broadcast::Sender<Response>,
    peer_tx: broadcast::Sender<PeerEvent>,
    worker_sem: Arc<Semaphore>,
}

impl Wire {
    pub fn new(blacklist_size: usize, _request_queue_size: usize, worker_queue_size: usize) -> Self {
        let (resp_tx, _rx) = broadcast::channel(1024);
        let (peer_tx, _prx) = broadcast::channel(2048);
        Self {
            blacklist: Arc::new(BlackList::new(blacklist_size)),
            queue: DashMap::new(),
            resp_tx,
            peer_tx,
            worker_sem: Arc::new(Semaphore::new(worker_queue_size.max(1))),
        }
    }

    pub fn response(&self) -> broadcast::Receiver<Response> { self.resp_tx.subscribe() }
    pub fn peer_events(&self) -> broadcast::Receiver<PeerEvent> { self.peer_tx.subscribe() }
}

// We can't subscribe to mpsc sender; implement a dedicated runner function
// Legacy placeholder removed: public start() not required; WireRunner drives requests.

// A simpler functional runner that accepts an owned receiver
pub struct WireRunner {
    wire: Wire,
    req_rx: mpsc::Receiver<Request>,
    req_tx: mpsc::Sender<Request>,
}

impl WireRunner {
    pub fn new(blacklist_size: usize, request_queue_size: usize, worker_queue_size: usize) -> (Self, WireHandle) {
        let wire = Wire::new(blacklist_size, request_queue_size, worker_queue_size);
        let (req_tx, req_rx) = mpsc::channel::<Request>(request_queue_size);
        let handle = WireHandle { req_tx: req_tx.clone(), resp_rx: wire.response(), peer_rx: wire.peer_events() };
        (Self { wire, req_rx, req_tx }, handle)
    }

    pub async fn run(mut self) {
        let bl = self.wire.blacklist.clone();
        tokio::spawn(async move {
            loop { tokio::time::sleep(Duration::from_secs(180)).await; bl.clear_expired(); }
        });

        while let Some(r) = self.req_rx.recv().await {
            // dedupe and blacklist checks
            let key = format!("{}:{}:{}", hex::encode(r.info_hash), r.ip, r.port);
            if r.info_hash.len() != 20 || self.wire.blacklist.contains(&r.ip, Some(r.port)) || self.wire.queue.contains_key(&key) {
                continue;
            }
            self.wire.queue.insert(key.clone(), ());
            let sem = self.wire.worker_sem.clone();
            let resp_tx = self.wire.resp_tx.clone();
            let peer_tx = self.wire.peer_tx.clone();
            let queue = self.wire.queue.clone();
            let bl2 = self.wire.blacklist.clone();
            // To re-enqueue within the same runner, create a lightweight forwarder channel
            let (forward_tx, mut forward_rx) = mpsc::channel::<Request>(128);
            let req_tx_for_loop = self.req_tx.clone();
            tokio::spawn(async move {
                while let Some(req) = forward_rx.recv().await {
                    let _ = req_tx_for_loop.send(req).await;
                }
            });
            tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let _ = fetch_metadata(r.clone(), resp_tx.clone(), peer_tx.clone(), forward_tx.clone(), bl2.clone()).await;
                queue.remove(&key);
            });
        }
    }
}

pub struct WireHandle {
    req_tx: mpsc::Sender<Request>,
    pub resp_rx: broadcast::Receiver<Response>,
    pub peer_rx: broadcast::Receiver<PeerEvent>,
}

impl WireHandle {
    pub async fn request(&self, info_hash: &[u8], ip: &str, port: u16) {
        if info_hash.len() != 20 { return; }
        let mut ih = [0u8; 20]; ih.copy_from_slice(info_hash);
        let _ = self.req_tx.send(Request { info_hash: ih, ip: ip.to_string(), port }).await;
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Response> { self.resp_rx.resubscribe() }
    pub fn subscribe_peers(&self) -> broadcast::Receiver<PeerEvent> { self.peer_rx.resubscribe() }
    pub fn clone_handle(&self) -> WireHandle { WireHandle{ req_tx: self.req_tx.clone(), resp_rx: self.resp_rx.resubscribe(), peer_rx: self.peer_rx.resubscribe() } }
}

async fn fetch_metadata(r: Request, resp_tx: broadcast::Sender<Response>, peer_tx: broadcast::Sender<PeerEvent>, req_forward: mpsc::Sender<Request>, blacklist: Arc<BlackList>) -> Result<(), ()> {
    let addr = format!("{}:{}", r.ip, r.port);
    let stream = match timeout(Duration::from_secs(15), TcpStream::connect(&addr)).await { Ok(Ok(s)) => s, _ => { blacklist.insert(&r.ip, Some(r.port)); return Err(()); } };
    let mut stream = stream;
    let _ = stream.set_nodelay(true);
    // Handshake
    let mut hs = vec![0u8; 68];
    hs[..HANDSHAKE_PREFIX.len()].copy_from_slice(&HANDSHAKE_PREFIX);
    hs[28..48].copy_from_slice(&r.info_hash);
    let peer_id: [u8; 20] = crate::util::random_id20();
    hs[48..].copy_from_slice(&peer_id);
    if timeout(Duration::from_secs(10), stream.write_all(&hs)).await.is_err() { return Err(()); }

    let mut resp = vec![0u8; 68];
    if timeout(Duration::from_secs(15), stream.read_exact(&mut resp)).await.is_err() { return Err(()); }
    // Validate handshake
    if !(resp[0] == 19 && &resp[1..20] == b"BitTorrent protocol" && (resp[25] & 0x10) != 0) { return Err(()); }

    // Send extended handshake (advertise ut_metadata and ut_pex)
    let ext_hs = be::encode(&be::dict(vec![("m".into(), be::dict(vec![("ut_metadata".into(), be::int(1)), ("ut_pex".into(), be::int(1))]))]));
    let mut buf = Vec::with_capacity(2 + ext_hs.len());
    buf.push(EXTENDED); buf.push(HANDSHAKE);
    buf.extend_from_slice(&ext_hs);
    send_message(&mut stream, &buf).await?;

    // Read messages until we get metadata
    let mut pieces: Option<Vec<Vec<u8>>> = None;
    let mut pieces_num = 0usize;
    let mut metadata_size = 0usize;
    let mut ut_pex_id: Option<i64> = None;

    loop {
    let (length, payload) = match read_message(&mut stream).await { Ok(v) => v, Err(_) => return Err(()) };
        if length == 0 { continue; }
        if payload.is_empty() { continue; }
        let msg_type = payload[0];
        if msg_type != EXTENDED { continue; }
        if payload.len() < 2 { return Err(()); }
        let ext_id = payload[1];
        let rest = &payload[2..];
        if ext_id == 0 {
            if pieces.is_some() { return Err(()); }
            // decode dict to get ut_metadata and metadata_size
            let (m, _idx) = match be::decode_dict(rest, 0) { Ok(v) => v, Err(_) => return Err(()) };
            let ut_meta_id = match m.get("m").and_then(|v| match v { BVal::Dict(mm) => mm.get("ut_metadata"), _ => None }) { Some(BVal::Int(n)) => Some(*n as i64), _ => None };
            ut_pex_id = match m.get("m").and_then(|v| match v { BVal::Dict(mm) => mm.get("ut_pex"), _ => None }) { Some(BVal::Int(n)) => Some(*n as i64), _ => None };
            let meta_size = match m.get("metadata_size") { Some(BVal::Int(n)) => *n as i64, _ => return Err(()) } as usize;
            if meta_size > MAX_METADATA_SIZE { return Err(()); }
            metadata_size = meta_size;
            pieces_num = meta_size / BLOCK + if meta_size % BLOCK != 0 { 1 } else { 0 };
            pieces = Some(vec![Vec::new(); pieces_num]);
            // send requests for all pieces
            if let Some(ut_meta) = ut_meta_id { for i in 0..pieces_num { let head = be::encode(&be::dict(vec![("msg_type".into(), be::int(REQUEST)), ("piece".into(), be::int(i as i64))])); let mut out = Vec::with_capacity(2 + head.len()); out.push(EXTENDED); out.push(ut_meta as u8); out.extend_from_slice(&head); let _ = send_message(&mut stream, &out).await; } }
            continue;
        }
        // data piece
        if pieces.is_none() { return Err(()); }
        let (d, index) = match be::decode_dict(rest, 0) { Ok(v) => v, Err(_) => return Err(()) };
        let dict = d;
        if let Some(BVal::Int(mt)) = dict.get("msg_type") {
            if *mt == DATA {
                let piece = match dict.get("piece") { Some(BVal::Int(n)) => *n as usize, _ => return Err(()) };
                let piece_len = length as usize - 2 - index;
                // Validate piece size
                if (piece != pieces_num - 1 && piece_len != BLOCK) || (piece == pieces_num - 1 && piece_len != metadata_size % BLOCK) { return Err(()); }
                let mut all = pieces.take().unwrap();
                if piece < all.len() { all[piece] = rest[index..index + piece_len].to_vec(); }
                let done = all.iter().all(|p| !p.is_empty());
                if done {
                    let joined = all.concat();
                    let mut hasher = Sha1::new(); hasher.update(&joined);
                    let digest = hasher.finalize();
                    if digest[..] != r.info_hash { return Err(()); }
                    let _ = resp_tx.send(Response { request: r.clone(), metadata_info: Arc::new(joined) });
                    return Ok(());
                } else { pieces = Some(all); }
                continue;
            }
        }
        // ut_pex handling
        if let Some(pex_id) = ut_pex_id { if (ext_id as i64) == pex_id { if let Ok((pex_dict, _j)) = be::decode_dict(rest, 0) { if let Some(BVal::Bytes(added)) = pex_dict.get("added") {
                        // parse compact peers (6 bytes each)
                        let mut i = 0usize; while i + 6 <= added.len() { let ip = std::net::Ipv4Addr::new(added[i], added[i+1], added[i+2], added[i+3]); let port = u16::from_be_bytes([added[i+4], added[i+5]]); i += 6; let ip_s = ip.to_string(); let _ = peer_tx.send(PeerEvent{ info_hash: r.info_hash, ip: ip_s.clone(), port }); let _ = req_forward.send(Request{ info_hash: r.info_hash, ip: ip_s, port }).await; } } } } }
    }
}

async fn read_message(stream: &mut TcpStream) -> Result<(u32, Vec<u8>), ()> {
    let mut len_buf = [0u8; 4];
    // read length
    let _ = timeout(Duration::from_secs(15), stream.read_exact(&mut len_buf)).await.map_err(|_| ())?;
    let length = u32::from_be_bytes(len_buf);
    if length == 0 { return Ok((0, Vec::new())); }
    let mut payload = vec![0u8; length as usize];
    let _ = timeout(Duration::from_secs(15), stream.read_exact(&mut payload)).await.map_err(|_| ())?;
    Ok((length, payload))
}

async fn send_message(stream: &mut TcpStream, data: &[u8]) -> Result<(), ()> {
    let len = (data.len() as u32).to_be_bytes();
    let mut buf = Vec::with_capacity(4 + data.len());
    buf.extend_from_slice(&len); buf.extend_from_slice(data);
    match timeout(Duration::from_secs(10), stream.write_all(&buf)).await {
        Ok(Ok(())) => Ok(()),
        _ => Err(())
    }
}
