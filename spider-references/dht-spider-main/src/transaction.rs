use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use std::net::SocketAddr;
use crate::util::int2bytes;
use std::time::{Instant, Duration};

pub struct Transaction {
    pub trans_id_bytes: Vec<u8>,
    pub addr: SocketAddr,
    pub qtype: String,
    pub responder: Option<oneshot::Sender<()>>,
    pub info_hash: Option<[u8;20]>,
    pub target: Option<[u8;20]>,
    // if set, after receiving get_peers token from this addr, send announce_peer
    pub announce_port: Option<u16>,
    pub announce_implied_port: bool,
    pub created_at: Instant,
    pub retries: u8,
}

pub struct TransactionManager {
    cursor: AtomicU64,
    max_cursor: u64,
    map: Arc<Mutex<HashMap<Vec<u8>, Arc<Mutex<Transaction>>>>>,
    index: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl TransactionManager {
    pub fn new(max_cursor: u64) -> Self { Self{ cursor: AtomicU64::new(0), max_cursor, map: Arc::new(Mutex::new(HashMap::new())), index: Arc::new(Mutex::new(HashMap::new())) } }
    pub fn gen_id(&self) -> Vec<u8> { let id = self.cursor.fetch_add(1, Ordering::SeqCst) % self.max_cursor; int2bytes(id) }
    pub fn gen_index_key(&self, query_type: &str, address: &str) -> String { format!("{}:{}", query_type, address) }
    pub fn insert(&self, trans: Transaction) {
        let key = trans.trans_id_bytes.clone();
        let idx = self.gen_index_key(&trans.qtype, &trans.addr.to_string());
        self.map.lock().unwrap().insert(key.clone(), Arc::new(Mutex::new(trans)));
        self.index.lock().unwrap().insert(idx, key);
    }
    pub fn get_by_trans_id(&self, tid: &[u8]) -> Option<Arc<Mutex<Transaction>>> { self.map.lock().unwrap().get(tid).cloned() }
    pub fn get_by_index(&self, query_type: &str, address: &str) -> Option<Arc<Mutex<Transaction>>> { let idx=self.gen_index_key(query_type, address); if let Some(key)=self.index.lock().unwrap().get(&idx) { self.map.lock().unwrap().get(key).cloned() } else { None } }
    pub fn finish_by_trans_id(&self, tid: &[u8]) {
        if let Some(t) = self.map.lock().unwrap().remove(tid) {
            if let Some(sender) = t.lock().unwrap().responder.take() { let _ = sender.send(()); }
        }
    }
    pub fn len(&self) -> usize { self.map.lock().unwrap().len() }

    pub fn scan_timeouts(&self, timeout: Duration, max_retries: u8) -> (Vec<Arc<Mutex<Transaction>>>, Vec<Vec<u8>>) {
        let mut to_retry = Vec::new();
        let mut to_remove = Vec::new();
        let map = self.map.lock().unwrap();
        for (tid, tr) in map.iter() {
            let t = tr.lock().unwrap();
            // exponential backoff: base timeout * 2^retries
            let shift = t.retries as u32;
            let factor: u64 = if shift >= 31 { u32::MAX as u64 } else { (1u32 << shift) as u64 };
            let base_secs = timeout.as_secs();
            let mul = base_secs.saturating_mul(factor).max(base_secs);
            let threshold = Duration::from_secs(mul);
            if t.created_at.elapsed() >= threshold {
                if t.retries >= max_retries { to_remove.push(tid.clone()); } else { to_retry.push(tr.clone()); }
            }
        }
        (to_retry, to_remove)
    }
    pub fn mark_retry(&self, tid: &[u8]) { if let Some(tr) = self.map.lock().unwrap().get(tid).cloned() { let mut t = tr.lock().unwrap(); t.retries = t.retries.saturating_add(1); t.created_at = Instant::now(); } }
}
