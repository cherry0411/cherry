use std::time::{Duration, Instant};
use dashmap::DashMap;

#[derive(Clone, Debug)]
struct BlockedItem { created: Instant }

pub struct BlackList {
    map: DashMap<String, BlockedItem>,
    max_size: usize,
    expiry: Duration,
}

impl BlackList {
    pub fn new(max_size: usize) -> Self { Self{ map: DashMap::new(), max_size, expiry: Duration::from_secs(3600) } }
    fn key(ip: &str, port: Option<u16>) -> String { match port { Some(p)=> format!("{}:{}", ip, p), None => ip.to_string() } }
    pub fn insert(&self, ip: &str, port: Option<u16>) { if self.map.len()>=self.max_size {return;} let k=Self::key(ip, port); self.map.insert(k, BlockedItem{created: Instant::now()}); }
    pub fn remove(&self, ip: &str, port: Option<u16>) { let k=Self::key(ip, port); self.map.remove(&k); }
    pub fn contains(&self, ip: &str, port: Option<u16>) -> bool { if self.map.contains_key(ip) { return true; } let k=Self::key(ip, port); if let Some(e)=self.map.get(&k) { if e.created.elapsed() < self.expiry { return true; } }
        false }
    pub fn clear_expired(&self) { let expiry=self.expiry; let now=Instant::now(); self.map.retain(|_, v| now.duration_since(v.created) < expiry ); }
}
