use dashmap::DashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};
use rand::RngCore;

#[derive(Clone)]
pub struct TokenManager {
    map: DashMap<IpAddr, (String, Instant)>,
    expiry: Duration,
}

impl TokenManager {
    pub fn new(expiry: Duration) -> Self { Self{ map: DashMap::new(), expiry } }
    pub fn token(&self, ip: IpAddr) -> String { if let Some(v)=self.map.get(&ip) { if v.value().1.elapsed() < self.expiry { return v.value().0.clone(); } }
        let mut bytes=[0u8;5]; rand::thread_rng().fill_bytes(&mut bytes); let s=String::from_utf8_lossy(&bytes).into_owned(); self.map.insert(ip, (s.clone(), Instant::now())); s }
    pub fn check(&self, ip: IpAddr, token: &str) -> bool { if let Some(v)=self.map.remove(&ip) { return v.1.0 == token; } false }
    pub fn clear_expired(&self) { let expiry=self.expiry; let now=Instant::now(); self.map.retain(|_, v| now.duration_since(v.1) < expiry ); }
}
