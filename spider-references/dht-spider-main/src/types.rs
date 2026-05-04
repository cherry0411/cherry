use std::time::{Duration, Instant};
use std::net::SocketAddr;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(pub [u8;20]);

impl NodeId {    
    pub fn as_bytes(&self) -> &[u8;20] { &self.0 }
    pub fn random() -> Self { use rand::RngCore; let mut id=[0u8;20]; rand::thread_rng().fill_bytes(&mut id); Self(id) }
}

#[derive(Clone, Debug)]
pub struct Node {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub last_active: Instant,
}

#[derive(Clone, Debug)]
pub struct Peer {
    pub ip: std::net::IpAddr,
    pub port: u16,
    pub token: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum Mode { Standard, Crawl }

#[derive(thiserror::Error, Debug)]
pub enum DhtError {
    #[error("DHT not ready")] NotReady,
    #[error("OnGetPeersResponse not set")] GetPeersResponseNotSet,
    #[error("Invalid info_hash length")] InvalidInfoHash,
    #[error("IO error: {0}")] Io(#[from] std::io::Error),
    #[error("Bencode error: {0}")] Bencode(String),
}

pub type InfoHash = [u8;20];

pub fn decode_info_hash(s: &str) -> Result<InfoHash, DhtError> {
    let bytes = if s.len()==40 { hex::decode(s).map_err(|e| DhtError::Bencode(e.to_string()))? } else { s.as_bytes().to_vec() };    
    if bytes.len()!=20 { return Err(DhtError::InvalidInfoHash); }
    let mut ih=[0u8;20]; ih.copy_from_slice(&bytes); Ok(ih)
}

pub const TOKEN_EXPIRY: Duration = Duration::from_secs(600);
