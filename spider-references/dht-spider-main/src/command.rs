//! Internal commands & messages for asynchronous DHT handle communication.

#[derive(Debug)]
pub enum DhtCommand {
    GetPeers { info_hash: String },
    AnnouncePeer { info_hash: String, port: u16 },
    Shutdown,
}

#[derive(Debug)]
pub enum DhtEvent {
    PeerFound { info_hash: String, ip: String, port: u16 },
    Announced { info_hash: String, ip: String, port: u16 },
    GetPeersRequest { info_hash: String, ip: String, port: u16 },
}
