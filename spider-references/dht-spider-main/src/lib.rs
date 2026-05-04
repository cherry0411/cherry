//! BitTorrent DHT (BEP-5) with crawler mode and optional metadata (BEP-9/10)
//!
//! - 提供 Standard 与 Crawl 两种模式
//! - 集成 Wire（BEP-9/10）元数据下载

pub mod types;
pub mod util;
pub mod bitmap;
pub mod blacklist;
pub mod peers;
pub mod bencode;
pub mod krpc;
pub mod routing;
pub mod dht;
pub mod token;
pub mod transaction;
pub mod command;
pub mod wire;

pub use dht::{Config, Dht};
pub use crate::types::Mode;
