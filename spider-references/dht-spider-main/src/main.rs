use dht_spider::{Config, Dht, Mode};
use dht_spider::wire::WireRunner;
use serde_json::json;
use std::sync::Arc;

#[tokio::main]
async fn main() {
	// 默认 spider + wire，无需参数
	let mut cfg = Config::default();
	cfg.mode = Mode::Crawl;
	cfg.address = "0.0.0.0:6881".into();

	let mut d = match Dht::new(cfg.clone()).await {
		Ok(d) => d,
		Err(e) => {
			let line = json!({"level":"error","event":"startup","error": e.to_string()});
			println!("{}", line.to_string());
			return;
		}
	};

	// 不输出入站 get_peers（专注于 peer/种子/DHT 节点输出）
	d.callbacks.on_get_peers = None;

	// JSONL：入站 announce_peer（并触发 wire 下载）
	let (runner, handle) = WireRunner::new(65536, 4096, 256);
	{
		let mut sub = handle.subscribe();
		tokio::spawn(async move { runner.run().await; });
			// metadata 订阅
			tokio::spawn(async move {
				while let Ok(resp) = sub.recv().await {
					// 解码 metadata（bencode 的 info 字典），统一输出：type=metadata
				let infohash_hex = hex::encode(resp.request.info_hash);
				match dht_spider::bencode::decode(&resp.metadata_info) {
					Ok(dht_spider::bencode::BVal::Dict(m)) => {
						// name
						let name = m.get("name").and_then(|v| match v { dht_spider::bencode::BVal::Bytes(b) => std::str::from_utf8(b).ok().map(|s| s.to_string()), _ => None });
						if name.is_none() { continue; }
						let name = name.unwrap();

						// 统一输出：始终包含 files 数组
						let mut out_files = Vec::new();
						if let Some(dht_spider::bencode::BVal::List(files)) = m.get("files") {
							// 多文件
							for item in files {
								if let dht_spider::bencode::BVal::Dict(fm) = item {
									let length = fm.get("length").and_then(|v| match v { dht_spider::bencode::BVal::Int(n) => Some(*n as i64), _ => None });
									let paths = fm.get("path").and_then(|v| match v { dht_spider::bencode::BVal::List(ps) => {
										let mut vec = Vec::new();
										for p in ps { if let dht_spider::bencode::BVal::Bytes(pb) = p { if let Ok(s)=std::str::from_utf8(pb) { vec.push(s.to_string()); } } }
										Some(vec)
									}, _ => None });
									if let (Some(length), Some(paths)) = (length, paths) {
										out_files.push(json!({"path": paths, "length": length}));
									}
								}
							}
						} else if let Some(dht_spider::bencode::BVal::Int(len)) = m.get("length") {
							// 单文件：规范化为 files 数组
							out_files.push(json!({"path": [name.clone()], "length": *len as i64}));
						}

						if !out_files.is_empty() {
							let line = json!({
								"type": "metadata",
								"infohash": infohash_hex,
								"name": name,
								"files": out_files
							});
							println!("{}", line.to_string());
						}
					}
					_ => {}
				}
			}
		});

		// Peer Exchange (PeX) 订阅：统一输出 type=peer（来源：ut_pex）
		let mut psub = handle.subscribe_peers();
		tokio::spawn(async move {
			while let Ok(evt) = psub.recv().await {
				let line = json!({
					"type": "peer",
					"ip": evt.ip,
					"port": evt.port,
					"info_hash": hex::encode(evt.info_hash)
				});
				println!("{}", line.to_string());
			}
		});
	}

	d.callbacks.on_announce_peer = Some(Arc::new(move |ih, ip, port| {
		// peer：type=peer，扁平化 ip/port
			let line = json!({
				"type": "peer",
				"ip": ip,
				"port": port,
				"info_hash": ih
			});
		println!("{}", line.to_string());
		if let Ok(bytes) = hex::decode(&ih) {
			let handle = handle.clone_handle();
			tokio::spawn(async move {
				handle.request(&bytes, &ip, port).await;
			});
		}
	}));

	// 出站 get_peers 的 values 回调：同样按 peer + info_hash 输出（若使用主动查询）
	d.callbacks.on_get_peers_response = Some(Arc::new(|ih, peer| {
		let line = json!({
			"type": "peer",
			"ip": peer.ip.to_string(),
			"port": peer.port,
			"info_hash": ih
		});
		println!("{}", line.to_string());
	}));

	// DHT 节点信息（最简格式）：node(id, ip, port)
	d.callbacks.on_node = Some(Arc::new(|id_hex, ip, port| {
		let line = json!({
			"type": "node",
			"id": id_hex,
			"ip": ip,
			"port": port
		});
		println!("{}", line.to_string());
	}));

	let _handle = d.start();

	// 常驻
	loop { tokio::time::sleep(std::time::Duration::from_secs(60)).await; }
}
