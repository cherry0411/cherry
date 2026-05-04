use rand::RngCore;
use std::net::{IpAddr, Ipv4Addr};

pub fn random_bytes(n: usize) -> Vec<u8> { let mut v=vec![0u8;n]; rand::thread_rng().fill_bytes(&mut v); v }

pub fn random_id20() -> [u8;20] { let mut id=[0u8;20]; rand::thread_rng().fill_bytes(&mut id); id }

// Encode compact IP/Port (IPv4) as in BEP5
pub fn encode_compact_ip_port(ip: IpAddr, port: u16) -> Option<[u8;6]> {
	match ip { IpAddr::V4(v4) => { let mut out=[0u8;6]; out[..4].copy_from_slice(&v4.octets()); out[4]=(port>>8) as u8; out[5]=(port & 0xff) as u8; Some(out) }, _=>None }
}

pub fn decode_compact_ip_port(info: &[u8]) -> Option<(IpAddr, u16)> {
	if info.len()!=6 { return None; }
	let ip=IpAddr::V4(Ipv4Addr::new(info[0],info[1],info[2],info[3]));
	let port = ((info[4] as u16) << 8) | (info[5] as u16);
	Some((ip, port))
}

// Big-endian minimal bytes for a positive u64
pub fn int2bytes(mut val: u64) -> Vec<u8> {
	let mut buf = [0u8;8];
	for i in (0..8).rev() { buf[i] = (val & 0xff) as u8; val >>= 8; }
	let first = buf.iter().position(|&b| b!=0).unwrap_or(7);
	buf[first..].to_vec()
}

pub fn bytes2int(data: &[u8]) -> u64 { data.iter().fold(0u64, |acc, &b| (acc<<8) | b as u64) }

// Decode concatenated compact node infos (26 bytes each): 20 bytes id + 6 bytes ip/port
pub fn decode_compact_nodes(data: &[u8]) -> Vec<([u8;20], IpAddr, u16)> {
	if data.len()%26 != 0 { return Vec::new(); }
	let mut out = Vec::with_capacity(data.len()/26);
	for i in 0..(data.len()/26) {
		let base = i*26;
		let mut id=[0u8;20]; id.copy_from_slice(&data[base..base+20]);
		if let Some((ip, port)) = decode_compact_ip_port(&data[base+20..base+26]) {
			out.push((id, ip, port));
		}
	}
	out
}
