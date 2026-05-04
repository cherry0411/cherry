//! KRPC helpers built on top of bencode helpers
use crate::types::DhtError;
use crate::bencode as be;
use be::{BVal};
use std::collections::BTreeMap;

pub fn make_query(t: &[u8], q: &str, a: BVal) -> Vec<u8> {
	let map = be::dict(vec![
		("t".into(), be::bytes(t)),
		("y".into(), be::bytes(b"q".to_vec())),
		("q".into(), be::bytes(q.as_bytes().to_vec())),
		("a".into(), a),
	]);
	be::encode(&map)
}

pub fn make_response(t: &[u8], r: BVal) -> Vec<u8> {
	let map = be::dict(vec![
		("t".into(), be::bytes(t)),
		("y".into(), be::bytes(b"r".to_vec())),
		("r".into(), r),
	]);
	be::encode(&map)
}

pub fn make_error(t: &[u8], code: i64, msg: &str) -> Vec<u8> {
	let list = be::list(vec![be::int(code), be::bytes(msg.as_bytes().to_vec())]);
	let map = be::dict(vec![
		("t".into(), be::bytes(t)),
		("y".into(), be::bytes(b"e".to_vec())),
		("e".into(), list),
	]);
	be::encode(&map)
}

pub fn decode_value(data: &[u8]) -> Result<BVal, DhtError> { be::decode(data).map_err(|e| DhtError::Bencode(format!("{:?}", e))) }

pub fn parse_message(val: &BVal) -> Result<(&[u8], &str, &BTreeMap<String, BVal>), DhtError> {
	let dict = match val { BVal::Dict(m) => m, _ => return Err(DhtError::Bencode("response is not dict".into())) };
	let t = match dict.get("t").ok_or_else(|| DhtError::Bencode("lack of key t".into()))? { BVal::Bytes(b)=> &b[..], _=> return Err(DhtError::Bencode("invalid t".into())) };
	let y = match dict.get("y").ok_or_else(|| DhtError::Bencode("lack of key y".into()))? { BVal::Bytes(b)=> std::str::from_utf8(b).map_err(|_| DhtError::Bencode("invalid y".into()))?, _=> return Err(DhtError::Bencode("invalid y".into())) };
	Ok((t, y, dict))
}

pub fn parse_key<'a>(m: &'a BTreeMap<String, BVal>, key: &str, t: &str) -> Result<&'a BVal, DhtError> {
	let v = m.get(key).ok_or_else(|| DhtError::Bencode("lack of key".into()))?;
	let ok = match (t, v) { ("string", BVal::Bytes(_)) | ("int", BVal::Int(_)) | ("map", BVal::Dict(_)) | ("list", BVal::List(_)) => true, _=>false };
	if !ok { return Err(DhtError::Bencode("invalid key type".into())); }
	Ok(v)
}

pub fn parse_keys<'a>(m: &'a BTreeMap<String, BVal>, pairs: &[(&str, &str)]) -> Result<(), DhtError> {
	for (k, t) in pairs { let _ = parse_key(m, k, t)?; } Ok(())
}
