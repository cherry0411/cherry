use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub enum BVal {
    Bytes(Vec<u8>),
    Int(i64),
    List(Vec<BVal>),
    Dict(BTreeMap<String, BVal>),
}

#[derive(thiserror::Error, Debug)]
pub enum BError {
    #[error("invalid bencode: {0}")] Invalid(&'static str),
    #[error("out of range")] OutOfRange,
    #[error("parse int error")] ParseInt,
}

fn find(data: &[u8], start: usize, target: u8) -> Option<usize> {
    data[start..].iter().position(|&c| c == target).map(|i| i + start)
}

pub fn decode_string(data: &[u8], i: usize) -> Result<(Vec<u8>, usize), BError> {
    if i >= data.len() || !(data[i].is_ascii_digit()) { return Err(BError::Invalid("invalid string bencode")); }
    let colon = find(data, i, b':').ok_or(BError::Invalid(": not found"))?;
    let len = std::str::from_utf8(&data[i..colon]).ok().and_then(|s| s.parse::<usize>().ok()).ok_or(BError::ParseInt)?;
    let end = colon + 1 + len;
    if end > data.len() || end < colon + 1 { return Err(BError::OutOfRange); }
    Ok((data[colon+1..end].to_vec(), end))
}

pub fn decode_int(data: &[u8], i: usize) -> Result<(i64, usize), BError> {
    if i >= data.len() || data[i] != b'i' { return Err(BError::Invalid("invalid int bencode")); }
    let end = find(data, i+1, b'e').ok_or(BError::Invalid("e not found"))?;
    let num = std::str::from_utf8(&data[i+1..end]).ok().and_then(|s| s.parse::<i64>().ok()).ok_or(BError::ParseInt)?;
    Ok((num, end+1))
}

fn decode_item(data: &[u8], i: usize) -> Result<(BVal, usize), BError> {
    if i>=data.len() { return Err(BError::OutOfRange); }
    match data[i] {
        b'l' => { let (v, j)= decode_list(data, i)?; Ok((BVal::List(v), j)) }
        b'd' => { let (m, j)= decode_dict(data, i)?; Ok((BVal::Dict(m), j)) }
        b'i' => { let (n, j)= decode_int(data, i)?; Ok((BVal::Int(n), j)) }
        b'0'..=b'9' => { let (s, j)= decode_string(data, i)?; Ok((BVal::Bytes(s), j)) }
        _ => Err(BError::Invalid("invalid item")),
    }
}

pub fn decode_list(data: &[u8], mut i: usize) -> Result<(Vec<BVal>, usize), BError> {
    if i >= data.len() || data[i] != b'l' { return Err(BError::Invalid("invalid list bencode")); }
    i += 1; let mut out = Vec::new();
    while i < data.len() {
        if data[i] == b'e' { return Ok((out, i+1)); }
        let (item, j) = decode_item(data, i)?; out.push(item); i = j;
    }
    Err(BError::Invalid("list missing 'e'"))
}

pub fn decode_dict(data: &[u8], mut i: usize) -> Result<(BTreeMap<String, BVal>, usize), BError> {
    if i >= data.len() || data[i] != b'd' { return Err(BError::Invalid("invalid dict bencode")); }
    i += 1; let mut out = BTreeMap::new();
    while i < data.len() {
        if data[i] == b'e' { return Ok((out, i+1)); }
        if !data[i].is_ascii_digit() { return Err(BError::Invalid("invalid dict key")); }
        let (k, j) = decode_string(data, i)?; i=j; if i>=data.len() { return Err(BError::OutOfRange); }
        let (v, j2) = decode_item(data, i)?; i=j2; out.insert(String::from_utf8_lossy(&k).into_owned(), v);
    }
    Err(BError::Invalid("dict missing 'e'"))
}

pub fn decode(data: &[u8]) -> Result<BVal, BError> { let (v, _) = decode_item(data, 0)?; Ok(v) }

fn encode_bytes(buf: &mut Vec<u8>, s: &[u8]) { buf.extend_from_slice(itoa::Buffer::new().format(s.len()).as_bytes()); buf.push(b':'); buf.extend_from_slice(s); }
fn encode_int(buf: &mut Vec<u8>, n: i64) { buf.push(b'i'); buf.extend_from_slice(itoa::Buffer::new().format(n).as_bytes()); buf.push(b'e'); }

pub fn encode_list(items: &[BVal]) -> Vec<u8> { let mut buf=Vec::new(); buf.push(b'l'); for it in items { encode_item_to(&mut buf, it); } buf.push(b'e'); buf }
pub fn encode_dict(m: &BTreeMap<String, BVal>) -> Vec<u8> { let mut buf=Vec::new(); buf.push(b'd'); for (k,v) in m { encode_bytes(&mut buf, k.as_bytes()); encode_item_to(&mut buf, v); } buf.push(b'e'); buf }

pub fn encode_item_to(buf: &mut Vec<u8>, v: &BVal) { match v { BVal::Bytes(b)=> encode_bytes(buf, b), BVal::Int(n)=> encode_int(buf, *n), BVal::List(l)=> { buf.push(b'l'); for it in l { encode_item_to(buf, it); } buf.push(b'e'); }, BVal::Dict(m)=> { buf.push(b'd'); for (k,v) in m { encode_bytes(buf, k.as_bytes()); encode_item_to(buf, v); } buf.push(b'e'); } } }
pub fn encode(v: &BVal) -> Vec<u8> { let mut buf=Vec::new(); encode_item_to(&mut buf, v); buf }

// helpers
pub fn dict(pairs: Vec<(String, BVal)>) -> BVal { let mut m=BTreeMap::new(); for (k,v) in pairs { m.insert(k,v); } BVal::Dict(m) }
pub fn list(items: Vec<BVal>) -> BVal { BVal::List(items) }
pub fn bytes<B: Into<Vec<u8>>>(b: B) -> BVal { BVal::Bytes(b.into()) }
pub fn int(n: i64) -> BVal { BVal::Int(n) }
