// Minimal bitmap implementation for prefix operations

#[derive(Clone, Debug)]
pub struct Bitmap { pub(crate) size: usize, data: Vec<u8> }

impl Bitmap {
    pub fn new(size_bits: usize) -> Self { let bytes=(size_bits+7)/8; Self{size: size_bits, data: vec![0;bytes]} }
    pub fn from_bytes(data: &[u8]) -> Self { Self{ size: data.len()*8, data: data.to_vec() } }
    pub fn from_str_bytes(s: &str) -> Self { Self::from_bytes(s.as_bytes()) }
    pub fn size(&self) -> usize { self.size }
    pub fn bit(&self, index: usize) -> u8 { assert!(index<self.size); let div=index>>3; let m=index & 7; (self.data[div] & (1u8 << (7-m))) >> (7-m) }
    pub fn set(&mut self, index: usize) { assert!(index<self.size); let div=index>>3; let m=index & 7; let sh=1u8 << (7-m); self.data[div]|=sh; }
    pub fn unset(&mut self, index: usize) { assert!(index<self.size); let div=index>>3; let m=index & 7; let sh=1u8 << (7-m); self.data[div]&=!sh; }
    pub fn compare_prefix(&self, other:&Bitmap, prefix_len: usize) -> i8 { assert!(prefix_len<=self.size && prefix_len<=other.size); let div=prefix_len>>3; let modb=prefix_len & 7; let ord=self.data[..div].cmp(&other.data[..div]); if ord!=std::cmp::Ordering::Equal { return if ord==std::cmp::Ordering::Less {-1} else {1}; }
        for i in (div<<3)..((div<<3)+modb) { let b1=self.bit(i); let b2=other.bit(i); if b1>b2 {return 1} else if b1<b2 {return -1} }
        0 }
    pub fn xor(&self, other:&Bitmap) -> Bitmap { assert_eq!(self.size, other.size); let mut d=vec![0u8; self.data.len()]; for i in 0..d.len(){ d[i]=self.data[i]^other.data[i]; } Bitmap{ size: self.size, data: d } }
    pub fn raw_string(&self) -> String { String::from_utf8_lossy(&self.data).into_owned() }
    pub fn as_bytes(&self) -> &[u8] { &self.data }
    pub fn extend_one(&self, bit: u8) -> Bitmap { let mut out = Bitmap{ size: self.size+1, data: self.data.clone() }; if out.data.len() < (out.size+7)/8 { out.data.resize((out.size+7)/8, 0); } if bit==1 { out.set(self.size); } out }
    pub fn matches_id_prefix(&self, id: &[u8;20]) -> bool {
        let div=self.size>>3; let rem=self.size & 7;
        if div>0 && &self.data[..div] != &id[..div] { return false; }
        if rem==0 { return true; }
        for i in (div<<3)..((div<<3)+rem) {
            let ddiv=i>>3; let dm=i & 7; let bit = (id[ddiv] & (1u8 << (7-dm))) >> (7-dm);
            if self.bit(i) != bit { return false; }
        }
        true
    }
}
