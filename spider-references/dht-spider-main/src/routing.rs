use crate::types::Node;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use crate::bitmap::Bitmap;

const MAX_PREFIX: usize = 160;

#[derive(Clone)]
pub struct KBucketInner {
    pub nodes: VecDeque<Node>,
    pub candidates: VecDeque<Node>,
    pub prefix: Bitmap,
    pub last_changed: Instant,
}

impl KBucketInner {
    pub fn new(prefix: Bitmap) -> Self {
        Self { nodes: VecDeque::new(), candidates: VecDeque::new(), prefix, last_changed: Instant::now() }
    }
    pub fn touch(&mut self){ self.last_changed=Instant::now(); }
    pub fn last_changed(&self) -> Instant { self.last_changed }
    fn reorder_by_last_active(&mut self) {
    // Keep nodes ordered by last_active ascending (oldest first)
        let mut v: Vec<Node> = self.nodes.drain(..).collect();
        v.sort_by_key(|n| n.last_active);
        self.nodes = v.into();
    }
    pub fn insert(&mut self, node: Node, k: usize) -> bool {
        // update if exists
        if let Some(pos) = self.nodes.iter().position(|n| n.id == node.id) {
            self.nodes[pos] = node;
            self.reorder_by_last_active();
            self.touch();
            return false;
        }
        if self.nodes.len() < k {
            self.nodes.push_back(node);
            self.reorder_by_last_active();
            self.touch();
            return true;
        }
        false
    }
    pub fn add_candidate(&mut self, node: Node, k: usize) {
        self.candidates.push_back(node);
        if self.candidates.len() > k { self.candidates.pop_front(); }
        self.touch();
    }
    pub fn replace(&mut self, removed_id: &[u8;20]) {
        if let Some(pos) = self.nodes.iter().position(|n| &n.id.0 == removed_id) {
            self.nodes.remove(pos);
            if let Some(cand) = self.candidates.pop_back() {
                self.nodes.push_back(cand);
                self.reorder_by_last_active();
            }
            self.touch();
        }
    }
    pub fn random_child_id(&self) -> [u8;20] {
        use rand::RngCore;
        let mut out=[0u8;20];
    let size=self.prefix.size();
        let full = size/8;
        let rem = size%8;
        // copy full bytes
        if full>0 { out[..full].copy_from_slice(&self.prefix.as_bytes()[..full]); }
        let mut rng = rand::thread_rng();
        if rem>0 {
            let mask = 0xFFu8 << (8 - rem);
            let prefix_byte = self.prefix.as_bytes()[full] & mask;
            let mut rbyte = 0u8; rng.fill_bytes(std::slice::from_mut(&mut rbyte));
            // keep top rem bits from prefix, randomize lower bits
            out[full] = prefix_byte | (rbyte & (!mask));
            if full+1 < 20 { rng.fill_bytes(&mut out[full+1..]); }
        } else {
            if full < 20 { rng.fill_bytes(&mut out[full..]); }
        }
        out
    }
}

type KBucket = Arc<Mutex<KBucketInner>>;

struct RoutingNode {
    left: Option<Box<RoutingNode>>, // bit = 0
    right: Option<Box<RoutingNode>>, // bit = 1
    bucket: Option<KBucket>,
}

impl RoutingNode {
    fn new(prefix: Bitmap) -> Self { Self{ left: None, right: None, bucket: Some(Arc::new(Mutex::new(KBucketInner::new(prefix)))) } }
    fn child(&self, bit: u8) -> Option<&Box<RoutingNode>> { if bit==0 { self.left.as_ref() } else { self.right.as_ref() } }
    fn child_mut(&mut self, bit: u8) -> Option<&mut Box<RoutingNode>> { if bit==0 { self.left.as_mut() } else { self.right.as_mut() } }
    fn set_child(&mut self, bit: u8, node: RoutingNode) { if bit==0 { self.left = Some(Box::new(node)); } else { self.right = Some(Box::new(node)); } }
    fn bucket(&self) -> Option<KBucket> { self.bucket.clone() }
    fn set_bucket(&mut self, b: Option<KBucket>) { self.bucket = b; }
}

pub struct RoutingTable {
    pub k: usize,
    pub cached_nodes: DashMap<String, Node>,
    cached_buckets: Vec<KBucket>,
    root: RoutingNode,
}

impl RoutingTable {
    pub fn new(k: usize) -> Self {
        let root = RoutingNode::new(Bitmap::new(0));
        let mut table = Self{ k, cached_nodes: DashMap::new(), cached_buckets: Vec::new(), root };
        if let Some(b)=table.root.bucket() { table.cached_buckets.push(b); }
        table
    }
    pub fn len(&self) -> usize { self.cached_nodes.len() }
    fn bit_of(id: &[u8;20], index: usize) -> u8 { let div=index>>3; let m=index & 7; (id[div] & (1u8 << (7-m))) >> (7-m) }
    fn split(node: &mut RoutingNode) {
        // split only when bucket exists and prefix size < 160
        let Some(bucket_arc) = node.bucket() else { return; };
    let prefix_len = bucket_arc.lock().unwrap().prefix.size();
        if prefix_len >= MAX_PREFIX { return; }

        // create child prefixes
    let left_prefix = bucket_arc.lock().unwrap().prefix.extend_one(0);
    let right_prefix = bucket_arc.lock().unwrap().prefix.extend_one(1);

    let left = RoutingNode::new(left_prefix);
    let right = RoutingNode::new(right_prefix);

        // redistribute nodes and candidates
        let mut bucket = bucket_arc.lock().unwrap();
        for n in bucket.nodes.drain(..) {
            let b = Self::bit_of(&n.id.0, prefix_len);
            if b==0 { left.bucket().unwrap().lock().unwrap().nodes.push_back(n); }
            else { right.bucket().unwrap().lock().unwrap().nodes.push_back(n); }
        }
        for n in bucket.candidates.drain(..) {
            let b = Self::bit_of(&n.id.0, prefix_len);
            if b==0 { left.bucket().unwrap().lock().unwrap().candidates.push_back(n); }
            else { right.bucket().unwrap().lock().unwrap().candidates.push_back(n); }
        }

        node.set_child(0, left);
        node.set_child(1, right);
        node.set_bucket(None);
    }

    pub fn insert(&mut self, node: Node) -> bool {
        // traverse
        let mut cur = &mut self.root;
        for prefix_len in 1..=MAX_PREFIX {
            let bit = Self::bit_of(&node.id.0, prefix_len-1);
            if cur.child(bit).is_some() {
                // descend
                let next_ptr: *mut RoutingNode = &mut **cur.child_mut(bit).unwrap();
                // SAFETY: next_ptr points to valid child we just borrowed mutably
                cur = unsafe { &mut *next_ptr };
                continue;
            }

            // we're at a leaf holding a bucket
            let bucket_arc = cur.bucket.clone().expect("leaf must have bucket");
            let mut bucket = bucket_arc.lock().unwrap();
            if bucket.nodes.len() < self.k || bucket.nodes.iter().any(|n| n.id == node.id) {
                let is_new = bucket.insert(node.clone(), self.k);
                self.cached_nodes.insert(node.addr.to_string(), node);
                // ensure cached buckets contain this leaf bucket
                if !self.cached_buckets.iter().any(|b| std::ptr::eq(&**b, &*bucket_arc)) { self.cached_buckets.push(bucket_arc.clone()); }
                return is_new;
            }

            // if node falls into this bucket's prefix, split
            let prefix = bucket.prefix.clone();
            drop(bucket); // release before split
            if prefix.matches_id_prefix(&node.id.0) {
                Self::split(cur);
                // add children buckets to cache list
                if let Some(ref c0)=cur.left { if let Some(lb)=c0.bucket.clone() { self.cached_buckets.push(lb); } }
                if let Some(ref c1)=cur.right { if let Some(rb)=c1.bucket.clone() { self.cached_buckets.push(rb); } }
                // move to child according to bit
                let next_ptr: *mut RoutingNode = &mut **cur.child_mut(bit).unwrap();
                cur = unsafe { &mut *next_ptr };
                continue;
            } else {
                // store as candidate and fresh bucket
                let mut b = cur.bucket.as_ref().unwrap().lock().unwrap();
                b.add_candidate(node, self.k);
                return false;
            }
        }
        false
    }

    pub fn neighbors_by_target(&self, target: &[u8;20], size: usize) -> Vec<Node> {
        let t = Bitmap::from_bytes(target);
        let mut list: Vec<Node> = self.cached_nodes.iter().map(|e| e.value().clone()).collect();
        list.sort_by(|a,b| {
            let da = t.xor(&Bitmap::from_bytes(&a.id.0));
            let db = t.xor(&Bitmap::from_bytes(&b.id.0));
            let cmp = da.compare_prefix(&db, MAX_PREFIX);
            if cmp<0 { std::cmp::Ordering::Less } else if cmp>0 { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Equal }
        });
        if list.len() > size { list.truncate(size); }
        list
    }

    pub fn neighbors(&self, limit: usize) -> Vec<Node> { self.cached_nodes.iter().take(limit).map(|e| e.value().clone()).collect() }
    pub fn get_neighbors_compact(&self, target: &[u8;20], size: usize) -> Vec<u8> {
        let list = self.neighbors_by_target(target, size);
        let mut out = Vec::with_capacity(list.len()*26);
        for n in list { out.extend_from_slice(&n.id.0); if let Some(comp)=crate::util::encode_compact_ip_port(n.addr.ip(), n.addr.port()) { out.extend_from_slice(&comp); } }
        out
    }

    pub fn remove_by_addr(&mut self, addr: &str) {
        if let Some((_k, no)) = self.cached_nodes.remove(addr) {
            // Find leaf bucket containing this node by walking tree
            self.remove_from_buckets(&no.id.0);
        }
    }

    fn remove_from_buckets(&mut self, id: &[u8;20]) {
        // traverse immutably to leaf by bits; then operate on bucket
        let mut cur = &self.root;
        for prefix_len in 1..=MAX_PREFIX {
            if let Some(next) = cur.child(Self::bit_of(id, prefix_len-1)) { cur = next; } else { break; }
        }
        if let Some(b) = cur.bucket.clone() { b.lock().unwrap().replace(id); }
    }

    pub fn fresh_plan(&self, now: Instant, kbucket_expired_after: Duration, refresh_node_num: usize, is_crawl_mode: bool) -> (Vec<(std::net::SocketAddr, [u8;20])>, Vec<String>) {
        let mut tasks = Vec::new();
        let mut clear_addrs = Vec::new();
        for b in &self.cached_buckets {
            let b_guard = b.lock().unwrap();
            if b_guard.nodes.is_empty() { continue; }
            if now.duration_since(b_guard.last_changed()) < kbucket_expired_after { continue; }
            let target = b_guard.random_child_id();
            for (i, n) in b_guard.nodes.iter().enumerate() { if i>=refresh_node_num { break; } tasks.push((n.addr, target)); if is_crawl_mode { clear_addrs.push(n.addr.to_string()); } }
        }
        (tasks, clear_addrs)
    }

    // Collect nodes that appear inactive beyond threshold; used to drive ping checks
    pub fn collect_expired_nodes(&self, now: Instant, node_expired_after: Duration) -> Vec<std::net::SocketAddr> {
        let mut out = Vec::new();
        for b in &self.cached_buckets {
            let b_guard = b.lock().unwrap();
            for n in b_guard.nodes.iter() {
                if now.duration_since(n.last_active) > node_expired_after {
                    out.push(n.addr);
                }
            }
        }
        out
    }

    // Remove nodes that are stale (inactive beyond threshold) and trim to max_nodes
    pub fn prune(&mut self, now: Instant, node_expired_after: Duration, max_nodes: usize) {
        // 1) Remove stale by last_active
        let to_remove: Vec<String> = self.cached_nodes.iter()
            .filter(|e| now.duration_since(e.value().last_active) > node_expired_after)
            .map(|e| e.key().clone()).collect();
        for addr in to_remove { self.remove_by_addr(&addr); }

        // 2) Enforce max_nodes: remove oldest last_active first
        let cur_len = self.cached_nodes.len();
        if cur_len > max_nodes {
            let mut nodes: Vec<Node> = self.cached_nodes.iter().map(|e| e.value().clone()).collect();
            nodes.sort_by_key(|n| n.last_active); // oldest first
            let excess = cur_len - max_nodes;
            for n in nodes.into_iter().take(excess) { self.remove_by_addr(&n.addr.to_string()); }
        }
    }
}
