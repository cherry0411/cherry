package dht

import (
	"container/heap"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// maxPrefixLength is the length of DHT node.
const maxPrefixLength = 160

// node represents a DHT node.
type node struct {
	id             *bitmap
	addr           *net.UDPAddr
	lastActiveTime time.Time
	compactInfo    string
}

// newNode returns a node pointer.
func newNode(id, network, address string) (*node, error) {
	if len(id) != 20 {
		return nil, errors.New("node id should be a 20-length string")
	}

	addr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, err
	}

	bm := newBitmapFromString(id)
	return &node{
		id:             bm,
		addr:           addr,
		lastActiveTime: time.Now(),
		compactInfo:    id + compactIPPortInfo(addr.IP, addr.Port),
	}, nil
}

// newNodeFromCompactInfo parses compactNodeInfo and returns a node pointer.
func newNodeFromCompactInfo(
	compactNodeInfo string, network string) (*node, error) {

	if len(compactNodeInfo) != 26 {
		return nil, errors.New("compactNodeInfo should be a 26-length string")
	}

	id := compactNodeInfo[:20]
	ip, port, err := decodeCompactIPPortInfo(compactNodeInfo[20:])
	if err != nil {
		return nil, err
	}

	// Compact node data already contains a parsed IPv4 address and port.
	// Going through genAddress -> net.ResolveUDPAddr here used to dominate
	// crawler allocations while routing tables warmed up.
	_ = network // kept in the signature for compatibility with callers
	addr := &net.UDPAddr{IP: ip, Port: port}
	return &node{
		id:             newBitmapFromString(id),
		addr:           addr,
		lastActiveTime: time.Now(),
		compactInfo:    compactNodeInfo,
	}, nil
}

// CompactIPPortInfo returns "Compact IP-address/port info".
// See http://www.bittorrent.org/beps/bep_0005.html.
func (node *node) CompactIPPortInfo() string {
	info, _ := encodeCompactIPPortInfo(node.addr.IP, node.addr.Port)
	return info
}

// CompactNodeInfo returns "Compact node info".
// See http://www.bittorrent.org/beps/bep_0005.html.
func (node *node) CompactNodeInfo() string {
	return node.compactInfo
}

// Peer represents a peer contact.
type Peer struct {
	IP    net.IP
	Port  int
	token string
}

// newPeer returns a new peer pointer.
func newPeer(ip net.IP, port int, token string) *Peer {
	return &Peer{
		IP:    ip,
		Port:  port,
		token: token,
	}
}

// newPeerFromCompactIPPortInfo create a peer pointer by compact ip/port info.
func newPeerFromCompactIPPortInfo(compactInfo, token string) (*Peer, error) {
	ip, port, err := decodeCompactIPPortInfo(compactInfo)
	if err != nil {
		return nil, err
	}

	return newPeer(ip, port, token), nil
}

// CompactIPPortInfo returns "Compact node info".
// See http://www.bittorrent.org/beps/bep_0005.html.
func (p *Peer) CompactIPPortInfo() string {
	info, _ := encodeCompactIPPortInfo(p.IP, p.Port)
	return info
}

// peersManager represents a proxy that manipulates peers.
type peersManager struct {
	sync.RWMutex
	table *syncedMap
	dht   *DHT
}

// newPeersManager returns a new peersManager.
func newPeersManager(dht *DHT) *peersManager {
	return &peersManager{
		table: newSyncedMap(),
		dht:   dht,
	}
}

// Insert adds a peer into peersManager.
func (pm *peersManager) Insert(infoHash string, peer *Peer) {
	pm.Lock()
	if _, ok := pm.table.Get(infoHash); !ok {
		pm.table.Set(infoHash, newKeyedDeque())
	}
	pm.Unlock()

	v, _ := pm.table.Get(infoHash)
	queue := v.(*keyedDeque)

	queue.Push(peer.CompactIPPortInfo(), peer)
	if queue.Len() > pm.dht.K {
		queue.Remove(queue.Front())
	}
}

// GetPeers returns size-length peers who announces having infoHash.
func (pm *peersManager) GetPeers(infoHash string, size int) []*Peer {
	peers := make([]*Peer, 0, size)

	v, ok := pm.table.Get(infoHash)
	if !ok {
		return peers
	}

	for e := range v.(*keyedDeque).Iter() {
		peers = append(peers, e.Value.(*Peer))
	}

	if len(peers) > size {
		peers = peers[len(peers)-size:]
	}
	return peers
}

// kbucket represents a k-size bucket.
type kbucket struct {
	sync.RWMutex
	nodes, candidates *keyedDeque
	lastChanged       time.Time
	prefix            *bitmap
}

// newKBucket returns a new kbucket pointer.
func newKBucket(prefix *bitmap) *kbucket {
	bucket := &kbucket{
		nodes:       newKeyedDeque(),
		candidates:  newKeyedDeque(),
		lastChanged: time.Now(),
		prefix:      prefix,
	}
	return bucket
}

// LastChanged return the last time when it changes.
func (bucket *kbucket) LastChanged() time.Time {
	bucket.RLock()
	defer bucket.RUnlock()

	return bucket.lastChanged
}

// RandomChildID returns a random id that has the same prefix with bucket.
func (bucket *kbucket) RandomChildID() string {
	prefixLen := bucket.prefix.Size / 8

	return strings.Join([]string{
		bucket.prefix.RawString()[:prefixLen],
		randomString(20 - prefixLen),
	}, "")
}

// UpdateTimestamp update bucket's last changed time..
func (bucket *kbucket) UpdateTimestamp() {
	bucket.Lock()
	defer bucket.Unlock()

	bucket.lastChanged = time.Now()
}

// Insert inserts node to the bucket. It returns whether the node is new in
// the bucket.
func (bucket *kbucket) Insert(no *node) bool {
	isNew := !bucket.nodes.HasKey(no.id.RawString())

	bucket.nodes.Push(no.id.RawString(), no)
	bucket.UpdateTimestamp()

	return isNew
}

// Replace removes node, then put bucket.candidates.Back() to the right
// place of bucket.nodes.
func (bucket *kbucket) Replace(no *node) {
	bucket.nodes.Delete(no.id.RawString())
	bucket.UpdateTimestamp()

	if bucket.candidates.Len() == 0 {
		return
	}

	replacement := bucket.candidates.Remove(bucket.candidates.Back())
	if replacement == nil {
		return
	}
	no = replacement.(*node)
	bucket.nodes.Push(no.id.RawString(), no)
}

// Fresh pings the expired nodes in the bucket.
func (bucket *kbucket) Fresh(dht *DHT) {
	for e := range bucket.nodes.Iter() {
		no := e.Value.(*node)
		if time.Since(no.lastActiveTime) > dht.NodeExpriedAfter {
			dht.transactionManager.ping(no)
		}
	}
}

// routingTableNode represents routing table tree node.
type routingTableNode struct {
	sync.RWMutex
	children []*routingTableNode
	bucket   *kbucket
}

// newRoutingTableNode returns a new routingTableNode pointer.
func newRoutingTableNode(prefix *bitmap) *routingTableNode {
	return &routingTableNode{
		children: make([]*routingTableNode, 2),
		bucket:   newKBucket(prefix),
	}
}

// Child returns routingTableNode's left or right child.
func (tableNode *routingTableNode) Child(index int) *routingTableNode {
	if index >= 2 {
		return nil
	}

	tableNode.RLock()
	defer tableNode.RUnlock()

	return tableNode.children[index]
}

// SetChild sets routingTableNode's left or right child. When index is 0, it's
// the left child, if 1, it's the right child.
func (tableNode *routingTableNode) SetChild(index int, c *routingTableNode) {
	tableNode.Lock()
	defer tableNode.Unlock()

	tableNode.children[index] = c
}

// KBucket returns the bucket routingTableNode holds.
func (tableNode *routingTableNode) KBucket() *kbucket {
	tableNode.RLock()
	defer tableNode.RUnlock()

	return tableNode.bucket
}

// SetKBucket sets the bucket.
func (tableNode *routingTableNode) SetKBucket(bucket *kbucket) {
	tableNode.Lock()
	defer tableNode.Unlock()

	tableNode.bucket = bucket
}

// Split splits current routingTableNode and sets it's two children.
func (tableNode *routingTableNode) Split() {
	prefixLen := tableNode.KBucket().prefix.Size

	if prefixLen == maxPrefixLength {
		return
	}

	for i := 0; i < 2; i++ {
		tableNode.SetChild(i, newRoutingTableNode(newBitmapFrom(
			tableNode.KBucket().prefix, prefixLen+1)))
	}

	tableNode.Lock()
	tableNode.children[1].bucket.prefix.Set(prefixLen)
	tableNode.Unlock()

	for e := range tableNode.KBucket().nodes.Iter() {
		nd := e.Value.(*node)
		tableNode.Child(nd.id.Bit(prefixLen)).KBucket().nodes.Push(nd.id.RawString(), nd)
	}

	for e := range tableNode.KBucket().candidates.Iter() {
		nd := e.Value.(*node)
		tableNode.Child(nd.id.Bit(prefixLen)).KBucket().candidates.Push(nd.id.RawString(), nd)
	}

	for i := 0; i < 2; i++ {
		tableNode.Child(i).KBucket().UpdateTimestamp()
	}
}

// routingTable implements the routing table in DHT protocol.
type routingTable struct {
	*sync.RWMutex
	k              int
	root           *routingTableNode
	cachedNodes    *syncedMap
	cachedKBuckets *keyedDeque
	dht            *DHT
	clearQueue     *syncedList

	// nodeCount 无锁近似节点计数，供爬虫模式快速路径判断路由表是否已满。
	// 使用 atomic.Int64 避免获取写锁。
	nodeCount int64

	// 爬虫模式预缓存：K 个邻居节点的 compact node info 拼接字符串，每秒刷新一次。
	// 热路径直接原子读取，替代每请求 O(n×logK) 路由表扫描。
	cachedNeighborNodes atomic.Pointer[string]
}

// newRoutingTable returns a new routingTable pointer.
func newRoutingTable(k int, dht *DHT) *routingTable {
	root := newRoutingTableNode(newBitmap(0))

	rt := &routingTable{
		RWMutex:        &sync.RWMutex{},
		k:              k,
		root:           root,
		cachedNodes:    newSyncedMap(),
		cachedKBuckets: newKeyedDeque(),
		dht:            dht,
		clearQueue:     newSyncedList(),
	}

	rt.cachedKBuckets.Push(root.bucket.prefix.String(), root.bucket)
	return rt
}

// Insert adds a node to routing table. It returns whether the node is new
// in the routingtable.
func (rt *routingTable) Insert(nd *node) bool {
	// 快速路径：原子检查节点计数，表满时直接返回不获取写锁。
	if atomic.LoadInt64(&rt.nodeCount) >= int64(rt.dht.MaxNodes) {
		return false
	}

	rt.Lock()
	defer rt.Unlock()

	if rt.dht.blackList.in(nd.addr.IP.String(), nd.addr.Port) ||
		rt.cachedNodes.Len() >= rt.dht.MaxNodes {
		return false
	}

	var (
		next   *routingTableNode
		bucket *kbucket
	)
	root := rt.root

	for prefixLen := 1; prefixLen <= maxPrefixLength; prefixLen++ {
		next = root.Child(nd.id.Bit(prefixLen - 1))

		if next != nil {
			// If next is not the leaf.
			root = next
		} else if root.KBucket().nodes.Len() < rt.k ||
			root.KBucket().nodes.HasKey(nd.id.RawString()) {

			bucket = root.KBucket()
			isNew := bucket.Insert(nd)

			rt.cachedNodes.Set(nd.addr.String(), nd)
			rt.cachedKBuckets.Push(bucket.prefix.String(), bucket)
			if isNew {
				atomic.AddInt64(&rt.nodeCount, 1)
			}

			return isNew
		} else if root.KBucket().prefix.Compare(nd.id, prefixLen-1) == 0 {
			// If node has the same prefix with bucket, split it.

			root.Split()

			rt.cachedKBuckets.Delete(root.KBucket().prefix.String())
			root.SetKBucket(nil)

			for i := 0; i < 2; i++ {
				bucket = root.Child(i).KBucket()
				rt.cachedKBuckets.Push(bucket.prefix.String(), bucket)
			}

			root = root.Child(nd.id.Bit(prefixLen - 1))
		} else {
			// Finally, store node as a candidate and fresh the bucket.
			root.KBucket().candidates.Push(nd.id.RawString(), nd)
			if root.KBucket().candidates.Len() > rt.k {
				root.KBucket().candidates.Remove(
					root.KBucket().candidates.Front())
			}

			go root.KBucket().Fresh(rt.dht)
			return false
		}
	}
	return false
}

// GetNeighbors returns the size-length nodes closest to id.
func (rt *routingTable) GetNeighbors(id *bitmap, size int) []*node {
	rt.cachedNodes.RLock()
	defer rt.cachedNodes.RUnlock()

	count := len(rt.cachedNodes.data)
	if count == 0 {
		return nil
	}
	if size <= 0 || size >= count {
		nodes := make([]*node, 0, count)
		for _, val := range rt.cachedNodes.data {
			nodes = append(nodes, val.(*node))
		}
		return nodes
	}

	// Select directly from the protected map. Active lookups call this path
	// hundreds of times per second; avoiding an all-node snapshot removes one
	// O(n) allocation per lookup. Calls are spread across many DHT instances,
	// so the short read-side critical section does not serialize insertions.
	topk := newTopKHeap(id, size)
	for _, val := range rt.cachedNodes.data {
		topk.add(val.(*node))
	}
	return topk.sorted()
}

// AllNodes returns a snapshot of all cached nodes without additional sorting.
func (rt *routingTable) AllNodes() []*node {
	rt.cachedNodes.RLock()
	defer rt.cachedNodes.RUnlock()

	nodes := make([]*node, 0, len(rt.cachedNodes.data))
	for _, val := range rt.cachedNodes.data {
		nodes = append(nodes, val.(*node))
	}
	return nodes
}

// GetNeighborIds return the size-length compact node info closest to id.
func (rt *routingTable) GetNeighborCompactInfos(id *bitmap, size int) []string {
	neighbors := rt.GetNeighbors(id, size)
	infos := make([]string, len(neighbors))

	for i, no := range neighbors {
		infos[i] = no.compactInfo
	}

	return infos
}

// refreshCachedNeighbors 随机取 K 个节点预计算 compact nodes 字符串，每秒调用一次。
// 爬虫模式下不需要精确的最近邻，随机节点已足够保持在对方路由表中。
func (rt *routingTable) refreshCachedNeighbors() {
	rt.cachedNodes.RLock()
	nodes := make([]*node, 0, rt.dht.K)
	for _, val := range rt.cachedNodes.data {
		if len(nodes) >= rt.dht.K {
			break
		}
		nodes = append(nodes, val.(*node))
	}
	rt.cachedNodes.RUnlock()

	if len(nodes) == 0 {
		return
	}

	var b strings.Builder
	b.Grow(len(nodes) * 26)
	for _, n := range nodes {
		b.WriteString(n.compactInfo)
	}
	s := b.String()
	rt.cachedNeighborNodes.Store(&s)
}

// GetCachedNeighborNodes 返回预计算的 compact nodes 字符串（O(1) 原子读，无锁）。
func (rt *routingTable) GetCachedNeighborNodes() string {
	p := rt.cachedNeighborNodes.Load()
	if p == nil {
		return ""
	}
	return *p
}

// GetNodeKBucktById returns node whose id is `id` and the bucket it
// belongs to.
func (rt *routingTable) GetNodeKBucktByID(id *bitmap) (
	nd *node, bucket *kbucket) {

	rt.RLock()
	defer rt.RUnlock()

	var next *routingTableNode
	root := rt.root

	for prefixLen := 1; prefixLen <= maxPrefixLength; prefixLen++ {
		next = root.Child(id.Bit(prefixLen - 1))
		if next == nil {
			v, ok := root.KBucket().nodes.Get(id.RawString())
			if !ok {
				return
			}
			nd, bucket = v.Value.(*node), root.KBucket()
			return
		}
		root = next
	}
	return
}

// GetNodeByAddress finds node by address.
func (rt *routingTable) GetNodeByAddress(address string) (no *node, ok bool) {
	rt.RLock()
	defer rt.RUnlock()

	v, ok := rt.cachedNodes.Get(address)
	if ok {
		no = v.(*node)
	}
	return
}

// Remove deletes the node whose id is `id`.
func (rt *routingTable) Remove(id *bitmap) {
	if nd, bucket := rt.GetNodeKBucktByID(id); nd != nil {
		bucket.Replace(nd)
		rt.cachedNodes.Delete(nd.addr.String())
		rt.cachedKBuckets.Push(bucket.prefix.String(), bucket)
		atomic.AddInt64(&rt.nodeCount, -1)
	}
}

// Remove deletes the node whose address is `ip:port`.
func (rt *routingTable) RemoveByAddr(address string) {
	v, ok := rt.cachedNodes.Get(address)
	if ok {
		rt.Remove(v.(*node).id)
	}
}

// Fresh sends findNode to all nodes in the expired nodes.
func (rt *routingTable) Fresh() {
	now := time.Now()

	for e := range rt.cachedKBuckets.Iter() {
		bucket := e.Value.(*kbucket)
		if now.Sub(bucket.LastChanged()) < rt.dht.KBucketExpiredAfter ||
			bucket.nodes.Len() == 0 {
			continue
		}

		i := 0
		for e := range bucket.nodes.Iter() {
			if i < rt.dht.RefreshNodeNum {
				no := e.Value.(*node)
				rt.dht.transactionManager.findNode(no, bucket.RandomChildID())
				rt.clearQueue.PushBack(no)
			}
			i++
		}
	}

	if rt.dht.IsCrawlMode() {
		for e := range rt.clearQueue.Iter() {
			rt.Remove(e.Value.(*node).id)
		}
	}

	rt.clearQueue.Clear()
}

// Len returns the number of nodes in table.
func (rt *routingTable) Len() int {
	rt.RLock()
	defer rt.RUnlock()

	return rt.cachedNodes.Len()
}

// Implementation of heap with heap.Interface.
type topKHeap struct {
	target *bitmap
	nodes  []*node
}

func newTopKHeap(target *bitmap, capacity int) *topKHeap {
	return &topKHeap{target: target, nodes: make([]*node, 0, capacity)}
}

func (kHeap *topKHeap) Len() int {
	return len(kHeap.nodes)
}

func (kHeap *topKHeap) Less(i, j int) bool {
	// A max-heap by XOR distance: the farthest retained node is the root.
	return compareDistanceToTarget(kHeap.nodes[i].id, kHeap.nodes[j].id, kHeap.target) == 1
}

func (kHeap *topKHeap) Swap(i, j int) {
	kHeap.nodes[i], kHeap.nodes[j] = kHeap.nodes[j], kHeap.nodes[i]
}

func (kHeap *topKHeap) Push(x interface{}) {
	kHeap.nodes = append(kHeap.nodes, x.(*node))
}

func (kHeap *topKHeap) Pop() interface{} {
	n := len(kHeap.nodes)
	x := kHeap.nodes[n-1]
	kHeap.nodes[n-1] = nil
	kHeap.nodes = kHeap.nodes[:n-1]
	return x
}

func (kHeap *topKHeap) add(value *node) {
	if len(kHeap.nodes) < cap(kHeap.nodes) {
		heap.Push(kHeap, value)
		return
	}
	// The previous implementation compared against the last heap element,
	// which is not the farthest element in a binary heap. Compare with the
	// root and repair in place to keep the true closest K nodes.
	if compareDistanceToTarget(kHeap.nodes[0].id, value.id, kHeap.target) == 1 {
		kHeap.nodes[0] = value
		heap.Fix(kHeap, 0)
	}
}

func (kHeap *topKHeap) sorted() []*node {
	tops := make([]*node, kHeap.Len())
	for i := len(tops) - 1; i >= 0; i-- {
		tops[i] = heap.Pop(kHeap).(*node)
	}
	return tops
}

// getTopKNodes solves the top-k problem with heap. It's time complexity is
// O(n*log(k)).
func getTopKNodes(queue []*node, id *bitmap, k int) []*node {
	if k <= 0 || len(queue) == 0 {
		return nil
	}
	if k >= len(queue) {
		return queue
	}

	topk := newTopKHeap(id, k)
	for _, value := range queue {
		topk.add(value)
	}
	return topk.sorted()
}

func compareDistanceToTarget(left, right, target *bitmap) int {
	for i := 0; i < len(target.data); i++ {
		leftDistance := left.data[i] ^ target.data[i]
		rightDistance := right.data[i] ^ target.data[i]
		if leftDistance > rightDistance {
			return 1
		}
		if leftDistance < rightDistance {
			return -1
		}
	}
	return 0
}

func compactIPPortInfo(ip net.IP, port int) string {
	ip4 := ip.To4()
	if ip4 == nil {
		return ""
	}
	buf := [6]byte{ip4[0], ip4[1], ip4[2], ip4[3], byte(port >> 8), byte(port)}
	return string(buf[:])
}
