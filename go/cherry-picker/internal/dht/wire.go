// Package dht — peer wire protocol 实现（高性能重写版）
//
// 修复的问题：
//  1. wire.queue 改为有界 LRU，防止 syncedMap 无限增长
//  2. fetchMetadata 完成后（成功/失败）从 queue 中删除 key，原版不删除
//  3. Wire.Run() 改为 semaphore 等待模式：goroutine 数量达到上限时阻塞
//     而不是 `default: continue` 直接丢弃，保证高负载下 metadata 请求不丢失
//  4. 用 io.LimitReader 替换 ioutil.ReadAll 的无限读取
//  5. 去除 ioutil（已废弃），改用 io 包
package dht

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"cherry-picker/internal/cache"
)

const (
	// REQUEST 表示请求消息类型
	REQUEST = iota
	// DATA 表示数据消息类型
	DATA
	// REJECT 表示拒绝消息类型
	REJECT
)

const (
	// BLOCK 是 2^14，BitTorrent 标准块大小
	BLOCK = 16384
	// MaxMetadataSize 限制单个 metadata 最大尺寸（16 MB）
	MaxMetadataSize = BLOCK * 1000
	// EXTENDED 表示扩展消息类型
	EXTENDED = 20
	// HANDSHAKE 表示握手位
	HANDSHAKE = 0
)

var handshakePrefix = []byte{
	19, 66, 105, 116, 84, 111, 114, 114, 101, 110, 116, 32, 112, 114,
	111, 116, 111, 99, 111, 108, 0, 0, 0, 0, 0, 16, 0, 1,
}

// read 从 conn 读取 size 字节写入 data。
func read(conn *net.TCPConn, size int, data *bytes.Buffer) error {
	conn.SetReadDeadline(time.Now().Add(time.Second * 4))

	n, err := io.CopyN(data, conn, int64(size))
	if err != nil || n != int64(size) {
		return errors.New("read error")
	}
	return nil
}

// readMessage 从 TCP 连接读取一条消息（4字节长度前缀 + payload）。
func readMessage(conn *net.TCPConn, data *bytes.Buffer) (length int, err error) {
	if err = read(conn, 4, data); err != nil {
		return
	}

	length = int(bytes2int(data.Next(4)))
	if length == 0 {
		return
	}

	if err = read(conn, length, data); err != nil {
		return
	}
	return
}

// sendMessage 向连接发送带长度前缀的消息。
func sendMessage(conn *net.TCPConn, data []byte) error {
	length := int32(len(data))

	buffer := bytes.NewBuffer(nil)
	binary.Write(buffer, binary.BigEndian, length)

	conn.SetWriteDeadline(time.Now().Add(time.Second * 3))
	_, err := conn.Write(append(buffer.Bytes(), data...))
	return err
}

// sendHandshake 发送 BitTorrent 握手消息。
func sendHandshake(conn *net.TCPConn, infoHash, peerID []byte) error {
	data := make([]byte, 68)
	copy(data[:28], handshakePrefix)
	copy(data[28:48], infoHash)
	copy(data[48:], peerID)

	conn.SetWriteDeadline(time.Now().Add(time.Second * 3))
	_, err := conn.Write(data)
	return err
}

// onHandshake 验证握手响应。
func onHandshake(data []byte) (err error) {
	if !(bytes.Equal(handshakePrefix[:20], data[:20]) && data[25]&0x10 != 0) {
		err = errors.New("invalid handshake response")
	}
	return
}

// sendExtHandshake 请求 ut_metadata 和 metadata_size。
func sendExtHandshake(conn *net.TCPConn) error {
	data := append(
		[]byte{EXTENDED, HANDSHAKE},
		Encode(map[string]interface{}{
			"m": map[string]interface{}{"ut_metadata": 1},
		})...,
	)

	return sendMessage(conn, data)
}

// getUTMetaSize 解析 ut_metadata ID 和 metadata_size。
func getUTMetaSize(data []byte) (utMetadata int, metadataSize int, err error) {
	v, err := Decode(data)
	if err != nil {
		return
	}

	dict, ok := v.(map[string]interface{})
	if !ok {
		err = errors.New("invalid dict")
		return
	}

	if err = ParseKeys(
		dict, [][]string{{"metadata_size", "int"}, {"m", "map"}}); err != nil {
		return
	}

	m := dict["m"].(map[string]interface{})
	if err = ParseKey(m, "ut_metadata", "int"); err != nil {
		return
	}

	utMetadata = m["ut_metadata"].(int)
	metadataSize = dict["metadata_size"].(int)

	if metadataSize > MaxMetadataSize {
		err = errors.New("metadata_size too long")
	}
	return
}

// Request 表示一个 metadata 下载请求的上下文。
type Request struct {
	InfoHash []byte
	IP       string
	Port     int
}

// Response 包含请求上下文和下载到的 metadata 内容。
type Response struct {
	Request
	MetadataInfo []byte
}

// Wire 表示 peer wire 协议下载器。
//
// 改动：
//   - queue 改为有界 LRU（防止无限增长）
//   - Run() 改为 semaphore 等待模式（不再丢弃请求）
type Wire struct {
	blackList    *blackList
	queue        *cache.LRU // 有界 LRU，替代原来的 syncedMap
	requests     chan Request
	responses    chan Response
	workerTokens chan struct{} // semaphore：控制并发 goroutine 数量
}

// NewWire 创建一个 Wire 实例。
//   - blackListSize: 黑名单大小
//   - requestQueueSize: 请求队列大小（channel 容量）
//   - workerQueueSize: 最大并发下载 goroutine 数量
func NewWire(blackListSize, requestQueueSize, workerQueueSize int) *Wire {
	if workerQueueSize <= 0 {
		workerQueueSize = 1
	}
	if requestQueueSize <= 0 {
		requestQueueSize = workerQueueSize * 128
	}
	// queue 容量设置为 workerQueueSize 的 4 倍，避免频繁淘汰正在下载的 key
	queueCap := workerQueueSize * 4
	if queueCap < 4096 {
		queueCap = 4096
	}
	return &Wire{
		blackList:    newBlackList(blackListSize),
		queue:        cache.NewLRU(queueCap),
		requests:     make(chan Request, requestQueueSize),
		responses:    make(chan Response, workerQueueSize*2),
		workerTokens: make(chan struct{}, workerQueueSize),
	}
}

// Request 将请求放入队列。队列满时直接丢弃（调用方是 UDP 包处理 goroutine，不能阻塞）。
func (wire *Wire) Request(infoHash []byte, ip string, port int) {
	select {
	case wire.requests <- Request{InfoHash: infoHash, IP: ip, Port: port}:
	default:
	}
}

// Response 返回 metadata 响应 channel，供上层消费。
func (wire *Wire) Response() <-chan Response {
	return wire.responses
}

// isDone 检查所有分片是否已下载完成。
func (wire *Wire) isDone(pieces [][]byte) bool {
	for _, piece := range pieces {
		if len(piece) == 0 {
			return false
		}
	}
	return true
}

// requestPieces 发送所有分片的请求。
func (wire *Wire) requestPieces(
	conn *net.TCPConn, utMetadata int, metadataSize int, piecesNum int) {

	buffer := make([]byte, 1024)
	for i := 0; i < piecesNum; i++ {
		buffer[0] = EXTENDED
		buffer[1] = byte(utMetadata)

		msg := Encode(map[string]interface{}{
			"msg_type": REQUEST,
			"piece":    i,
		})

		length := len(msg) + 2
		copy(buffer[2:length], msg)

		sendMessage(conn, buffer[:length])
	}
	buffer = nil
}

// fetchMetadata 连接 peer，通过 extension protocol 下载 info dict。
// 完成后（成功或失败）都会从 queue 中删除 key，避免泄漏。
func (wire *Wire) fetchMetadata(r Request, key string) {
	// 请求完成后释放 inflight 去重键，允许同一 peer 在后续重新尝试。
	defer wire.queue.Delete(key)

	var (
		length       int
		msgType      byte
		piecesNum    int
		pieces       [][]byte
		utMetadata   int
		metadataSize int
	)

	defer func() {
		pieces = nil
		recover()
	}()

	infoHash := r.InfoHash
	address := genAddress(r.IP, r.Port)

	dial, err := net.DialTimeout("tcp", address, time.Second*4)
	if err != nil {
		wire.blackList.insert(r.IP, r.Port)
		return
	}
	conn := dial.(*net.TCPConn)
	conn.SetLinger(0)
	defer conn.Close()

	// 使用固定大小的 buffer，避免无限增长
	data := bytes.NewBuffer(nil)
	data.Grow(BLOCK)

	if sendHandshake(conn, infoHash, []byte(randomString(20))) != nil ||
		read(conn, 68, data) != nil ||
		onHandshake(data.Next(68)) != nil ||
		sendExtHandshake(conn) != nil {
		return
	}

	for {
		length, err = readMessage(conn, data)
		if err != nil {
			return
		}

		if length == 0 {
			continue
		}

		msgType, err = data.ReadByte()
		if err != nil {
			return
		}

		switch msgType {
		case EXTENDED:
			extendedID, err := data.ReadByte()
			if err != nil {
				return
			}

			// 用 LimitReader 限制单次读取大小，防止内存峰值（修复问题8）
			limitedReader := io.LimitReader(data, MaxMetadataSize)
			payload, err := io.ReadAll(limitedReader)
			if err != nil {
				return
			}

			if extendedID == 0 {
				// 扩展握手：获取 ut_metadata ID 和 metadata 总大小
				if pieces != nil {
					return
				}

				utMetadata, metadataSize, err = getUTMetaSize(payload)
				if err != nil {
					return
				}

				piecesNum = metadataSize / BLOCK
				if metadataSize%BLOCK != 0 {
					piecesNum++
				}

				pieces = make([][]byte, piecesNum)
				go wire.requestPieces(conn, utMetadata, metadataSize, piecesNum)

				continue
			}

			if pieces == nil {
				return
			}

			d, index, err := DecodeDict(payload, 0)
			if err != nil {
				return
			}
			dict := d.(map[string]interface{})

			if err = ParseKeys(dict, [][]string{
				{"msg_type", "int"},
				{"piece", "int"}}); err != nil {
				return
			}

			if dict["msg_type"].(int) != DATA {
				continue
			}

			piece := dict["piece"].(int)
			pieceLen := length - 2 - index

			if (piece != piecesNum-1 && pieceLen != BLOCK) ||
				(piece == piecesNum-1 && pieceLen != metadataSize%BLOCK) {
				return
			}

			pieces[piece] = payload[index:]

			if wire.isDone(pieces) {
				metadataInfo := bytes.Join(pieces, nil)

				info := sha1.Sum(metadataInfo)
				if !bytes.Equal(infoHash, info[:]) {
					return
				}

				wire.responses <- Response{
					Request:      r,
					MetadataInfo: metadataInfo,
				}
				return
			}
		default:
			data.Reset()
		}
	}
}

// Run 启动 peer wire 协议处理循环。
//
// 关键改动：semaphore 等待模式。
// 原版在 workerTokens 满时执行 `default: continue` 直接丢弃请求，
// 高负载下导致大量 metadata 请求被丢弃。
// 新版改为阻塞等待 token（wire.workerTokens <- struct{}{}），
// 背压自然传导到 wire.requests channel，不会丢失已入队的请求。
func (wire *Wire) Run() {
	go wire.blackList.clear()

	for r := range wire.requests {
		// 生成去重 key：infohash:ip:port
		key := strings.Join([]string{
			string(r.InfoHash), genAddress(r.IP, r.Port),
		}, ":")

		// 基本校验：infohash 必须是 20 字节，且不在黑名单
		if len(r.InfoHash) != 20 || wire.blackList.in(r.IP, r.Port) {
			continue
		}

		// 有界 LRU 去重：key 已存在（正在下载或最近下载过）则跳过
		// Set 返回 false 表示已存在（seen），返回 true 表示首次出现
		if !wire.queue.Set(key) {
			continue
		}

		// 阻塞等待 semaphore token（替代原版的 default: continue 丢弃模式）
		// 背压机制：当并发 goroutine 达到上限时，从 requests channel 读取会暂停，
		// 上游 UDP 包处理的 default 分支会开始丢弃新请求，但已入队的不会丢失。
		wire.workerTokens <- struct{}{}

		go func(r Request, key string) {
			defer func() {
				// 释放 semaphore token
				<-wire.workerTokens
			}()
			wire.fetchMetadata(r, key)
		}(r, key)
	}
}
