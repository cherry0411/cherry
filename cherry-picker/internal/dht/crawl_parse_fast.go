package dht

import "unsafe"

type crawlPacket struct {
	t          string
	y          string
	q          string
	id         string
	target     string
	infoHash   string
	token      string
	nodes      string
	samples    string
	values     []string
	hasValues  bool
	hasSamples bool
	interval   int
	num        int
	port       int
	implied    int
	hasPayload bool
}

func parseCrawlPacket(data []byte) (crawlPacket, bool) {
	var pkt crawlPacket
	if len(data) == 0 || data[0] != 'd' {
		return pkt, false
	}
	ok := parseCrawlTopDict(data, 1, &pkt)
	if !ok || pkt.t == "" || pkt.y == "" {
		return pkt, false
	}
	return pkt, true
}

func parseCrawlTopDict(data []byte, i int, pkt *crawlPacket) bool {
	for i < len(data) {
		if data[i] == 'e' {
			return true
		}
		key, next, ok := parseCrawlString(data, i)
		if !ok {
			return false
		}
		i = next
		switch key {
		case "t":
			pkt.t, i, ok = parseCrawlString(data, i)
		case "y":
			pkt.y, i, ok = parseCrawlString(data, i)
		case "q":
			pkt.q, i, ok = parseCrawlString(data, i)
		case "a":
			if i >= len(data) || data[i] != 'd' {
				return false
			}
			i, ok = parseCrawlArgsDict(data, i+1, pkt)
			pkt.hasPayload = ok
		case "r":
			if i >= len(data) || data[i] != 'd' {
				return false
			}
			i, ok = parseCrawlRespDict(data, i+1, pkt)
			pkt.hasPayload = ok
		default:
			i, ok = skipCrawlValue(data, i)
		}
		if !ok {
			return false
		}
	}
	return false
}

func parseCrawlArgsDict(data []byte, i int, pkt *crawlPacket) (int, bool) {
	for i < len(data) {
		if data[i] == 'e' {
			return i + 1, true
		}
		key, next, ok := parseCrawlString(data, i)
		if !ok {
			return i, false
		}
		i = next
		switch key {
		case "id":
			pkt.id, i, ok = parseCrawlString(data, i)
		case "target":
			pkt.target, i, ok = parseCrawlString(data, i)
		case "info_hash":
			pkt.infoHash, i, ok = parseCrawlString(data, i)
		case "token":
			pkt.token, i, ok = parseCrawlString(data, i)
		case "port":
			pkt.port, i, ok = parseCrawlInt(data, i)
		case "implied_port":
			pkt.implied, i, ok = parseCrawlInt(data, i)
		default:
			i, ok = skipCrawlValue(data, i)
		}
		if !ok {
			return i, false
		}
	}
	return i, false
}

func parseCrawlRespDict(data []byte, i int, pkt *crawlPacket) (int, bool) {
	for i < len(data) {
		if data[i] == 'e' {
			return i + 1, true
		}
		key, next, ok := parseCrawlString(data, i)
		if !ok {
			return i, false
		}
		i = next
		switch key {
		case "id":
			pkt.id, i, ok = parseCrawlString(data, i)
		case "nodes":
			pkt.nodes, i, ok = parseCrawlString(data, i)
		case "token":
			pkt.token, i, ok = parseCrawlString(data, i)
		case "values":
			pkt.values, i, ok = parseCrawlStringList(data, i)
			pkt.hasValues = ok
		case "samples":
			pkt.samples, i, ok = parseCrawlString(data, i)
			pkt.hasSamples = ok
		case "interval":
			pkt.interval, i, ok = parseCrawlInt(data, i)
		case "num":
			pkt.num, i, ok = parseCrawlInt(data, i)
		default:
			i, ok = skipCrawlValue(data, i)
		}
		if !ok {
			return i, false
		}
	}
	return i, false
}

func parseCrawlStringList(data []byte, i int) ([]string, int, bool) {
	if i >= len(data) || data[i] != 'l' {
		return nil, i, false
	}
	i++
	values := make([]string, 0, 8)
	for i < len(data) {
		if data[i] == 'e' {
			return values, i + 1, true
		}
		v, next, ok := parseCrawlString(data, i)
		if !ok {
			return nil, i, false
		}
		values = append(values, v)
		i = next
	}
	return nil, i, false
}

func parseCrawlString(data []byte, i int) (string, int, bool) {
	if i >= len(data) || data[i] < '0' || data[i] > '9' {
		return "", i, false
	}
	n := 0
	for i < len(data) && data[i] >= '0' && data[i] <= '9' {
		n = n*10 + int(data[i]-'0')
		i++
		if n > len(data) {
			return "", i, false
		}
	}
	if i >= len(data) || data[i] != ':' {
		return "", i, false
	}
	i++
	end := i + n
	if end < i || end > len(data) {
		return "", i, false
	}
	if n == 0 {
		return "", end, true
	}
	return unsafe.String(&data[i], n), end, true
}

func parseCrawlInt(data []byte, i int) (int, int, bool) {
	if i >= len(data) || data[i] != 'i' {
		return 0, i, false
	}
	i++
	sign := 1
	if i < len(data) && data[i] == '-' {
		sign = -1
		i++
	}
	n := 0
	hasDigit := false
	for i < len(data) && data[i] >= '0' && data[i] <= '9' {
		n = n*10 + int(data[i]-'0')
		i++
		hasDigit = true
	}
	if !hasDigit || i >= len(data) || data[i] != 'e' {
		return 0, i, false
	}
	return n * sign, i + 1, true
}

func skipCrawlValue(data []byte, i int) (int, bool) {
	if i >= len(data) {
		return i, false
	}
	switch {
	case data[i] >= '0' && data[i] <= '9':
		_, next, ok := parseCrawlString(data, i)
		return next, ok
	case data[i] == 'i':
		_, next, ok := parseCrawlInt(data, i)
		return next, ok
	case data[i] == 'l':
		i++
		for i < len(data) {
			if data[i] == 'e' {
				return i + 1, true
			}
			next, ok := skipCrawlValue(data, i)
			if !ok {
				return i, false
			}
			i = next
		}
	case data[i] == 'd':
		i++
		for i < len(data) {
			if data[i] == 'e' {
				return i + 1, true
			}
			_, next, ok := parseCrawlString(data, i)
			if !ok {
				return i, false
			}
			i, ok = skipCrawlValue(data, next)
			if !ok {
				return i, false
			}
		}
	}
	return i, false
}
