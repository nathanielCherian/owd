package main

import (
	"encoding/binary"
	"math"
	"net"
	"time"
)

// Wire protocol
//
// TCP control channel (newline-delimited JSON, same port number as UDP):
//
//	client -> server  {"type":"hello","version":1,"per_probe":true,"stats_interval_ms":1000}
//	server -> client  {"type":"welcome","session":N,"udp_port":P,"server_time_ns":T,"max_reply":B}
//	server -> client  {"type":"probe",...}   one per probe the server received (if per_probe)
//	server -> client  {"type":"stats",...}   every stats_interval_ms (window + running total)
//	client -> server  {"type":"bye","sent":N}
//	server -> client  {"type":"final",...}   then the server closes the connection
//
// UDP probe (client -> server), big endian, padded to the requested size:
//
//	magic u32 | session u32 | seq u64 | t1 i64 | reply_len u16
//
// UDP echo (server -> client), padded to reply_len:
//
//	magic u32 | session u32 | seq u64 | t1 i64 | t2 i64 | t3 i64
//
// t1 = client send, t2 = server receive (kernel timestamp), t3 = server send,
// t4 = client receive (kernel timestamp). All are unix nanoseconds on the
// clock of the host that took them.
const (
	protoVersion = 1
	magicProbe   = 0x4F574450 // "OWDP"
	magicEcho    = 0x4F574445 // "OWDE"
	probeHdrLen  = 26
	echoHdrLen   = 40
	sockBufBytes = 4 << 20
)

type probeHdr struct {
	Session  uint32
	Seq      uint64
	T1       int64
	ReplyLen uint16
}

func (p *probeHdr) put(b []byte) {
	binary.BigEndian.PutUint32(b[0:], magicProbe)
	binary.BigEndian.PutUint32(b[4:], p.Session)
	binary.BigEndian.PutUint64(b[8:], p.Seq)
	binary.BigEndian.PutUint64(b[16:], uint64(p.T1))
	binary.BigEndian.PutUint16(b[24:], p.ReplyLen)
}

func parseProbe(b []byte) (p probeHdr, ok bool) {
	if len(b) < probeHdrLen || binary.BigEndian.Uint32(b) != magicProbe {
		return p, false
	}
	p.Session = binary.BigEndian.Uint32(b[4:])
	p.Seq = binary.BigEndian.Uint64(b[8:])
	p.T1 = int64(binary.BigEndian.Uint64(b[16:]))
	p.ReplyLen = binary.BigEndian.Uint16(b[24:])
	return p, true
}

type echoHdr struct {
	Session    uint32
	Seq        uint64
	T1, T2, T3 int64
}

func (e *echoHdr) put(b []byte) {
	binary.BigEndian.PutUint32(b[0:], magicEcho)
	binary.BigEndian.PutUint32(b[4:], e.Session)
	binary.BigEndian.PutUint64(b[8:], e.Seq)
	binary.BigEndian.PutUint64(b[16:], uint64(e.T1))
	binary.BigEndian.PutUint64(b[24:], uint64(e.T2))
	binary.BigEndian.PutUint64(b[32:], uint64(e.T3))
}

func parseEcho(b []byte) (e echoHdr, ok bool) {
	if len(b) < echoHdrLen || binary.BigEndian.Uint32(b) != magicEcho {
		return e, false
	}
	e.Session = binary.BigEndian.Uint32(b[4:])
	e.Seq = binary.BigEndian.Uint64(b[8:])
	e.T1 = int64(binary.BigEndian.Uint64(b[16:]))
	e.T2 = int64(binary.BigEndian.Uint64(b[24:]))
	e.T3 = int64(binary.BigEndian.Uint64(b[32:]))
	return e, true
}

// ctrlMsg is the union of every JSON message on the TCP channel.
type ctrlMsg struct {
	Type    string `json:"type"`
	Version int    `json:"version,omitempty"`
	Error   string `json:"error,omitempty"`

	// hello
	PerProbe        bool `json:"per_probe,omitempty"`
	StatsIntervalMs int  `json:"stats_interval_ms,omitempty"`

	// welcome
	Session      uint32 `json:"session,omitempty"`
	UDPPort      int    `json:"udp_port,omitempty"`
	ServerTimeNs int64  `json:"server_time_ns,omitempty"`
	MaxReply     int    `json:"max_reply,omitempty"`

	// probe
	Seq   uint64 `json:"seq,omitempty"`
	T1    int64  `json:"t1,omitempty"`
	T2    int64  `json:"t2,omitempty"`
	OWDNs int64  `json:"owd_ns,omitempty"`
	Size  int    `json:"size,omitempty"`

	// stats / final / bye
	ElapsedMs int64      `json:"elapsed_ms,omitempty"`
	Window    *statsJSON `json:"window,omitempty"`
	Total     *statsJSON `json:"total,omitempty"`
	Sent      int64      `json:"sent,omitempty"`
}

type statsJSON struct {
	Received  int64   `json:"received"`
	Bytes     int64   `json:"bytes"`
	Lost      int64   `json:"lost"`
	Reordered int64   `json:"reordered"`
	OWDMinMs  float64 `json:"owd_min_ms"`
	OWDAvgMs  float64 `json:"owd_avg_ms"`
	OWDMaxMs  float64 `json:"owd_max_ms"`
	JitterMs  float64 `json:"jitter_ms"`
}

// delayStats accumulates min/avg/max and RFC 3550 interarrival jitter of a
// series of delays in nanoseconds.
type delayStats struct {
	N             int64
	Bytes         int64
	Min, Max, Sum int64
	Jitter        float64
	last          int64
}

func (s *delayStats) add(d int64, size int) {
	if s.N == 0 {
		s.Min, s.Max = d, d
	} else {
		s.Min = min(s.Min, d)
		s.Max = max(s.Max, d)
		s.Jitter += (math.Abs(float64(d-s.last)) - s.Jitter) / 16
	}
	s.last = d
	s.N++
	s.Sum += d
	s.Bytes += int64(size)
}

func (s *delayStats) avg() float64 {
	if s.N == 0 {
		return math.NaN()
	}
	return float64(s.Sum) / float64(s.N)
}

func ms(ns float64) float64 { return math.Round(ns/1e3)/1e3 + 0 } // +0 drops -0

func (s *delayStats) json(lost, reordered int64) *statsJSON {
	j := &statsJSON{Received: s.N, Bytes: s.Bytes, Lost: lost, Reordered: reordered}
	if s.N > 0 {
		j.OWDMinMs = ms(float64(s.Min))
		j.OWDAvgMs = ms(s.avg())
		j.OWDMaxMs = ms(float64(s.Max))
		j.JitterMs = ms(s.Jitter)
	}
	return j
}

func now() int64 { return time.Now().UnixNano() }

func setSockBufs(c *net.UDPConn) {
	c.SetReadBuffer(sockBufBytes)
	c.SetWriteBuffer(sockBufBytes)
}
