package main

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type server struct {
	udp       *net.UDPConn
	udpPort   int
	maxReply  int
	maxSess   int
	strictIP  bool
	logDir    string
	sessions  sync.Map // uint32 -> *session
	nSessions atomic.Int32
}

type session struct {
	id       uint32
	peerIP   net.IP
	perProbe bool
	start    time.Time
	out      chan []byte // per-probe report lines; dropped when full
	done     chan struct{}

	mu        sync.Mutex
	total     delayStats
	window    delayStats
	maxSeq    int64
	winMaxSeq int64
	winFirst  int64
	reordered int64
	winReord  int64
	dropped   int64
	udpAddr   *net.UDPAddr
	csv       *bufio.Writer
	csvFile   *os.File
}

func runServer(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	port := fs.Int("port", 9910, "TCP control port (and UDP port unless -udp-port is set)")
	udpPort := fs.Int("udp-port", 0, "UDP probe port (default: same as -port)")
	bind := fs.String("bind", "", "address to bind (default: all, IPv4 and IPv6)")
	maxReply := fs.Int("max-reply", 1472, "largest UDP echo the server will send (bytes)")
	maxSess := fs.Int("max-sessions", 64, "maximum concurrent client sessions")
	strictIP := fs.Bool("strict-ip", true, "only accept probes from the IP that opened the TCP session (prevents reflection abuse)")
	logDir := fs.String("log-dir", "", "if set, write a CSV of every received probe per session into this directory")
	fs.Parse(args)
	if *udpPort == 0 {
		*udpPort = *port
	}
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	var bindIP net.IP
	if *bind != "" {
		if bindIP = net.ParseIP(*bind); bindIP == nil {
			log.Fatalf("bad -bind address %q", *bind)
		}
	}
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: bindIP, Port: *udpPort})
	if err != nil {
		log.Fatalf("udp listen: %v", err)
	}
	enableRxTimestamps(udp)
	setSockBufs(udp)
	ln, err := net.Listen("tcp", net.JoinHostPort(*bind, strconv.Itoa(*port)))
	if err != nil {
		log.Fatalf("tcp listen: %v", err)
	}
	if *logDir != "" {
		if err := os.MkdirAll(*logDir, 0o755); err != nil {
			log.Fatalf("log dir: %v", err)
		}
	}
	s := &server{udp: udp, udpPort: *udpPort, maxReply: max(*maxReply, echoHdrLen),
		maxSess: *maxSess, strictIP: *strictIP, logDir: *logDir}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		log.Printf("received %v, shutting down", <-sig)
		ln.Close()
		udp.Close()
	}()

	log.Printf("owd server %s listening: tcp %s, udp %s", version, ln.Addr(), udp.LocalAddr())
	go s.udpLoop()
	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				s.sessions.Range(func(_, v any) bool { v.(*session).closeLog(); return true })
				return
			}
			log.Printf("accept: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		go s.handleTCP(c)
	}
}

func (s *server) udpLoop() {
	buf := make([]byte, 65536)
	oob := make([]byte, 256)
	reply := make([]byte, 65536) // bytes past the header stay zero
	for {
		n, oobn, _, addr, err := s.udp.ReadMsgUDP(buf, oob)
		t2 := rxTimestamp(oob[:oobn])
		if t2 == 0 {
			t2 = now()
		}
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		p, ok := parseProbe(buf[:n])
		if !ok {
			continue
		}
		v, ok := s.sessions.Load(p.Session)
		if !ok {
			continue
		}
		sess := v.(*session)
		if s.strictIP && !sess.peerIP.Equal(addr.IP) {
			continue
		}
		rl := min(max(int(p.ReplyLen), echoHdrLen), s.maxReply)
		e := echoHdr{Session: p.Session, Seq: p.Seq, T1: p.T1, T2: t2}
		e.T3 = now()
		e.put(reply)
		s.udp.WriteToUDP(reply[:rl], addr)
		sess.record(p, t2, n, addr)
	}
}

func (ss *session) record(p probeHdr, t2 int64, size int, addr *net.UDPAddr) {
	owd := t2 - p.T1
	seq := int64(p.Seq)
	ss.mu.Lock()
	ss.udpAddr = addr
	if seq < ss.maxSeq {
		ss.reordered++
		ss.winReord++
	}
	ss.maxSeq = max(ss.maxSeq, seq)
	ss.winMaxSeq = max(ss.winMaxSeq, seq)
	if ss.winFirst < 0 {
		ss.winFirst = seq
	}
	ss.total.add(owd, size)
	ss.window.add(owd, size)
	if ss.csv != nil {
		fmt.Fprintf(ss.csv, "%d,%d,%d,%d,%d\n", p.Seq, p.T1, t2, owd, size)
	}
	ss.mu.Unlock()

	if ss.perProbe {
		line := fmt.Appendf(nil, `{"type":"probe","seq":%d,"t1":%d,"t2":%d,"owd_ns":%d,"size":%d}`+"\n",
			p.Seq, p.T1, t2, owd, size)
		select {
		case ss.out <- line:
		default:
			ss.mu.Lock()
			ss.dropped++
			ss.mu.Unlock()
		}
	}
}

// snapshot returns the window and total stats and starts a new window.
// Must be called with ss.mu held.
func (ss *session) snapshot(sent int64) (win, tot *statsJSON) {
	winLost := int64(0)
	if ss.winFirst >= 0 {
		winLost = max(ss.winMaxSeq-ss.winFirst+1-ss.window.N, 0)
	}
	expected := ss.maxSeq + 1
	if sent > 0 {
		expected = sent
	}
	win = ss.window.json(winLost, ss.winReord)
	tot = ss.total.json(max(expected-ss.total.N, 0), ss.reordered)
	ss.window = delayStats{}
	ss.winFirst, ss.winMaxSeq, ss.winReord = -1, -1, 0
	return win, tot
}

func (ss *session) closeLog() {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.csv != nil {
		ss.csv.Flush()
		ss.csvFile.Close()
		ss.csv = nil
	}
}

func newSessionID() uint32 {
	var b [4]byte
	rand.Read(b[:])
	return binary.BigEndian.Uint32(b[:]) | 1 // never 0
}

func (s *server) handleTCP(c net.Conn) {
	defer c.Close()
	peer := c.RemoteAddr().(*net.TCPAddr)
	br := bufio.NewReader(c)
	enc := json.NewEncoder(c)

	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	line, err := br.ReadBytes('\n')
	if err != nil {
		return
	}
	var hello ctrlMsg
	if json.Unmarshal(line, &hello) != nil || hello.Type != "hello" {
		enc.Encode(ctrlMsg{Type: "error", Error: "expected hello"})
		return
	}
	if int(s.nSessions.Add(1)) > s.maxSess {
		s.nSessions.Add(-1)
		enc.Encode(ctrlMsg{Type: "error", Error: "server busy"})
		return
	}
	defer s.nSessions.Add(-1)
	c.SetReadDeadline(time.Time{})

	interval := time.Duration(hello.StatsIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = time.Second
	}
	interval = max(interval, 100*time.Millisecond)

	ss := &session{id: newSessionID(), peerIP: peer.IP, perProbe: hello.PerProbe, start: time.Now(),
		out: make(chan []byte, 65536), done: make(chan struct{}),
		maxSeq: -1, winMaxSeq: -1, winFirst: -1}
	for {
		if _, loaded := s.sessions.LoadOrStore(ss.id, ss); !loaded {
			break
		}
		ss.id = newSessionID()
	}
	if s.logDir != "" {
		name := filepath.Join(s.logDir, fmt.Sprintf("%s_%s_%08x.csv",
			ss.start.Format("20060102-150405"), peer.IP, ss.id))
		if f, err := os.Create(name); err == nil {
			ss.csvFile, ss.csv = f, bufio.NewWriterSize(f, 1<<16)
			ss.csv.WriteString("seq,t1_ns,t2_ns,owd_ns,size\n")
		} else {
			log.Printf("session %08x: %v", ss.id, err)
		}
	}
	defer func() {
		s.sessions.Delete(ss.id)
		close(ss.done)
		ss.closeLog()
	}()
	log.Printf("session %08x opened by %s (per_probe=%v)", ss.id, peer, ss.perProbe)

	bw := bufio.NewWriterSize(c, 1<<16)
	if err := json.NewEncoder(bw).Encode(ctrlMsg{Type: "welcome", Version: protoVersion, Session: ss.id,
		UDPPort: s.udpPort, ServerTimeNs: now(), MaxReply: s.maxReply}); err != nil {
		return
	}
	bw.Flush()

	// Reader: waits for "bye" (or EOF) from the client.
	bye := make(chan int64, 1)
	go func() {
		defer close(bye)
		for {
			line, err := br.ReadBytes('\n')
			if err != nil {
				return
			}
			var m ctrlMsg
			if json.Unmarshal(line, &m) == nil && m.Type == "bye" {
				bye <- m.Sent
				return
			}
		}
	}()

	// Writer: the only goroutine that writes to the TCP connection.
	tick := time.NewTicker(interval)
	defer tick.Stop()
	writeStats := func(typ string, sent int64) error {
		ss.mu.Lock()
		win, tot := ss.snapshot(sent)
		ss.mu.Unlock()
		m := ctrlMsg{Type: typ, ElapsedMs: time.Since(ss.start).Milliseconds(), Window: win, Total: tot, Sent: sent}
		if typ == "final" {
			m.Window = nil
		}
		if err := json.NewEncoder(bw).Encode(m); err != nil {
			return err
		}
		return bw.Flush()
	}
	c.SetWriteDeadline(time.Time{})
	for {
		select {
		case line := <-ss.out:
			bw.Write(line)
			if len(ss.out) == 0 {
				if bw.Flush() != nil {
					return
				}
			}
		case <-tick.C:
			if writeStats("stats", 0) != nil {
				return
			}
		case sent, ok := <-bye:
			if !ok {
				log.Printf("session %08x: client disconnected", ss.id)
				return
			}
			for len(ss.out) > 0 {
				bw.Write(<-ss.out)
			}
			c.SetWriteDeadline(time.Now().Add(5 * time.Second))
			writeStats("final", sent)
			ss.mu.Lock()
			t, dropped := ss.total, ss.dropped
			ss.mu.Unlock()
			log.Printf("session %08x closed: sent=%d received=%d avg_owd=%.3fms dropped_reports=%d",
				ss.id, sent, t.N, t.avg()/1e6, dropped)
			// Let the client read the final message and close first.
			c.SetReadDeadline(time.Now().Add(2 * time.Second))
			br.ReadByte()
			return
		}
	}
}
