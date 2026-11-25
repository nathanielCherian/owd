package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type clientStats struct {
	mu        sync.Mutex
	sent      int64
	echoed    int64
	rtt, up   delayStats // up = t2-t1 as seen in echoes
	down      delayStats // t4-t3
	wRTT, wUp delayStats
	wDown     delayStats
	bestRTT   int64
	offset    int64 // server clock - client clock, from the min-RTT echo
	srvTotal  *statsJSON
	srvProbes int64 // per-probe TCP reports received
	srvFinal  *ctrlMsg
}

func runClient(args []string) {
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	host := fs.String("host", "", "server host or IP (required)")
	port := fs.Int("port", 9910, "server TCP control port")
	interval := fs.Duration("i", 10*time.Millisecond, "interval between probes")
	dur := fs.Duration("t", 10*time.Second, "test duration (0 = until Ctrl-C)")
	size := fs.Int("size", 64, "UDP probe payload size in bytes (min 26)")
	replySize := fs.Int("reply-size", 0, "UDP echo payload size in bytes (default: same as -size, min 40)")
	perProbe := fs.Bool("per-probe", true, "ask the server to report every received probe over TCP")
	statsEvery := fs.Duration("stats", time.Second, "print / server stats interval")
	linger := fs.Duration("linger", 2*time.Second, "wait this long for late echoes after the last probe")
	csvPath := fs.String("csv", "", "write per-probe client-side records (UDP echoes) to this CSV")
	srvCSVPath := fs.String("server-csv", "", "write the server's per-probe TCP reports to this CSV")
	jsonPath := fs.String("json", "", "write the final summary as JSON to this file")
	raw := fs.Bool("raw", false, "show OWD from raw clocks (use when both hosts are NTP/PTP synced) instead of offset-corrected")
	quiet := fs.Bool("q", false, "only print the final summary")
	fs.Parse(args)
	if *host == "" {
		fs.Usage()
		os.Exit(2)
	}
	*size = max(*size, probeHdrLen)
	if *replySize == 0 {
		*replySize = *size
	}
	*replySize = min(max(*replySize, echoHdrLen), 65507)

	fail := func(format string, a ...any) {
		fmt.Fprintf(os.Stderr, "owd: "+format+"\n", a...)
		os.Exit(1)
	}

	tcp, err := net.DialTimeout("tcp", net.JoinHostPort(*host, strconv.Itoa(*port)), 10*time.Second)
	if err != nil {
		fail("connect: %v", err)
	}
	defer tcp.Close()
	br := bufio.NewReader(tcp)
	json.NewEncoder(tcp).Encode(ctrlMsg{Type: "hello", Version: protoVersion, PerProbe: *perProbe,
		StatsIntervalMs: int(statsEvery.Milliseconds())})
	tcp.SetReadDeadline(time.Now().Add(10 * time.Second))
	line, err := br.ReadBytes('\n')
	if err != nil {
		fail("handshake: %v", err)
	}
	tcp.SetReadDeadline(time.Time{})
	var welcome ctrlMsg
	if err := json.Unmarshal(line, &welcome); err != nil || welcome.Type != "welcome" {
		fail("handshake: server said %s", line)
	}
	if *replySize > welcome.MaxReply {
		fmt.Fprintf(os.Stderr, "owd: server caps echoes at %d bytes\n", welcome.MaxReply)
		*replySize = welcome.MaxReply
	}

	// Send UDP to the exact IP the TCP connection reached, so the server's
	// strict-ip check sees the same source address for both.
	srvIP := tcp.RemoteAddr().(*net.TCPAddr).IP
	udp, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: srvIP, Port: welcome.UDPPort})
	if err != nil {
		fail("udp: %v", err)
	}
	enableRxTimestamps(udp)
	setSockBufs(udp)
	sid := welcome.Session

	var csvW, srvW *bufio.Writer
	openCSV := func(path, header string) *bufio.Writer {
		if path == "" {
			return nil
		}
		f, err := os.Create(path)
		if err != nil {
			fail("%v", err)
		}
		w := bufio.NewWriterSize(f, 1<<16)
		w.WriteString(header)
		return w
	}
	csvW = openCSV(*csvPath, "seq,t1_ns,t2_ns,t3_ns,t4_ns,rtt_ns,up_raw_ns,down_raw_ns,size\n")
	srvW = openCSV(*srvCSVPath, "seq,t1_ns,t2_ns,owd_raw_ns,size\n")

	st := &clientStats{bestRTT: math.MaxInt64}
	if !*quiet {
		fmt.Printf("owd: session %08x with %s (tcp :%d, udp :%d), probe %dB / echo %dB every %v\n",
			sid, srvIP, *port, welcome.UDPPort, *size, *replySize, *interval)
	}

	// UDP echo receiver.
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		buf := make([]byte, 65536)
		oob := make([]byte, 256)
		for {
			n, oobn, _, _, err := udp.ReadMsgUDP(buf, oob)
			t4 := rxTimestamp(oob[:oobn])
			if t4 == 0 {
				t4 = now()
			}
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				continue
			}
			e, ok := parseEcho(buf[:n])
			if !ok || e.Session != sid {
				continue
			}
			rtt := (t4 - e.T1) - (e.T3 - e.T2)
			up, down := e.T2-e.T1, t4-e.T3
			st.mu.Lock()
			st.echoed++
			st.rtt.add(rtt, n)
			st.up.add(up, n)
			st.down.add(down, n)
			st.wRTT.add(rtt, n)
			st.wUp.add(up, n)
			st.wDown.add(down, n)
			if rtt < st.bestRTT {
				st.bestRTT = rtt
				st.offset = ((e.T2 - e.T1) + (e.T3 - t4)) / 2
			}
			if csvW != nil {
				fmt.Fprintf(csvW, "%d,%d,%d,%d,%d,%d,%d,%d,%d\n", e.Seq, e.T1, e.T2, e.T3, t4, rtt, up, down, n)
			}
			st.mu.Unlock()
		}
	}()

	// TCP report reader.
	finalCh := make(chan struct{})
	go func() {
		defer close(finalCh)
		for {
			line, err := br.ReadBytes('\n')
			if err != nil {
				return
			}
			var m ctrlMsg
			if json.Unmarshal(line, &m) != nil {
				continue
			}
			switch m.Type {
			case "probe":
				st.mu.Lock()
				st.srvProbes++
				if srvW != nil {
					fmt.Fprintf(srvW, "%d,%d,%d,%d,%d\n", m.Seq, m.T1, m.T2, m.OWDNs, m.Size)
				}
				st.mu.Unlock()
			case "stats":
				st.mu.Lock()
				st.srvTotal = m.Total
				st.mu.Unlock()
			case "final":
				st.mu.Lock()
				st.srvFinal = &m
				st.mu.Unlock()
				return
			case "error":
				fmt.Fprintf(os.Stderr, "owd: server error: %s\n", m.Error)
			}
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *dur > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *dur)
		defer cancel()
	}

	// Live printer.
	start := time.Now()
	if !*quiet {
		go func() {
			t := time.NewTicker(*statsEvery)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
				st.mu.Lock()
				off := float64(st.offset)
				if *raw {
					off = 0
				}
				srv := "-"
				if *perProbe {
					srv = strconv.FormatInt(st.srvProbes, 10)
				} else if st.srvTotal != nil {
					srv = strconv.FormatInt(st.srvTotal.Received, 10)
				}
				fmt.Printf("[%6.1fs] sent %-7d srv %-7s echo %-7d | rtt %s | up %s | down %s ms\n",
					time.Since(start).Seconds(), st.sent, srv, st.echoed,
					fmtAvgJit(&st.wRTT, 0), fmtAvgJit(&st.wUp, -off), fmtAvgJit(&st.wDown, off))
				st.wRTT, st.wUp, st.wDown = delayStats{}, delayStats{}, delayStats{}
				st.mu.Unlock()
			}
		}()
	}

	// Sender, paced against absolute deadlines so it does not drift.
	pkt := make([]byte, *size)
	var seq uint64
	next := time.Now()
send:
	for {
		select {
		case <-ctx.Done():
			break send
		case <-finalCh:
			fmt.Fprintln(os.Stderr, "owd: server closed the control connection; stopping")
			break send
		default:
		}
		if d := time.Until(next); d > 0 {
			select {
			case <-ctx.Done():
				break send
			case <-finalCh:
				fmt.Fprintln(os.Stderr, "owd: server closed the control connection; stopping")
				break send
			case <-time.After(d):
			}
		}
		p := probeHdr{Session: sid, Seq: seq, ReplyLen: uint16(*replySize)}
		p.T1 = now()
		p.put(pkt)
		udp.Write(pkt)
		seq++
		st.mu.Lock()
		st.sent++
		st.mu.Unlock()
		next = next.Add(*interval)
		if lag := time.Since(next); lag > time.Second {
			next = time.Now() // fell far behind (e.g. suspended); don't burst
		}
	}

	elapsed := time.Since(start)
	time.Sleep(*linger)
	st.mu.Lock()
	sent := st.sent
	st.mu.Unlock()
	json.NewEncoder(tcp).Encode(ctrlMsg{Type: "bye", Sent: sent})
	select {
	case <-finalCh:
	case <-time.After(5 * time.Second):
		fmt.Fprintln(os.Stderr, "owd: no final report from server")
	}
	udp.Close()
	<-recvDone
	for _, w := range []*bufio.Writer{csvW, srvW} {
		if w != nil {
			w.Flush()
		}
	}

	sum := buildSummary(st, *host, elapsed)
	printSummary(sum)
	if *jsonPath != "" {
		b, _ := json.MarshalIndent(sum, "", "  ")
		if err := os.WriteFile(*jsonPath, append(b, '\n'), 0o644); err != nil {
			fail("%v", err)
		}
	}
}

func fmtAvgJit(s *delayStats, shift float64) string {
	if s.N == 0 {
		return fmt.Sprintf("%8s ±%-6s", "-", "-")
	}
	return fmt.Sprintf("%8.3f ±%-6.3f", (s.avg()+shift)/1e6, s.Jitter/1e6)
}

type dist struct {
	MinMs    float64 `json:"min_ms"`
	AvgMs    float64 `json:"avg_ms"`
	MaxMs    float64 `json:"max_ms"`
	JitterMs float64 `json:"jitter_ms"`
}

func mkDist(s *delayStats, shift float64) *dist {
	if s.N == 0 {
		return nil
	}
	return &dist{ms(float64(s.Min) + shift), ms(s.avg() + shift), ms(float64(s.Max) + shift), ms(s.Jitter)}
}

func distFromServer(j *statsJSON, shiftMs float64) *dist {
	if j == nil || j.Received == 0 {
		return nil
	}
	r := func(v float64) float64 { return math.Round(v*1e3) / 1e3 }
	return &dist{r(j.OWDMinMs + shiftMs), r(j.OWDAvgMs + shiftMs), r(j.OWDMaxMs + shiftMs), j.JitterMs}
}

type summary struct {
	Server            string  `json:"server"`
	DurationS         float64 `json:"duration_s"`
	Sent              int64   `json:"sent"`
	ServerReceived    int64   `json:"server_received"`
	EchoesReceived    int64   `json:"echoes_received"`
	UplinkLossPct     float64 `json:"uplink_loss_pct"`
	DownlinkLossPct   float64 `json:"downlink_loss_pct"`
	ServerReordered   int64   `json:"server_reordered"`
	ClockOffsetMs     float64 `json:"clock_offset_ms"`
	OffsetMinRTTMs    float64 `json:"offset_min_rtt_ms"`
	RTT               *dist   `json:"rtt"`
	UplinkOWD         *dist   `json:"uplink_owd"`
	DownlinkOWD       *dist   `json:"downlink_owd"`
	UplinkOWDRaw      *dist   `json:"uplink_owd_raw_clocks"`
	DownlinkOWDRaw    *dist   `json:"downlink_owd_raw_clocks"`
	ServerReportFinal bool    `json:"server_report_final"`
}

func buildSummary(st *clientStats, host string, d time.Duration) *summary {
	st.mu.Lock()
	defer st.mu.Unlock()
	off := float64(st.offset)
	s := &summary{Server: host, DurationS: math.Round(d.Seconds()*10) / 10, Sent: st.sent,
		EchoesReceived: st.echoed, ClockOffsetMs: ms(off),
		RTT:          mkDist(&st.rtt, 0),
		DownlinkOWD:  mkDist(&st.down, off),
		UplinkOWDRaw: mkDist(&st.up, 0), DownlinkOWDRaw: mkDist(&st.down, 0)}
	if st.rtt.N > 0 {
		s.OffsetMinRTTMs = ms(float64(st.bestRTT))
	}
	// The server saw every probe that arrived, including those whose echo was
	// lost, so prefer its uplink numbers when the final report came through.
	// Without the final report the server's count is stale, so per-direction
	// loss can't be split; only the OWD distribution from it is still useful.
	srv := st.srvTotal
	if st.srvFinal != nil {
		srv = st.srvFinal.Total
		s.ServerReportFinal = true
	}
	if srv != nil && s.ServerReportFinal {
		s.ServerReceived = srv.Received
		s.ServerReordered = srv.Reordered
		s.UplinkOWDRaw = distFromServer(srv, 0)
		s.UplinkOWD = distFromServer(srv, -ms(off))
	} else {
		s.ServerReceived = -1
		s.UplinkOWD = mkDist(&st.up, -off)
	}
	pct := func(a, b int64) float64 {
		if b <= 0 {
			return 0
		}
		return math.Round(float64(b-a)/float64(b)*1e5) / 1e3
	}
	if s.ServerReceived >= 0 {
		s.UplinkLossPct = pct(s.ServerReceived, s.Sent)
		s.DownlinkLossPct = pct(s.EchoesReceived, s.ServerReceived)
	} else {
		s.UplinkLossPct = pct(s.EchoesReceived, s.Sent) // combined loss only
	}
	return s
}

func printSummary(s *summary) {
	fmt.Printf("\n--- owd summary: %s, %.1fs ---\n", s.Server, s.DurationS)
	fmt.Printf("probes sent        %d\n", s.Sent)
	if s.ServerReceived >= 0 {
		fmt.Printf("server received    %d   (uplink loss %.3f%%, reordered %d)\n", s.ServerReceived, s.UplinkLossPct, s.ServerReordered)
		fmt.Printf("echoes received    %d   (downlink loss %.3f%%)\n", s.EchoesReceived, s.DownlinkLossPct)
	} else {
		fmt.Printf("echoes received    %d   (round-trip loss %.3f%%; no server report)\n", s.EchoesReceived, s.UplinkLossPct)
	}
	if !s.ServerReportFinal {
		fmt.Println("warning: no final server report; uplink numbers come from the UDP echoes only")
	}
	fmt.Printf("clock offset       %+.3f ms (server - client, from min-RTT probe of %.3f ms)\n\n", s.ClockOffsetMs, s.OffsetMinRTTMs)
	fmt.Printf("%-26s %10s %10s %10s %10s\n", "(ms)", "min", "avg", "max", "jitter")
	row := func(name string, d *dist) {
		if d == nil {
			fmt.Printf("%-26s %10s\n", name, "-")
			return
		}
		fmt.Printf("%-26s %10.3f %10.3f %10.3f %10.3f\n", name, d.MinMs, d.AvgMs, d.MaxMs, d.JitterMs)
	}
	row("RTT", s.RTT)
	row("uplink OWD", s.UplinkOWD)
	row("downlink OWD", s.DownlinkOWD)
	row("uplink OWD (raw clocks)", s.UplinkOWDRaw)
	row("downlink OWD (raw clocks)", s.DownlinkOWDRaw)
	fmt.Println("\nuplink/downlink OWD are corrected by the clock offset, which assumes the minimum")
	fmt.Println("delay is the same in both directions; raw-clock rows are exact only if both hosts")
	fmt.Println("are NTP/PTP synchronised. Variation (jitter, spikes) is exact either way.")
}
