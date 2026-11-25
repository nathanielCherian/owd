#!/usr/bin/env python3
"""Python client for the owd server, written independently of the Go client to
test the server's protocol. Standard library only (Python >= 3.8).

    python3 owd_client.py HOST [--port 9910] [-i 0.01] [-t 5] [--size 64] [--reply-size N]

It runs a normal measurement and prints the same kind of summary as
`owd client`. It also checks the server's behaviour and exits non-zero if
any check fails:

  * handshake: welcome with a non-zero session id; a bad hello gets an error
  * every UDP echo carries back our session, seq and t1, and has the size asked for
  * the server's timestamps are ordered (t2 <= t3) and every RTT is positive
  * every UDP echo has a matching per-probe TCP report with the same t2
  * the per-probe TCP reports, the final report's count and its `sent` agree
  * no duplicate echoes or reports
  * probes with an unknown session id are ignored (no echo)
  * uplink and downlink loss are under --max-loss percent

Use --no-negative to skip the bad-session and bad-hello tests.
"""
import argparse, csv, json, math, socket, struct, sys, threading, time

MAGIC_PROBE = 0x4F574450
MAGIC_ECHO = 0x4F574445
PROBE = struct.Struct("!IIQqH")    # magic, session, seq, t1, reply_len   (26 bytes)
ECHO = struct.Struct("!IIQqqq")    # magic, session, seq, t1, t2, t3      (40 bytes)
SO_TIMESTAMPNS = getattr(socket, "SO_TIMESTAMPNS", 35)
now_ns = time.time_ns


class Stats:
    """min/avg/max and RFC 3550 jitter of a series of delays in ns."""

    def __init__(self):
        self.n, self.sum, self.min, self.max, self.jitter, self.last = 0, 0, None, None, 0.0, None

    def add(self, d):
        if self.last is not None:
            self.jitter += (abs(d - self.last) - self.jitter) / 16
        self.last = d
        self.n += 1
        self.sum += d
        self.min = d if self.min is None else min(self.min, d)
        self.max = d if self.max is None else max(self.max, d)

    def row(self, name, shift=0):
        if not self.n:
            return f"{name:<26} {'-':>10}"
        f = lambda v: f"{(v + shift) / 1e6:10.3f}"
        return f"{name:<26} {f(self.min)} {f(self.sum / self.n)} {f(self.max)} {self.jitter / 1e6:10.3f}"


def rx_timestamp(ancdata):
    for level, typ, data in ancdata:
        if level == socket.SOL_SOCKET and typ == SO_TIMESTAMPNS and len(data) >= 16:
            sec, nsec = struct.unpack("qq", data[:16])
            return sec * 1_000_000_000 + nsec
    return None


class Checks:
    def __init__(self):
        self.results = []

    def check(self, ok, name, detail=""):
        self.results.append((bool(ok), name, detail))

    def report(self):
        print("\n--- checks ---")
        for ok, name, detail in self.results:
            print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))
        failed = sum(1 for ok, *_ in self.results if not ok)
        print(f"{len(self.results) - failed}/{len(self.results)} checks passed")
        return failed == 0


def tcp_lines(sock):
    buf = b""
    while True:
        try:
            chunk = sock.recv(65536)
        except OSError:
            return
        if not chunk:
            return
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            if line.strip():
                yield json.loads(line)


def test_bad_hello(host, port, checks):
    try:
        with socket.create_connection((host, port), timeout=5) as s:
            s.sendall(b'{"type":"nonsense"}\n')
            msg = next(tcp_lines(s), None)
        checks.check(msg is not None and msg.get("type") == "error",
                     "bad hello is rejected with an error", f"got {msg}")
    except OSError as e:
        checks.check(False, "bad hello is rejected with an error", str(e))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("host")
    ap.add_argument("--port", type=int, default=9910)
    ap.add_argument("-i", "--interval", type=float, default=0.01, help="seconds between probes")
    ap.add_argument("-t", "--duration", type=float, default=5.0, help="seconds to send")
    ap.add_argument("--size", type=int, default=64, help="UDP probe payload bytes (min 26)")
    ap.add_argument("--reply-size", type=int, default=0, help="UDP echo payload bytes (default = --size, min 40)")
    ap.add_argument("--linger", type=float, default=1.0, help="seconds to wait for late echoes")
    ap.add_argument("--max-loss", type=float, default=5.0, help="loss %% per direction that fails the check")
    ap.add_argument("--csv", help="write per-probe records here")
    ap.add_argument("--no-negative", action="store_true", help="skip the bad-session / bad-hello tests")
    a = ap.parse_args()
    size = max(a.size, PROBE.size)
    reply_size = max(a.reply_size or size, ECHO.size)
    checks = Checks()

    # --- handshake ---
    tcp = socket.create_connection((a.host, a.port), timeout=10)
    tcp.sendall(json.dumps({"type": "hello", "version": 1, "per_probe": True,
                            "stats_interval_ms": 1000}).encode() + b"\n")
    lines = tcp_lines(tcp)
    welcome = next(lines, None)
    tcp.settimeout(None)
    ok = welcome is not None and welcome.get("type") == "welcome" and welcome.get("session")
    checks.check(ok, "handshake returns welcome with a session id", f"got {welcome}")
    if not ok:
        checks.report()
        sys.exit(1)
    sid, udp_port = welcome["session"], welcome["udp_port"]
    reply_size = min(reply_size, welcome.get("max_reply", reply_size))
    srv_ip = tcp.getpeername()[0]
    print(f"session {sid:08x} with {srv_ip} (tcp :{a.port}, udp :{udp_port}), "
          f"probe {size}B / echo {reply_size}B every {a.interval * 1e3:g}ms for {a.duration:g}s")

    udp = socket.socket(tcp.family, socket.SOCK_DGRAM)
    try:
        udp.setsockopt(socket.SOL_SOCKET, SO_TIMESTAMPNS, 1)
    except OSError:
        pass
    udp.connect((srv_ip, udp_port))
    udp.settimeout(0.2)

    sent_t1 = {}              # seq -> t1
    echoes = {}               # seq -> (t1, t2, t3, t4, size)
    reports = {}              # seq -> (t1, t2, owd, size)
    bad_echo, dup_echo, dup_report, stray = [], 0, 0, 0
    final, stats_msgs = {}, []
    lock = threading.Lock()
    stop_rx = threading.Event()

    def udp_rx():
        nonlocal dup_echo, stray
        while not stop_rx.is_set():
            try:
                data, anc, _, _ = udp.recvmsg(65536, 256)
            except socket.timeout:
                continue
            except OSError:
                return
            t4 = rx_timestamp(anc) or now_ns()
            if len(data) < ECHO.size:
                stray += 1
                continue
            magic, s, seq, t1, t2, t3 = ECHO.unpack_from(data)
            with lock:
                if magic != MAGIC_ECHO or s != sid:
                    stray += 1
                    continue
                if seq in echoes:
                    dup_echo += 1
                    continue
                echoes[seq] = (t1, t2, t3, t4, len(data))
                if sent_t1.get(seq) != t1:
                    bad_echo.append(seq)

    def tcp_rx():
        nonlocal dup_report
        for m in lines:
            with lock:
                if m["type"] == "probe":
                    seq = m.get("seq", 0)
                    if seq in reports:
                        dup_report += 1
                    reports[seq] = (m.get("t1", 0), m.get("t2", 0), m.get("owd_ns", 0), m.get("size", 0))
                elif m["type"] == "stats":
                    stats_msgs.append(m)
                elif m["type"] == "final":
                    final.update(m)
                    return

    rx1 = threading.Thread(target=udp_rx, daemon=True)
    rx2 = threading.Thread(target=tcp_rx, daemon=True)
    rx1.start()
    rx2.start()

    # --- send probes, paced on absolute deadlines ---
    pad = bytes(size - PROBE.size)
    start = time.monotonic()
    seq, next_t, last_print = 0, start, start
    try:
        while time.monotonic() - start < a.duration:
            d = next_t - time.monotonic()
            if d > 0:
                time.sleep(d)
            t1 = now_ns()
            with lock:
                sent_t1[seq] = t1
            udp.send(PROBE.pack(MAGIC_PROBE, sid, seq, t1, reply_size) + pad)
            seq += 1
            next_t += a.interval
            if time.monotonic() - last_print >= 1:
                last_print = time.monotonic()
                with lock:
                    print(f"[{last_print - start:5.1f}s] sent {seq:<7} srv {len(reports):<7} echo {len(echoes)}")
    except KeyboardInterrupt:
        print("interrupted")
    sent = seq
    elapsed = time.monotonic() - start

    # --- negative test: an unknown session id must get no echo ---
    if not a.no_negative:
        bogus = sid ^ 0xFFFFFFFF
        probe = socket.socket(tcp.family, socket.SOCK_DGRAM)
        probe.settimeout(0.5)
        probe.connect((srv_ip, udp_port))
        for i in range(5):
            probe.send(PROBE.pack(MAGIC_PROBE, bogus, i, now_ns(), reply_size) + pad)
        try:
            probe.recv(65536)
            got = True
        except socket.timeout:
            got = False
        probe.close()
        checks.check(not got, "probes with an unknown session id are ignored")
        test_bad_hello(a.host, a.port, checks)

    time.sleep(a.linger)
    tcp.sendall(json.dumps({"type": "bye", "sent": sent}).encode() + b"\n")
    rx2.join(5)
    stop_rx.set()
    rx1.join(1)
    tcp.close()
    udp.close()

    # --- analysis ---
    rtt, up, down = Stats(), Stats(), Stats()
    best_rtt, offset = None, 0
    bad_order = bad_rtt = 0
    for s in sorted(echoes):
        t1, t2, t3, t4, _ = echoes[s]
        r = (t4 - t1) - (t3 - t2)
        rtt.add(r)
        up.add(t2 - t1)
        down.add(t4 - t3)
        bad_order += t3 < t2
        bad_rtt += r <= 0
        if best_rtt is None or r < best_rtt:
            best_rtt, offset = r, ((t2 - t1) + (t3 - t4)) // 2
    srv_up = Stats()
    for s in sorted(reports):
        srv_up.add(reports[s][2])

    total = final.get("total", {})
    srv_rcv = total.get("received", len(reports))
    up_loss = 100 * (sent - srv_rcv) / sent if sent else 0
    down_loss = 100 * (srv_rcv - len(echoes)) / srv_rcv if srv_rcv else 0

    print(f"\n--- summary: {srv_ip}, {elapsed:.1f}s ---")
    print(f"probes sent        {sent}")
    print(f"server received    {srv_rcv}   (uplink loss {up_loss:.3f}%)")
    print(f"echoes received    {len(echoes)}   (downlink loss {down_loss:.3f}%)")
    print(f"clock offset       {offset / 1e6:+.3f} ms (server - client, min-RTT probe "
          f"{(best_rtt or 0) / 1e6:.3f} ms)\n")
    print(f"{'(ms)':<26} {'min':>10} {'avg':>10} {'max':>10} {'jitter':>10}")
    print(rtt.row("RTT"))
    print(srv_up.row("uplink OWD", -offset))
    print(down.row("downlink OWD", offset))
    print(srv_up.row("uplink OWD (raw clocks)"))
    print(down.row("downlink OWD (raw clocks)"))

    if a.csv:
        with open(a.csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["seq", "t1_ns", "t2_ns", "t3_ns", "t4_ns", "rtt_ns", "up_raw_ns", "down_raw_ns",
                        "tcp_reported"])
            for s in range(sent):
                e = echoes.get(s)
                if e:
                    t1, t2, t3, t4, _ = e
                    w.writerow([s, t1, t2, t3, t4, (t4 - t1) - (t3 - t2), t2 - t1, t4 - t3, int(s in reports)])
                else:
                    r = reports.get(s)
                    w.writerow([s, sent_t1[s], r[1] if r else "", "", "", "", r[2] if r else "", "", int(bool(r))])

    # --- checks ---
    checks.check(final.get("type") == "final", "server sent a final report over TCP")
    checks.check(final.get("sent") == sent, "final report echoes our sent count", f"{final.get('sent')} vs {sent}")
    checks.check(srv_rcv == len(reports), "final 'received' equals number of per-probe TCP reports",
                 f"{srv_rcv} vs {len(reports)}")
    checks.check(len(stats_msgs) >= int(elapsed) - 1, "periodic stats arrived about once a second",
                 f"{len(stats_msgs)} in {elapsed:.1f}s")
    checks.check(not bad_echo, "echoes carry our seq and t1", f"{len(bad_echo)} mismatched")
    checks.check(stray == 0, "no stray or foreign UDP packets", f"{stray}")
    sizes = {e[4] for e in echoes.values()}
    checks.check(sizes <= {reply_size}, f"echo size is {reply_size}B as requested", f"saw {sorted(sizes)}")
    rsizes = {r[3] for r in reports.values()}
    checks.check(rsizes <= {size}, f"TCP reports give probe size {size}B", f"saw {sorted(rsizes)}")
    checks.check(bad_order == 0, "server timestamps ordered (t2 <= t3)", f"{bad_order} violations")
    checks.check(bad_rtt == 0, "every RTT is positive", f"{bad_rtt} violations")
    mismatch = [s for s, e in echoes.items() if s not in reports or reports[s][1] != e[1]]
    checks.check(not mismatch, "every UDP echo has a TCP report with the same t2", f"{len(mismatch)} mismatched")
    owd_bad = [s for s, r in reports.items() if r[2] != r[1] - r[0] or sent_t1.get(s) != r[0]]
    checks.check(not owd_bad, "TCP owd_ns == t2 - t1 and t1 matches what we sent", f"{len(owd_bad)} wrong")
    checks.check(dup_echo == 0 and dup_report == 0, "no duplicate echoes or reports",
                 f"{dup_echo} echoes, {dup_report} reports")
    checks.check(up_loss <= a.max_loss, f"uplink loss <= {a.max_loss}%", f"{up_loss:.3f}%")
    checks.check(down_loss <= a.max_loss, f"downlink loss <= {a.max_loss}%", f"{down_loss:.3f}%")
    sys.exit(0 if checks.report() else 1)


if __name__ == "__main__":
    main()
