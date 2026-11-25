#!/usr/bin/env python3
"""Measure one-way delay to several owd servers at the same time and compare them.

    python3 owd_compare.py HOST1 HOST2:7000 lab=[2001:db8::1]:9910 [-i 0.01] [-t 30]

A server is HOST, HOST:PORT or [IPV6]:PORT, optionally prefixed with NAME= to
label it. The default port is 9910. Every server gets its own session, and all
of them start probing at the same moment with the same interval and packet
sizes, so they see the same conditions on the local link.

While it runs, it prints one line per server every --every seconds: the
average and jitter of RTT, uplink OWD and downlink OWD over that window. At the
end it prints a comparison table sorted by RTT. Uplink numbers come from the
server's per-probe TCP reports, so they include probes whose echo was lost.

The OWD columns are corrected by each server's own clock offset. That offset
is estimated from its lowest-RTT probe, which assumes the minimum delay is the
same in both directions. Use --raw if every host is NTP/PTP synchronised.

Servers that can't be reached are reported and skipped. The exit status is 1 if
no server could be reached or any server ended without sending its final report.

Standard library only; needs owd_client.py in the same directory.
"""
import argparse, csv, json, math, socket, sys, threading, time

from owd_client import (ECHO, MAGIC_ECHO, MAGIC_PROBE, PROBE, SO_TIMESTAMPNS,
                        now_ns, rx_timestamp, tcp_lines)


def parse_target(spec, default_port):
    label, _, addr = spec.rpartition("=")
    if addr.startswith("["):
        host, _, rest = addr[1:].partition("]")
        port = int(rest[1:]) if rest.startswith(":") else default_port
    elif addr.count(":") == 1:
        host, port = addr.split(":")
        port = int(port)
    else:
        host, port = addr, default_port  # name, IPv4, or bare IPv6
    return label or addr, host, port


def pct(sorted_vals, p):
    if not sorted_vals:
        return math.nan
    k = (len(sorted_vals) - 1) * p / 100
    lo, hi = math.floor(k), math.ceil(k)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def jitter(series):
    """RFC 3550 jitter of a delay series in ns."""
    j = 0.0
    for a, b in zip(series, series[1:]):
        j += (abs(b - a) - j) / 16
    return j


class Session:
    def __init__(self, label, host, port, a):
        self.label, self.host, self.port, self.a = label, host, port, a
        self.error = None
        self.lock = threading.Lock()
        self.sent = 0
        self.echoes = []        # (seq, t1, t2, t3, t4) in arrival order
        self.reports = {}       # seq -> (t1, t2)
        self.final = None
        self.best_rtt, self.offset = None, 0
        self.win_echo = self.win_rep = 0   # indices where the current window starts
        self.rep_order = []     # seqs in TCP report order, for windows

    def connect(self):
        a = self.a
        self.tcp = socket.create_connection((self.host, self.port), timeout=5)
        self.tcp.sendall(json.dumps({"type": "hello", "version": 1, "per_probe": True,
                                     "stats_interval_ms": 1000}).encode() + b"\n")
        self.lines = tcp_lines(self.tcp)
        w = next(self.lines, None)
        if not w or w.get("type") != "welcome":
            raise RuntimeError(f"handshake failed: {w}")
        self.tcp.settimeout(None)
        self.sid = w["session"]
        self.reply = min(max(a.reply_size or a.size, ECHO.size), w.get("max_reply", 1472))
        ip = self.tcp.getpeername()[0]
        self.udp = socket.socket(self.tcp.family, socket.SOCK_DGRAM)
        try:
            self.udp.setsockopt(socket.SOL_SOCKET, SO_TIMESTAMPNS, 1)
        except OSError:
            pass
        self.udp.connect((ip, w["udp_port"]))
        self.udp.settimeout(0.2)
        self.pad = bytes(max(a.size, PROBE.size) - PROBE.size)
        self.closing = threading.Event()
        for fn in (self.udp_rx, self.tcp_rx):
            threading.Thread(target=fn, daemon=True).start()

    def udp_rx(self):
        while not self.closing.is_set():
            try:
                data, anc, _, _ = self.udp.recvmsg(65536, 256)
            except socket.timeout:
                continue
            except OSError:
                return
            t4 = rx_timestamp(anc) or now_ns()
            if len(data) < ECHO.size:
                continue
            magic, sid, seq, t1, t2, t3 = ECHO.unpack_from(data)
            if magic != MAGIC_ECHO or sid != self.sid:
                continue
            rtt = (t4 - t1) - (t3 - t2)
            with self.lock:
                self.echoes.append((seq, t1, t2, t3, t4))
                if self.best_rtt is None or rtt < self.best_rtt:
                    self.best_rtt, self.offset = rtt, ((t2 - t1) + (t3 - t4)) // 2

    def tcp_rx(self):
        for m in self.lines:
            with self.lock:
                if m["type"] == "probe":
                    seq = m.get("seq", 0)
                    self.reports[seq] = (m.get("t1", 0), m.get("t2", 0))
                    self.rep_order.append(seq)
                elif m["type"] == "final":
                    self.final = m
                    return
                elif m["type"] == "error":
                    self.error = m.get("error")
        self.closing.set()  # control channel gone: server stopped or crashed

    def send(self, seq):
        if self.error or self.closing.is_set():
            return
        try:
            self.udp.send(PROBE.pack(MAGIC_PROBE, self.sid, seq, now_ns(), self.reply) + self.pad)
            with self.lock:
                self.sent += 1
        except OSError as e:
            self.error = str(e)

    def finish(self):
        try:
            self.tcp.sendall(json.dumps({"type": "bye", "sent": self.sent}).encode() + b"\n")
        except OSError:
            pass

    def close(self):
        self.closing.set()
        for s in (self.tcp, self.udp):
            s.close()

    def offset_ns(self):
        return 0 if self.a.raw else self.offset

    def window(self):
        """Delays (ns) since the previous call: rtt, up, down lists."""
        with self.lock:
            echoes = self.echoes[self.win_echo:]
            reps = [self.reports[s] for s in self.rep_order[self.win_rep:]]
            self.win_echo, self.win_rep = len(self.echoes), len(self.rep_order)
            off = self.offset_ns()
        rtt = [(t4 - t1) - (t3 - t2) for _, t1, t2, t3, t4 in echoes]
        down = [t4 - t3 + off for _, _, _, t3, t4 in echoes]
        up = [t2 - t1 - off for t1, t2 in reps]
        return rtt, up, down

    def summary(self):
        with self.lock:
            echoes = sorted(self.echoes)
            reps = [self.reports[s] for s in sorted(self.reports)]
            off = self.offset_ns()
            final = self.final
        rtt = [(t4 - t1) - (t3 - t2) for _, t1, t2, t3, t4 in echoes]
        down = [t4 - t3 + off for _, _, _, t3, t4 in echoes]
        up = [t2 - t1 - off for t1, t2 in reps]
        srv = final["total"]["received"] if final else len(reps)
        r = {"server": self.label, "address": f"{self.host}:{self.port}", "sent": self.sent,
             "server_received": srv, "echoes_received": len(echoes),
             "uplink_loss_pct": 100 * (self.sent - srv) / self.sent if self.sent else 0,
             "downlink_loss_pct": 100 * (srv - len(echoes)) / srv if srv else 0,
             "clock_offset_ms": self.offset / 1e6, "final_report": final is not None}
        for name, vals in (("rtt", rtt), ("up", up), ("down", down)):
            ordered = sorted(vals)
            r[name] = None if not vals else {
                "min_ms": ordered[0] / 1e6, "avg_ms": sum(vals) / len(vals) / 1e6,
                "p50_ms": pct(ordered, 50) / 1e6, "p95_ms": pct(ordered, 95) / 1e6,
                "p99_ms": pct(ordered, 99) / 1e6, "max_ms": ordered[-1] / 1e6,
                "jitter_ms": jitter(vals) / 1e6}
        return r


def avg_jit(vals):
    if not vals:
        return f"{'-':>8} {'':6}"
    return f"{sum(vals) / len(vals) / 1e6:8.3f} ±{jitter(vals) / 1e6:<5.3f}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("servers", nargs="+", help="[NAME=]HOST[:PORT] or [NAME=][IPV6]:PORT")
    ap.add_argument("--port", type=int, default=9910, help="default port")
    ap.add_argument("-i", "--interval", type=float, default=0.01, help="seconds between probes, per server")
    ap.add_argument("-t", "--duration", type=float, default=10.0, help="seconds to probe (0 = until Ctrl-C)")
    ap.add_argument("--size", type=int, default=64, help="UDP probe payload bytes")
    ap.add_argument("--reply-size", type=int, default=0, help="UDP echo payload bytes (default = --size)")
    ap.add_argument("--every", type=float, default=1.0, help="seconds between live lines")
    ap.add_argument("--linger", type=float, default=1.0, help="seconds to wait for late echoes")
    ap.add_argument("--raw", action="store_true", help="trust the hosts' clocks instead of correcting the offset")
    ap.add_argument("--json", help="write the per-server summary to this file")
    ap.add_argument("--csv", help="write every probe of every server to this file")
    a = ap.parse_args()

    sessions = []
    for spec in a.servers:
        s = Session(*parse_target(spec, a.port), a)
        try:
            s.connect()
            sessions.append(s)
        except (OSError, RuntimeError, ValueError) as e:
            print(f"{s.label}: cannot connect to {s.host}:{s.port}: {e}", file=sys.stderr)
    if not sessions:
        sys.exit(1)
    width = max(12, *(len(s.label) for s in sessions))
    print(f"probing {len(sessions)} server(s) every {a.interval * 1e3:g} ms, "
          f"{max(a.size, PROBE.size)}B probes; OWD {'raw clocks' if a.raw else 'offset-corrected'}")
    head = f"{'':8} {'server':<{width}} {'sent':>6} {'srv':>6} {'echo':>6}   {'rtt ms':^15} {'up ms':^15} {'down ms':^15}"

    # All sessions send from one loop, so each tick's probes leave back to back.
    start = time.monotonic()
    next_send, next_print, seq = start, start + a.every, 0
    try:
        while a.duration <= 0 or time.monotonic() - start < a.duration:
            t = time.monotonic()
            if t >= next_print:
                print(head)
                for s in sessions:
                    rtt, up, down = s.window()
                    status = f"  [{s.error or 'disconnected'}]" if s.error or s.closing.is_set() else ""
                    print(f"[{t - start:5.1f}s] {s.label:<{width}} {s.sent:>6} {len(s.reports):>6} "
                          f"{len(s.echoes):>6}   {avg_jit(rtt)} {avg_jit(up)} {avg_jit(down)}{status}")
                next_print += a.every
            if t >= next_send:
                for s in sessions:
                    s.send(seq)
                seq += 1
                next_send += a.interval
                if t - next_send > 1:
                    next_send = t  # fell far behind; don't burst
            time.sleep(max(0, min(next_send, next_print) - time.monotonic()))
    except KeyboardInterrupt:
        print()
    elapsed = time.monotonic() - start

    time.sleep(a.linger)
    for s in sessions:
        s.finish()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and any(s.final is None and not s.closing.is_set() for s in sessions):
        time.sleep(0.05)
    for s in sessions:
        s.close()

    results = [s.summary() for s in sessions]
    results.sort(key=lambda r: r["rtt"]["avg_ms"] if r["rtt"] else math.inf)
    print(f"\n--- comparison, {elapsed:.1f}s, sorted by average RTT (ms) ---")
    cols = ("loss up%", "loss dn%", "rtt avg", "rtt p95", "up avg", "up p95", "up jit",
            "down avg", "down p95", "down jit", "offset")
    print(f"{'server':<{width}} " + " ".join(f"{c:>9}" for c in cols))
    for r in results:
        f = lambda d, k: f"{d[k]:9.3f}" if d else f"{'-':>9}"
        print(f"{r['server']:<{width}} {r['uplink_loss_pct']:9.2f} {r['downlink_loss_pct']:9.2f} "
              f"{f(r['rtt'], 'avg_ms')} {f(r['rtt'], 'p95_ms')} "
              f"{f(r['up'], 'avg_ms')} {f(r['up'], 'p95_ms')} {f(r['up'], 'jitter_ms')} "
              f"{f(r['down'], 'avg_ms')} {f(r['down'], 'p95_ms')} {f(r['down'], 'jitter_ms')} "
              f"{r['clock_offset_ms']:+9.3f}" + ("" if r["final_report"] else "  (no final report)"))
    if not a.raw:
        print("\nup/down are corrected by each server's clock offset (server - client), which assumes\n"
              "equal minimum delay in both directions. RTT and jitter don't depend on clocks.")

    if a.json:
        with open(a.json, "w") as f:
            json.dump({"duration_s": round(elapsed, 1), "servers": results}, f, indent=2)
    if a.csv:
        with open(a.csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["server", "seq", "t1_ns", "t2_ns", "t3_ns", "t4_ns", "rtt_ns", "up_raw_ns", "down_raw_ns",
                        "offset_ns"])
            for s in sessions:
                echo = {e[0]: e for e in s.echoes}
                for seq in sorted(set(echo) | set(s.reports)):
                    if seq in echo:
                        _, t1, t2, t3, t4 = echo[seq]
                        w.writerow([s.label, seq, t1, t2, t3, t4, (t4 - t1) - (t3 - t2), t2 - t1, t4 - t3, s.offset])
                    else:  # echo lost, but the server reported the probe over TCP
                        t1, t2 = s.reports[seq]
                        w.writerow([s.label, seq, t1, t2, "", "", "", t2 - t1, "", s.offset])
    sys.exit(0 if all(r["final_report"] for r in results) else 1)


if __name__ == "__main__":
    main()
