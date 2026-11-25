# owd API reference

This document specifies everything a client needs to talk to `owd server`. It
covers the TCP control channel, the UDP probe and echo formats, and the
session lifecycle. It also covers the command-line client's output files
(CSV and JSON summary). Protocol version: **1**.

Two clients implement this protocol and can serve as references:

- `client.go`, the Go client (`owd client`)
- `owd_client.py`, a Python client that uses only the standard library and
  also checks the server's behaviour

## 1. Overview

```
client                                        server
  │ TCP connect ─────────────────────────────▶ │  port P (default 9910)
  │ hello ───────────────────────────────────▶ │
  │ ◀─────────────────────────────────── welcome {session, udp_port}
  │                                            │
  │ UDP probe {session, seq, t1} ────────────▶ │  t2 = receive time
  │ ◀──────────────── UDP echo {seq, t1, t2, t3}  t3 = send time
  │ ◀───────────────── TCP probe {seq, t1, t2, owd_ns}   (if per_probe)
  │        ... repeat ...                      │
  │ ◀───────────────── TCP stats {window, total}         (every interval)
  │                                            │
  │ bye {sent} ──────────────────────────────▶ │
  │ ◀──────────────────────────────── final {total}
  │ close                                      │  server closes too
```

A **session** is tied to one TCP connection. The session exists from the
`welcome` until that connection closes. UDP probes are accepted only while the
session exists.

### Timestamps

All timestamps are **signed 64-bit integers, nanoseconds since the Unix
epoch**. Each one is read from the clock of the host that takes it.

| name | taken by | when |
|---|---|---|
| `t1` | client | just before sending the probe |
| `t2` | server | when the probe arrives. This is the kernel receive timestamp (`SO_TIMESTAMPNS`), or the userspace time if the kernel gives none. |
| `t3` | server | just before sending the echo |
| `t4` | client | when the echo arrives. Clients should use the kernel receive timestamp too. |

From these, a client computes:

```
uplink OWD    = t2 - t1          (server clock minus client clock)
downlink OWD  = t4 - t3          (client clock minus server clock)
RTT           = (t4 - t1) - (t3 - t2)        (does not depend on clock sync)
clock offset  = ((t2 - t1) + (t3 - t4)) / 2  (server minus client; take it from the probe with the lowest RTT)
```

The OWD values are correct only if both clocks are synchronised. Otherwise,
subtract the clock offset from the uplink OWD and add it to the downlink OWD.
This correction assumes the minimum delay is the same in both directions.

## 2. Transport

| channel | protocol | port |
|---|---|---|
| control | TCP | `-port` (default 9910) |
| probes and echoes | UDP | `udp_port` from `welcome`. It equals the TCP port unless the server runs with `-udp-port`. |

Both listen on all addresses, IPv4 and IPv6, unless the server runs with
`-bind`.

**Send UDP probes to the same IP address the TCP connection reached, and from
the same source IP.** By default (`-strict-ip=true`), the server drops probes
whose source IP differs from the TCP peer's IP. The source *port* can be
anything, which makes the protocol work through NAT. The server replies to the
address and port each probe came from.

## 3. TCP control channel

The control channel carries **newline-delimited JSON**: one UTF-8 JSON object
per line, ending in `\n`. Every message has a `"type"` string.

The server may leave out any numeric field whose value is `0`, and any
boolean field whose value is `false`. Treat a missing number as `0` and a
missing boolean as `false`. The `probe` message is the exception: it always
includes every field. Ignore fields and message types you don't recognise.

### 3.1 `hello` (client → server)

This must be the first line the client sends, within **10 s** of connecting.

```json
{"type":"hello","version":1,"per_probe":true,"stats_interval_ms":1000}
```

| field | type | default | meaning |
|---|---|---|---|
| `version` | int | – | protocol version. Send `1`. The server doesn't check it yet. |
| `per_probe` | bool | `false` | send a `probe` message for every UDP probe received |
| `stats_interval_ms` | int | `1000` | how often to send `stats`. Values below 100 are raised to 100; 0 or missing means 1000. |

### 3.2 `welcome` (server → client)

```json
{"type":"welcome","version":1,"session":4160755647,"udp_port":9910,"server_time_ns":1795645440624480478,"max_reply":1472}
```

| field | type | meaning |
|---|---|---|
| `version` | int | the server's protocol version |
| `session` | uint32 | random, non-zero session id. Put it in every UDP probe. |
| `udp_port` | int | send probes to this port |
| `server_time_ns` | int64 | the server's clock when it sent `welcome`. This gives only a rough clock offset, because it includes half an RTT. |
| `max_reply` | int | the largest echo the server will send, in UDP payload bytes |

### 3.3 `error` (server → client)

```json
{"type":"error","error":"server busy"}
```

The server closes the connection right after sending an error.

| `error` | cause |
|---|---|
| `expected hello` | the first line was not valid JSON, or its `type` was not `hello` |
| `server busy` | the server already has `-max-sessions` sessions (default 64) |

If no `hello` arrives within 10 s, the server closes the connection without
sending a message.

### 3.4 `probe` (server → client)

The server sends one of these for each valid UDP probe it receives, but only
if the client asked for `per_probe`:

```json
{"type":"probe","seq":0,"t1":1795645440624608470,"t2":1795645440624638900,"owd_ns":30430,"size":64}
```

| field | type | meaning |
|---|---|---|
| `seq` | uint64 | the probe's sequence number |
| `t1` | int64 | the client send time, copied from the probe |
| `t2` | int64 | the server receive time. It is the same value as in the UDP echo. |
| `owd_ns` | int64 | `t2 - t1`. It is raw, not corrected for clock offset, and can be negative if the clocks differ. |
| `size` | int | the size of the probe's UDP payload, in bytes |

These messages arrive in the order the server received the probes. They are
the authoritative record of what reached the server: they cover every probe
that arrived, including those whose echo was later lost. If the client reads
too slowly and the server's queue of 65,536 messages per session fills up, the
server drops `probe` messages. The server's counters in `stats` and `final`
still include those probes.

### 3.5 `stats` (server → client)

The server sends this every `stats_interval_ms`, from the moment the session
opens:

```json
{"type":"stats","elapsed_ms":1000,
 "window":{"received":100,"bytes":6400,"lost":0,"reordered":0,"owd_min_ms":11.2,"owd_avg_ms":11.9,"owd_max_ms":14.0,"jitter_ms":0.41},
 "total": {"received":200,"bytes":12800,"lost":1,"reordered":0,"owd_min_ms":11.1,"owd_avg_ms":12.0,"owd_max_ms":30.1,"jitter_ms":0.40}}
```

- `elapsed_ms` is the time since the session opened.
- `window` covers the probes received since the previous `stats` message.
- `total` covers the whole session.

Both are **stats objects**:

| field | type | meaning |
|---|---|---|
| `received` | int | probes received |
| `bytes` | int | total UDP payload bytes received |
| `lost` | int | an estimate of lost probes. In `window`: `(highest seq − first seq in the window + 1) − received`. In `total`: `(highest seq + 1) − received`. Never negative. |
| `reordered` | int | probes that arrived with a lower `seq` than one already received |
| `owd_min_ms`, `owd_avg_ms`, `owd_max_ms` | float | raw `t2 − t1` in milliseconds, rounded to 1 µs |
| `jitter_ms` | float | RFC 3550 interarrival jitter of the OWD series, in ms: `J += (|D(i-1,i)| − J) / 16`. In `window`, it starts again from 0 for each window. |

When `received` is 0, the `owd_*` and `jitter_ms` fields are 0 and carry no
meaning.

`lost` counts only probes lost before the highest-numbered probe received, so
probes lost at the very end of a run don't appear until `final`. `final` uses
the client's `sent` count instead.

### 3.6 `bye` (client → server)

```json
{"type":"bye","sent":3}
```

| field | type | meaning |
|---|---|---|
| `sent` | int | the total number of probes the client sent. The server uses it for the final loss count. |

Before sending `bye`, wait long enough for in-flight probes to arrive. The Go
client waits 2 s by default (`-linger`). Probes that arrive after `bye` are
not included in `final`.

### 3.7 `final` (server → client)

```json
{"type":"final","elapsed_ms":631,"sent":3,
 "total":{"received":3,"bytes":192,"lost":0,"reordered":0,"owd_min_ms":0.016,"owd_avg_ms":0.021,"owd_max_ms":0.03,"jitter_ms":0.001}}
```

When it receives `bye`, the server:

1. sends any queued `probe` messages
2. sends `final`, with a `total` stats object in which `lost = sent − received`
3. waits up to 2 s for the client to close, then closes the connection

`final` is always the last message on the connection, and it has no `window`.

### 3.8 Ending a session

| event | result |
|---|---|
| the client sends `bye` | the server sends `final` and closes the connection (§3.7) |
| the client closes the TCP connection without `bye` | the session ends immediately, with no `final` |
| the server stops (SIGTERM) or crashes | the TCP connection closes. Clients should stop sending probes when that happens. |

Once a session has ended, the server silently ignores UDP probes for its id.
The server enables TCP keepalive (Go's default, 15 s), so it detects dead
clients eventually.

## 4. UDP datagrams

All integers are **big-endian** (network byte order). `int64` values are
two's complement.

### 4.1 Probe (client → server)

```
offset size type    field
0      4    uint32  magic      = 0x4F574450 ("OWDP")
4      4    uint32  session    from welcome
8      8    uint64  seq        sequence number; start at 0 and add 1 for each probe
16     8    int64   t1         client send time, Unix ns
24     2    uint16  reply_len  requested echo size, in UDP payload bytes
26     ...          padding    optional, any content; makes the probe the size you want
```

The minimum probe size is **26 bytes** and the maximum is 65,507, the limit
for a UDP payload. Pick a size that avoids IP fragmentation on your path
unless you mean to test fragmentation. With a 1500-byte MTU, that means at
most 1472 bytes over IPv4 or 1452 over IPv6.

The server **silently drops** a probe in any of these cases:

- it is shorter than 26 bytes, or the magic is wrong
- the session id is unknown or has ended
- `-strict-ip` is on and the source IP differs from the session's TCP peer

Dropped probes are never echoed or counted.

`seq` doesn't have to start at 0 or be contiguous. The server's `lost` and
`reordered` counters assume it does both, though.

### 4.2 Echo (server → client)

```
offset size type    field
0      4    uint32  magic      = 0x4F574445 ("OWDE")
4      4    uint32  session
8      8    uint64  seq        copied from the probe
16     8    int64   t1         copied from the probe
24     8    int64   t2         server receive time
32     8    int64   t3         server send time
40     ...          padding    zero bytes
```

The echo's size is `reply_len` from the probe, raised to at least 40 bytes
and capped at `max_reply` from the `welcome`. Because `reply_len` can differ
from the probe size, a client can load one direction more heavily than the
other. The server sends exactly one echo for each accepted probe, to the
probe's source address and port.

## 5. Writing a client: checklist

1. Open a TCP connection, send `hello`, and read `welcome`. If you get
   `error`, stop.
2. Create a UDP socket in the same address family as the TCP connection.
   Connect it to the TCP peer's IP and `udp_port`. Turn on `SO_TIMESTAMPNS`
   to get kernel receive timestamps.
3. Read the TCP channel in a separate thread or task, so `probe` and `stats`
   messages never back up.
4. Send probes on absolute deadlines, so the sending rate doesn't drift.
   Record how many you send.
5. Receive echoes, take `t4`, and match them to probes by `seq`.
6. Stop sending, wait for stragglers, send `bye`, and read until `final`
   arrives or the connection closes.
7. Compute the results:
   - **Uplink loss** = `sent − final.total.received`. Use the server's count,
     not the echo count.
   - **Downlink loss** = `final.total.received − echoes received`.

## 6. Command-line client output (`owd client`)

### 6.1 `-csv`: client-side record of each echo

Header: `seq,t1_ns,t2_ns,t3_ns,t4_ns,rtt_ns,up_raw_ns,down_raw_ns,size`

There is one row per echo, in arrival order. `up_raw_ns` is `t2-t1`,
`down_raw_ns` is `t4-t3`, and `size` is the echo's size. Probes whose echo was
lost have no row.

### 6.2 `-server-csv`: the server's TCP reports

Header: `seq,t1_ns,t2_ns,owd_raw_ns,size`

There is one row per `probe` message, so this file includes probes whose echo
was lost. The server's `-log-dir` CSV has the same columns, except that the fourth
is named `owd_ns`. The server writes it whatever the client asks for.

### 6.3 `-json`: final summary

```json
{
  "server": "10.0.0.2",
  "duration_s": 10,
  "sent": 1000,
  "server_received": 998,
  "echoes_received": 997,
  "uplink_loss_pct": 0.2,
  "downlink_loss_pct": 0.1,
  "server_reordered": 0,
  "clock_offset_ms": 3.112,
  "offset_min_rtt_ms": 23.95,
  "rtt":                     {"min_ms": 23.95, "avg_ms": 24.79, "max_ms": 41.2, "jitter_ms": 0.598},
  "uplink_owd":              {"min_ms": 11.12, "avg_ms": 11.91, "max_ms": 30.1, "jitter_ms": 0.41},
  "downlink_owd":            {"min_ms": 12.83, "avg_ms": 12.88, "max_ms": 15.6, "jitter_ms": 0.355},
  "uplink_owd_raw_clocks":   {"...": "same shape"},
  "downlink_owd_raw_clocks": {"...": "same shape"},
  "server_report_final": true
}
```

The values above are illustrative.

| field | meaning |
|---|---|
| `server_received` | `final.total.received`, or `-1` if `final` never arrived |
| `uplink_loss_pct` | `(sent − server_received) / sent`. If there was no `final`, this is the round-trip loss instead. |
| `downlink_loss_pct` | `(server_received − echoes_received) / server_received`. It is 0 if there was no `final`. |
| `clock_offset_ms` | the server clock minus the client clock, taken from the probe with the lowest RTT |
| `offset_min_rtt_ms` | the RTT of that probe. A lower value means a tighter estimate. |
| `uplink_owd`, `downlink_owd` | OWD corrected by `clock_offset_ms` |
| `*_raw_clocks` | uncorrected OWD; valid only if both hosts are synchronised |
| `server_report_final` | `false` if the server never sent `final`, for example because it died. In that case the uplink figures come from the UDP echoes only. |

Every distribution object has `min_ms`, `avg_ms`, `max_ms` and `jitter_ms`.
An object is `null` when there is no data for it.

## 7. Compatibility

- Clients must ignore message types and fields they don't recognise. Future
  versions may add fields to any message.
- An incompatible change will raise `version` in `welcome`. Clients should
  refuse to run against a version they don't support.
- The 26-byte probe header and 40-byte echo header are fixed for version 1.
  Use the padding to make packets larger.
