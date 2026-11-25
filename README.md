# owd: one-way delay server and client

A single static binary (no dependencies, no libc) that measures one-way delay
(OWD) between a client and an always-on server.

```
client                                   server (background service)
  |--- TCP  hello ------------------------->|
  |<-- TCP  welcome {session} --------------|
  |--- UDP  probe  {seq, t1} -------------->|  t2 = kernel rx timestamp
  |<-- UDP  echo   {seq, t1, t2, t3} -------|  t3 = server send time
  |<-- TCP  {"type":"probe", seq, owd_ns}   |  one per probe received
  |<-- TCP  {"type":"stats", window, total} |  every second
  |--- TCP  bye {sent} -------------------->|
  |<-- TCP  {"type":"final", total} --------|
```

- **Over TCP** the server reports every probe it received (`t2 - t1`) and
  periodic stats: count, loss, reordering, min/avg/max OWD and RFC 3550 jitter.
  Because TCP is reliable, the client still learns the uplink OWD of a probe
  whose UDP echo was lost, which separates uplink loss from downlink loss.
- **Over UDP** the server echoes each probe with its receive and send
  timestamps. The client stamps the echo on arrival (`t4`) and gets the
  downlink OWD (`t4 - t3`) and the RTT (`(t4-t1) - (t3-t2)`).

Both ends use kernel receive timestamps (`SO_TIMESTAMPNS`), so scheduling
delay on either host doesn't show up as network delay.

## Clocks

OWD needs the two clocks to agree. The client reports OWD both ways:

- **offset-corrected** (default): the clock offset is estimated NTP-style from
  the lowest-RTT probe. This assumes the *minimum* delay is the same in both
  directions. If it isn't, the up/down split carries a constant bias of
  `(min_up - min_down)/2`.
- **raw clocks** (`-raw` for the live view): use when both hosts are
  synchronised by NTP/chrony/PTP.

Changes in OWD (jitter, queuing spikes) are exact either way. Clock drift over
the length of a run is not corrected.

## Build

Needs Go ≥ 1.21, on any machine:

```sh
./build.sh          # -> dist/owd-linux-{amd64,arm64,arm,386} + owd-1.0.0-linux.tar.gz
```

## Deploy the server

Copy `owd-1.0.0-linux.tar.gz` to the target host and run:

```sh
tar xzf owd-1.0.0-linux.tar.gz
sudo ./install.sh                          # system service on port 9910
sudo ./install.sh --port 7000 --args "-log-dir /var/lib/owd"
./install.sh --user                        # no root: systemd --user service
./install.sh --uninstall [--user]
```

- **As root** it installs `/usr/local/bin/owd` and a hardened
  `owd.service` (DynamicUser, restart on failure, starts at boot). Options go in
  `/etc/default/owd` (`OWD_ARGS=...`).
- **Without root** it installs into `~/.local/bin` and `~/.config/systemd/user`.
  Run `loginctl enable-linger $USER` so the service survives logout and starts at boot.
- **Without systemd** it starts the server with `nohup` and logs to `~/owd.log`.

Open TCP **and** UDP on the port in any firewall. Logs: `journalctl -u owd -f`
(add `--user` for a user install).

Server options (`owd server -h`):

| flag | default | |
|---|---|---|
| `-port` | 9910 | TCP control port; also the UDP port |
| `-udp-port` | same as `-port` | separate UDP port |
| `-bind` | all | listen address |
| `-log-dir` | off | write `seq,t1,t2,owd,size` CSV per session |
| `-max-sessions` | 64 | concurrent clients |
| `-max-reply` | 1472 | largest UDP echo, in bytes |
| `-strict-ip` | true | accept probes only from the IP that opened the TCP session |

## Run the client

```sh
owd client -host server.example.com                      # 10 s, 100 probes/s
owd client -host H -i 1ms -t 60s -size 1200 -reply-size 64   # load the uplink
owd client -host H -t 0 -csv cli.csv -server-csv srv.csv -json summary.json
```

Example output (illustrative numbers):

```
owd: session 5b480a89 with 10.0.0.2 (tcp :9910, udp :9910), probe 64B / echo 64B every 10ms
[   1.0s] sent 100     srv 100     echo 100     | rtt   24.812 ±0.611  | up   11.904 ±0.402  | down   12.908 ±0.355  ms
...
--- owd summary: 10.0.0.2, 10.0s ---
probes sent        1000
server received    998   (uplink loss 0.200%, reordered 0)
echoes received    997   (downlink loss 0.100%)
clock offset       +3.112 ms (server - client, from min-RTT probe of 23.950 ms)

(ms)                              min        avg        max     jitter
RTT                            23.950     24.790     41.200      0.598
uplink OWD                     11.120     11.910     30.102      0.410
...
```

Live columns show the window's average ± jitter. Client options: `owd client -h`.

Output files:

- `-csv`: one row per UDP echo: `seq,t1_ns,t2_ns,t3_ns,t4_ns,rtt_ns,up_raw_ns,down_raw_ns,size`
- `-server-csv`: one row per TCP report from the server: `seq,t1_ns,t2_ns,owd_raw_ns,size`.
  It includes probes whose echo was lost.
- `-json`: the final summary.

## Python test client

`owd_client.py` is a second client written separately from the Go one, using
the Python standard library only. It runs a measurement and then checks that
the server behaves correctly. It exits non-zero if any check fails, so you can
use it in scripts or CI after a deploy.

```sh
python3 owd_client.py HOST [--port 9910] [-i 0.01] [-t 5] [--size 64] [--reply-size N] [--csv out.csv]
```

The checks cover:

- the handshake, including that a bad hello is rejected
- that echoes carry back our seq and t1, at the requested size
- that UDP echoes and TCP reports agree on t2 for every probe
- that `owd_ns == t2 - t1`
- that the final report's counts match
- that probes with an unknown session id get no echo
- that loss is under `--max-loss` (default 5%) in each direction

## Comparing several servers

`owd_compare.py` measures several servers at once and prints their delays side
by side. Each server gets its own session, and all of them are probed from one
loop with the same timing and packet sizes, so they share the same conditions
on the local link.

```sh
python3 owd_compare.py nyc=203.0.113.5 lon=198.51.100.7:7000 lab=[2001:db8::1]:9910 -t 30
```

A server is given as `[NAME=]HOST[:PORT]`, with IPv6 addresses in brackets.
Every second it prints one line per server with the average and jitter of RTT,
uplink OWD and downlink OWD. At the end it prints a table sorted by RTT, with
loss in each direction, averages, 95th percentiles, jitter and each server's
clock offset. Use `--json` for the per-server summary, `--csv` for every probe
of every server, and `--raw` if all the hosts' clocks are synchronised.
It needs `owd_client.py` in the same directory and uses only the standard library.

## Protocol

The full API is in [docs/API.md](docs/API.md): TCP messages, UDP packet
layouts, the session lifecycle and the client's output formats.

In short: UDP headers are big-endian, the probe header is 26 bytes and the
echo header is 40 bytes, and both are padded to the requested size. TCP
carries newline-delimited JSON, so writing a client in another language is
straightforward.

## Security notes

There is no authentication, so anyone who can reach the port can open a
session. Two limits stop the server from being used to reflect traffic:

- UDP probes are accepted only from the IP of the session's TCP connection.
- Echoes are capped at `-max-reply` bytes.

To restrict who can connect, use the firewall.
