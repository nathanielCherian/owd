#!/bin/sh
# Install (or remove) the owd server as a background service.
#
#   sudo ./install.sh [--port 9910] [--args "-log-dir /var/lib/owd"]   system service
#   ./install.sh --user [--port 9910]                                  per-user service (no root)
#   ./install.sh --uninstall [--user]
#
# Without systemd it falls back to starting the server with nohup.
set -eu
cd "$(dirname "$0")"

PORT=9910
EXTRA=""
MODE=system
ACTION=install
[ "$(id -u)" -eq 0 ] || MODE=user
while [ $# -gt 0 ]; do
	case "$1" in
	--port) PORT=$2; shift 2 ;;
	--args) EXTRA=$2; shift 2 ;;
	--user) MODE=user; shift ;;
	--uninstall) ACTION=uninstall; shift ;;
	-h|--help) sed -n '2,9p' "$0"; exit 0 ;;
	*) echo "unknown option $1" >&2; exit 2 ;;
	esac
done

case "$(uname -m)" in
x86_64|amd64) ARCH=amd64 ;;
aarch64|arm64) ARCH=arm64 ;;
armv6*|armv7*|armhf) ARCH=arm ;;
i?86) ARCH=386 ;;
*) echo "unsupported CPU $(uname -m)" >&2; exit 1 ;;
esac

if [ "$MODE" = system ]; then
	BIN=/usr/local/bin/owd
	ENVFILE=/etc/default/owd
	UNIT_DIR=/etc/systemd/system
	SYSTEMCTL="systemctl"
	WANTEDBY=multi-user.target
	SYSONLY=""
else
	BIN=$HOME/.local/bin/owd
	ENVFILE=$HOME/.config/owd.env
	UNIT_DIR=$HOME/.config/systemd/user
	SYSTEMCTL="systemctl --user"
	WANTEDBY=default.target
	SYSONLY="#"
fi
HAVE_SYSTEMD=0
[ -d /run/systemd/system ] && $SYSTEMCTL show-environment >/dev/null 2>&1 && HAVE_SYSTEMD=1

if [ "$ACTION" = uninstall ]; then
	if [ $HAVE_SYSTEMD = 1 ]; then
		$SYSTEMCTL disable --now owd.service 2>/dev/null || true
		rm -f "$UNIT_DIR/owd.service"
		$SYSTEMCTL daemon-reload
	fi
	[ -f "$HOME/.owd.pid" ] && kill "$(cat "$HOME/.owd.pid")" 2>/dev/null && rm -f "$HOME/.owd.pid"
	rm -f "$BIN"
	echo "removed owd ($MODE); config left at $ENVFILE"
	exit 0
fi

SRC=owd-linux-$ARCH
[ -f "$SRC" ] || SRC=dist/owd-linux-$ARCH
[ -f "$SRC" ] || { echo "binary owd-linux-$ARCH not found next to install.sh" >&2; exit 1; }

mkdir -p "$(dirname "$BIN")" "$(dirname "$ENVFILE")"
install -m 0755 "$SRC" "$BIN.new" && mv -f "$BIN.new" "$BIN"
if [ ! -f "$ENVFILE" ] || [ -n "$EXTRA" ] || [ "$PORT" != 9910 ]; then
	echo "OWD_ARGS=-port $PORT $EXTRA" > "$ENVFILE"
fi
echo "installed $BIN ($("$BIN" version)), config $ENVFILE: $(cat "$ENVFILE")"

if [ $HAVE_SYSTEMD = 1 ]; then
	mkdir -p "$UNIT_DIR"
	sed -e "s|@BIN@|$BIN|" -e "s|@ENVFILE@|$ENVFILE|" -e "s|@WANTEDBY@|$WANTEDBY|" \
		-e "s|@SYSTEM_ONLY@|$SYSONLY|" owd.service 2>/dev/null > "$UNIT_DIR/owd.service" ||
		sed -e "s|@BIN@|$BIN|" -e "s|@ENVFILE@|$ENVFILE|" -e "s|@WANTEDBY@|$WANTEDBY|" \
			-e "s|@SYSTEM_ONLY@|$SYSONLY|" deploy/owd.service > "$UNIT_DIR/owd.service"
	$SYSTEMCTL daemon-reload
	$SYSTEMCTL enable owd.service >/dev/null
	$SYSTEMCTL restart owd.service
	sleep 1
	$SYSTEMCTL --no-pager --lines=3 status owd.service || true
	if [ "$MODE" = user ] && command -v loginctl >/dev/null &&
		[ "$(loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null)" != yes ]; then
		echo "note: run 'loginctl enable-linger $(id -un)' (may need root) so the service keeps"
		echo "      running after you log out and starts at boot."
	fi
	if [ "$MODE" = user ]; then echo "logs: journalctl --user -u owd -f"; else echo "logs: journalctl -u owd -f"; fi
else
	. "$ENVFILE"
	[ -f "$HOME/.owd.pid" ] && kill "$(cat "$HOME/.owd.pid")" 2>/dev/null || true
	nohup "$BIN" server $OWD_ARGS >> "$HOME/owd.log" 2>&1 &
	echo $! > "$HOME/.owd.pid"
	echo "no systemd: started with nohup (pid $(cat "$HOME/.owd.pid"), log ~/owd.log); not restarted on reboot"
fi
echo "open TCP and UDP port $PORT in the firewall if one is active."
