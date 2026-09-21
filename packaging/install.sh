#!/bin/bash
# Install cline as a set of services on one host. Idempotent, and it never
# touches a config that is already there.
#
#   sudo packaging/install.sh [path/to/cline]
#
# What it does, which is the list in "Running them as services" in
# docs/operations.md: the binary, a system account that owns neither tier, the
# directories, the units and the logrotate file. What it does not do is write
# /etc/cline/cline.yaml, because the two URIs in it are the only thing here
# nobody can guess, and overwriting a working one during an upgrade is the kind
# of help nobody wants.
#
# test/testservice.sh runs this script rather than repeating its steps, so the
# procedure and the thing under test cannot drift apart.
set -euo pipefail

BIN=${1:-bin/cline}
PREFIX=${PREFIX:-/usr/local/bin}
CONFDIR=${CONFDIR:-/etc/cline}
LOGDIR=${LOGDIR:-/var/log/cline}
UNITDIR=${UNITDIR:-/etc/systemd/system}
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

[ -x "$BIN" ] || { echo "install.sh: $BIN is not there; run make build first" >&2; exit 1; }
[ "$(id -u)" = "0" ] || { echo "install.sh: run it as root" >&2; exit 1; }

id cline >/dev/null 2>&1 || useradd --system --no-create-home --shell /sbin/nologin cline
install -d -o cline -g cline "$CONFDIR" "$LOGDIR"
# cmp rather than a path comparison: it covers installing a file over itself,
# which is what a re-run does, and it also skips the copy when two paths hold
# the same build.
if ! cmp -s "$BIN" "$PREFIX/cline" 2>/dev/null; then
  install -m 0755 "$BIN" "$PREFIX/cline"
  echo "install.sh: $PREFIX/cline replaced."
else
  echo "install.sh: $PREFIX/cline is already this build."
fi
install -m 0644 "$HERE"/systemd/cline-*.service "$HERE"/systemd/cline-*.timer "$UNITDIR/"
if [ -d /etc/logrotate.d ]; then
  install -m 0644 "$HERE/logrotate/cline" /etc/logrotate.d/cline
fi

if [ -f "$CONFDIR/cline.yaml" ]; then
  echo "install.sh: $CONFDIR/cline.yaml is already there and was left alone."
else
  echo "install.sh: write $CONFDIR/cline.yaml next, owned by cline and mode 0640."
  echo "            cline.example.yaml documents every setting; the two URIs at the"
  echo "            top are the only ones you must change."
fi

systemctl daemon-reload

# An upgrade rather than a first install. Nothing here restarts anything, on
# purpose, so what the operator needs is the order to do it in: the feeder
# records what the routers are about to start acting on, so the routers go last.
# That order is docs/operations.md's, not this script's invention.
if systemctl is-active --quiet cline-feeder || systemctl is-active --quiet cline-router; then
  cat <<'UPGRADE'

Installed over a running deployment. Nothing here restarted anything, so a
daemon goes on running whatever binary and unit file it started with. When you
do restart them, the order is not arbitrary:

  systemctl restart cline-feeder
  systemctl restart cline-evictor
  systemctl restart cline-router          # the routers last, one host at a time

A new feeder records the documents the archive refused; an old router ignores
those records and answers as every version did before. Restarting the routers
last means the protection is never half-applied in the direction that answers
short. See "One of each daemon" in docs/operations.md.
UPGRADE
  exit 0
fi

cat <<'NEXT'

Installed. Then, in this order:

  cline preflight --config /etc/cline/cline.yaml     # writes nothing, refuses a bad pair
  systemctl enable --now cline-feeder                # and let it catch up first
  systemctl enable --now cline-router
  systemctl enable --now cline-warmer.timer cline-evictor

docs/adoption.md is the numbered order for a cluster already serving traffic,
and it matters: the feeder has to be running before anything is seeded, because
a change stream starts at now.
NEXT
