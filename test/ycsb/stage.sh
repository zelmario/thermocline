#!/bin/bash
# Drives test/ycsb/app.sh on the Hetzner staging app node, from this machine.
#
#   test/ycsb/stage.sh setup                        java, YCSB and the rig on the app node
#   RECORDS=100000000 test/ycsb/stage.sh load       once: load and date the archive
#   MODE=alone LABEL=alone-1 SECS=600 test/ycsb/stage.sh run
#   MODE=tc LABEL=tc-1 RATE=20000 test/ycsb/stage.sh run
#   test/ycsb/stage.sh fetch DIR                    copy every result here
#
# A run also records vmstat and iostat every 5s on both primaries, so a
# result says which machine ran out first.
#
# YCSB comes from the Docker box (BOX), where the 0.17 MongoDB binding has its
# 3.8 driver replaced by 3.12.14, which speaks to MongoDB 8.0.
set -uo pipefail
cd "$(dirname "$0")/../.."
H=deploy/hetzner
BOX=${BOX:-zelmario@192.168.1.50}
SSH="ssh -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$PWD/$H/.known_hosts -o ConnectTimeout=10 -o BatchMode=yes -o ServerAliveInterval=30"
ip() { (cd $H && bash tf.sh output -json public_ips) | jq -r --arg n "$1" '.[$n]'; }
APP=$(ip app)
[ -n "$APP" ] && [ "$APP" != null ] || { echo "no app node in the terraform state" >&2; exit 2; }
ENV="RECORDS=${RECORDS:-100000000} SECS=${SECS:-600} RATE=${RATE:-0} THREADS=${THREADS:-64} DIST=${DIST:-latest} AGE=${AGE:-365d} LIVE=${LIVE:-true}"

ship() {
  tar -cz -C test/ycsb app.sh workload_recent datehistory.js | $SSH root@"$APP" "mkdir -p /root/ycsb-rig && tar -xz -C /root/ycsb-rig"
}

# primary SET prints the public address of SET's primary, by its node name.
primary() {
  local host
  host=$($SSH root@"$APP" "mongosh \"\$(awk -v s='$1:' '\$1==s{f=1;next} f&&\$1==\"uri:\"{gsub(/\"/,\"\",\$2);print \$2;exit}' /etc/thermocline/cline.yaml)\" --quiet --eval 'print(db.hello().primary)'" | tail -1)
  local priv=${host%%:*}
  (cd $H && bash tf.sh output -json private_ips) | jq -r --arg p "$priv" 'to_entries[] | select(.value==$p) | .key' |
    while read -r n; do ip "$n"; done
}

case "${1:-}" in
setup)
  $SSH root@"$APP" "command -v java >/dev/null || { timeout 600 apt-get install -y -q openjdk-17-jre-headless >/dev/null; };
    ln -sfn \$(dirname \$(dirname \$(readlink -f \$(command -v java)))) /root/ycsb-jre && /root/ycsb-jre/bin/java -version 2>&1 | head -1" || exit 2
  ssh "$BOX" 'tar -cz -C ~ ycsb' | $SSH root@"$APP" "rm -rf /root/ycsb && tar -xz -C /root && ls /root/ycsb/lib | grep mongo-java-driver" || exit 2
  ship && echo "setup: done on $APP"
  ;;
load)
  ship
  $SSH root@"$APP" "cd /root/ycsb-rig && MODE=load LABEL=load $ENV bash app.sh"
  ;;
run)
  ship
  L=${LABEL:-${MODE:-alone}}
  TMP=$(mktemp -d)
  PIDS=()
  for set in cold hot; do
    p=$(primary $set)
    [ -n "$p" ] || continue
    $SSH root@"$p" "vmstat -t 5" > "$TMP/$L-$set.vmstat" 2>&1 & PIDS+=($!)
    $SSH root@"$p" "iostat -xmt 5" > "$TMP/$L-$set.iostat" 2>&1 & PIDS+=($!)
  done
  $SSH root@"$APP" "cd /root/ycsb-rig && MODE=${MODE:-alone} LABEL=$L $ENV bash app.sh"
  rc=$?
  kill "${PIDS[@]}" 2>/dev/null; wait 2>/dev/null
  for f in "$TMP"/*; do
    [ -f "$f" ] && $SSH root@"$APP" "cat > /root/ycsb-out/$(basename "$f")" < "$f"
  done
  rm -rf "$TMP"
  exit $rc
  ;;
fetch)
  D=${2:?fetch needs a directory}
  mkdir -p "$D" && $SSH root@"$APP" "tar -cz -C /root ycsb-out" | tar -xz -C "$D" && echo "fetch: $D/ycsb-out"
  ;;
*)
  sed -n 2,14p "$0"; exit 2
  ;;
esac
