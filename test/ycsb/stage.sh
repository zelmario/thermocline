#!/bin/bash
# Drives test/ycsb/app.sh on the Hetzner staging app node, from this machine.
#
#   test/ycsb/stage.sh setup                        java, YCSB, sysstat and the rig
#   RECORDS=100000000 test/ycsb/stage.sh load       once: load and date the archive
#   FROM=41000000 NOW=... test/ycsb/stage.sh date   resume a dating that stopped
#   MODE=alone LABEL=alone-1 SECS=600 test/ycsb/stage.sh run
#   MODE=tc LABEL=tc-1 RATE=20000 test/ycsb/stage.sh run
#   test/ycsb/stage.sh fetch DIR                    copy every result here
#
# A run records vmstat and iostat every 5s on every database node and on the
# app node, and pidstat for the daemons and YCSB there, so a result says which
# machine ran out first, the client's included.
#
# COLDSTART=1 (the default) restarts all six mongod and drops the page cache
# before a run, so neither arm starts on pages the run before it warmed.
#
# Everything long runs detached on the nodes and is polled, so a dropped ssh
# loses a status line, never a load or a run.
#
# YCSB comes from the Docker box (BOX), where the 0.17 MongoDB binding has its
# 3.8 driver replaced by 3.12.14, which speaks to MongoDB 8.0.
set -uo pipefail
cd "$(dirname "$0")/../.."
H=deploy/hetzner
# The Docker test box, user@host: BOX, or the repo's .box file (not in git).
BOX=${BOX:-$(cat "$(dirname "$0")/$(git -C "$(dirname "$0")" rev-parse --show-cdup).box" 2>/dev/null)}
: "${BOX:?set BOX to the Docker test box as user@host, or write it to .box}"
SSH="ssh -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$PWD/$H/.known_hosts -o ConnectTimeout=10 -o BatchMode=yes -o ServerAliveInterval=30"
IPS=$(cd $H && bash tf.sh output -json public_ips)
ip() { echo "$IPS" | jq -r --arg n "$1" '.[$n]'; }
APP=$(ip app)
[ -n "$APP" ] && [ "$APP" != null ] || { echo "no app node in the terraform state" >&2; exit 2; }
DBS=$(echo "$IPS" | jq -r 'keys[] | select(test("^(hot|cold)[0-9]$"))')
ENV="RECORDS=${RECORDS:-100000000} SECS=${SECS:-600} RATE=${RATE:-0} THREADS=${THREADS:-64} DIST=${DIST:-latest} AGE=${AGE:-365d} LIVE=${LIVE:-true}"
ENV="$ENV ${OPLOG_MB:+OPLOG_MB=$OPLOG_MB} ${FROM:+FROM=$FROM} ${NOW:+NOW=$NOW} ${WORKLOAD:+WORKLOAD=$WORKLOAD} ${KEEPHOT:+KEEPHOT=$KEEPHOT} ${CLINE:+CLINE=$CLINE} ${COPYUP:+COPYUP=$COPYUP} ${WARMNOW:+WARMNOW=$WARMNOW} ${FEEDERSTREAM:+FEEDERSTREAM=$FEEDERSTREAM}"

ship() {
  tar -cz -C test/ycsb app.sh workload_recent datehistory.js | $SSH root@"$APP" "mkdir -p /root/ycsb-rig /root/ycsb-out && tar -xz -C /root/ycsb-rig"
}

# detach LABEL MODE: app.sh started on the app node outside this ssh session,
# then waited on. Its exit code lands in LABEL.rc; progress lines are echoed
# here as they appear.
detach() {
  local l=$1 mode=$2 seen rc n
  seen=$($SSH root@"$APP" "touch /root/ycsb-out/progress.out; wc -l < /root/ycsb-out/progress.out") || return 2
  $SSH root@"$APP" "rm -f /root/ycsb-out/$l.rc; cd /root/ycsb-rig && setsid nohup bash -c 'MODE=$mode LABEL=$l $ENV bash app.sh > /root/ycsb-out/$l.console 2>&1; echo \$? > /root/ycsb-out/$l.rc' > /dev/null 2>&1 < /dev/null &" || return 2
  while :; do
    sleep 30
    n=$($SSH root@"$APP" "wc -l < /root/ycsb-out/progress.out" 2>/dev/null) || continue
    if [ "${n:-0}" -gt "$seen" ]; then
      $SSH root@"$APP" "sed -n '$((seen + 1)),${n}p' /root/ycsb-out/progress.out" 2>/dev/null && seen=$n
    fi
    rc=$($SSH root@"$APP" "cat /root/ycsb-out/$l.rc 2>/dev/null") || continue
    if [ -n "$rc" ]; then
      $SSH root@"$APP" "tail -n +$((seen + 1)) /root/ycsb-out/progress.out" 2>/dev/null
      [ "$rc" = 0 ] || $SSH root@"$APP" "tail -5 /root/ycsb-out/$l.console" 2>/dev/null
      return "$rc"
    fi
  done
}

# coldstart: every mongod stopped, the page cache dropped, started again, then
# waited on until both sets have a primary and every member is healthy.
coldstart() {
  local n
  for n in $DBS; do
    $SSH root@"$(ip "$n")" "systemctl stop mongod && sync && echo 3 > /proc/sys/vm/drop_caches && systemctl start mongod" &
  done
  wait
  for _ in $(seq 60); do
    $SSH root@"$APP" "for s in hot cold; do u=\$(awk -v s=\$s: '\$1==s{f=1;next} f&&\$1==\"uri:\"{gsub(/\"/,\"\",\$2);print \$2;exit}' /etc/thermocline/cline.yaml);
      mongosh \"\$u\" --quiet --eval 'const s=rs.status(); print(s.members.every(m => [\"PRIMARY\",\"SECONDARY\"].includes(m.stateStr)) && s.members.some(m => m.stateStr===\"PRIMARY\"))' 2>/dev/null | tail -1; done" 2>/dev/null |
      grep -c true | grep -q 2 && { echo "coldstart: both sets up with cold caches"; return 0; }
    sleep 5
  done
  echo "coldstart: the sets did not come back in 5 minutes" >&2
  return 2
}

case "${1:-}" in
setup)
  $SSH root@"$APP" "command -v java >/dev/null || { timeout 600 apt-get install -y -q openjdk-17-jre-headless >/dev/null; };
    ln -sfn \$(dirname \$(dirname \$(readlink -f \$(command -v java)))) /root/ycsb-jre && /root/ycsb-jre/bin/java -version 2>&1 | head -1" || exit 2
  # vmstat ships with Ubuntu, iostat and pidstat do not, and a node unparked is
  # built from scratch: without this the iostat files held "command not found".
  for n in app $DBS; do
    $SSH root@"$(ip "$n")" "command -v iostat >/dev/null || timeout 300 apt-get install -y -q sysstat >/dev/null; command -v iostat pidstat | wc -l" |
      grep -q 2 || { echo "setup: no sysstat on $n" >&2; exit 2; }
  done
  ssh "$BOX" 'tar -cz -C ~ ycsb' | $SSH root@"$APP" "rm -rf /root/ycsb && tar -xz -C /root && ls /root/ycsb/lib | grep mongo-java-driver" || exit 2
  ship && echo "setup: done on $APP"
  ;;
load|date)
  ship
  detach "$1" "$1"
  ;;
run)
  ship
  L=${LABEL:-${MODE:-alone}}
  if [ "${COLDSTART:-1}" = 1 ]; then coldstart || exit 2; fi
  for n in app $DBS; do
    $SSH root@"$(ip "$n")" "mkdir -p /root/mon && cd /root/mon && pkill -x vmstat; pkill -x iostat; pkill -x pidstat;
      setsid nohup vmstat -t 5 > $L-$n.vmstat 2>&1 < /dev/null & setsid nohup iostat -xmt 5 > $L-$n.iostat 2>&1 < /dev/null &
      if [ $n = app ]; then setsid nohup pidstat -u -r -h -C 'cline|java' 5 > $L-$n.pidstat 2>&1 < /dev/null & fi
      true" || echo "run: no monitoring on $n" >&2
  done
  detach "$L" "${MODE:-alone}"
  rc=$?
  for n in $DBS app; do
    $SSH root@"$(ip "$n")" "pkill -x vmstat; pkill -x iostat; pkill -x pidstat; tar -cz -C /root/mon ." 2>/dev/null |
      $SSH root@"$APP" "mkdir -p /root/ycsb-out/mon && tar -xz -C /root/ycsb-out/mon" 2>/dev/null
  done
  exit $rc
  ;;
fetch)
  D=${2:?fetch needs a directory}
  mkdir -p "$D" && $SSH root@"$APP" "tar -cz -C /root ycsb-out" | tar -xz -C "$D" && echo "fetch: $D/ycsb-out"
  ;;
*)
  sed -n 2,22p "$0"; exit 2
  ;;
esac
