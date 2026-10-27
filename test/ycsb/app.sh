#!/bin/bash
# YCSB on the Hetzner staging app node: the archive alone against the same
# archive adopted by Thermocline. test/ycsb/stage.sh ships this and drives it;
# rig.sh is the same measurement on the Docker box.
#
#   MODE=load RECORDS=100000000 bash app.sh        once: load and date the archive
#   MODE=date FROM=41000000 NOW=... bash app.sh    resume a dating that stopped
#   MODE=alone LABEL=alone-1 bash app.sh           a run against rsCold directly
#   MODE=tc    LABEL=tc-1    bash app.sh           a run through a fresh router
#   WORKLOAD=workloadd MODE=tc LABEL=d-tc bash app.sh   YCSB's own workload D
#   MODE=warm LABEL=warm bash app.sh               fill the fast tier with the last AGE
#   KEEPHOT=1 MODE=tc LABEL=tc-hot bash app.sh     a run on that, not on an empty one
#   KEEPHOT=1 MODE=direct LABEL=d bash app.sh      the fast tier's own ceiling, no router
#
# A run lasts SECS seconds at RATE operations a second, or as fast as it can
# with RATE=0. Every run first removes what earlier runs inserted and
# Thermocline's own state, and empties the fast tier, so each starts from the
# same archive and a tc run starts cold, as an adoption does.
#
# The URIs come from /etc/thermocline/cline.yaml, which bootstrap.sh writes.
set -u
cd "$(dirname "$0")"
MODE=${MODE:-alone}
RECORDS=${RECORDS:-100000000}
SECS=${SECS:-600}
RATE=${RATE:-0}
THREADS=${THREADS:-64}
DIST=${DIST:-latest}
AGE=${AGE:-365d}
LABEL=${LABEL:-$MODE-$DIST}
OUT=${OUT:-/root/ycsb-out}
CFG=/root/ycsb.yaml
export PATH=/root/ycsb-jre/bin:$PATH
mkdir -p "$OUT"
t() { echo "$(date -u +%T) $*" | tee -a "$OUT/progress.out"; }

uri() { awk -v s="$1:" '$1==s{f=1;next} f&&$1=="uri:"{gsub(/"/,"",$2);print $2;exit}' /etc/thermocline/cline.yaml; }
HOT=$(uri hot)
COLD=$(uri cold)
RT="mongodb://${COLD#mongodb://}"; RT="${RT%%@*}@127.0.0.1:27801/?authSource=admin"
[ -n "$HOT" ] && [ -n "$COLD" ] || { echo "no URIs in /etc/thermocline/cline.yaml" >&2; exit 2; }

ycsb() { # ycsb load|run URI [-p k=v ...]
  local what=-t uri=$2
  [ "$1" = load ] && what=-load
  shift 2
  local tgt=()
  [ "$RATE" -gt 0 ] && tgt=(-target "$RATE")
  # WORKLOAD names one of YCSB's own (workloada, workloadd, ...), run with its
  # mix and distribution as shipped. Only the key layout is ours: the stock
  # files load with insertorder=hashed, and this archive was loaded ordered so
  # datehistory.js can date it by key, so a stock run names its keys the same
  # way or every read is a NOT_FOUND.
  local wl=(-P workload_recent -p requestdistribution="$DIST")
  [ -n "${WORKLOAD:-}" ] && [ "$what" = -t ] &&
    wl=(-P "/root/ycsb/workloads/$WORKLOAD" -p insertorder=ordered -p zeropadding=12)
  java -cp "/root/ycsb/lib/*" site.ycsb.Client "$what" -db site.ycsb.db.MongoDbClient "${wl[@]}" \
    -p mongodb.url="${uri%%\?*}ycsb?${uri#*\?}" -p recordcount="$RECORDS" \
    -threads "$THREADS" "${tgt[@]}" -s "$@" \
    > "$OUT/$LABEL.out" 2> "$OUT/$LABEL.err"
}

# health SET: every member of a set PRIMARY or SECONDARY and within a minute of
# its primary. 100M records push about 110GB through the oplog, and a member
# that fell off the end of it during the load is a degraded set every later run
# would measure without saying so.
health() {
  mongosh "$1" --quiet --eval '
    const s = rs.status(), p = s.members.find(m => m.stateStr === "PRIMARY");
    const bad = s.members.filter(m => !["PRIMARY", "SECONDARY"].includes(m.stateStr) ||
      (p && (p.optimeDate - m.optimeDate) > 60000)).map(m => m.name + " " + m.stateStr);
    print(p && bad.length === 0 ? "ok" : "unhealthy: " + (p ? bad.join(", ") : "no primary"))'
}

if [ "$MODE" = load ]; then
  t "loading $RECORDS records into rsCold"
  mongosh "$COLD" --quiet --eval 'db.getSiblingDB("ycsb").dropDatabase()' >/dev/null
  # OPLOG_MB sizes every cold member's oplog before the load, so a secondary
  # that falls behind while 100M records go in has room to catch up.
  if [ -n "${OPLOG_MB:-}" ]; then
    for m in $(mongosh "$COLD" --quiet --eval 'print(rs.status().members.map(m => m.name).join(" "))'); do
      u="${COLD%%@*}@$m/?directConnection=true&authSource=admin"
      mongosh "$u" --quiet --eval "printjson(db.adminCommand({replSetResizeOplog: 1, size: $OPLOG_MB}).ok)" |
        grep -q 1 || { t "load: could not resize the oplog on $m"; exit 3; }
    done
    t "oplog: ${OPLOG_MB}MB on every cold member"
  fi
  ycsb load "$COLD" -p insertorder=ordered -p batchsize=100 -p operationcount="$RECORDS"
  t "loaded: $(grep -E 'INSERT\], Operations|OVERALL\], RunTime' "$OUT/$LABEL.out" | tr '\n' ' ')"
  # YCSB 0.17's MongoDB binding never sends a thread's last partial batch:
  # 20,000 records on 16 threads stored 19,200, and every read of a missing
  # key is a NOT_FOUND. Each thread's share must be a whole number of batches.
  HAVE=$(mongosh "$COLD" --quiet --eval "print(db.getSiblingDB('ycsb').usertable.estimatedDocumentCount())")
  if [ "$HAVE" != "$RECORDS" ]; then
    t "load: $HAVE records stored of $RECORDS; make RECORDS a multiple of THREADS x 100"
    exit 3
  fi
fi
# MODE=date dates a loaded archive again, or the rest of it: FROM and NOW
# from the first attempt's dating.out resume it where it stopped.
if [ "$MODE" = load ] || [ "$MODE" = date ]; then
  mongosh "$COLD" --quiet --eval "var DB='ycsb', COLL='usertable', YEARS=3, LAST=$((RECORDS - 1)), FROM=${FROM:-0}${NOW:+, NOW='$NOW'}" --file datehistory.js |
    tee -a "$OUT/dating.out" | tail -2
  [ "${PIPESTATUS[0]}" = 0 ] && tail -1 "$OUT/dating.out" | grep -q "index at_1__id_1 built" ||
    { t "dating stopped part way, see dating.out; resume with MODE=date FROM=<first undated key> NOW=<its NOW>"; exit 3; }
  t "dated"
  h=$(health "$COLD")
  [ "$h" = ok ] || { t "load: rsCold is $h after the load"; exit 3; }
  t "load: rsCold healthy"
  exit 0
fi

# What was loaded, so a run with another RECORDS refuses rather than reset by
# the wrong key: the reset deletes from user<RECORDS> up, and a smaller number
# would delete loaded history that only a reload brings back. Two point reads
# tell, with nothing stored beside the data (a collection of the rig's own in
# the archive is one the router warns nothing tiers): the last loaded key is
# there, and the next one is either absent or a run's insert, which carries no
# date (datehistory.js dates only the load).
LOADED=$(mongosh "$COLD" --quiet --eval "
  const c = db.getSiblingDB('ycsb').usertable, k = n => 'user' + String(n).padStart(12, '0');
  const last = c.findOne({_id: k($RECORDS - 1)}, {at: 1}), next = c.findOne({_id: k($RECORDS)}, {at: 1});
  print(last && last.at !== undefined && (!next || next.at === undefined) ? $RECORDS : 'not $RECORDS');")
if [ "$LOADED" != "$RECORDS" ]; then
  t "$LABEL: the archive does not hold a load of RECORDS=$RECORDS (user0..user$((RECORDS - 1)), dated); not running"
  exit 3
fi

# checked LABEL: what a run's own output says went wrong, printed, and a
# failure when anything did. Throughput alone hid a run of failed operations:
# a router that was not up yet answers errors fast.
#
# YCSB 0.17 reads operationcount=0 as zero threads, not as no limit: a run
# then ended in 0ms with a throughput of NaN and nothing marked failed. So a
# run that did nothing, or stopped well short of SECS, fails here too.
checked() {
  local out="$OUT/$1.out" bad=0 n
  n=$(grep -E '^\[OVERALL\], RunTime' "$out" | awk -F', ' '{print int($NF)}')
  if [ -z "$n" ] || [ "$n" -lt $((SECS * 900)) ]; then
    t "$1: ran ${n:-no}ms of ${SECS}s"; bad=1
  fi
  grep -qE '^\[OVERALL\], Throughput\(ops/sec\), [0-9]' "$out" || { t "$1: no throughput"; bad=1; }
  for k in READ-FAILED UPDATE-FAILED INSERT-FAILED; do
    n=$(grep -E "^\[$k\], Operations" "$out" | awk -F', ' '{print $NF}')
    [ -n "$n" ] && [ "$n" != "0" ] && { t "$1: $k $n"; bad=1; }
  done
  for k in ERROR NOT_FOUND; do
    n=$(grep -E "Return=$k" "$out" | awk -F', ' '{s+=$NF} END{print s+0}')
    [ "$n" != "0" ] && { t "$1: Return=$k $n"; bad=1; }
  done
  [ "$bad" = 0 ] || { t "$1: FAILED, the numbers above are not a measurement"; return 4; }
}

X=${CLINE:-/usr/local/bin/cline}
stopAll() {
  for p in $(pgrep -f "^$X (router|feeder|evictor|warmer) --config $CFG"); do kill "$p"; done
  for _ in $(seq 50); do pgrep -f "^$X (router|feeder|evictor|warmer) --config $CFG" >/dev/null || return 0; sleep 0.2; done
  pkill -9 -f "^$X (router|feeder|evictor|warmer) --config $CFG"
}
stopAll
# A run ended by ssh dropping or ^C stops its daemons too: started with setsid
# they outlived the script and kept running against the next run's data.
trap stopAll EXIT

for set in "$COLD" "$HOT"; do
  h=$(health "$set")
  [ "$h" = ok ] || { t "$LABEL: a replica set is $h, not running"; exit 3; }
done

# Reset: what earlier runs inserted, Thermocline's state, the fast tier.
PAD=$(printf "user%012d" "$RECORDS")
# By _id range only. YCSB deletes nothing, so a run leaves no deletion marks,
# and clearing them with an updateMany read all 100M records before every run:
# the archive started each one warm on the newest keys, which favours alone, and
# vmstat recorded the scan. Looking for a mark costs the same scan, so nothing
# looks; the router's own counters in each run's snapshot say whether it
# deleted anything.
#
# KEEPHOT=1 keeps the fast tier as MODE=warm left it, with Thermocline's state:
# the steady state of a deployment that has run for a while, where the recent
# year is on the fast tier and the archive answers only for older data. Then
# only the earlier runs' inserts go, from both tiers.
if [ "${KEEPHOT:-0}" = 1 ] && [ "$MODE" != warm ]; then
  for set in "$COLD" "$HOT"; do
    mongosh "$set" --quiet --eval "
      print('removed ' + db.getSiblingDB('ycsb').usertable.deleteMany({_id: {\$gte: '$PAD'}}).deletedCount + ' records inserted by earlier runs')" |
      tee -a "$OUT/progress.out"
    [ "${PIPESTATUS[0]}" = 0 ] || { t "$LABEL: the reset failed, not running"; exit 3; }
  done
  HOTN=$(mongosh "$HOT" --quiet --eval "print(db.getSiblingDB('ycsb').usertable.estimatedDocumentCount())")
  [ "${HOTN:-0}" -gt 0 ] || { t "$LABEL: KEEPHOT=1 but the fast tier holds nothing; run MODE=warm first"; exit 3; }
  t "$LABEL: the fast tier holds $HOTN records"
else
  mongosh "$COLD" --quiet --eval "
    const c = db.getSiblingDB('ycsb').usertable;
    print('removed ' + c.deleteMany({_id: {\$gte: '$PAD'}}).deletedCount + ' records inserted by earlier runs');
    db.getSiblingDB('_thermocline_ycsb').dropDatabase();" | tee -a "$OUT/progress.out"
  [ "${PIPESTATUS[0]}" = 0 ] || { t "$LABEL: the reset failed, not running"; exit 3; }
  mongosh "$HOT" --quiet --eval 'for (const n of ["ycsb","_thermocline_ycsb"]) db.getSiblingDB(n).dropDatabase()' >/dev/null ||
    { t "$LABEL: could not empty the fast tier, not running"; exit 3; }
fi
# Let the archive's cache and disk settle after the deletes before measuring.
sleep "${SETTLE:-30}"

# MODE=direct points YCSB at the fast tier itself, with nothing of Thermocline
# running: what the fast tier's machines do for this workload on their own. The
# two tiers are different machines, so a tc run limited by the fast tier is read
# against this, not against alone. Its reads of records the fast tier does not
# hold come back empty, and its writes are not mirrored; the next run's reset
# removes its inserts.
if [ "$MODE" = direct ]; then
  [ "${KEEPHOT:-0}" = 1 ] || { t "$LABEL: MODE=direct needs KEEPHOT=1, or there is nothing on the fast tier"; exit 3; }
  t "$LABEL: direct to the fast tier, $DIST, ${SECS}s at $RATE/s, $THREADS threads"
  ycsb run "$HOT" -p operationcount=2000000000 -p maxexecutiontime="$SECS" || { t "$LABEL: YCSB exited $?"; exit 4; }
  t "$LABEL: $(grep -E 'OVERALL\], Throughput' "$OUT/$LABEL.out")"
  exit 0
fi
if [ "$MODE" = alone ]; then
  t "$LABEL: alone, $DIST, ${SECS}s at $RATE/s, $THREADS threads"
  ycsb run "$COLD" -p operationcount=2000000000 -p maxexecutiontime="$SECS" || { t "$LABEL: YCSB exited $?"; exit 4; }
  t "$LABEL: $(grep -E 'OVERALL\], Throughput' "$OUT/$LABEL.out")"
  checked "$LABEL" || exit 4
  exit 0
fi

# The daemons' config: cline.yaml's URIs, this collection, its age rule, and a
# router on loopback where YCSB runs. The live cache runs read-your-writes,
# max_staleness 0, because YCSB cannot read a staleness contract: with latest it
# reads records inserted milliseconds ago (rig.sh has the measurement).
# LIVE=0 is rig.sh's spelling; the config wants a YAML bool, and "0" there
# stopped the router at load with an unmarshal error.
case "${LIVE:-true}" in 0|false|no) LIVEB=false;; *) LIVEB=true;; esac
cat > "$CFG" <<EOF
hot:
  uri: "$HOT"
cold:
  uri: "$COLD"
log:
  file: ""
meta:
  db: "_thermocline_ycsb"
feeder:
  id: "ycsb"
  namespaces:
    - "ycsb.usertable"
  checkpoint_every: 5s
  # FEEDERSTREAM=secondary reads the change stream off the fast tier's
  # primary. See feeder.stream_from.
  stream_from: ${FEEDERSTREAM:-primary}
evictor:
  rate: 5000
  batch_size: 500
  max_feeder_lag: 30s
  # Off: a fast tier built the same day has an oplog window under the 1d
  # default, so the evictor paused for the first day and swept from then on,
  # and runs a day apart would have measured two different systems.
  backoff:
    min_oplog_window: 0s
  rules:
    - ns: "ycsb.usertable"
      older_than: $AGE
      date_field: "at"
warmer:
  rate: 100000
  batch_size: 1000
  min_free_disk: 0.1
  # Off for the same reason as the evictor's: the warmer paused on a fast
  # tier whose oplog had not yet lived a day.
  backoff:
    min_oplog_window: 0s
  rules:
    - ns: "ycsb.usertable"
      back: $AGE
      date_field: "at"
router:
  listen: "127.0.0.1:27801"
  dashboard: "127.0.0.1:27811"
  # Go's profiler, loopback only: a run's CPU and heap can be read while it
  # runs (curl 127.0.0.1:6060/debug/pprof/heap).
  pprof: "127.0.0.1:6060"
  completeness: archive
  freshness: strict
  copy_up_reads: ${COPYUP:-repeated}
  live_cache:
    enabled: $LIVEB
    max_staleness: 0s
    stream_from: secondary
EOF

T0=$(date -u +%Y-%m-%dT%H:%M:%SZ)
setsid nohup $X feeder --config $CFG --log-file "$OUT/$LABEL.feeder.log" >/dev/null 2>&1 < /dev/null &
sleep 3
setsid nohup $X router --config $CFG --log-file "$OUT/$LABEL.router.log" >/dev/null 2>&1 < /dev/null &
up=0
for _ in $(seq 1 60); do mongosh "$RT" --quiet --eval "db.adminCommand({ping:1}).ok" 2>/dev/null | grep -q 1 && { up=1; break; }; sleep 1; done
[ "$up" = 1 ] || { t "$LABEL: the router did not answer in 60s, see $LABEL.router.log"; exit 3; }
$X watermark --config $CFG --ns ycsb.usertable --complete-from "$T0" > "$OUT/$LABEL.watermark.out" 2>&1 ||
  { t "$LABEL: cline watermark failed, see $LABEL.watermark.out"; exit 3; }
# MODE=warm fills the fast tier with the recent AGE of history, one pass of the
# warmer, and leaves it for the KEEPHOT=1 runs.
if [ "$MODE" = warm ]; then
  t "$LABEL: warming the fast tier with the last $AGE"
  W0=$(date +%s)
  $X warmer --config $CFG --log-file "$OUT/$LABEL.warmer.log" > "$OUT/$LABEL.warmer.out" 2>&1 ||
    { t "$LABEL: the warmer failed, see $LABEL.warmer.log"; exit 3; }
  t "$LABEL: warmed in $(( $(date +%s) - W0 ))s, the fast tier holds $(mongosh "$HOT" --quiet --eval "print(db.getSiblingDB('ycsb').usertable.estimatedDocumentCount())") records"
  exit 0
fi
setsid nohup $X evictor --config $CFG --log-file "$OUT/$LABEL.evictor.log" >/dev/null 2>&1 < /dev/null &
# WARMNOW=1 runs the warmer beside the application from the first second of an
# adoption: it reads the archive newest first, which is what latest reads most,
# so the fast tier holds the hot records within minutes instead of waiting for
# traffic to miss on each one.
if [ "${WARMNOW:-0}" = 1 ]; then
  setsid nohup $X warmer --config $CFG --log-file "$OUT/$LABEL.warmer.log" > "$OUT/$LABEL.warmer.out" 2>&1 < /dev/null &
fi
# A fast tier kept from earlier runs had their inserts deleted by the reset
# above, and every one of those deletes is the feeder's to mirror: after a
# 45-minute adoption at 10,000 ops/s that is 1.35 million, and a run started
# on top of it measured the feeder working through that backlog, 63s behind,
# with 832 reads and updates refused. A deployment in its steady state has a
# feeder that is current, so the run waits for it, and says how long it took.
if [ "${KEEPHOT:-0}" = 1 ]; then
  D0=$(date +%s); calm=0
  for _ in $(seq 1 360); do
    # Both: the lag of the last event mirrored, and how far the feeder's
    # position is behind the fast tier's cluster time. A feeder working
    # through a backlog of old writes reads 0.4s on the first and 1,198s on
    # the second, and a run started then had 2,304 reads refused.
    m=$(curl -s http://127.0.0.1:27811/metrics)
    lag=$(echo "$m" | awk '/^thermocline_mirroring_lag_seconds /{print $2}')
    behind=$(echo "$m" | awk '/^thermocline_mirroring_cluster_time_behind_seconds /{print $2}')
    if [ -n "$lag" ] && [ -n "$behind" ] && awk -v l="$lag" -v b="$behind" 'BEGIN{exit !(l < 1 && b < 2)}'; then calm=$((calm + 1)); else calm=0; fi
    [ $calm -ge 3 ] && break
    sleep 5
  done
  [ $calm -ge 3 ] || { t "$LABEL: the feeder did not catch up in 30 minutes, see $LABEL.feeder.log"; exit 3; }
  t "$LABEL: the feeder caught up with the reset in $(( $(date +%s) - D0 ))s"
fi
t "$LABEL: thermocline adopted at $T0, $DIST, ${SECS}s at $RATE/s, $THREADS threads"
for d in feeder evictor; do
  pgrep -f "^$X $d --config $CFG" >/dev/null || { t "$LABEL: the $d is not running, see $LABEL.$d.log"; exit 3; }
done
# Mirroring's lag every 10s through the run: the feeder is the ceiling on the
# write rate the fast tier can take before archive reads wait, so a tc result
# is read with this beside it. Lag and cluster-time lag as the router sees them.
# With the router's counters beside it, which make the adoption curve: how many
# reads still went to the archive and how many records traffic copied up, every
# 10s from an empty fast tier until it holds what the workload uses.
( while :; do
    echo "$(date -u +%T) $(curl -s http://127.0.0.1:27811/metrics |
      grep -E '^thermocline_mirroring_(lag|cluster_time_behind)_seconds ' | awk '{printf "%s=%s ", $1, $2}')$(
      curl -s http://127.0.0.1:27811/api/snapshot | python3 -c '
import json, sys
c = json.load(sys.stdin).get("counters", {})
print(" ".join(f"{k}={c.get(k)}" for k in ("commands", "from_archive", "copied_up", "read_misses", "refused")))' 2>/dev/null)"
    sleep 10
  done ) > "$OUT/$LABEL.lag" 2>&1 &
LAGP=$!
ycsb run "$RT" -p operationcount=2000000000 -p maxexecutiontime="$SECS"
rc=$?
kill $LAGP 2>/dev/null; wait $LAGP 2>/dev/null
t "$LABEL: mirroring lag max $(awk '{for (i=2;i<=NF;i++) if ($i ~ /^thermocline_mirroring_lag_seconds=/) {split($i,a,"="); if (a[2]+0>m) m=a[2]+0}} END{printf "%.1fs", m}' "$OUT/$LABEL.lag")"
curl -s http://127.0.0.1:27811/api/snapshot > "$OUT/$LABEL.snapshot.json"
t "$LABEL: $(grep -E 'OVERALL\], Throughput' "$OUT/$LABEL.out")"
# What the router did for those numbers: a refusal is a YCSB ERROR as well, and
# copy-ups and archive reads say how much of the run the archive carried.
t "$LABEL: router $(python3 -c '
import json, sys
c = json.load(open(sys.argv[1])).get("counters", {})
print(" ".join(f"{k}={c.get(k)}" for k in ("from_archive", "copied_up", "refused", "stale_reads")))' "$OUT/$LABEL.snapshot.json" 2>&1)"
[ "$rc" = 0 ] || { t "$LABEL: YCSB exited $rc"; exit 4; }
# A daemon that died during the run measured a different system.
for d in feeder router evictor; do
  pgrep -f "^$X $d --config $CFG" >/dev/null || { t "$LABEL: the $d stopped during the run, see $LABEL.$d.log"; exit 4; }
done
checked "$LABEL" || exit 4
stopAll
