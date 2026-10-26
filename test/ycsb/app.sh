#!/bin/bash
# YCSB on the Hetzner staging app node: the archive alone against the same
# archive adopted by Thermocline. test/ycsb/stage.sh ships this and drives it;
# rig.sh is the same measurement on the Docker box.
#
#   MODE=load RECORDS=100000000 bash app.sh        once: load and date the archive
#   MODE=alone LABEL=alone-1 bash app.sh           a run against rsCold directly
#   MODE=tc    LABEL=tc-1    bash app.sh           a run through a fresh router
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
  java -cp "/root/ycsb/lib/*" site.ycsb.Client "$what" -db site.ycsb.db.MongoDbClient -P workload_recent \
    -p mongodb.url="${uri%%\?*}ycsb?${uri#*\?}" -p recordcount="$RECORDS" \
    -p requestdistribution="$DIST" -threads "$THREADS" "${tgt[@]}" -s "$@" \
    > "$OUT/$LABEL.out" 2> "$OUT/$LABEL.err"
}

if [ "$MODE" = load ]; then
  t "loading $RECORDS records into rsCold"
  mongosh "$COLD" --quiet --eval 'db.getSiblingDB("ycsb").dropDatabase()' >/dev/null
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
  mongosh "$COLD" --quiet --eval 'var DB="ycsb", COLL="usertable", YEARS=3' --file datehistory.js | tail -2
  t "dated"
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

X=/usr/local/bin/cline
stopAll() {
  for p in $(pgrep -f "^$X (router|feeder|evictor) --config $CFG"); do kill "$p"; done
  for _ in $(seq 50); do pgrep -f "^$X (router|feeder|evictor) --config $CFG" >/dev/null || return 0; sleep 0.2; done
  pkill -9 -f "^$X (router|feeder|evictor) --config $CFG"
}
stopAll
# A run ended by ssh dropping or ^C stops its daemons too: started with setsid
# they outlived the script and kept running against the next run's data.
trap stopAll EXIT

# Reset: what earlier runs inserted, Thermocline's state, the fast tier.
PAD=$(printf "user%012d" "$RECORDS")
# By _id range only. YCSB deletes nothing, so a run leaves no deletion marks,
# and clearing them with an updateMany read all 100M records before every run:
# the archive started each one warm on the newest keys, which favours alone, and
# vmstat recorded the scan. Looking for a mark costs the same scan, so nothing
# looks; the router's own counters in each run's snapshot say whether it
# deleted anything.
mongosh "$COLD" --quiet --eval "
  const c = db.getSiblingDB('ycsb').usertable;
  print('removed ' + c.deleteMany({_id: {\$gte: '$PAD'}}).deletedCount + ' records inserted by earlier runs');
  db.getSiblingDB('_thermocline_ycsb').dropDatabase();" | tee -a "$OUT/progress.out"
[ "${PIPESTATUS[0]}" = 0 ] || { t "$LABEL: the reset failed, not running"; exit 3; }
mongosh "$HOT" --quiet --eval 'for (const n of ["ycsb","_thermocline_ycsb"]) db.getSiblingDB(n).dropDatabase()' >/dev/null ||
  { t "$LABEL: could not empty the fast tier, not running"; exit 3; }
# Let the archive's cache and disk settle after the deletes before measuring.
sleep "${SETTLE:-30}"

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
evictor:
  rate: 5000
  batch_size: 500
  max_feeder_lag: 30s
  rules:
    - ns: "ycsb.usertable"
      older_than: $AGE
      date_field: "at"
router:
  listen: "127.0.0.1:27801"
  dashboard: "127.0.0.1:27811"
  completeness: archive
  freshness: strict
  copy_up_reads: repeated
  live_cache:
    enabled: ${LIVE:-true}
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
setsid nohup $X evictor --config $CFG --log-file "$OUT/$LABEL.evictor.log" >/dev/null 2>&1 < /dev/null &
t "$LABEL: thermocline adopted at $T0, $DIST, ${SECS}s at $RATE/s, $THREADS threads"
for d in feeder evictor; do
  pgrep -f "^$X $d --config $CFG" >/dev/null || { t "$LABEL: the $d is not running, see $LABEL.$d.log"; exit 3; }
done
ycsb run "$RT" -p operationcount=2000000000 -p maxexecutiontime="$SECS"
rc=$?
curl -s http://127.0.0.1:27811/api/snapshot > "$OUT/$LABEL.snapshot.json"
t "$LABEL: $(grep -E 'OVERALL\], Throughput' "$OUT/$LABEL.out")"
[ "$rc" = 0 ] || { t "$LABEL: YCSB exited $rc"; exit 4; }
# A daemon that died during the run measured a different system.
for d in feeder router evictor; do
  pgrep -f "^$X $d --config $CFG" >/dev/null || { t "$LABEL: the $d stopped during the run, see $LABEL.$d.log"; exit 4; }
done
checked "$LABEL" || exit 4
stopAll
