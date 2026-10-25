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
  mongosh "$COLD" --quiet --eval 'var DB="ycsb", COLL="usertable", YEARS=3' --file datehistory.js | tail -2
  t "dated"
  exit 0
fi

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
  ycsb run "$COLD" -p operationcount=0 -p maxexecutiontime="$SECS"
  t "$LABEL: $(grep -E 'OVERALL\], Throughput' "$OUT/$LABEL.out")"
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
for _ in $(seq 1 60); do mongosh "$RT" --quiet --eval "db.adminCommand({ping:1}).ok" 2>/dev/null | grep -q 1 && break; sleep 1; done
$X watermark --config $CFG --ns ycsb.usertable --complete-from "$T0" > "$OUT/$LABEL.watermark.out" 2>&1 ||
  { t "$LABEL: cline watermark failed, see $LABEL.watermark.out"; exit 3; }
setsid nohup $X evictor --config $CFG --log-file "$OUT/$LABEL.evictor.log" >/dev/null 2>&1 < /dev/null &
t "$LABEL: thermocline adopted at $T0, $DIST, ${SECS}s at $RATE/s, $THREADS threads"
ycsb run "$RT" -p operationcount=0 -p maxexecutiontime="$SECS"
curl -s http://127.0.0.1:27811/api/snapshot > "$OUT/$LABEL.snapshot.json"
t "$LABEL: $(grep -E 'OVERALL\], Throughput' "$OUT/$LABEL.out")"
stopAll
