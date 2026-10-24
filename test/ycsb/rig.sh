#!/bin/bash
# YCSB on the Docker test box: the cluster alone against the same cluster
# adopted by Thermocline, from the box itself. Not a suite, a measurement.
#
#   MODE=load RECORDS=1000000 bash test/ycsb/rig.sh   once: load and date the archive
#   bash test/ycsb/rig.sh                             reset, alone run, Thermocline run
#
# The records are loaded into rsCold, which plays the existing cluster being
# adopted; rsHot starts empty and traffic fills it, as an adoption does. Every
# run first removes what the previous one inserted and Thermocline's own state,
# so runs start from the same data.
#
# Needs on the box: ~/ycsb (the MongoDB binding), ~/jre, ~/mongodb/bin, and the
# repository at ~/thermocline with bin/cline (test/boxhostsweep.sh ships both).
set -u
cd "$(dirname "$0")/../.."
NODE=${NODE:-thermocline-zelmario-node0}
NODEIP=${NODEIP:-172.18.0.2}
COLD="mongodb://dba:secret@172.18.0.2:27017,172.18.0.3:27017,172.18.0.4:27017/?replicaSet=rsCold&authSource=admin"
HOT="mongodb://dba:secret@172.18.0.5:27017,172.18.0.6:27017,172.18.0.7:27017/?replicaSet=rsHot&authSource=admin"
RT="mongodb://dba:secret@$NODEIP:27801/?authSource=admin"
RECORDS=${RECORDS:-1000000}
OPS=${OPS:-2000000}
THREADS=${THREADS:-32}
RATE=${RATE:-4000}
DIST=${DIST:-latest}
OUT=${OUT:-$HOME/ycsb-out}
W=test/ycsb/workload_recent
export PATH=$HOME/jre/bin:$HOME/mongodb/bin:$PATH
mkdir -p "$OUT"
t() { echo "$(date -u +%T) $*" | tee -a "$OUT/progress.out"; }

ycsb() { # ycsb load|run URI LABEL [-p k=v ...]
  local what=-t uri=$2 label=$3
  [ "$1" = load ] && what=-load
  shift 3
  java -cp "$HOME/ycsb/lib/*" site.ycsb.Client "$what" -db site.ycsb.db.MongoDbClient -P "$W" \
    -p mongodb.url="${uri%%\?*}ycsb?${uri#*\?}" -p recordcount="$RECORDS" -p operationcount="$OPS" \
    -p requestdistribution="$DIST" -threads "$THREADS" -target "$RATE" -s "$@" \
    > "$OUT/$label.out" 2> "$OUT/$label.err"
}

if [ "${MODE:-run}" = load ]; then
  t "loading $RECORDS records into rsCold"
  mongosh "$COLD" --quiet --eval 'db.getSiblingDB("ycsb").dropDatabase()' >/dev/null
  ycsb load "$COLD" load -p insertorder=ordered
  t "loaded: $(grep -E 'INSERT\], Operations' "$OUT/load.out")"
  mongosh "$COLD" --quiet --eval 'var DB="ycsb", COLL="usertable", YEARS=3' --file test/ycsb/datehistory.js | tail -2
  t "dated"
  exit 0
fi

# The node's daemons, with a config for this collection and a router the box
# can reach.
docker exec "$NODE" bash -c "sed -e 's/bigshop.orders/ycsb.usertable/' -e 's/_thermocline_dkb/_thermocline_ycsb/' \
  -e 's/127.0.0.1:27800/$NODEIP:27801/' -e 's/127.0.0.1:27810/127.0.0.1:27811/' -e 's/127.0.0.1:6070/127.0.0.1:6071/' \
  -e 's/id: \"bench\"/id: \"ycsb\"/' /root/dkb/mode-cache.yaml > /root/ycsb.yaml"
# The live cache runs read-your-writes, max_staleness 0, unless STALE says
# otherwise. The shop's 50ms bound is a contract YCSB cannot read: a record
# inserted under 50ms ago may be answered as missing, and with latest those are
# the records it reads most. On 2026-10-24, 200,000 operations at 50ms gave 1,135
# NOT_FOUND, every one within 48ms of the record's insert.
# LIVE=0 runs the router without its live cache, to tell its cost or its
# answers from the rest of the path.
docker exec "$NODE" sed -i "s/max_staleness: .*/max_staleness: ${STALE:-0s}/" /root/ycsb.yaml
# AGE is the evictor's older_than and the warmer's back. The shop's 60d holds
# about 55,000 of a million records dated over three years, and latest reads
# the rest about one time in five: each such record is copied up when read
# twice and evicted again on the next sweep, so the fast tier never holds it.
# 365d holds a third of the history and nine reads in ten.
docker exec "$NODE" sed -i -e "s/older_than: .*/older_than: ${AGE:-365d}/" -e "s/back: .*/back: ${AGE:-365d}/" /root/ycsb.yaml
[ "${LIVE:-1}" = 0 ] && docker exec "$NODE" sed -i '/live_cache:/,/stream_from/s/enabled: true/enabled: false/' /root/ycsb.yaml
X=/usr/local/bin/cline
stopAll() { docker exec "$NODE" bash -c "for p in \$(pgrep -f '^$X (router|feeder|evictor) --config /root/ycsb.yaml'); do kill \$p; done"; }
stopAll; sleep 2

# Reset: what the last run inserted, Thermocline's state, the fast tier.
PAD=$(printf "user%012d" "$RECORDS")
mongosh "$COLD" --quiet --eval "
  const c = db.getSiblingDB('ycsb').usertable;
  print('removed ' + c.deleteMany({_id: {\$gte: '$PAD'}}).deletedCount + ' records inserted by the last run');
  c.updateMany({_tcDeletedAt: {\$exists: true}}, {\$unset: {_tcDeletedAt: 1}});
  db.getSiblingDB('_thermocline_ycsb').dropDatabase();" | tee -a "$OUT/progress.out"
mongosh "$HOT" --quiet --eval 'for (const n of ["ycsb","_thermocline_ycsb"]) db.getSiblingDB(n).dropDatabase()' >/dev/null

t "alone: $DIST, $OPS operations at $RATE/s"
ycsb run "$COLD" "alone-$DIST"
t "alone: $(grep -E 'OVERALL\], Throughput' "$OUT/alone-$DIST.out")"
mongosh "$COLD" --quiet --eval "db.getSiblingDB('ycsb').usertable.deleteMany({_id: {\$gte: '$PAD'}})" >/dev/null

T0=$(date -u +%Y-%m-%dT%H:%M:%SZ)
docker exec -d "$NODE" bash -c "$X feeder --config /root/ycsb.yaml --log-file /root/ycsb-f.log"
sleep 3
docker exec -d "$NODE" bash -c "$X router --config /root/ycsb.yaml --log-file /root/ycsb-r.log"
for i in $(seq 1 30); do mongosh "$RT" --quiet --eval "db.adminCommand({ping:1}).ok" 2>/dev/null | grep -q 1 && break; sleep 1; done
docker exec "$NODE" $X watermark --config /root/ycsb.yaml --ns ycsb.usertable --complete-from "$T0" > "$OUT/watermark.out" 2>&1
docker exec -d "$NODE" bash -c "$X evictor --config /root/ycsb.yaml --log-file /root/ycsb-e.log"
t "adopted at $T0"
ycsb run "$RT" "tc-$DIST"
docker exec "$NODE" curl -s http://127.0.0.1:27811/api/snapshot > "$OUT/tc-$DIST.snapshot.json"
t "thermocline: $(grep -E 'OVERALL\], Throughput' "$OUT/tc-$DIST.out")"
stopAll
for f in alone tc; do
  echo "== $f"
  grep -E '\[(READ|UPDATE|INSERT|SCAN)\], (AverageLatency|95thPercentileLatency|99thPercentileLatency)|\[(READ|UPDATE|INSERT|SCAN)-FAILED\]|Return=(ERROR|NOT_FOUND)' "$OUT/$f-$DIST.out"
done | tee -a "$OUT/progress.out"
t "done"
