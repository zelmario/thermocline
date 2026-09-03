#!/bin/bash
# Two applications writing at once, which is blocker 4 of the plan.
#
# Everything else in this repository writes from one application, or from a BI
# tool that mostly reads. What two writers test is the parts they share: one
# feeder reading one change stream for both, one evictor sweeping both, one
# router holding one claim cache, one key registry.
#
#   bash test/store/two.sh
#
# Each shop checks its own five invariants, and the run fails if either does.
# Between them a warm and an evictor move both shops' documents, so neither is
# reading a tier that stands still.
NODE=${NODE:-thermocline-zelmar-node0}
ORDERS=${ORDERS:-8000}
ROUNDS=${ROUNDS:-300}
CONC=${CONC:-6}
CFG=/root/two.yaml
ROUTER=${ROUTER:-'mongodb://dba:secret@127.0.0.1:27600/?authSource=admin'}

N() { docker exec "$NODE" bash -c "$1"; }
ND() { docker exec -d "$NODE" bash -c "$1"; }

echo "== installing =="
docker cp test/store/two.yaml "$NODE":/root/two.yaml >/dev/null
docker cp bin/store "$NODE":/usr/local/bin/store >/dev/null
# Nothing of a previous run: both shops, and this deployment's own bookkeeping.
# Both the daemons, which carry the config path, and the loop that starts them,
# which does not: a previous run's churn shell survived a kill matched on the
# config alone and its evictor then emptied this run's seed out of the fast tier
# while the setup was still checking it.
N "for p in \$(pgrep -f 'config /root/two[.]yaml'; pgrep -f 'two_[c]hurn'); do kill -9 \$p 2>/dev/null; done" >/dev/null 2>&1
sleep 1
docker exec "$NODE" mongosh "mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin" \
  --quiet --eval 'db.getSiblingDB("shopA").dropDatabase(); db.getSiblingDB("shopB").dropDatabase(); db.getSiblingDB("_thermocline_two").dropDatabase()' >/dev/null
docker exec "$NODE" mongosh "mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin" \
  --quiet --eval 'db.getSiblingDB("shopA").dropDatabase(); db.getSiblingDB("shopB").dropDatabase(); db.getSiblingDB("_thermocline_two").dropDatabase()' >/dev/null

echo "== the feeder, before anything is seeded =="
# Before, and this is the whole trick of setting this up. A change stream starts
# at now, so a feeder started after the seeding never sees it: measured, the
# fast tier held 8,121 orders and the archive 397, and both shops then reported
# a month's total changing under them, which is a short answer from an archive
# that was never given the history. The router says so at startup, in the words
# "the fast tier holds documents the archive has none of, and mirroring will not
# fetch them".
N "mkdir -p /root/twolog"
ND "cline feeder --config $CFG --log-file /root/twolog/feeder.log"
sleep 4

echo "== seeding both shops, straight at the fast tier =="
# Straight at the fast tier rather than through the router: seeding through it
# would measure the router rather than set up the test, and the feeder carries
# what lands there either way.
HOT='mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'
for shop in shopA shopB; do
  N "store -uri '$HOT' -db $shop -orders $ORDERS -days 120 -customers 800 -products 300 -seed-only" | tail -2
done

echo "== waiting for mirroring to catch up on both seeds =="
# The feeder's own position, not a count. Counts move for reasons other than
# mirroring: an evictor sweep takes documents off the fast tier while this
# watches, and comparing the two tiers then never agrees. The checkpoint says
# how far behind the feeder is, which is the question being asked.
COLD='mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'
lag=9999
for _ in $(seq 1 300); do
  lag=$(docker exec "$NODE" mongosh "$COLD" --quiet --eval \
    'const c=db.getSiblingDB("_thermocline_two").checkpoints.findOne({_id:"two"}); print(c?Math.round((Date.now()-c.eventAt.getTime())/1000):9999)' 2>/dev/null | tail -1)
  [ "$lag" -lt 5 ] 2>/dev/null && break
  sleep 2
done
[ "$lag" -lt 5 ] 2>/dev/null || { echo "  FATAL mirroring is ${lag}s behind, so the archive cannot answer for the past"; exit 2; }
echo "  mirroring is ${lag}s behind, which is the bar this waited for"
# Printed as information, and the two numbers are allowed to differ. The bar
# above is the feeder's own position, so up to that many seconds of the seed can
# still be in flight when these are read. Sitting under a line that says
# "waiting for mirroring to catch up", a bare pair of counts reads as a
# discrepancy report, which is a double-take every time somebody reads this log.
for shop in shopA shopB; do
  echo "  $shop at this instant: fast tier $(docker exec "$NODE" mongosh "$HOT" --quiet --eval "print(db.getSiblingDB('$shop').orders.countDocuments({}))" 2>/dev/null | tail -1), archive $(docker exec "$NODE" mongosh "$COLD" --quiet --eval "print(db.getSiblingDB('$shop').orders.countDocuments({}))" 2>/dev/null | tail -1)"
done

echo "== the router =="
ND "cline router --config $CFG --log-file /root/twolog/router.log"
for _ in $(seq 1 30); do
  docker exec "$NODE" mongosh "$ROUTER" --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
docker exec "$NODE" mongosh "$ROUTER" --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 || {
  echo "  FATAL the router never came up"; N "tail -5 /root/twolog/router.log"; exit 2; }

# A warm and an evictor looping underneath both shops for as long as the run
# lasts, so neither application is reading a tier that stands still.
cat > /tmp/two_churn.sh <<'CH'
#!/bin/bash
trap 'for p in $(pgrep -f "config /root/two.yaml" | grep -v $$); do kill -9 $p 2>/dev/null; done' EXIT
for i in $(seq 1 40); do
  cline warmer --config /root/two.yaml >/dev/null 2>&1
  cline evictor --config /root/two.yaml --once >/dev/null 2>&1
  sleep 5
done
CH
docker cp /tmp/two_churn.sh "$NODE":/root/two_churn.sh >/dev/null
ND "bash /root/two_churn.sh"

echo "== both shops open at once =="
# The store exits non-zero when it notices something wrong, so the exit codes
# are the result and the output is the explanation.
pids=()
for shop in shopA shopB; do
  docker exec "$NODE" bash -c "store -uri '$ROUTER' -db $shop -rounds $ROUNDS -conc $CONC -skip-seed \
      -snapshot http://127.0.0.1:27610/api/snapshot > /root/twolog/$shop.out 2>&1" &
  pids+=($!)
done
rc=0
for p in "${pids[@]}"; do wait "$p" || rc=1; done

for shop in shopA shopB; do
  echo "-- $shop --"
  N "tail -12 /root/twolog/$shop.out"
done

echo "== what the router did while both were writing =="
N "grep -E 'level=ERROR|panic' /root/twolog/router.log | head -3"
N "curl -s http://127.0.0.1:27610/api/snapshot" 2>/dev/null | python3 -c "
import json,sys
try: s=json.load(sys.stdin)
except Exception: print('  no snapshot'); raise SystemExit
c=s.get('counters',{})
for k in ('commands','read_misses','write_misses','copied_up','from_archive','incomplete','refused','stale_reads','duplicate_keys','panics'):
    print('  %-16s %s' % (k, c.get(k)))
" 2>/dev/null

N "for p in \$(pgrep -f 'config /root/two[.]yaml'; pgrep -f 'two_[c]hurn'); do kill -9 \$p 2>/dev/null; done" >/dev/null 2>&1
exit $rc
