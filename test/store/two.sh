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
#
# Item 6 of docs/production-readiness.md asks for this shape running for hours,
# not for minutes, so the length is a duration rather than a round count:
#
#   FOR=4h bash test/store/two.sh
#
# A round count cannot express it. A round costs more as the archive grows and
# as the evictor moves documents under it, so the number that runs for twenty
# minutes at the start of a run runs for an hour at the end of it.
NODE=${NODE:-thermocline-zelmar-node0}
ORDERS=${ORDERS:-8000}
ROUNDS=${ROUNDS:-300}
FOR=${FOR:-}
BEAT=${BEAT:-5m}
CONC=${CONC:-6}
CFG=/root/two.yaml
# terminate_auth moves the decision about who may read what out of mongod and
# into this proxy, and it is off by default precisely because it should not ship
# on until it has carried traffic that was not a fixture. Two shops writing
# continuously is the closest thing here to that traffic:
#
#   TERMINATE_AUTH=1 bash test/store/two.sh
#
# With it on the router owns its upstream connections and pools them, verifies
# each client's password against the keys mongod stores, and loads that user's
# privileges to check every command against. What the shops assert does not
# change, which is the point: the invariants are about orders, not about how the
# connection was authenticated.
if [ -n "${TERMINATE_AUTH:-}" ]; then CFG=/root/two_ta.yaml; fi
ROUTER=${ROUTER:-'mongodb://dba:secret@127.0.0.1:27600/?authSource=admin'}

# Two things need the length: the shops, in Go duration form, and the churn
# loop, in seconds. The churn's own cap is the one that matters if this script
# is interrupted, so it is derived here rather than left to a cleanup that an
# interrupt never reaches.
if [ -n "$FOR" ]; then
  RUNARG="-for $FOR -heartbeat $BEAT"
  CHURN_SECS=$(python3 -c "
import re,sys
d=sys.argv[1]; u={'h':3600,'m':60,'s':1}; t=0
for n,s in re.findall(r'([0-9.]+)([hms])', d): t += float(n)*u[s]
print(int(t)+600 if t else 0)" "$FOR") || CHURN_SECS=0
  [ "$CHURN_SECS" -gt 0 ] 2>/dev/null || { echo "FOR=$FOR is not a duration this understands (4h, 90m, 30s)"; exit 2; }
else
  RUNARG="-rounds $ROUNDS"
  CHURN_SECS=3600
fi

N() { docker exec "$NODE" bash -c "$1"; }
ND() { docker exec -d "$NODE" bash -c "$1"; }

echo "== installing =="
docker cp test/store/two.yaml "$NODE":/root/two.yaml >/dev/null
if [ "$CFG" != /root/two.yaml ]; then
  N "sed 's|^  listen:|  terminate_auth: true\n  listen:|' /root/two.yaml > $CFG"
  echo "  terminate_auth on, from $CFG"
fi
docker cp bin/store "$NODE":/usr/local/bin/store >/dev/null
# Nothing of a previous run: both shops, and this deployment's own bookkeeping.
# Both the daemons, which carry the config path, and the loop that starts them,
# which does not: a previous run's churn shell survived a kill matched on the
# config alone and its evictor then emptied this run's seed out of the fast tier
# while the setup was still checking it.
N "for p in \$(pgrep -f 'config /root/tw[o]'; pgrep -f 'two_[c]hurn'); do kill -9 \$p 2>/dev/null; done" >/dev/null 2>&1
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

# The archive gets the reference index too, which an adopted one would already
# have. Without it the router cannot ask the archive whether another spelling of
# a reference is taken, says so once at WARN, and stops enforcing that
# constraint across the tiers: the reuse check below would then pass because
# nothing was being enforced rather than because the constraint held.
#
# Not unique there. The archive holds tombstones of deleted documents and a
# unique index over them refuses the feeder's next write; it only has to serve
# the lookup.
for shop in shopA shopB; do
  docker exec "$NODE" mongosh "mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin" \
    --quiet --eval "db.getSiblingDB('$shop').orders.createIndex({ref:1},{collation:{locale:'en',strength:2}})" >/dev/null
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
#
# Two stopping conditions, and the run needs both. The sentinel is removed when
# the shops finish, so the churn does not outlive them; the deadline is what
# stops it when this script is interrupted and never gets to remove anything. A
# churn left behind moves documents under whatever runs next, and that has been
# read as a product defect here before.
#
# What it must not do is kill the feeder and the router on the way out. It did,
# by matching every process carrying this config, which was invisible while the
# churn always outlasted the shops and is a run that loses its daemons halfway
# as soon as the shops run for hours.
cat > /tmp/two_churn.sh <<CH
#!/bin/bash
trap 'for p in \$(pgrep -f "cline (warmer|evictor) --config /root/tw[o]"); do kill -9 \$p 2>/dev/null; done' EXIT
deadline=\$(( \$(date +%s) + $CHURN_SECS ))
while [ -f /root/two_churn.run ] && [ \$(date +%s) -lt \$deadline ]; do
  cline warmer --config $CFG >/dev/null 2>&1
  cline evictor --config $CFG --once >/dev/null 2>&1
  sleep 5
done
CH
docker cp /tmp/two_churn.sh "$NODE":/root/two_churn.sh >/dev/null
N "touch /root/two_churn.run"
ND "bash /root/two_churn.sh"

echo "== both shops open at once =="
# The store exits non-zero when it notices something wrong, so the exit codes
# are the result and the output is the explanation.
pids=()
for shop in shopA shopB; do
  docker exec "$NODE" bash -c "store -uri '$ROUTER' -db $shop $RUNARG -conc $CONC -skip-seed \
      -snapshot http://127.0.0.1:27610/api/snapshot > /root/twolog/$shop.out 2>&1" &
  pids+=($!)
done
rc=0
for p in "${pids[@]}"; do wait "$p" || rc=1; done
N "rm -f /root/two_churn.run"

for shop in shopA shopB; do
  echo "-- $shop --"
  N "grep VIOLATION /root/twolog/$shop.out | head -5"
  N "tail -30 /root/twolog/$shop.out"
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

N "rm -f /root/two_churn.run; for p in \$(pgrep -f 'config /root/tw[o]'; pgrep -f 'two_[c]hurn'); do kill -9 \$p 2>/dev/null; done" >/dev/null 2>&1
exit $rc
