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

# And the feeder's published position, which is a different collection and the
# one the router actually gates on.
#
# The wait above reads checkpoints, which says how far the feeder has processed.
# The router's freshness barrier reads positions, which says mirroring is alive.
# Waiting on the first and starting the shops proves nothing about the second,
# and when the position had not landed yet the router logged "mirroring has
# published no position at all", gave up waiting and answered from the archive.
# The shops then reported a cancelled order coming back and counts that did not
# move: this workload's own writes, read before mirroring could account for
# them, arriving as violations of the system.
#
# Measured on 2026-09-24: nine violations in one shop and eight in the other,
# every one of them stamped with the same instant, 21 seconds after the feeder
# started and with no error in its log.
pos=9999
for _ in $(seq 1 120); do
  pos=$(docker exec "$NODE" mongosh "$COLD" --quiet --eval \
    'const p=db.getSiblingDB("_thermocline_two").positions.findOne({_id:"two"});
     print(p && p.savedAt ? Math.round((Date.now()-p.savedAt.getTime())/1000) : 9999)' 2>/dev/null | tail -1)
  [ "$pos" -lt 5 ] 2>/dev/null && break
  sleep 1
done
[ "$pos" -lt 5 ] 2>/dev/null || { echo "  FATAL the feeder has published no position, so every fall-through read starts stale"; exit 2; }
echo "  the feeder published its position ${pos}s ago, which is what the router's barrier reads"
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

# A second application with credentials of its own.
#
# Both shops have always connected as dba, a superuser on both tiers, so this
# workload had never once run as the kind of account an application actually
# gets. What that leaves untested is the decision the router makes on every
# read: a fall-through is served on the router's OWN archive connection, not the
# client's, so "the archive answered it" must never quietly become "the archive
# answered it for a client that could not have asked".
#
# The account is made on the fast tier because that is where the router sends
# the authentication conversation with terminate_auth off. With it on the router
# verifies against the same stored keys and loads this user's privileges to
# check each command against, so TERMINATE_AUTH=1 runs the whole workload
# through the router's own authorisation as a non-superuser, which nothing else
# here does.
SHOPB_USER=${SHOPB_USER:-shopb_app}
SHOPB_PW=${SHOPB_PW:-shopbpw}
docker exec "$NODE" mongosh "$HOT" --quiet --eval "
  const a = db.getSiblingDB('admin');
  try { a.dropUser('$SHOPB_USER') } catch (e) {}
  a.createUser({user:'$SHOPB_USER', pwd:'$SHOPB_PW',
                roles:[{role:'readWrite', db:'shopB'}]})" >/dev/null 2>&1
SHOPB_URI="mongodb://$SHOPB_USER:$SHOPB_PW@127.0.0.1:27600/?authSource=admin"

# Credentials that stop nothing are not credentials, and a run where the second
# application silently kept superuser rights would pass every check below while
# testing nothing new. Asked before the shops start, so it fails in a second
# rather than after the run.
AUTHFAIL=0
ownread=$(docker exec "$NODE" mongosh "$SHOPB_URI" --quiet --eval "
  try { db.getSiblingDB('shopB').orders.findOne({}); print('allowed') }
  catch (e) { print('refused:' + e.codeName) }" 2>&1 | tail -1)
crossread=$(docker exec "$NODE" mongosh "$SHOPB_URI" --quiet --eval "
  try { db.getSiblingDB('shopA').orders.findOne({}); print('allowed') }
  catch (e) { print(e.codeName == 'Unauthorized' ? 'refused' : 'other:' + e.codeName) }" 2>&1 | tail -1)
echo "  shopB's own account on shopB: $ownread, on shopA: $crossread"
if [ "$ownread" != "allowed" ]; then
  echo "  FATAL shopB's account cannot read its own database, so the run below would prove nothing"
  AUTHFAIL=1
fi
if [ "$crossread" != "refused" ]; then
  echo "  FAIL shopB's account could read shopA through the router"
  AUTHFAIL=1
fi

echo "== both shops open at once =="
# The store exits non-zero when it notices something wrong, so the exit codes
# are the result and the output is the explanation.
pids=()
for shop in shopA shopB; do
  # shopA stays dba and shopB runs as itself, so one run covers both the
  # superuser path everything here has always used and the least-privilege one
  # an application is actually given.
  uri=$ROUTER
  [ "$shop" = shopB ] && uri=$SHOPB_URI
  docker exec "$NODE" bash -c "store -uri '$uri' -db $shop $RUNARG -conc $CONC -skip-seed \
      -snapshot http://127.0.0.1:27610/api/snapshot > /root/twolog/$shop.out 2>&1" &
  pids+=($!)
done
rc=$AUTHFAIL
for p in "${pids[@]}"; do wait "$p" || rc=1; done
N "rm -f /root/two_churn.run"

for shop in shopA shopB; do
  echo "-- $shop --"
  N "grep VIOLATION /root/twolog/$shop.out | head -5"
  N "tail -30 /root/twolog/$shop.out"
done

# A consumer that never resumed is a run that proved nothing about resuming, and
# it looks exactly like a run that resumed and found nothing.
#
# The resumed stream is the only thing here that makes the aggregate's own
# firstBatch carry events: a stream opened on a live collection comes back with
# an empty one, so a router filtering getMore alone passes every test that opens
# a stream and then writes. That batch reached applications unfiltered once.
#
# Only asked of a timed run. A round count can finish inside one resume
# interval, and failing a short run for that would be failing it for its length.
if [ -n "$FOR" ]; then
  for shop in shopA shopB; do
    r=$(N "grep -oE 'consumer: [0-9]+ resume' /root/twolog/$shop.out | grep -oE '[0-9]+' | tail -1")
    if [ "${r:-0}" -gt 0 ] 2>/dev/null; then
      echo "  $shop: the consumer resumed $r time(s) from a stored token"
    else
      echo "  $shop: FAIL the consumer never resumed, so nothing here read a resumed stream's first batch"
      rc=1
    fi
  done
fi

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
