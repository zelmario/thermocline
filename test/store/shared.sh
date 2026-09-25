#!/bin/bash
# Two applications writing the same collections, each through a router of its
# own. This is what a deployment with more than one app server looks like, and
# nothing in this repository ran it before.
#
#   bash test/store/shared.sh
#   FOR=1h bash test/store/shared.sh
#
# two.sh gives each application its own database. What it cannot reach is
# everything a router holds for itself and another router cannot see: its
# claim cache, its listing of unique indexes, its cursors, and the copy-ups it
# has in flight. Here the second application's writes to an order the first
# one is reading arrive through a router that never saw them, and an archived
# order can be brought up by both routers at once.
#
# Customers are partitioned across both applications as they are across the
# workers of one, so each shop's count checks stay sound. Each shop also writes
# down what it is about to do in shopC.store_ledger, so its stream consumer can
# tell the other application's orders from phantoms.
NODE=${NODE:-thermocline-zelmar-node0}
ORDERS=${ORDERS:-8000}
ROUNDS=${ROUNDS:-300}
FOR=${FOR:-}
BEAT=${BEAT:-5m}
CONC=${CONC:-6}
# The consumer falls further behind than the oplog this often and recovers the
# way an application does: told history is lost, it reads every order through
# the router and tails from where it noted. Judged at the end against the tiers
# themselves. RESYNC_EVERY=0 turns it off.
RESYNC_EVERY=${RESYNC_EVERY:-45s}
CFG=/root/shared.yaml
CFG2=/root/shared2.yaml
HOT='mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'
COLD='mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'
R1='mongodb://dba:secret@127.0.0.1:27700/?authSource=admin'
APPB_USER=shopc_app
APPB_PW=shopcpw
R2="mongodb://$APPB_USER:$APPB_PW@127.0.0.1:27720/?authSource=admin"
RESYNC=""
if [ "$RESYNC_EVERY" != 0 ]; then
  RESYNC="-resync-every $RESYNC_EVERY -truth-hot 'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin' -truth-cold 'mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'"
fi

if [ -n "$FOR" ]; then
  RUNARG="-for $FOR -heartbeat $BEAT"
  CHURN_SECS=$(python3 -c "
import re,sys
t=sum(float(n)*{'h':3600,'m':60,'s':1}[u] for n,u in re.findall(r'([0-9.]+)([hms])', sys.argv[1]))
print(int(t)+600 if t else 0)" "$FOR") || CHURN_SECS=0
  [ "$CHURN_SECS" -gt 0 ] 2>/dev/null || { echo "FOR=$FOR is not a duration this understands (4h, 90m, 30s)"; exit 2; }
else
  RUNARG="-rounds $ROUNDS"
  CHURN_SECS=3600
fi

N() { docker exec "$NODE" bash -c "$1"; }
ND() { docker exec -d "$NODE" bash -c "$1"; }
Q() { docker exec "$NODE" mongosh "$1" --quiet --eval "$2" 2>/dev/null | tail -1; }

echo "== installing =="
docker cp test/store/shared.yaml "$NODE":$CFG >/dev/null
# The second router is the first with its ports moved, derived rather than kept
# as a second file for the reason two.sh gives: a copy drifts, and the drift is
# silent in exactly the run that needs it.
N "sed -e 's|0.0.0.0:27700|0.0.0.0:27720|' -e 's|0.0.0.0:27710|0.0.0.0:27730|' $CFG > $CFG2"
N "grep -q 27720 $CFG2 && grep -q 27730 $CFG2" || { echo "FATAL could not derive the second router's config"; exit 2; }
if [ -n "${TERMINATE_AUTH:-}" ]; then
  for c in $CFG $CFG2; do
    N "sed -i 's|^  completeness:|  terminate_auth: true\n  completeness:|' $c"
    N "grep -q 'terminate_auth: true' $c" || { echo "FATAL could not turn terminate_auth on in $c"; exit 2; }
  done
  echo "  terminate_auth is on in both routers"
fi
docker cp bin/store "$NODE":/usr/local/bin/store >/dev/null
N "for p in \$(pgrep -f 'config /root/share[d]'; pgrep -f 'shared_[c]hurn'); do kill -9 \$p 2>/dev/null; done" >/dev/null 2>&1
sleep 1
for u in "$HOT" "$COLD"; do
  Q "$u" 'db.getSiblingDB("shopC").dropDatabase(); db.getSiblingDB("_thermocline_shared").dropDatabase()' >/dev/null
done

echo "== the feeder, before anything is seeded =="
N "mkdir -p /root/sharedlog; rm -f /root/sharedlog/*"
ND "cline feeder --config $CFG --log-file /root/sharedlog/feeder.log"
sleep 4

echo "== seeding the one shop both applications share =="
N "store -uri '$HOT' -db shopC -orders $ORDERS -days 120 -customers 800 -products 300 -seed-only" | tail -2
# The archive's reference index, as two.sh explains: without it the router
# stops enforcing the constraint across tiers and the reuse check passes for
# the wrong reason.
Q "$COLD" "db.getSiblingDB('shopC').orders.createIndex({ref:1},{collation:{locale:'en',strength:2}})" >/dev/null

echo "== waiting on what the routers gate on =="
# Both, for the reasons two.sh gives: checkpoints for how far mirroring has
# got, positions for what the freshness barrier actually reads.
lag=9999
for _ in $(seq 1 300); do
  lag=$(Q "$COLD" 'const c=db.getSiblingDB("_thermocline_shared").checkpoints.findOne({_id:"shared"}); print(c?Math.round((Date.now()-c.eventAt.getTime())/1000):9999)')
  [ "$lag" -lt 5 ] 2>/dev/null && break
  sleep 2
done
[ "$lag" -lt 5 ] 2>/dev/null || { echo "  FATAL mirroring is ${lag}s behind"; exit 2; }
pos=9999
for _ in $(seq 1 120); do
  pos=$(Q "$COLD" 'const p=db.getSiblingDB("_thermocline_shared").positions.findOne({_id:"shared"}); print(p && p.savedAt ? Math.round((Date.now()-p.savedAt.getTime())/1000) : 9999)')
  [ "$pos" -lt 5 ] 2>/dev/null && break
  sleep 1
done
[ "$pos" -lt 5 ] 2>/dev/null || { echo "  FATAL the feeder has published no position"; exit 2; }
echo "  mirroring ${lag}s behind, position published ${pos}s ago"

echo "== two routers =="
ND "cline router --config $CFG --log-file /root/sharedlog/router1.log"
ND "cline router --config $CFG2 --log-file /root/sharedlog/router2.log"
Q "$HOT" "
  const a = db.getSiblingDB('admin');
  try { a.dropUser('$APPB_USER') } catch (e) {}
  a.createUser({user:'$APPB_USER', pwd:'$APPB_PW', roles:[{role:'readWrite', db:'shopC'}]})" >/dev/null
for r in "$R1" "$R2"; do
  for _ in $(seq 1 30); do
    [ "$(Q "$r" 'print(db.adminCommand({ping:1}).ok)')" = 1 ] && break
    sleep 1
  done
  [ "$(Q "$r" 'print(db.adminCommand({ping:1}).ok)')" = 1 ] || {
    echo "  FATAL a router never came up: $r"; N "tail -5 /root/sharedlog/router*.log"; exit 2; }
done
echo "  both routers answer"

cat > /tmp/shared_churn.sh <<CH
#!/bin/bash
trap 'for p in \$(pgrep -f "cline (warmer|evictor) --config /root/share[d]"); do kill -9 \$p 2>/dev/null; done' EXIT
deadline=\$(( \$(date +%s) + $CHURN_SECS ))
while [ -f /root/shared_churn.run ] && [ \$(date +%s) -lt \$deadline ]; do
  cline warmer --config $CFG >/dev/null 2>&1
  cline evictor --config $CFG --once >/dev/null 2>&1
  sleep 5
done
CH
docker cp /tmp/shared_churn.sh "$NODE":/root/shared_churn.sh >/dev/null
N "touch /root/shared_churn.run"
ND "bash /root/shared_churn.sh"

echo "== two applications, one shop, two routers =="
# One instant for both, so the history pageHistory scrolls is the same stable
# slice for each of them.
BEGAN=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)
pids=()
i=0
for uri in "$R1" "$R2"; do
  snap=http://127.0.0.1:27710/api/snapshot
  [ $i = 1 ] && snap=http://127.0.0.1:27730/api/snapshot
  docker exec "$NODE" bash -c "store -uri '$uri' -db shopC $RUNARG -conc $CONC -skip-seed $RESYNC \
      -part $i -parts 2 -began $BEGAN -seed $((1 + 1000*i)) \
      -snapshot $snap > /root/sharedlog/app$i.out 2>&1" &
  pids+=($!)
  i=$((i+1))
done
rc=0
for p in "${pids[@]}"; do wait "$p" || rc=1; done
N "rm -f /root/shared_churn.run"

for i in 0 1; do
  echo "-- application $i --"
  N "grep VIOLATION /root/sharedlog/app$i.out | head -8; grep ^index: /root/sharedlog/app$i.out"
  N "tail -32 /root/sharedlog/app$i.out"
done

# Sharing that never happened proves nothing about sharing. Each consumer has
# to have seen the other application's orders, and each application's writes
# have to have gone through its own router.
for i in 0 1; do
  f=$(N "grep -oE 'the stream carried [0-9]+ insert' /root/sharedlog/app$i.out | grep -oE '[0-9]+'")
  if [ "${f:-0}" -gt 0 ] 2>/dev/null; then
    echo "  application $i's consumer saw $f of the other application's orders"
  else
    echo "  FAIL application $i's consumer saw none of the other application's orders, so nothing was shared"
    rc=1
  fi
done

echo "== what the routers did =="
for i in 1 2; do
  echo "-- router $i --"
  N "grep -E 'level=ERROR|panic' /root/sharedlog/router$i.log | head -3"
  port=27710; [ $i = 2 ] && port=27730
  N "curl -s http://127.0.0.1:$port/api/snapshot" 2>/dev/null | python3 -c "
import json,sys
try: s=json.load(sys.stdin)
except Exception: print('  no snapshot'); raise SystemExit
c=s.get('counters',{})
for k in ('commands','read_misses','write_misses','copied_up','from_archive','incomplete','refused','stale_reads','duplicate_keys','panics'):
    print('  %-16s %s' % (k, c.get(k)))
" 2>/dev/null
done

N "rm -f /root/shared_churn.run; for p in \$(pgrep -f 'config /root/share[d]'; pgrep -f 'shared_[c]hurn'); do kill -9 \$p 2>/dev/null; done" >/dev/null 2>&1
exit $rc
