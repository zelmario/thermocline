#!/bin/bash
# A write issued while the fast tier is changing primary.
#
# This runs on the HOST, because it has to move the primary of a replica set
# whose members are other containers.
#
#   bash test/testfailover.sh
#
# The limitation being measured is the third of the three that shared one root
# cause. While authentication was proxied, the router's one upstream connection
# was bound to the client's SCRAM exchange, so when the primary went away there
# was nothing to replay the write onto and the client was told the write failed.
# testchaos.sh pins that behaviour and its honesty: nothing is lost and nothing
# is applied twice, but the client has to retry.
#
# With connections the router owns, a freshly elected primary is one checkout
# away. What must not happen is the obvious version of that: replaying a write
# that had already been applied and whose acknowledgement was lost, which from
# here looks identical to one that never ran. So only writes MongoDB itself
# deduplicates are replayed, and this checks both halves, that the write
# succeeds and that it was applied exactly once.
NODE=thermocline-zelmar-node0
BASE=/root/cline.yaml
FOCFG=/root/cline_fo.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
NS=tfail
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
H() { docker exec $NODE mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
# Through the router, with retryable writes, which is the driver default.
RW() { docker exec $NODE mongosh "mongodb://dba:secret@127.0.0.1:27100/?authSource=admin&retryWrites=true" --quiet --eval "$1" 2>&1 | tail -1; }

cleanup() { docker exec $NODE pkill -9 -f '[c]line router' 2>/dev/null; }
trap cleanup EXIT

stopRouter() {
  docker exec $NODE pkill -9 -f '[c]line router' 2>/dev/null
  sleep 2
}
startRouter() { # config
  docker exec -d $NODE cline router --config "$1" --log-file /var/log/cline/fo_r.log
  for _ in $(seq 1 30); do
    docker exec $NODE mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on 27100"; exit 2
}
# Move the primary and do not let the old one take it back immediately.
stepDown() {
  docker exec $NODE mongosh "$HOT" --quiet --eval 'try { rs.stepDown(20) } catch (e) {}' >/dev/null 2>&1
}
waitForPrimary() {
  for _ in $(seq 1 60); do
    [ "$(H 'print(db.hello().isWritablePrimary === true || !!db.hello().primary)')" = "true" ] && return 0
    sleep 1
  done
}
primaryNow() { H 'print(db.hello().primary)'; }

echo "== setup =="
docker exec $NODE bash -c "sed 's/^  listen:/  terminate_auth: true\n  listen:/' $BASE > $FOCFG"
chk "the config asks for terminated authentication"    "1" \
    "$(docker exec $NODE grep -c 'terminate_auth: true' $FOCFG)"
stopRouter
H "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('shop').$NS.insertOne({_id:1,v:0})" >/dev/null
chk "the document starts at zero"                      "0" "$(H "const d=db.getSiblingDB('shop').$NS.findOne({_id:1}); print(d?d.v:'MISSING')")"

echo "== a retryable write across a change of primary =="
startRouter $FOCFG
BEFORE=$(primaryNow)
RW "db.getSiblingDB('shop').$NS.updateOne({_id:1},{\$inc:{v:1}})" >/dev/null 2>&1
chk "a write before the failover applies once"         "1" \
    "$(H "const d=db.getSiblingDB('shop').$NS.findOne({_id:1}); print(d?d.v:'MISSING')")"

stepDown
# The router still believes in the old primary, so the next write reaches a node
# that has stepped down. That is the case being tested, whether or not a new
# primary has been elected yet.
OUT=$(RW "const r=db.getSiblingDB('shop').$NS.updateOne({_id:1},{\$inc:{v:1}}); print('matched='+r.matchedCount)")
waitForPrimary
AFTER=$(primaryNow)
echo "      primary moved from $BEFORE to $AFTER"
chk "the primary actually moved"                       "true" \
    "$([ -n "$BEFORE" ] && [ -n "$AFTER" ] && [ "$BEFORE" != "$AFTER" ] && echo true || echo false)"
chk "the write across the failover succeeded"          "matched=1" "$OUT"
# The number that matters. Replaying a write that had already been applied would
# leave three here, and the whole design exists to make that impossible.
chk "and it applied exactly once"                      "2" \
    "$(H "const d=db.getSiblingDB('shop').$NS.findOne({_id:1}); print(d?d.v:'MISSING')")"
REPLAYS=$(docker exec $NODE bash -c "curl -s http://127.0.0.1:27110/metrics 2>/dev/null | awk '/^thermocline_failover_replays_total /{print \$2}'" | tail -1)
chk "the router counted the replay"                    "true" \
    "$(awk -v n="$REPLAYS" 'BEGIN{print (n+0 > 0) ? "true" : "false"}')"

echo "== the tier is still usable afterwards =="
chk "a later write goes to the new primary"            "3" \
    "$(RW "db.getSiblingDB('shop').$NS.updateOne({_id:1},{\$inc:{v:1}}); const d=db.getSiblingDB('shop').$NS.findOne({_id:1}); print(d?d.v:'MISSING')")"
chk "and a read still answers"                         "3" \
    "$(RW "const d=db.getSiblingDB('shop').$NS.findOne({_id:1}); print(d?d.v:'MISSING')")"

echo "== and with authentication proxied it still fails, as it always did =="
# The control, and the point of the whole change. Without terminated
# authentication there is no connection to replay onto, so the client sees the
# failover. testchaos.sh pins the same behaviour under load; this is the same
# thing beside its replacement, so the difference is not taken on faith.
stopRouter
H "db.getSiblingDB('shop').$NS.updateOne({_id:1},{\$set:{v:100}})" >/dev/null
startRouter $BASE
stepDown
PROXOUT=$(RW "const r=db.getSiblingDB('shop').$NS.updateOne({_id:1},{\$inc:{v:1}}); print('matched='+r.matchedCount)")
waitForPrimary
case "$PROXOUT" in
  matched=1) PROXRES=succeeded ;;
  *)         PROXRES=failed ;;
esac
chk "a proxied write across a failover fails"          "failed" "$PROXRES"
# And still without applying anything twice, which is what makes that failure an
# honest one rather than a silent loss.
chk "and nothing was applied twice"                    "true" \
    "$(H "const d=db.getSiblingDB('shop').$NS.findOne({_id:1}); print(d && d.v <= 101)")"

# A cursor open across a change of primary.
#
# Two cursors are involved in one tiered read and they live on different
# clusters. A cursor the client holds against the fast tier is an ordinary
# MongoDB cursor on the node that made it. A cursor over an archive answer is
# one this router owns: the client is given an id from the router's own
# registry, and the driver cursor behind it sits on the archive's primary. Both
# die when the node under them stops being primary, and the question is not
# whether they die, it is what the client is told when they do.
#
# The one outcome that must not happen is a short answer with no error. A cursor
# that stops early and reports itself exhausted hands the application half a
# collection and an ok:1, which is the failure mode this whole system is built
# to avoid. Either every document arrives, or the read fails loudly enough for
# the driver to retry it.
COLDU=${COLDU:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
CNS=tfoc
CNSU=tfocu
C() { docker exec $NODE mongosh "$COLDU" --quiet --eval "$1" 2>&1 | tail -1; }

# Read a cursor in two halves, moving a primary in between, and report what the
# client saw: how many documents arrived and whether it was told anything.
readAcrossStepDown() { # collection, uri-of-the-set-to-step-down
  docker exec $NODE mongosh "mongodb://dba:secret@127.0.0.1:27100/?authSource=admin" --quiet --eval "
    const c = db.getSiblingDB('shop').$1;
    const cur = c.find({}).sort({_id:1}).batchSize(5);
    let n = 0, err = '';
    while (n < 5 && cur.hasNext()) { cur.next(); n++; }
    try {
      const other = new Mongo('$2');
      try { other.getDB('admin').runCommand({replSetStepDown: 20, force: true}); } catch (e) {}
    } catch (e) {}
    try { while (cur.hasNext()) { cur.next(); n++; } }
    catch (e) { err = e.codeName || String(e.code) || 'error'; }
    print(n + ' ' + (err === '' ? 'no-error' : err));
  " 2>&1 | tail -1
}
# 40 documents, either all of them or a failure. Anything else is a truncation.
verdict() { # "count word"
  local n=${1%% *} err=${1##* }
  if [ "$n" = "40" ]; then echo complete
  elif [ "$err" = "no-error" ]; then echo "TRUNCATED at $n"
  else echo "failed at $n"; fi
}

echo "== a cursor over an archive answer, across a change of the archive's primary =="
stopRouter
docker exec $NODE mongosh "$HOT" --quiet --eval "db.getSiblingDB('shop').$CNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB('shop').$CNS.drop();
   db.getSiblingDB('shop').$CNS.insertMany(Array.from({length:40},(_,i)=>({_id:i+1,v:i})))" >/dev/null 2>&1
docker exec $NODE cline watermark --config $BASE --ns shop.$CNS --complete-from 2030-01-01T00:00:00Z >/dev/null 2>&1
chk "the fast tier holds none of it"                   "0" \
    "$(docker exec $NODE mongosh "$HOT" --quiet --eval "print(db.getSiblingDB('shop').$CNS.countDocuments({}))" 2>&1 | tail -1)"
chk "the archive holds all forty"                      "40" \
    "$(C "print(db.getSiblingDB('shop').$CNS.countDocuments({}))")"
startRouter $BASE
ARCH=$(readAcrossStepDown $CNS "$COLDU")
echo "      the client saw: $ARCH"
chk "an archive cursor is not silently truncated"      "true" \
    "$([ "$(verdict "$ARCH")" != "TRUNCATED at ${ARCH%% *}" ] && echo true || echo false)"
waitForPrimary

echo "== a cursor over the fast tier, across a change of its primary =="
stopRouter
docker exec $NODE mongosh "$HOT" --quiet --eval \
  "db.getSiblingDB('shop').$CNSU.drop();
   db.getSiblingDB('shop').$CNSU.insertMany(Array.from({length:40},(_,i)=>({_id:i+1,v:i})))" >/dev/null 2>&1
startRouter $FOCFG
FAST=$(readAcrossStepDown $CNSU "$HOT")
echo "      the client saw: $FAST"
chk "a fast tier cursor is not silently truncated"     "true" \
    "$([ "$(verdict "$FAST")" != "TRUNCATED at ${FAST%% *}" ] && echo true || echo false)"
waitForPrimary
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
