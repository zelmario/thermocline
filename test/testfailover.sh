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

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
