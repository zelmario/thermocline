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
NODE=${NODE:-thermocline-zelmar-node0}
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

echo "== and with authentication proxied the client's own retry finishes it =="
# This used to be the control that showed what terminated authentication bought:
# with authentication proxied there is no connection to replay onto, so the
# client saw the failover. Half of that is still true, and the half that mattered
# turned out to be a defect rather than a limitation.
#
# The router re-resolved which member is primary only when a dial failed, and a
# member that steps down stays up and answers, so nothing ever failed to dial.
# The pinned address stayed on the demoted member for ever: measured with the
# shop running, 4,226 refusals three minutes after an election decided in one
# second, and every client that connected afterwards refused too. It now
# re-resolves on the first refusal and ends the connection once the refusal has
# gone out, which is what mongod does to the clients of a member that stops
# being primary. The driver opens a new one, authenticates against the member
# that is primary now, and its retry lands.
#
# The line that must not move is the one below it: still exactly once.
stopRouter
H "db.getSiblingDB('shop').$NS.updateOne({_id:1},{\$set:{v:100}})" >/dev/null
startRouter $BASE
stepDown
PT0=$(date +%s%N)
PROXOUT=$(RW "const r=db.getSiblingDB('shop').$NS.updateOne({_id:1},{\$inc:{v:1}}); print('matched='+r.matchedCount)")
PT1=$(date +%s%N)
waitForPrimary
case "$PROXOUT" in
  matched=1) PROXRES=succeeded ;;
  # The measured limitation, and the only failure this check accepts. A
  # startup read refused on a connection still bound to the old member ends
  # that connection; a write queued for a connection gets it before Node has
  # read the close, fails on it, and the cleared pool takes the retry with it.
  # 4 of 20 on a quiet box on 2026-10-07, never applied twice. See "a single
  # write across a step-down can still fail" in docs/limitations.md.
  *"connection "*" closed"*|*MongoPoolClearedError*) PROXRES=succeeded-or-closed ;;
  *)         PROXRES=failed ;;
esac
[ "$PROXRES" = succeeded-or-closed ] && echo "      (the documented limitation: the connection closed under a queued write)"
[ "$PROXRES" = succeeded ] && PROXRES=succeeded-or-closed
# Printed whether it passed or failed. Anything other than success or that one
# shape fails: "not primary" means the router refused on a member it should
# have moved off, and a timeout means the election outlasted the router's 20
# second wait, which failed once under a full hostsweep on a busy box.
echo "      the client waited $(( (PT1-PT0)/1000000 ))ms and saw: $PROXOUT"
chk "a proxied write across a failover lands or is closed" "succeeded-or-closed" "$PROXRES"
# And still without applying anything twice, which is what makes the retry an
# honest one rather than a silent double.
chk "and nothing was applied twice"                    "true" \
    "$(H "const d=db.getSiblingDB('shop').$NS.findOne({_id:1}); print(d && d.v <= 101)")"
# The regression this exists for. A client that connects after the election is
# the plainest way to ask whether the router is still pointing at the member
# that stepped down, and before the fix this was refused for as long as the
# router stayed up.
chk "a client connecting after it reaches the new primary" "true" \
    "$(RW "const d=db.getSiblingDB('shop').$NS.findOne({_id:1}); print(d !== null)")"

echo "== a client's other connections, which were told nothing =="
# A limitation of proxied authentication, pinned here rather than fixed, because
# the fix was tried and cost more than it bought.
#
# Every client connection has an upstream of its own in that mode, and a refusal
# ends only the connection that got it. A client with a pool has the rest still
# pointing at the member that stepped down, and each one has to be refused in
# turn before anything ends it. A driver retries a write once, so that retry can
# be spent on a sibling that was always going to fail. Measured with two
# connections made by one client before the stepDown and the driver's own
# retries on: five runs out of five left one write applied where two were sent.
#
# What was tried: end a connection whose upstream is provably no longer the
# primary before sending its command, which is what mongod does to the clients
# of a member that stops being primary. It works, 3 runs of 3, and only because
# it also ends the driver's *monitoring* connection and forces a topology
# rescan. Restricted to the commands a demoted member would actually refuse, so
# that monitoring keeps working, it goes back to 0 runs out of 10. Left
# unrestricted, it ends the monitor and a write that should pause for an
# election comes back "MongoServerSelectionError: connection <monitor> closed",
# which undoes the wait added earlier in this same suite.
#
# So the check below records what happens rather than demanding a value. The
# line that matters is the one after it: whichever way the sibling goes, no
# write is applied twice.
stopRouter
H "db.getSiblingDB('shop').$NS.updateOne({_id:1},{\$set:{v:300}})" >/dev/null
startRouter $BASE
# Single-quoted at the shell and the namespace passed through the environment,
# because half of what follows is $-prefixed in two languages at once.
SIB=$(docker exec -e TCNS="$NS" -e TCHOT="$HOT" $NODE mongosh "mongodb://dba:secret@127.0.0.1:27100/?authSource=admin&retryWrites=true" --quiet --eval '
  const ns = process.env.TCNS;
  const uri = "mongodb://dba:secret@127.0.0.1:27100/?authSource=admin&retryWrites=true";
  const a = new Mongo(uri), b = new Mongo(uri);
  const ca = a.getDB("shop")[ns], cb = b.getDB("shop")[ns];
  // Both used before anything moves, so both have an upstream of their own.
  ca.countDocuments({}); cb.countDocuments({});
  const s = new Mongo(process.env.TCHOT);
  try { s.getDB("admin").runCommand({replSetStepDown: 20, force: true}) } catch (e) {}
  sleep(1500);
  try { ca.updateOne({_id:1},{$inc:{v:1}}) } catch (e) {}
  let second = "";
  try { cb.updateOne({_id:1},{$inc:{v:1}}); second = "applied" } catch (e) { second = (e.codeName || "refused") }
  print(second);' 2>&1 | tail -1)
waitForPrimary
echo "      the second connection: $SIB"
# Either outcome is within the contract. What is not is a write applied twice,
# which is what this section is really guarding: the fast tier's value is set to
# 300 above and two increments are sent, so anything over 302 is a replay.
SIBV=$(H "const d=db.getSiblingDB('shop').$NS.findOne({_id:1}); print(d?d.v:'MISSING')")
chk "and nothing was applied twice"                    "true" \
    "$(awk -v v="$SIBV" 'BEGIN{print (v+0 >= 300 && v+0 <= 302) ? "true" : "false"}')"

echo "== and while the fast tier has no primary at all =="
# The check above races an election. The driver retries a write once, and if a
# primary exists by then the retry lands, which it did nine runs in ten. The
# tenth looked like a flake and was not.
#
# A member that steps down stays up and keeps accepting connections, so the
# router's dial succeeded against a member that refuses every command, and the
# 20 second wait for a primary never engaged because nothing ever failed to
# dial. That is the same defect the comment above describes for a connection
# already open, left unfixed on the path a reconnecting client takes. It now
# asks who is primary before dialing anybody whenever the last thing it heard
# was a refusal, and keeps asking until somebody is. Measured with every member
# frozen so no election could finish: 3 of 3 writes refused before, 4 of 4
# paused and landed after, applied exactly once each time, and a window longer
# than the wait still fails rather than hanging.
#
# Holding the window open is the awkward part, and the first version of this
# lied about having it: a stepDown with the other members frozen is refused
# often enough to matter, and reading the topology straight afterwards reads a
# view the driver has not rescanned yet. So freeze everybody, prove there is no
# primary, and lift it on a timer. If the window does not open the section says
# so rather than passing on a question nobody was asked.
MEMBERS="172.24.0.5:27017 172.24.0.6:27017 172.24.0.7:27017"
# Three seconds, not eight, and the difference is the election that follows.
# Lifting the freeze does not make a primary, it makes the members eligible: the
# one that will win still has to wait out its election timeout, which is ten
# seconds by default. So the window the client actually waits is this plus that,
# and at eight it measured 18.5s and 16.9s against the router's own 20 second
# bound and failed the run in between. What this section is for is whether the
# router waits at all, not where its ceiling is.
NOPRIMFOR=3
D() { docker exec $NODE mongosh "mongodb://dba:secret@$1/?authSource=admin&directConnection=true" --quiet --eval "$2" 2>&1 | tail -1; }
unfreezeAll() { for a in $MEMBERS; do D $a 'try{rs.freeze(0)}catch(e){}' >/dev/null 2>&1; done; }
# Whatever happens below, the set must be left electable for the sections after
# it, and a 60 second freeze outlives this suite.
trap 'unfreezeAll; cleanup' EXIT

stopRouter
H "db.getSiblingDB('shop').$NS.updateOne({_id:1},{\$set:{v:200}})" >/dev/null
startRouter $BASE
# The client is connected before the primary goes away, the way an application
# is, and waits for a signal to write. Connecting inside the window instead gave
# it one connection, and one connection could never show what an application's
# pool did: the driver's retry went out on a sibling connection still
# authenticated against the old primary and was refused at once, 0 of 5 rounds
# on 2026-10-02 against 5 of 5 straight to the set. The router now closes those
# siblings while they are idle.
#
# And with the drivers' own 30 second server selection rather than mongosh's.
# mongosh gives a single-host connection string 2 seconds for the whole session,
# which is the router's case, and the drivers' 30 to a replica set string, which
# is the direct case. An application has the 30 either way; at 2 the write fails
# through the router and would through any one member alone.
docker exec $NODE rm -f /root/fo_go
( docker exec $NODE mongosh "mongodb://dba:secret@127.0.0.1:27100/?authSource=admin&retryWrites=true&serverSelectionTimeoutMS=30000" --quiet --eval "
    const fs = require('fs');
    db.getSiblingDB('shop').$NS.findOne({_id:1});
    while (!fs.existsSync('/root/fo_go')) sleep(50);
    try { const r = db.getSiblingDB('shop').$NS.updateOne({_id:1},{\$inc:{v:1}}); print('matched='+r.matchedCount) }
    catch (e) { print(e.name + ': ' + e.message) }" 2>&1 | tail -1 > /tmp/fo_np.out ) &
NPCLIENT=$!
sleep 4
# Three goes at it. The section above has just moved the primary, so the first
# read of who holds it can be a member that no longer does: the freeze then
# skips the real primary and the stepDown is sent to a secondary, both are
# refused, and nothing happens. That is how this reported a skip the first time
# it ran.
waitForPrimary
NOPRIM=""
for _ in 1 2 3; do
  PRIM=$(primaryNow)
  [ -n "$PRIM" ] || { waitForPrimary; continue; }
  for a in $MEMBERS; do [ "$a" = "$PRIM" ] || D $a 'try{ rs.freeze(60) }catch(e){}' >/dev/null; done
  # force, because an ordinary stepDown wants an electable secondary caught up
  # and the two frozen ones do not count as electable: measured, it refused
  # with ExceededTimeLimit about half the time and the window never opened.
  D "$PRIM" 'try { db.adminCommand({replSetStepDown: 60, force: true}) } catch (e) {}' >/dev/null
  sleep 3
  NOPRIM=$(H 'print(db.hello().primary || "NONE")')
  case "$NOPRIM" in NONE|*Error*) break ;; esac
  unfreezeAll
  waitForPrimary
done
case "$NOPRIM" in
  NONE|*Error*)
    ( sleep $NOPRIMFOR; unfreezeAll ) &
    LIFTER=$!
    T0=$(date +%s%N)
    docker exec $NODE touch /root/fo_go
    wait $NPCLIENT
    T1=$(date +%s%N)
    NPOUT=$(cat /tmp/fo_np.out)
    wait $LIFTER
    waitForPrimary
    echo "      no primary for ${NOPRIMFOR}s, the client waited $(( (T1-T0)/1000000 ))ms"
    chk "a write with nowhere to go waits and lands"    "matched=1" "$NPOUT"
    # 201 means it waited and applied once. 200 means it was refused, which is
    # the defect. 202 means it was applied twice, which would be worse than
    # either.
    chk "and it was applied exactly once"               "201" \
        "$(H "const d=db.getSiblingDB('shop').$NS.findOne({_id:1}); print(d?d.v:'MISSING')")"
    ;;
  *)
    unfreezeAll
    docker exec $NODE touch /root/fo_go
    wait $NPCLIENT
    waitForPrimary
    echo "      SKIP  the window never opened, primary is $NOPRIM (2 checks)"
    ;;
esac
unfreezeAll

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
# What a client is told about this proxy during an election is pinned by a unit
# test rather than here, and that is worth explaining. The state is real and
# measured: sampling hello every 150ms on one held connection across a forced
# step-down gave 153 of 173 replies saying isWritablePrimary=false, still false
# 25 seconds later, because the reply was the pinned upstream's and that upstream
# was the demoted node. After the fix the same measurement gave 0 of 161.
#
# An end-to-end check of it was written and thrown away: it passed against the
# defect three times, even with the step-down verified to have moved the primary,
# because whether it reproduces depends on which member the connection happens to
# be pinned to and when the pool redials. A check that passes either way is worse
# than no check. See TestMongosIdentityIsAlwaysWritable.
waitForPrimary


echo "== a read that falls through while the archive has no primary =="
# A read the fast tier cannot answer waits for mirroring to reach what this
# router has already acknowledged, and gives up after archive_wait. That is the
# documented trade for a slow feeder. It was also what happened when the archive
# had no primary, where it bought nothing: the read goes to the archive's
# primary and blocks until there is one, and the feeder, stopped by the same
# election, has not yet mirrored the write. Found on the Hetzner staging pair on
# 2026-10-08, where a priority takeover failed its vote under load and left the
# archive without a primary for 10.6s; a forced 21s window gave 27 violations
# of read-your-writes on the build before and none after.
#
# The insert itself waits for the archive's primary, so the read that follows
# often starts just after one is back, before the feeder has published again.
# That is the second way to get it wrong: the position has been quiet for longer
# than archive_wait and reads as a feeder that has stopped. Asking the archive
# then finds a primary and does not help; the router remembers when the driver
# last saw it without one instead. Measured on 2026-10-09, eight runs of this
# section each: 2 of 8 answered 1 with only the first fix, 0 of 8 with both.
#
# One document written through the router inside the window, then read by a
# query that has to fall through. The answer must include it.
NPNS=tfonoprim
CMEMBERS="172.24.0.2:27017 172.24.0.3:27017 172.24.0.4:27017"
unfreezeCold() { for a in $CMEMBERS; do D $a 'try{rs.freeze(0)}catch(e){}' >/dev/null 2>&1; done; }
stopFeeder() { docker exec $NODE pkill -9 -f '[c]line feeder' 2>/dev/null; sleep 1; }
trap 'unfreezeAll; unfreezeCold; stopFeeder; cleanup' EXIT
RT() { docker exec $NODE mongosh "mongodb://dba:secret@127.0.0.1:27100/?authSource=admin" --quiet --eval "$1" 2>&1 | tail -1; }
coldPrimary() { for a in $CMEMBERS; do p=$(D $a 'print(db.hello().primary || "none")'); [ -n "$p" ] && { echo "$p"; return; }; done; }

stopRouter
stopFeeder
H "db.getSiblingDB('shop').$NPNS.drop()" >/dev/null
C "db.getSiblingDB('shop').$NPNS.drop()" >/dev/null
docker exec -d $NODE cline feeder --config $BASE --log-file /var/log/cline/fo_f.log
FEEDING=false
for _ in $(seq 1 30); do
  [ "$(C 'const p=db.getSiblingDB("_thermocline").positions.findOne({_id:"test"}); print(!!p && Date.now()-p.savedAt.getTime() < 3000)')" = "true" ] && { FEEDING=true; break; }
  sleep 1
done
# Said here rather than left to the checks below. On 2026-10-19 the feeder
# refused to start, its checkpoint older than an oplog shrunk for another test,
# and this section reported two refused reads instead, which is the router
# being right about a feeder that was never running.
$FEEDING || echo "      the feeder never published: $(docker exec $NODE sh -c "grep 'level=ERROR' /var/log/cline/fo_f.log | tail -1 | cut -c1-200")"
startRouter $BASE
RT "db.getSiblingDB('shop').$NPNS.insertOne({_id:1})" >/dev/null
for _ in $(seq 1 30); do [ "$(C "print(db.getSiblingDB('shop').$NPNS.countDocuments({}))")" = "1" ] && break; sleep 0.5; done
docker exec $NODE cline watermark --config $BASE --ns shop.$NPNS --complete-from 2030-01-01T00:00:00Z >/dev/null 2>&1
sleep 7
chk "  a read of it falls through to the archive"       "1" "$(RT "print(db.getSiblingDB('shop').$NPNS.countDocuments({}))")"

# Freeze the secondaries so nobody can be elected, then force the primary down.
# Twelve seconds plus the election timeout that follows the thaw: about twenty,
# far past the five a read used to wait and inside the thirty it waits now.
#
# Only from a set that has settled. The section before this one steps the
# archive's primary down and then waits for the fast tier, not the archive, and
# coldPrimary reports one member's view. In three runs of four on 2026-10-12 the
# window never opened and this section passed on a question nobody was asked.
# So wait until every member names the same primary and that member says it is
# writable, and print what each freeze and the step-down answered.
coldAgreed() {
  local views
  views=$(for a in $CMEMBERS; do D $a 'print(db.hello().primary || "none")'; done | sort -u)
  [ "$(echo "$views" | wc -l)" = 1 ] && [ "$views" != none ] || return 1
  [ "$(D "$views" 'print(db.hello().isWritablePrimary === true)')" = true ] && echo "$views"
}
P=""
for _ in $(seq 1 60); do P=$(coldAgreed) && break; sleep 1; done
[ -n "$P" ] || echo "      the archive's members never agreed on a primary"
for a in $CMEMBERS; do
  [ "$a" != "$P" ] || continue
  r=$(D $a 'try{print(rs.freeze(12).ok)}catch(e){print(e.codeName || e.message)}')
  [ "$r" = 1 ] || echo "      freezing $a answered: $r"
done
r=$(D "$P" 'try{print(db.adminCommand({replSetStepDown: 30, force: true}).ok)}catch(e){print(e.codeName || e.message)}')
[ "$r" = 1 ] || echo "      stepping $P down answered: $r"
# Every member, and given a moment. A member learns of a step-down from the next
# heartbeat, so for up to two seconds it still names the member that stepped
# down. coldPrimary asks the first member only, which answers for itself when it
# was the primary and from that stale view when it was not: every failure of
# this check on 2026-10-12 named 172.24.0.3, never the first member.
coldNone() {
  for _ in $(seq 1 10); do
    [ "$(for a in $CMEMBERS; do D $a 'print(db.hello().primary || "none")'; done | sort -u)" = none ] && { echo none; return; }
    sleep 0.5
  done
  coldPrimary
}
chk "  the archive has no primary"                      "none" "$(coldNone)"
OUT=$(RT "const c=db.getSiblingDB('shop').$NPNS; c.insertOne({_id:2});
  const t=Date.now(); const n=c.countDocuments({}); print(n+' '+Math.round((Date.now()-t)/1000)+'s')")
echo "      the read answered: $OUT"
chk "  the write made inside the window is in the answer" "2" "${OUT%% *}"
unfreezeCold
for _ in $(seq 1 60); do [ "$(coldPrimary)" != "none" ] && break; sleep 1; done
stopFeeder

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
