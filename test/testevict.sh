#!/bin/bash
# The evictor's documented precondition is a running feeder. Test it both ways.
# Parallel-safe knobs. Every default reproduces the single-worker layout, so a
# suite run by hand behaves exactly as it always has. psweep.sh sets them per
# worker: its own databases, its own bookkeeping database, its own port block,
# and its own directory for configs and logs. That directory is also what scopes
# the pkills below to this worker's daemons, because every one of them carries
# --config $WDIR/... on its command line.
WDIR=${WDIR:-/root}
LOGDIR=${LOGDIR:-/var/log/cline}
PORT=${PORT:-27100}
DB=${DB:-shop}
DB2=${DB2:-wh}
META=${META:-_thermocline}
# mongosh is node, so the suites reach these through process.env rather than
# shell expansion. Half the eval strings are single-quoted at the shell, where a
# $VAR would not expand and would silently query the wrong database.
export TC_DB=$DB TC_DB2=$DB2 TC_META=$META
P1=$((PORT+1)); P2=$((PORT+2)); P3=$((PORT+3)); PDASH=$((PORT+10)); PALT=$((PORT+17))
CFG=${CFG:-$WDIR/cline.yaml}
# The tier endpoints, defaulted the same way every other suite defaults them.
# Without a default these were ${HOT:?}, which reads as strict and is not: a
# sweep does not export them, so the two checks that use them exited before
# querying anything and reported got= want=1. A check that cannot run looks
# exactly like a check that failed, and this pair guards the evictor destroying
# a live record.
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
PASS=0; FAIL=0; declare -a FAILED
ok() {
  local name="$1" want="$2" pat="$3"; shift 3; [ "$1" = "--" ] && shift
  local out rc; out=$("$@" 2>&1); rc=$?
  local good=1
  [ -n "$want" ] && [ "$rc" != "$want" ] && good=0
  [ -n "$pat" ] && ! grep -qE "$pat" <<<"$out" && good=0
  if [ $good = 1 ]; then PASS=$((PASS+1)); printf '  PASS  %s\n' "$name"
  else FAIL=$((FAIL+1)); FAILED+=("$name"); printf '  FAIL  %s (exit=%s want=%s)\n    %s\n' "$name" "$rc" "$want" "$(tail -2 <<<"$out"|tr '\n' ' ')"; fi
}

echo "== fail-closed once mirroring goes stale (the safety guard) =="
# Killing the feeder does not make lag stale instantly: the archive is current
# until enough time passes. Wait past max_feeder_lag so the guard has something
# to trip on, which is the condition it exists for.
pkill -f "cline feeder --config $WDIR/"; sleep 35
ok "refuses when mirroring lag is unknown/stale" 1 "not healthy enough to evict" \
   -- cline evictor --config $CFG --once --health-timeout 5s

echo "== with the feeder running, as documented =="
cline feeder --config $CFG --log-file $LOGDIR/t_ev_feeder.log &
FP=$!
# The evictor's precondition is mirroring lag inside max_feeder_lag. After the
# feeder has been down it has to catch up first, so wait for that rather than
# racing it.
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
for i in $(seq 1 60); do
  LAG=$(mongosh "$COLD" --quiet --eval 'const c=db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}); print(c?Math.round((Date.now()-c.eventAt.getTime())/1000):9999)' 2>/dev/null | tail -1)
  [ "$LAG" -lt 20 ] 2>/dev/null && { echo "  (feeder caught up, lag ${LAG}s)"; break; }
  sleep 2
done
ok "--once --dry-run"          0 "sweep complete" -- cline evictor --config $CFG --once --dry-run
ok "--once"                    0 "sweep complete" -- cline evictor --config $CFG --once
ok "--once -v"                 0 "sweep complete" -- cline evictor --config $CFG --once -v
ok "--once --health-timeout"   0 "sweep complete" -- cline evictor --config $CFG --once --health-timeout 30s

rm -f $LOGDIR/t_ev.log
# A dry run, because this check is about looping and nothing else. Left as a
# real one it evicted while it waited, and the fixtures below then measured a
# collection this check had emptied: two failures with nothing wrong.
cline evictor --config $CFG --every 2s --dry-run --log-file $LOGDIR/t_ev.log &
EP=$!
# Wait for the second sweep rather than for a fixed nine seconds.
#
# A sweep itself is fast: 0.05s over this config's thirty namespaces with nothing
# to evict. What is slow is the health backoff, and correctly so. The feeder was
# restarted a few sections above, so it resumes from a checkpoint that may be far
# behind, and the evictor pauses five seconds at a time while mirroring lag is
# over max_feeder_lag. A fixed nine second window therefore counted one sweep or
# two depending on how far behind the feeder happened to be, and this check
# failed for a reason that had nothing to do with looping.
N=0
for _ in $(seq 1 90); do
  N=$(grep -c "sweep complete" $LOGDIR/t_ev.log 2>/dev/null); N=${N:-0}
  [ "$N" -ge 2 ] && break
  sleep 1
done
if [ "$N" -ge 2 ]; then PASS=$((PASS+1)); echo "  PASS  --every loops ($N sweeps)"
else FAIL=$((FAIL+1)); FAILED+=("--every"); echo "  FAIL  --every loops (saw $N)"; fi
kill $EP 2>/dev/null; wait $EP 2>/dev/null

ok "dry run never blocks on health" 0 "dry run" -- cline evictor --config $CFG --once --dry-run

echo "== two sweeps at once, which the README promises cannot happen =="
# The evictor takes its lease per sweep rather than per process, because a sweep
# is what must not overlap: two of them announce one eviction twice and delete it
# once, and the count left standing then answers for the application's own delete
# of that _id whenever it comes. The README says a second --once refuses while a
# sweep is running, and nothing checked it.
#
# This suite has no chk, so the pass and fail are counted the way the rest of it
# does. The race is made winnable rather than hoped for: the first sweep is given
# a health timeout long enough to be holding the lease when the second starts.
cline evictor --config $CFG --once --health-timeout 20s > $LOGDIR/ev_race1.log 2>&1 &
RACE1=$!
sleep 0.2
cline evictor --config $CFG --once > $LOGDIR/ev_race2.log 2>&1
wait $RACE1 2>/dev/null
if grep -qiE "another evictor|lease|already running" $LOGDIR/ev_race2.log; then
  PASS=$((PASS+1)); echo "  PASS  a second sweep is refused while one runs"
else
  FAIL=$((FAIL+1)); FAILED+=("second sweep refused")
  echo "  FAIL  a second sweep is refused while one runs: $(tail -1 $LOGDIR/ev_race2.log | cut -c1-90)"
fi
if grep -q "sweep complete" $LOGDIR/ev_race1.log; then
  PASS=$((PASS+1)); echo "  PASS  and the one holding the lease finishes"
else
  FAIL=$((FAIL+1)); FAILED+=("first sweep finishes"); echo "  FAIL  and the one holding the lease finishes"
fi
# A dry run announces nothing and removes nothing, so it has nothing to wait for,
# and the README says it always runs. That is the half that makes the refusal
# above a lease rather than a lock on the whole command.
cline evictor --config $CFG --once --health-timeout 20s > $LOGDIR/ev_race3.log 2>&1 &
RACE3=$!
sleep 0.2
cline evictor --config $CFG --once --dry-run > $LOGDIR/ev_race4.log 2>&1
wait $RACE3 2>/dev/null
if grep -qE "sweep complete|dry run" $LOGDIR/ev_race4.log; then
  PASS=$((PASS+1)); echo "  PASS  but a dry run beside a sweep still runs"
else
  FAIL=$((FAIL+1)); FAILED+=("dry run beside a sweep")
  echo "  FAIL  but a dry run beside a sweep still runs: $(tail -1 $LOGDIR/ev_race4.log | cut -c1-90)"
fi

echo "== a tombstone is not a copy of the document =="
# The evictor removes a document once the archive is confirmed to hold it, and
# a tombstoned copy is not that. It is a record that the application deleted the
# document; reads filter it out, so counting it would confirm the archive holds
# something no read will ever return.
#
# The state is ordinary: an application deletes a record and writes the same _id
# again, which is what a natural key or a reprocessed record looks like. Until
# the feeder mirrors the second write the fast tier holds the only live copy and
# the archive holds a tombstone. Measured before the fix: one sweep and the
# document was on neither tier, and the router answered GONE for a document the
# application had just written.
#
# Set up directly rather than raced, because the evictor's decision depends on
# the state it finds and not on how the state arose.
chkv() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-50s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-50s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
H() { mongosh "$HOT"  --quiet --eval "$1" 2>&1 | tail -1; }
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }

# routerUp is the same ping R makes, without the pipe, and it exists because a
# wait loop has to test the command's own status.
#
# R ends in `| tail -1`, so a pipeline's status is tail's and is zero however
# badly mongosh failed. Every `do R 'ping' && break` loop in this file therefore
# broke on its first iteration and waited for nothing at all: the router was
# given no time to come up, and whatever ran next met a closed port. On an idle
# box the router wins that race and nothing shows; under a parallel sweep it
# does not, and the result is a write failing into /dev/null and a later check
# reporting something else entirely. Measured: testindexes reported "carol
# reached the archive got=2 want=3" for a router that was still starting.
routerUp() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
  --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
# The feeder is stopped for this, so it cannot mirror the fast tier write and
# clear the tombstone underneath the sweep. Briefly, though: the evictor fails
# closed once mirroring looks stale, and a sweep that refuses to run would pass
# this check while proving nothing. A few seconds is far inside max_feeder_lag.
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
for _ in $(seq 1 20); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || break; sleep 0.5; done
TOID=$(printf '%08x%016x' $(( $(date +%s) - 7200 )) 4242)
H "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$TOID')})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$TOID')})" >/dev/null
C "db.getSiblingDB(process.env.TC_META).residency.deleteMany({})" >/dev/null
# The live copy is put on the fast tier while the feeder is stopped further up,
# so the archive keeps its tombstone rather than being caught up over it.
H "db.getSiblingDB(process.env.TC_DB).orders.insertOne({_id:ObjectId('$TOID'), n:'written again'})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).orders.insertOne({_id:ObjectId('$TOID'), n:'the older one', _tcDeletedAt:new Date()})" >/dev/null
chkv "the fast tier holds the only live copy" "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$TOID')}))")"
chkv "and the archive holds none"             "0" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$TOID'),_tcDeletedAt:{\$exists:false}}))")"
# Make the health gate deterministic rather than racing it. The feeder is
# stopped for this setup, so mirroring lag grows while the fixture is written,
# and inside a four-worker sweep that took longer than max_feeder_lag: the sweep
# refused on health and the check below would have been meaningless. The control
# caught it, which is what it is for. Lag is what the first section of this
# suite tests; this one is about what counts as the archive holding a document,
# so the position is stamped fresh immediately before the sweep.
NOWMS=$(date +%s000)
C "db.getSiblingDB(process.env.TC_META).checkpoints.updateOne({_id:'test'},{\$set:{eventAt:new Date($NOWMS), savedAt:new Date($NOWMS)}})" >/dev/null
C "db.getSiblingDB(process.env.TC_META).positions.updateOne({_id:'test'},{\$set:{at:new Date($NOWMS), savedAt:new Date($NOWMS)}},{upsert:true})" >/dev/null
EVOUT=$(cline evictor --config $CFG --once 2>&1)
# The control. A sweep that refused to run for health reasons would leave the
# document alone too, and prove nothing at all about tombstones.
chkv "the sweep actually ran"                  "true" \
    "$(grep -q "not healthy enough to evict" <<<"$EVOUT" && echo false || echo true)"
chkv "the sweep left the only live copy alone" "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$TOID')}))")"
H "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$TOID')})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).orders.deleteOne({_id:ObjectId('$TOID')})" >/dev/null

echo "== a sweep narrows a claim and does not invent one =="
# The state every adoption starts in: the archive holds the collection, the fast
# tier has never held any of it, no warm has run, so nothing has claimed
# anything. One sweep used to leave a claim behind saying the fast tier holds
# everything since the eviction boundary, about a tier holding nothing, and a
# sweep that scanned zero documents and evicted zero was enough to do it.
#
# Measured through the router on 4,000 archived orders and an empty fast tier:
# 240 in the last thirty days before the sweep, 0 after it, reported as success.
# The router already running kept the claim it had cached, so this only showed
# itself on the next restart, which is what made it worth a check here.
#
# The claim belongs to whatever put the documents there: the warmer, which knows
# how far back it filled, or an operator who has checked.
WMWAS=$(cline watermark --config $CFG --ns $DB.orders 2>&1 | tail -1)
cline watermark --config $CFG --ns $DB.orders --clear >/dev/null 2>&1
chkv "the claim starts withdrawn"              "true" \
    "$(cline watermark --config $CFG --ns $DB.orders 2>&1 | grep -qc 'claims nothing' && echo true || echo false)"
NOWMS=$(date +%s000)
C "db.getSiblingDB(process.env.TC_META).checkpoints.updateOne({_id:'test'},{\$set:{eventAt:new Date($NOWMS), savedAt:new Date($NOWMS)}})" >/dev/null
C "db.getSiblingDB(process.env.TC_META).positions.updateOne({_id:'test'},{\$set:{at:new Date($NOWMS), savedAt:new Date($NOWMS)}},{upsert:true})" >/dev/null
WMOUT=$(cline evictor --config $CFG --once 2>&1)
# The control again. A sweep that refused on health would leave the claim
# withdrawn too, and prove nothing.
chkv "the sweep actually ran"                  "true" \
    "$(grep -q "not healthy enough to evict" <<<"$WMOUT" && echo false || echo true)"
chkv "and it claimed nothing on its own"       "true" \
    "$(cline watermark --config $CFG --ns $DB.orders 2>&1 | grep -qc 'claims nothing' && echo true || echo false)"
# Put back exactly what was there, so nothing after this suite inherits a
# withdrawn claim.
case "$WMWAS" in
  *"newer than "*) cline watermark --config $CFG --ns $DB.orders \
      --complete-from "${WMWAS##*newer than }" >/dev/null 2>&1 ;;
  *"all of history"*) cline watermark --config $CFG --ns $DB.orders --complete-all >/dev/null 2>&1 ;;
esac

echo "== a claim narrowed and acted on in the same breath =="
# The evictor raises the completeness watermark before removing, so an
# interrupted sweep never leaves a claim wider than the truth. That protects a
# reader who reads the claim fresh. The router does not: it caches one, because
# reading it per query would put an archive round trip on the hot path.
#
# So for a few seconds after the raise there are two copies of the claim and the
# router's is the older, wider one. Removing in that window means it answers
# from a fast tier that no longer holds what it believes it holds.
#
# Measured before the wait existed, with a fast tier claiming all of history and
# a sweep removing 905 of 2,000 documents: the router answered countDocuments
# with 1,095 where the archive holds 2,000, and answered 2,000 again seven
# seconds later. No error either time, which is this system's worst failure.
CNS=tevclaim
stopFeeder 2>/dev/null || true
H "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null
OLDD=$(python3 -c "import datetime;print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=400)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
# Fixed here rather than written as new Date() inside the loop. The loop runs
# the same eval against each tier in its own mongosh, so new Date() gives each
# tier a different instant, and two documents with the same _id and different
# dates are two documents. Nothing here compares that value today, which is
# exactly why it would sit unnoticed until something did.
NOWD=$(python3 -c "import datetime;print(datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))")
for U in "$HOT" "$COLD"; do
  mongosh "$U" --quiet --eval "db.getSiblingDB('$DB').$CNS.insertMany([
    {_id:1, at:new Date('$OLDD')}, {_id:2, at:new Date('$OLDD')},
    {_id:3, at:new Date('$NOWD')}, {_id:4, at:new Date('$NOWD')}])" >/dev/null
done
DB=$DB WDIR=$WDIR python3 - <<'PY'
import os
db, wdir = os.environ['DB'], os.environ['WDIR']
s = open(wdir + '/cline.yaml').read()
head, tail = s[:s.index('evictor:')], s[s.index('warmer:'):]
open(wdir + '/evclaim.yaml', 'w').write(head + """evictor:
  feeder_id: "test"
  rate: 500
  batch_size: 100
  max_feeder_lag: 30s
  rules:
    - ns: "%s.tevclaim"
      older_than: 40m
      date_field: "at"
  backoff:
    max_replication_lag: 60s
    max_cache_dirty: 0.9
    min_write_tickets_free: 0.01
    min_oplog_window: 1m
""" % db + tail)
PY
NOWMS=$(date +%s000)
C "db.getSiblingDB(process.env.TC_META).positions.updateOne({_id:'test'},{\$set:{at:new Date($NOWMS), savedAt:new Date($NOWMS), everyMS:1000}},{upsert:true});
   db.getSiblingDB(process.env.TC_META).checkpoints.updateOne({_id:'test'},{\$set:{eventAt:new Date($NOWMS), savedAt:new Date($NOWMS)}},{upsert:true})" >/dev/null
cline watermark --config $WDIR/evclaim.yaml --ns $DB.$CNS --complete-all >/dev/null 2>&1
cline router --config $WDIR/evclaim.yaml --log-file $LOGDIR/evclaim_r.log >/dev/null 2>&1 & CRP=$!
for _ in $(seq 1 30); do routerUp && break; sleep 1; done
# The router reads once, which is what puts the claim in its cache.
chkv "the router sees all four"                  "4" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({}))")"
cline evictor --config $WDIR/evclaim.yaml --once >/dev/null 2>&1
# Immediately, which is the whole point: no sleep between the sweep and this.
chkv "and still sees all four straight afterwards" "4" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({}))")"
# The control: the sweep really did remove something, or this proves nothing.
chkv "the sweep did remove the old ones"          "2" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({}))")"
chkv "and the archive still holds them all"       "4" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
kill $CRP 2>/dev/null; wait $CRP 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null

echo "== a cursor held open while a sweep empties the fast tier =="
# The completeness claim is read once, when the query arrives. A cursor outlives
# that: every getMore after the first batch is forwarded to the fast tier, and
# the evictor may have taken the documents off it in between. Nothing checked
# whether the decision still held.
#
# Measured before this, on forty documents held by both tiers with a claim over
# all of history: a client took a first batch of three, a sweep took the fast
# tier from forty documents to nought, and the cursor delivered **3 of 40**.
# Nothing had been deleted. The archive held all forty throughout and a query
# issued after the sweep answered forty. Only the cursor was short, and it was
# short in silence.
#
# It is refused rather than corrected, which is this project's rule: a refusal is
# a limitation and a wrong answer is a bug. Correcting it would mean continuing
# the read from the archive at the point the fast tier's cursor reached, which
# the router cannot know without tracking the sort key of everything it has
# handed over, and which is not defined at all for an unsorted query.
# Its own daemons. The section before this one leaves none running, and a
# refusal check against a router that is not there passes on a connection
# error, which is a green check that proves nothing.
pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
sleep 2
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
cline feeder --config $CFG --log-file $LOGDIR/ev_cursor_f.log & CFP=$!
sleep 3
cline router --config $CFG --log-file $LOGDIR/ev_cursor_r.log & CRP=$!
for _ in $(seq 1 30); do routerUp && break; sleep 1; done
chkv "a router is up for this section"             "1" \
    "$(R 'print(db.adminCommand({ping:1}).ok)')"
CNS=tevcursor
H "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null
CMK='Array.from({length:40},(_,i)=>({_id:i+1, n:i+1, when:new Date(Date.UTC(2024,i%12,1+i%27))}))'
C "db.getSiblingDB(process.env.TC_DB).$CNS.insertMany($CMK)" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$CNS.insertMany($CMK)" >/dev/null
cline watermark --config $CFG --ns $DB.$CNS --complete-all --force >/dev/null 2>&1
sleep 2
chkv "both tiers hold all forty"                    "40" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({}))")"
chkv "and the router answers forty"                 "40" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({}))")"
# The evictor runs from the shell rather than from inside mongosh, which cannot
# start a process, and it starts only once the cursor says it is open.
#
# It used to start first and sweep every two seconds, which is a race the shell
# could not see. mongosh takes a second or three to reach the first batch and
# longer under a parallel sweep, so a sweep could land before there was a cursor
# at all: the claim was already narrow when the query arrived, the router sent it
# to the archive, and the answer came back whole and unrefused from a cursor
# eviction never touched. That is the right answer to a different question, and
# it was reported as the guard failing. Seen twice under contention, delivering
# 40 of 40 with the evictor's log showing the watermark already raised.
#
# So the cursor opens the gate and the shell sweeps behind it. The wait below is
# still on the fast tier's own count rather than a sleep.
GATE=evgate
export TC_HOT="$HOT"
openGate() { # $1 marker, $2 evictor log; echoes the evictor's pid
  H "db.getSiblingDB(process.env.TC_DB).$GATE.deleteMany({_id:'$1'})" >/dev/null
}
waitGate() { # $1 marker
  for _ in $(seq 1 60); do
    [ "$(H "print(db.getSiblingDB(process.env.TC_DB).$GATE.countDocuments({_id:'$1'}))")" = "1" ] && return 0
    sleep 0.5
  done
  return 1
}
openGate find
CUROUT_F=$LOGDIR/ev_cursor.out
( R "
  const c = db.getSiblingDB(process.env.TC_DB).$CNS;
  const cur = c.find({}).sort({_id:1}).batchSize(3);
  let first = 0;
  for (let i=0;i<3 && cur.hasNext();i++) { cur.next(); first++; }
  const hot = Mongo(process.env.TC_HOT).getDB(process.env.TC_DB);
  const before = hot.$CNS.countDocuments({});
  // The cursor is open and the first batch is in hand: sweep now.
  hot.$GATE.insertOne({_id:'find'});
  for (let i=0;i<60 && hot.$CNS.countDocuments({}) > 10; i++) sleep(500);
  const left = hot.$CNS.countDocuments({});
  let got = first, refused = false;
  try { while (cur.hasNext()) { cur.next(); got++; } }
  catch (e) { refused = /^thermocline:/.test(String(e.message)); }
  print(before + '/' + left + '/' + got + '/' + refused);" > "$CUROUT_F" ) & CURPID=$!
waitGate find
cline evictor --config $CFG --every 2s --log-file $LOGDIR/ev_cursor.log & CEVP=$!
wait $CURPID
kill $CEVP 2>/dev/null; wait $CEVP 2>/dev/null
CUROUT=$(tail -1 "$CUROUT_F")
CUR_BEFORE=$(echo "$CUROUT" | cut -d/ -f1)
echo "  the fast tier held $CUR_BEFORE when the cursor opened and $(echo "$CUROUT" | cut -d/ -f2) after, and the cursor delivered $(echo "$CUROUT" | cut -d/ -f3)"
# The precondition the rest of the section rests on, asserted rather than
# assumed: there is no cursor to protect if the sweep got there first.
chkv "the cursor opened while the fast tier still held them" "true" \
    "$([ "$CUR_BEFORE" -gt 10 ] 2>/dev/null && echo true || echo false)"
chkv "the sweep did empty the fast tier"            "true" \
    "$([ "$(echo "$CUROUT" | cut -d/ -f2)" -lt 10 ] 2>/dev/null && echo true || echo false)"
chkv "the getMore is refused, not answered short"   "true" "$(echo "$CUROUT" | cut -d/ -f4)"
# And the remedy the refusal names has to work.
chkv "re-running the query answers in full"         "40" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$CNS.find({}).sort({_id:1}).batchSize(3).itcount())")"
chkv "the archive kept every one of them"           "40" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
H "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null

echo "== the same, for an aggregation, which is judged before it runs =="
# The twin of the section above, and it was open. A find is inspected on the way
# back, so the claim that authorised it was to hand and could be remembered
# beside the cursor. An aggregation is judged before it runs, because it can end
# in $out, and the claim it was judged against was read in a different function
# and thrown away. Nothing registered the cursor, so nothing could refuse its
# getMore.
#
# It is the same wrong answer and it is easier to reach: mongod puts 101
# documents in an aggregation's first batch and a cursor holds the rest, so any
# pipeline over a collection larger than that leaves one, without the client
# asking for a batchSize at all. That is the shape a BI tool sends.
#
# Measured before the fix on the fixture below: the fast tier went to nought
# under an open cursor and the aggregation delivered 3 of 40, with ok: 1.
CNS=tevaggcur
H "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$CNS.insertMany($CMK)" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$CNS.insertMany($CMK)" >/dev/null
cline watermark --config $CFG --ns $DB.$CNS --complete-all --force >/dev/null 2>&1
# A router of its own, started after the claim is recorded. The one the section
# above left running has already read this namespace and cached the answer it
# got then, which was that nothing claimed it: the router caches a claim for
# watermark.ClaimCacheTTL and re-asks a "no" after ten seconds. Measured with
# the section sharing that router: the aggregation was judged incomplete, went
# to the archive, and delivered all forty from a cursor the router owns. Forty
# is the right number and it proves nothing at all about the fast tier's cursor,
# which is what this section is here to test.
kill $CRP 2>/dev/null; wait $CRP 2>/dev/null
cline router --config $CFG --log-file $LOGDIR/ev_aggcursor_r.log & CRP=$!
for _ in $(seq 1 30); do routerUp && break; sleep 1; done
chkv "both tiers hold all forty"                    "40" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({}))")"
# And the fast tier is what answers it, which is the precondition the whole
# section rests on. An aggregation the router sends to the archive is answered
# in full from a cursor of its own, and it would pass every check below for the
# wrong reason: forty documents and no refusal, from a cursor eviction cannot
# touch. Asked of explain, because that is where the router already publishes
# this verdict: a plan it leaves untouched is one the fast tier can answer
# whole, and a thermocline field on it says the opposite.
chkv "and the fast tier is what will answer it"     "false" \
    "$(R "print(!!db.getSiblingDB(process.env.TC_DB).$CNS.explain().aggregate([{\$sort:{_id:1}}]).thermocline)")"
openGate agg
AGGOUT_F=$LOGDIR/ev_aggcursor.out
( R "
  const c = db.getSiblingDB(process.env.TC_DB).$CNS;
  const cur = c.aggregate([{\$sort:{_id:1}}], {cursor:{batchSize:3}});
  let first = 0;
  for (let i=0;i<3 && cur.hasNext();i++) { cur.next(); first++; }
  const hot = Mongo(process.env.TC_HOT).getDB(process.env.TC_DB);
  // What the fast tier holds with the cursor open and the first batch in hand.
  //
  // The evictor was started from the shell just above, and mongosh takes a
  // second or three to get this far. Under a parallel sweep it takes longer
  // than the two the evictor pauses for, so the sweep can finish before the
  // cursor is opened at all: the claim is already narrow, the router sends the
  // aggregation to the archive, and it comes back whole and unrefused from a
  // cursor eviction never touched. That is the right answer to a different
  // question, and it was reported as the guard failing.
  //
  // Measured under contention twice: 40 delivered and no refusal, with the
  // evictor's log showing one sweep and the watermark already raised. Printed
  // here so the run says which question it answered.
  const before = hot.$CNS.countDocuments({});
  hot.$GATE.insertOne({_id:'agg'});
  for (let i=0;i<60 && hot.$CNS.countDocuments({}) > 10; i++) sleep(500);
  const left = hot.$CNS.countDocuments({});
  let got = first, refused = false;
  try { while (cur.hasNext()) { cur.next(); got++; } }
  catch (e) { refused = /^thermocline:/.test(String(e.message)); }
  print(before + '/' + left + '/' + got + '/' + refused);" > "$AGGOUT_F" ) & AGGPID=$!
waitGate agg
cline evictor --config $CFG --every 2s --log-file $LOGDIR/ev_aggcursor.log & AEVP=$!
wait $AGGPID
kill $AEVP 2>/dev/null; wait $AEVP 2>/dev/null
AGGOUT=$(tail -1 "$AGGOUT_F")
AGG_BEFORE=$(echo "$AGGOUT" | cut -d/ -f1)
AGG_LEFT=$(echo "$AGGOUT" | cut -d/ -f2)
AGG_GOT=$(echo "$AGGOUT" | cut -d/ -f3)
AGG_REFUSED=$(echo "$AGGOUT" | cut -d/ -f4)
echo "  the fast tier held $AGG_BEFORE when the cursor opened and $AGG_LEFT after, and the aggregation delivered $AGG_GOT"
# The precondition, asserted rather than assumed. Everything below is about a
# cursor the fast tier was answering when it was opened, and if the sweep got
# there first there is no such cursor and nothing to say about it.
chkv "the cursor opened while the fast tier still held them" "true" \
    "$([ "$AGG_BEFORE" -gt 10 ] 2>/dev/null && echo true || echo false)"
chkv "the sweep did empty the fast tier"            "true" \
    "$([ "$AGG_LEFT" -lt 10 ] 2>/dev/null && echo true || echo false)"
chkv "the aggregation getMore is refused too"       "true" "$AGG_REFUSED"
chkv "re-running the aggregation answers in full"   "40" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$CNS.aggregate([{\$sort:{_id:1}}],{cursor:{batchSize:3}}).itcount())")"
H "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null
kill $CRP $CFP 2>/dev/null; wait $CRP 2>/dev/null; wait $CFP 2>/dev/null

echo "== documents the rule can never remove =="
# The whole-namespace version of this is already refused: a rule with no
# date_field against non-ObjectId _ids matches nothing at all. The partial
# version is quieter and reads as success. MongoDB brackets a range predicate by
# type, so a cutoff on a date selects only documents whose field holds a date,
# and one whose field is missing, null or a string is invisible to every sweep.
#
# Measured on a fast tier of three documents, one properly dated and two not:
# "sweep complete, scanned 1, evicted 1", and the two that cannot be aged stayed
# where they were with nothing mentioning them. That is a fast tier growing
# without bound while every sweep reports success.
ENS=tevunaged
OLDDATE=$(python3 -c "import datetime;print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=400)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
H "db.getSiblingDB(process.env.TC_DB).$ENS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$ENS.drop()" >/dev/null
for U in "$HOT" "$COLD"; do
  mongosh "$U" --quiet --eval "db.getSiblingDB('$DB').$ENS.insertMany([
    {_id:1, at:new Date('$OLDDATE'), n:'old and evictable'},
    {_id:2, at:'$OLDDATE',           n:'old, but a string'},
    {_id:3,                          n:'no field at all'}])" >/dev/null
done
DB=$DB WDIR=$WDIR python3 - <<'PY'
import os
db, wdir = os.environ['DB'], os.environ['WDIR']
s = open(wdir + '/cline.yaml').read()
head, tail = s[:s.index('evictor:')], s[s.index('warmer:'):]
open(wdir + '/evunaged.yaml', 'w').write(head + """evictor:
  feeder_id: "test"
  rate: 500
  batch_size: 100
  max_feeder_lag: 30s
  rules:
    - ns: "%s.tevunaged"
      older_than: 40m
      date_field: "at"
  backoff:
    max_replication_lag: 60s
    max_cache_dirty: 0.9
    min_write_tickets_free: 0.01
    min_oplog_window: 1m
""" % db + tail)
PY
NOWMS=$(date +%s000)
C "db.getSiblingDB(process.env.TC_META).positions.updateOne({_id:'test'},{\$set:{at:new Date($NOWMS), savedAt:new Date($NOWMS), everyMS:1000}},{upsert:true});
   db.getSiblingDB(process.env.TC_META).checkpoints.updateOne({_id:'test'},{\$set:{eventAt:new Date($NOWMS), savedAt:new Date($NOWMS)}},{upsert:true})" >/dev/null
rm -f $LOGDIR/ev_unaged.log
cline evictor --config $WDIR/evunaged.yaml --once --log-file $LOGDIR/ev_unaged.log >/dev/null 2>&1
chkv "the sweep removed the one it could"        "2" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$ENS.countDocuments({}))")"
chkv "and says the other two can never go"       "true" \
    "$(grep -q 'this rule can never remove' $LOGDIR/ev_unaged.log && echo true || echo false)"
chkv "counting them by the field they lack a date in" "true" \
    "$(grep -q 'not_dated_by_at=2' $LOGDIR/ev_unaged.log && echo true || echo false)"
chkv "and says the fast tier grows by that much" "true" \
    "$(grep -q 'grows by that much' $LOGDIR/ev_unaged.log && echo true || echo false)"
# And the one type bracketing does not keep out. A range predicate on an array
# matches when *any* element matches, so a document whose age field holds
# [old, new] is older than the cutoff and newer than it at the same time. The
# claim says the fast tier holds every live document newer than the cutoff, and
# removing this one makes that false.
#
# Measured before it was excluded, on four documents with one holding
# [four hours ago, thirty seconds ago]: the archive answered 1,2,3,9 for a read
# bounded at the claim and the router answered 1,2,3. A short answer inside the
# range the claim promises, with no error on it.
#
# It is left where it is, which is what this rule already does for every other
# shape it cannot address, and it is counted separately because the reason is
# the opposite one: those cannot be selected at all, and this one could be.
NOWD=$(python3 -c "import datetime;print(datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))")
for U in "$HOT" "$COLD"; do
  mongosh "$U" --quiet --eval "db.getSiblingDB('$DB').$ENS.insertOne(
    {_id:9, at:[new Date('$OLDDATE'), new Date('$NOWD')], n:'old and new at once'})" >/dev/null
done
rm -f $LOGDIR/ev_unagedarr.log
cline evictor --config $WDIR/evunaged.yaml --once --log-file $LOGDIR/ev_unagedarr.log >/dev/null 2>&1
chkv "an array age is not removed for its old half"  "true" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$ENS.countDocuments({_id:9}) === 1)")"
chkv "and the sweep says why, counting it"           "true" \
    "$(grep -q 'array_at=1' $LOGDIR/ev_unagedarr.log && echo true || echo false)"
chkv "  naming what removing it would cost"          "true" \
    "$(grep -q 'bounded inside the completeness claim short' $LOGDIR/ev_unagedarr.log && echo true || echo false)"
H "db.getSiblingDB(process.env.TC_DB).$ENS.deleteOne({_id:9})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$ENS.deleteOne({_id:9})" >/dev/null

# The control: a namespace whose documents are all properly dated must stay
# quiet, or the warning is noise on every start and nobody reads it.
H "db.getSiblingDB(process.env.TC_DB).$ENS.deleteMany({at:{\$not:{\$type:'date'}}})" >/dev/null
rm -f $LOGDIR/ev_unaged2.log
cline evictor --config $WDIR/evunaged.yaml --once --log-file $LOGDIR/ev_unaged2.log >/dev/null 2>&1
chkv "and says nothing when they are all dated"  "0" \
    "$(grep -c 'this rule can never remove' $LOGDIR/ev_unaged2.log || true)"
H "db.getSiblingDB(process.env.TC_DB).$ENS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$ENS.drop()" >/dev/null

echo "== a restored archive under a live fast tier =="
# The reason `cline pause` exists, and the reason the backup procedure puts it
# before the restore rather than mentioning it afterwards.
#
# The evictor's per document check is presence only. An archive restored to an
# older backup still *has* the document, so the check passes and the fast tier's
# newer copy is removed. The writes made since the backup are then in neither
# tier: the restore costs them rather than recovering them.
#
# Both arms, because the guard is only worth having if the danger is real.
# Measured: paused, the evictor removes nothing; unpaused, all five newer copies
# go and the archive holds the older ones.
RNS=trestore
kill $FP 2>/dev/null; wait $FP 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).$RNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$RNS.drop()" >/dev/null
cline feeder --config $CFG --log-file $LOGDIR/ev_restore_f.log & RFP=$!
sleep 3
# Minted two hours ago, so the rule below reaches them.
H "const ts=Math.floor((Date.now()-7200000)/1000).toString(16).padStart(8,'0');
   const d=[]; for(let i=0;i<5;i++) d.push({_id:new ObjectId(ts+i.toString(16).padStart(16,'0')), v:'v1'});
   db.getSiblingDB(process.env.TC_DB).$RNS.insertMany(d)" >/dev/null
for _ in $(seq 1 40); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({}))")" = "5" ] && break
  sleep 1
done
chkv "five documents mirrored at v1"              "5"     "$(C "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({v:'v1'}))")"
rm -rf $WDIR/restbak
mongodump --uri="$COLD" --db $DB --collection $RNS -o $WDIR/restbak --quiet >/dev/null 2>&1
H "db.getSiblingDB(process.env.TC_DB).$RNS.updateMany({},{\$set:{v:'v2'}})" >/dev/null
for _ in $(seq 1 40); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({v:'v2'}))")" = "5" ] && break
  sleep 1
done
chkv "then updated to v2, and that mirrored too"  "5"     "$(C "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({v:'v2'}))")"
kill $RFP 2>/dev/null; wait $RFP 2>/dev/null
mongorestore --uri="$COLD" --drop $WDIR/restbak --quiet >/dev/null 2>&1
chkv "the restore puts the archive back to v1"    "5"     "$(C "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({v:'v1'}))")"
sed "/^evictor:/,/^warmer:/ s|^  rules:|  rules:\n    - ns: \"$DB.$RNS\"\n      older_than: 40m|" $CFG > $WDIR/ev_restore.yaml
cline watermark --config $WDIR/ev_restore.yaml --ns $DB.$RNS --complete-all >/dev/null 2>&1
cline pause --config $WDIR/ev_restore.yaml --reason "restoring the archive" >/dev/null 2>&1
cline evictor --config $WDIR/ev_restore.yaml --once >/dev/null 2>&1
chkv "paused, the fast tier keeps its newer copies" "5"     "$(H "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({v:'v2'}))")"
cline resume --config $WDIR/ev_restore.yaml >/dev/null 2>&1
cline evictor --config $WDIR/ev_restore.yaml --once >/dev/null 2>&1
# The documented loss, pinned deliberately. If this ever stops being true
# because the evictor learned to compare versions rather than presence, this
# check should fail and be rewritten on purpose, not quietly pass.
chkv "unpaused, it removes them: this is the loss" "0"     "$(H "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({v:'v2'}))")"
chkv "and the newer value is in neither tier"      "0"     "$(C "print(db.getSiblingDB(process.env.TC_DB).$RNS.countDocuments({v:'v2'}))")"
H "db.getSiblingDB(process.env.TC_DB).$RNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$RNS.drop()" >/dev/null
rm -rf $WDIR/restbak

echo
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
