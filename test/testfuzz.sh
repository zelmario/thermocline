#!/bin/bash
# Runner for the randomized differential test. See test/fuzz.js for what it
# generates and why; this file builds the two collections it compares and calls
# it once per round with its own seed.
#
# The seeds are fixed by default, so a sweep runs the same operations every time
# and a failure is something that changed rather than something that came up.
# Pass TC_SEED to explore instead, TC_ROUNDS and TC_OPS to run longer. The seed
# of every round is printed, and fuzz.js generates from it alone, so a
# disagreement can be asked for again.
#
# A round is one mongosh process running many operations, because mongosh costs
# about a second of startup and the database work per operation is microseconds.
# Rounds exist for one reason: the writes warm the tiered collection up, so
# after a while nothing is archive-only any more and the comparison stops
# proving anything. Each round puts the split back.
WDIR=${WDIR:-/root}
LOGDIR=${LOGDIR:-/var/log/cline}
PORT=${PORT:-27100}
# The strict router below needs a port of its own. It used to be spelled $P1
# without anything defining it, so cline was told --listen 0.0.0.0: and mongosh
# was told --port with nothing after it, which means its default of 27017: the
# node's own mongod. Every check in that section then passed or failed against a
# plain MongoDB with no router in the path at all.
DB=${DB:-shop}
META=${META:-_thermocline}
P1=$((PORT+1))
CFG=${CFG:-$WDIR/cline.yaml}
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
ROUNDS=${TC_ROUNDS:-4}
OPS=${TC_OPS:-40}
SEED0=${TC_SEED:-1}
export TC_DB=$DB TC_META=$META TC_COLD=$COLD TC_HOT=$HOT TC_OPS=$OPS
HERE=$(cd "$(dirname "$0")" && pwd)

PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-44s %.30s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-44s\n          want=%.150s\n          got =%.150s\n' "$1" "$2" "$3"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT"  --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/fuzz_f.log &
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/fuzz_r.log &
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}

# Sixty documents, the same shape test/fuzz.js generates predicates for: numbers
# a string, an array, an array of subdocuments, a subdocument, a date, and a
# field only some of them carry.
DOCS='Array.from({length:60},(_,i)=>({
  _id: i+1,
  n: i+1,
  f: (i+1)/4,
  name: "user" + String(i+1).padStart(2,"0"),
  grp: ["alpha","beta","gamma"][i%3],
  tags: ["t"+((i%3)+1), "common"],
  items: [{sku:"s"+(i%4), qty:i+1},{sku:"s9", qty:2}],
  meta: {score: (i*7)%13, active: i%4!==0},
  when: new Date(Date.UTC(2024, i%12, 1+(i%27), 13,45,30,250))
}))'
OPT='[3,9,15,21,27,33,39,45,51,57]'

reseed() {
  RESEED=$(mongosh "$HOT" --quiet --eval "
    const hot  = db.getSiblingDB(process.env.TC_DB);
    const cold = Mongo(process.env.TC_COLD).getDB(process.env.TC_DB);
    function mkIndexes(c) {
      c.createIndex({n:1}); c.createIndex({grp:1}); c.createIndex({when:1});
      c.createIndex({'meta.score':1}); c.createIndex({tags:1});
    }
    const all = $DOCS;
    hot.tcref.drop();
    hot.tcref.insertMany(all);
    hot.tcref.updateMany({_id:{\$in:$OPT}},{\$set:{opt:'yes'}});

    // The second pair, for the joins. Forty documents over the same group
    // values, so a join on grp matches, and its own split so the joined side
    // is short on the fast tier too.
    const other = all.slice(0,40).map(x => Object.assign({}, x, {_id: 1000 + x._id}));
    hot.tcref2.drop();
    hot.tcref2.insertMany(other);
    hot.tcfuzz2.drop();
    cold.tcfuzz2.drop();
    hot.tcfuzz2.insertMany(other.slice(0,20));
    cold.tcfuzz2.bulkWrite(
      other.map(d=>({replaceOne:{filter:{_id:d._id}, replacement:d, upsert:true}})),
      {ordered:false});

    hot.tcfuzz.drop();
    cold.tcfuzz.drop();
    // 1 to 30 on both tiers, 31 to 60 only in the archive, which is what an
    // evicted document looks like. Upserts on the archive, not insertMany: a
    // late mirror of the fast tier can put documents there first and an ordered
    // insert then abandons the whole seed on the first duplicate.
    hot.tcfuzz.insertMany(all.slice(0,30));
    cold.tcfuzz.bulkWrite(
      all.map(d=>({replaceOne:{filter:{_id:d._id}, replacement:d, upsert:true}})),
      {ordered:false});
    hot.tcfuzz.updateMany({_id:{\$in:$OPT}},{\$set:{opt:'yes'}});
    cold.tcfuzz.updateMany({_id:{\$in:$OPT}},{\$set:{opt:'yes'}});
    mkIndexes(hot.tcref); mkIndexes(hot.tcfuzz); mkIndexes(cold.tcfuzz);
    mkIndexes(hot.tcref2); mkIndexes(hot.tcfuzz2); mkIndexes(cold.tcfuzz2);

    // Block until the feeder has processed everything above, proved by a
    // sentinel reaching the archive. The claim below has to be written after
    // the feeder has seen the drop, or the withdrawal lands on top of it.
    hot.tcfuzz.insertOne({_id:'__sentinel'});
    hot.tcfuzz2.insertOne({_id:'__sentinel'});
    let caught = false;
    // Two minutes, not one. This is a wait rather than an assertion, and on a
    // machine sharing itself with another cluster a minute was not enough: the
    // reseed gave up, every round after it reported no tally, and the suite
    // failed for want of patience rather than for anything it tests.
    for (let i = 0; i < 240; i++) {
      if (cold.tcfuzz.countDocuments({_id:'__sentinel'}) === 1 &&
          cold.tcfuzz2.countDocuments({_id:'__sentinel'}) === 1) { caught = true; break; }
      sleep(500);
    }
    hot.tcfuzz.deleteOne({_id:'__sentinel'});
    cold.tcfuzz.deleteOne({_id:'__sentinel'});
    hot.tcfuzz2.deleteOne({_id:'__sentinel'});
    cold.tcfuzz2.deleteOne({_id:'__sentinel'});
    print(caught ? 'RESEEDED' : 'FEEDER-NEVER-CAUGHT-UP');
  " 2>&1 | tail -1)
  [ "$RESEED" = "RESEEDED" ] || echo "  WARN  reseed: $RESEED"
  # Complete from 2030 means the fast tier claims nothing here is complete, so
  # every read has to be resolved across both tiers. That is the point.
  cline watermark --config $CFG --ns $DB.tcfuzz  --complete-from 2030-01-01T00:00:00Z >/dev/null
  cline watermark --config $CFG --ns $DB.tcfuzz2 --complete-from 2030-01-01T00:00:00Z >/dev/null
}

# Seeds the same documents with the fast tier holding every one of them, and a
# claim that says so. For the strict section below, where the question is not
# whether the answer is right but whether the router agrees to give one.
reseedComplete() {
  RC=$(mongosh "$HOT" --quiet --eval "
    const hot  = db.getSiblingDB(process.env.TC_DB);
    const cold = Mongo(process.env.TC_COLD).getDB(process.env.TC_DB);
    const all = $DOCS;
    const other = all.slice(0,40).map(x => Object.assign({}, x, {_id: 1000 + x._id}));
    for (const [h, c, docs] of [[hot.tcfuzz, cold.tcfuzz, all], [hot.tcfuzz2, cold.tcfuzz2, other]]) {
      h.drop(); c.drop();
      h.insertMany(docs);
      c.bulkWrite(docs.map(d=>({replaceOne:{filter:{_id:d._id}, replacement:d, upsert:true}})), {ordered:false});
    }
    hot.tcref.drop();  hot.tcref.insertMany(all);
    hot.tcref2.drop(); hot.tcref2.insertMany(other);
    hot.tcfuzz.insertOne({_id:'__sentinel'});
    let caught = false;
    for (let i = 0; i < 240; i++) {
      if (cold.tcfuzz.countDocuments({_id:'__sentinel'}) === 1) { caught = true; break; }
      sleep(500);
    }
    hot.tcfuzz.deleteOne({_id:'__sentinel'});
    cold.tcfuzz.deleteOne({_id:'__sentinel'});
    print(caught ? 'RESEEDED' : 'FEEDER-NEVER-CAUGHT-UP');
  " 2>&1 | tail -1)
  [ "$RC" = "RESEEDED" ] || echo "  WARN  reseedComplete: $RC"
  cline watermark --config $CFG --ns $DB.tcfuzz  --complete-all >/dev/null
  cline watermark --config $CFG --ns $DB.tcfuzz2 --complete-all >/dev/null
}

echo "== setup =="
stopDaemons
C "db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'});
   db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({});
   db.getSiblingDB(process.env.TC_META).residency.deleteMany({});
   db.getSiblingDB(process.env.TC_META).evicting.deleteMany({})" >/dev/null
startFeeder
startRouter
reseed
sleep 5
chk "the reference holds sixty"        "60" "$(R "print(db.getSiblingDB(process.env.TC_DB).tcref.countDocuments({}))")"
chk "the tiered fast tier holds thirty" "30" "$(H "print(db.getSiblingDB(process.env.TC_DB).tcfuzz.countDocuments({}))")"
chk "its archive holds sixty"          "60" "$(C "print(db.getSiblingDB(process.env.TC_DB).tcfuzz.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
chk "and the router answers sixty"     "60" "$(R "print(db.getSiblingDB(process.env.TC_DB).tcfuzz.countDocuments({}))")"
chk "the joined pair holds forty too"  "40" "$(R "print(db.getSiblingDB(process.env.TC_DB).tcfuzz2.countDocuments({}))")"
chk "and its reference does"           "40" "$(R "print(db.getSiblingDB(process.env.TC_DB).tcref2.countDocuments({}))")"

# The negative control, and the reason to trust everything below it. A harness
# that compares two collections and reports agreement is worth nothing until it
# has been shown reporting a disagreement that is really there. Five documents
# the fast tier does not hold are removed from the archive, so the tiered
# collection is genuinely short of the reference, and a read that reaches them
# has to say so. Reads only: a write would copy documents up and hide it.
echo
echo "== the comparison can see a divergence that is really there =="
C "print(db.getSiblingDB(process.env.TC_DB).tcfuzz.deleteMany({_id:{\$in:[41,42,43,44,45]}}).deletedCount)" >/dev/null
NEG=$(TC_SEED=$((SEED0+13)) TC_OPS=30 TC_READONLY=1 mongosh --port $PORT -u dba -p secret         --authenticationDatabase admin --quiet --file "$HERE/fuzz.js" 2>&1 | grep -m1 '^AGREED ')
echo "  $NEG"
set -- $NEG
chk "five documents hidden from the archive are noticed" "yes" "$([ "${8:-0}" -gt 0 ] && echo yes || echo no)"
# Put the five back before anything else runs. Without this the first round
# inherits the damage and reports sixteen disagreements that were put there on
# purpose, which reads exactly like a batch of real ones.
reseed

TA=0; TR=0; TS=0; TD=0
for r in $(seq 1 $ROUNDS); do
  SEED=$((SEED0 + r*7919))
  echo
  echo "== round $r of $ROUNDS, seed $SEED, $OPS operations =="
  [ $r -gt 1 ] && reseed
  OUT=$(TC_SEED=$SEED mongosh --port $PORT -u dba -p secret --authenticationDatabase admin \
          --quiet --file "$HERE/fuzz.js" 2>&1)
  # The continuation lines carry the two answers and start with a real tab.
  # grep -E reads \t as a literal t, so the pattern that looked like it matched
  # them dropped exactly the part of a disagreement worth reading.
  # TC_TRACE prints every operation the round ran, in order, before the report.
  # A disagreement is usually about state something earlier left behind, and the
  # report names only the operation that noticed.
  [ "${TC_TRACE:-0}" = "1" ] && echo "$OUT" | grep -E "^  (op|wait)$(printf '\t')"
  echo "$OUT" | grep -E "^  HARNESS|^  txn$(printf '\t')|^DISAGREE|^REFUSAL|^STALE|^$(printf '\t')" | head -60
  # What the round actually compared, as shape=done/vacuous. A clean tally over
  # a grammar that never generated a shape is not evidence about that shape.
  echo "$OUT" | grep -m1 '^SHAPES ' | fold -w 150 | sed 's/^/  /'
  LINE=$(echo "$OUT" | grep -m1 '^AGREED ')
  if [ -z "$LINE" ]; then
    echo "  FATAL  the round produced no tally:"; echo "$OUT" | tail -20
    FAIL=$((FAIL+1)); FAILED+=("round $r produced no tally"); continue
  fi
  set -- $LINE
  echo "  $LINE"
  TA=$((TA+$2)); TR=$((TR+$4)); TS=$((TS+$6)); TD=$((TD+$8))
  chk "round $r: no disagreement" "0" "$8"
done

echo
echo "== nothing is refused when the fast tier holds it all =="
# Everything above asks whether the answer is right. This asks whether the
# router agrees to give one at all. With completeness: strict it refuses
# anything it cannot prove the fast tier can answer whole, and with a fast tier
# that holds every document there is nothing it cannot. So a refusal here is
# not a wrong answer, it is the guard being more cautious than the data
# requires, and this is the only configuration where that shows.
sed 's/completeness: archive/completeness: strict/' $CFG > $WDIR/fuzz_strict.yaml
reseedComplete
cline router --config $WDIR/fuzz_strict.yaml --listen 0.0.0.0:$P1 --log-file $LOGDIR/fuzz_strict.log &
for _ in $(seq 1 30); do
  mongosh --port $P1 -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
# isdbgrid, not ping. A ping answers on anything, and what this has to
# establish is that the thing on that port is the strict router rather than a
# mongod that happens to be listening.
chk "a strict router is up"            "isdbgrid" "$(mongosh --port $P1 -u dba -p secret --authenticationDatabase admin --quiet --eval 'print(db.hello().msg)' 2>&1 | tail -1)"
chk "and the fast tier holds them all" "60" "$(H "print(db.getSiblingDB(process.env.TC_DB).tcfuzz.countDocuments({}))")"
SOUT=$(TC_SEED=$((SEED0+4242)) TC_OPS=$OPS mongosh --port $P1 -u dba -p secret \
         --authenticationDatabase admin --quiet --file "$HERE/fuzz.js" 2>&1)
echo "$SOUT" | grep -E '^  HARNESS|^DISAGREE|^REFUSAL|^\t' | head -40
echo "$SOUT" | grep -m1 '^SHAPES ' | fold -w 150 | sed 's/^/  /'
SLINE=$(echo "$SOUT" | grep -m1 '^AGREED ')
echo "  $SLINE"
set -- $SLINE
chk "strict: nothing refused"      "0" "${4:-none}"
chk "strict: no disagreement"      "0" "${8:-none}"

echo
echo "== totals =="
echo "  agreed $TA   refused $TR   settled on a retry $TS   disagreed $TD"
stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
for f in "${FAILED[@]}"; do echo "  - $f"; done
[ $FAIL -eq 0 ]
