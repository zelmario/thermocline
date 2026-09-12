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
# oneRound runs the generator once and reports what it found. Named because the
# churn section below runs the same thing with the tiers moving underneath it,
# and a second copy of this loop would drift from the first.
#   $1  the label for the round
#   $2  the seed
oneRound() {
  local label="$1" SEED="$2"
  echo
  echo "== $label, seed $SEED, $OPS operations =="
  OUT=$(TC_SEED=$SEED mongosh --port $PORT -u dba -p secret --authenticationDatabase admin \
          --quiet --file "$HERE/fuzz.js" 2>&1)
  # The continuation lines carry the two answers and start with a real tab.
  # grep -E reads \t as a literal t, so the pattern that looked like it matched
  # them dropped exactly the part of a disagreement worth reading.
  # TC_TRACE prints every operation the round ran, in order, before the report.
  # A disagreement is usually about state something earlier left behind, and the
  # report names only the operation that noticed.
  [ "${TC_TRACE:-0}" = "1" ] && echo "$OUT" | grep -E "^  (op|wait)$(printf '\t')"
  # VOID is in this list because a line nothing prints is a line nobody reads:
  # the contents comparison stopping matters more than most of what is here.
  echo "$OUT" | grep -E "^  HARNESS|^  VOID$(printf '\t')|^  txn$(printf '\t')|^DISAGREE|^REFUSAL|^STALE|^$(printf '\t')" | head -60
  # What the round actually compared, as shape=done/vacuous. A clean tally over
  # a grammar that never generated a shape is not evidence about that shape.
  # -s so it breaks at spaces. Plain fold -w splits shape names down the middle,
  # which turns the one line that says what was actually exercised into tokens
  # like "ndOneAndDelete" and "ter_updateMany": unreadable by a person and
  # unparseable by anything counting coverage, which is the only thing it is for.
  echo "$OUT" | grep -m1 '^SHAPES ' | fold -s -w 150 | sed 's/^/  /'
  LINE=$(echo "$OUT" | grep -m1 '^AGREED ')
  if [ -z "$LINE" ]; then
    echo "  FATAL  the round produced no tally:"; echo "$OUT" | tail -20
    FAIL=$((FAIL+1)); FAILED+=("round $r produced no tally"); continue
  fi
  set -- $LINE
  echo "  $LINE"
  TA=$((TA+$2)); TR=$((TR+$4)); TS=$((TS+$6)); TD=$((TD+$8))
  chk "$label: no disagreement" "0" "$8"
}

for r in $(seq 1 $ROUNDS); do
  [ $r -gt 1 ] && reseed
  oneRound "round $r of $ROUNDS" $((SEED0 + r*7919))
done

# TC_ONLY=rounds stops here.
#
# A disagreement that reproduces some of the time has to be run again and again,
# and the four sections below take about four minutes between them. None of them
# is about the seed under investigation, and a run that costs four minutes is a
# run nobody does twenty times. The sweep sets nothing, so it still runs all of
# it.
if [ "${TC_ONLY:-}" = "rounds" ]; then
  echo
  echo "  agreed $TA   refused $TR   settled on a retry $TS   disagreed $TD"
  stopDaemons
  echo "PASS $PASS   FAIL $FAIL"
  for f in "${FAILED[@]}"; do echo "  - $f"; done
  [ $FAIL -eq 0 ]
  exit
fi

echo
echo "== the same operations with the tiers moving underneath them =="
# Every round above ran with documents moving one way only: a read or a write
# that needs an archived document copies it up, and nothing ever takes one back.
# That is half of what this system does. The other half is the evictor, and it
# removes documents from the fast tier while clients are reading and writing
# them.
#
# It is the shape behind the worst defect this project has had, where an
# eviction sweep narrowed a claim and acted on it in the same breath and the
# router answered 1,095 of 2,000 for five seconds. The hand-written suite for
# that asks the questions somebody thought of; this asks the ones nobody did,
# against a collection that is being drained as it is asked.
#
# The comparison is unchanged and still sound: the archive holds every document
# whatever the evictor does, the untiered reference is not touched by it at all,
# and both sides must still answer the same. What moves is only which tier the
# answer has to come from, which is the thing being tested.
CHURN_ROUNDS=${TC_CHURN_ROUNDS:-2}
if [ "$CHURN_ROUNDS" -gt 0 ]; then
  # An eviction rule for the tiered collection alone, appended to the evictor's
  # own rules. Aged by `when`, because these documents carry integer _ids and a
  # rule with no date_field reads the age out of an ObjectId timestamp, which
  # they do not have. Every document is dated 2024, so every one of them is
  # older than the window and the sweep drains what the copy-ups put back.
  # residency goes with the rule. A document the router copied up is held
  # against the next sweep for router.residency, five minutes in this config,
  # and the setup copies every one of them up: measured, the sweep reported
  # `scanned=32 evicted=0 held=30` every five seconds and the section ran with
  # nothing moving at all. One second still exercises the hold, and lets the
  # sweep reach the documents inside a round.
  awk '
    /^evictor:/ { inev = 1 }
    /^warmer:/  { inev = 0 }
    { sub(/residency: 5m/, "residency: 1s"); print }
    inev && /^  rules:/ && !done {
      print "    - ns: \"'"$DB"'.tcfuzz\""
      print "      older_than: 40m"
      print "      date_field: \"when\""
      done = 1
    }
  ' $CFG > $WDIR/fuzz_churn.yaml
  grep -q "$DB.tcfuzz" $WDIR/fuzz_churn.yaml || {
    echo "  FATAL  the churn config does not carry an eviction rule"; exit 2; }
  # Proving the rule is live before the rounds run, or a clean tally would mean
  # the evictor never did anything and the section would be the section above
  # under another name.
  reseed
  rm -f $LOGDIR/fuzz_ev.log
  cline evictor --config $WDIR/fuzz_churn.yaml --every 5s --log-file $LOGDIR/fuzz_ev.log &
  EVP=$!
  for r in $(seq 1 $CHURN_ROUNDS); do
    reseed
    oneRound "churn round $r of $CHURN_ROUNDS" $((SEED0 + 555 + r*7919))
  done
  kill $EVP 2>/dev/null; wait $EVP 2>/dev/null
  # What the evictor actually did, from its own log, rather than a count taken
  # before and after. A before-and-after says nothing here, because a reseed
  # puts the documents straight back: measured, it read 30 to 30 across a
  # section in which the evictor had removed nothing at all, and read 30 to 29
  # across another, and neither number was about the rounds.
  #
  # Without this the section is the section above under another name, and a
  # clean tally from it would be evidence of nothing.
  SWEPT=$({ grep -c 'msg="sweep complete"' $LOGDIR/fuzz_ev.log 2>/dev/null || true; } | head -1)
  MOVED=$({ grep -c 'evicted=[1-9]' $LOGDIR/fuzz_ev.log 2>/dev/null || true; } | head -1)
  chk "the evictor swept while the rounds ran"        "true" \
      "$([ "${SWEPT:-0}" -gt 0 ] && echo true || echo false)"
  chk "and took documents out from under them"        "true" \
      "$([ "${MOVED:-0}" -gt 0 ] && echo true || echo false)"
  echo "  $SWEPT sweeps, $MOVED of which removed something"
  # The evictor writes the watermark itself, so the claim it leaves is its own
  # rather than the one the rounds above were set up with. Put it back before
  # anything else runs.
  reseed
fi

echo
echo "== what a change stream on each collection delivered =="
# This section has found two defects and both of them are fixed. Against the
# binary from before the promotion suppression was judged by event time it
# reports thirty extra inserts, and against the binary from before promotedAt was
# written once per promotion it reports five of the application's own inserts
# missing. Neither is subtle once the sequences are printed side by side, and
# neither was reachable by any hand-written check in this repository.
# The router serves a change stream from the fast tier and has to hide this
# system's own writes from it. A copy-up is an insert on the fast tier that the
# application did not make; an eviction is a delete of a document that still
# exists. Either one arriving as an ordinary event is a change stream telling an
# application its data changed when it did not, which is the shape of all three
# defects found on 2026-09-05.
#
# So the same generated writes run against both collections with a stream open on
# each, and the two sequences have to match. The reference is untiered and its
# stream is MongoDB's own.
#
# Writes only, and fewer of them: a stream round holds two cursors open for its
# whole length, and reads contribute nothing to what a stream delivers except
# the copy-ups they cause, which is the point and which the writes cause too.
# The fast tier's own count before and after, because a round in which nothing
# was copied up has nothing for the router to suppress and would agree for the
# wrong reason. Measured: the same section passed against the binary from before
# the fix at thirty operations and failed at forty, and the difference was
# whether a write happened to touch a document the fast tier did not hold.
STREAM_HOT_BEFORE=$(H "print(db.getSiblingDB(process.env.TC_DB).tcfuzz.countDocuments({}))")
SOUT=$(TC_SEED=$((SEED0+31337)) TC_OPS=$((OPS/2+20)) TC_STREAM=1 mongosh --port $PORT -u dba -p secret \
         --authenticationDatabase admin --quiet --file "$HERE/fuzz.js" 2>&1)
STREAM_HOT_AFTER=$(H "print(db.getSiblingDB(process.env.TC_DB).tcfuzz.countDocuments({}))")
echo "$SOUT" | grep -E "^  HARNESS|^  stream$(printf '\t')|^DISAGREE|^REFUSAL|^$(printf '\t')" | head -30
SLINE=$(echo "$SOUT" | grep -m1 '^AGREED ')
echo "  $SLINE"
set -- $SLINE
chk "streams: no disagreement"     "0" "${8:-none}"
# Two shapes, not one: the tiered collection against the untiered reference, and
# the same collection's own stream against a database-level one. The second
# needs no reference at all, and it is the one that catches a filter that was
# never applied rather than one applied at the wrong moment.
# A round in which neither stream delivered anything would agree trivially, and
# the SHAPES line is what says whether it did.
echo "$SOUT" | grep -m1 '^SHAPES ' | tr ' ' '\n' | grep -m1 'change_stream' | sed 's/^/  shape /'
chk "streams: the shape ran"       "true" \
    "$(echo "$SOUT" | grep -m1 '^SHAPES ' | grep -qE 'change_stream=[0-9]+/0' && echo true || echo false)"
chk "streams: the database shape ran too" "true" \
    "$(echo "$SOUT" | grep -m1 '^SHAPES ' | grep -qE 'change_stream_on_the_database=[0-9]+/0' && echo true || echo false)"
chk "streams: something was copied up to suppress" "true" \
    "$([ "${STREAM_HOT_AFTER:-0}" -gt "${STREAM_HOT_BEFORE:-99999}" ] 2>/dev/null && echo true || echo false)"
echo "  the fast tier went from $STREAM_HOT_BEFORE to $STREAM_HOT_AFTER during the stream round"

echo
echo "== the same collections read through views =="
# Every operation the sections above generate names a collection. A view is the
# other way to name one, and what a view adds is a pipeline stored on the server
# that the query does not carry, so every judgement the router makes about a read
# of one is made without it.
#
# That is where four wrong answers came from on 2026-09-08, and none of them was
# reachable from anything this file generated. Two of the four were found by hand
# and the other two by asking what the first fix did not cover, which is a way of
# finding defects that does not scale. This is the shape put into the grammar so
# it does not have to be remembered.
#
# Two views on each side. A plain one, where the two answers have to match
# exactly. And one joining the second pair, where the tiered side's joined
# collection is short on the fast tier: there the tiered side has to match or
# refuse, and a short join answered is the defect.
VOUT=$(TC_SEED=$((SEED0+4242)) TC_OPS=1 TC_READONLY=1 TC_VIEWS=1 mongosh --port $PORT -u dba -p secret \
         --authenticationDatabase admin --quiet --file "$HERE/fuzz.js" 2>&1)
echo "$VOUT" | grep -E "^  HARNESS|^DISAGREE|^$(printf '\t')" | head -20
VLINE=$(echo "$VOUT" | grep -m1 '^AGREED ')
echo "  $VLINE"
set -- $VLINE
chk "views: no disagreement"       "0" "${8:-none}"
# And the shapes have to have run. A view that could not be created leaves every
# comparison ungenerated and the line above says 0 disagreements, which is what
# a section proving nothing looks like.
echo "$VOUT" | grep -m1 '^SHAPES ' | tr ' ' '\n' | grep -E 'view' | sed 's/^/  shape /'
for shape in view_read joining_view joining_view_distinct joining_view_point_read; do
  chk "views: the shape $shape ran" "true" \
      "$(echo "$VOUT" | grep -m1 '^SHAPES ' | grep -qE "(^| )$shape=[0-9]+/0" && echo true || echo false)"
done
reseed

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
SOUT=$(TC_SEED=$((SEED0+4242)) TC_OPS=$OPS TC_NO_TTL=1 mongosh --port $P1 -u dba -p secret \
         --authenticationDatabase admin --quiet --file "$HERE/fuzz.js" 2>&1)
echo "$SOUT" | grep -E '^  HARNESS|^DISAGREE|^REFUSAL|^\t' | head -40
echo "$SOUT" | grep -m1 '^SHAPES ' | fold -s -w 150 | sed 's/^/  /'
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
