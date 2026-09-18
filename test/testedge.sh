#!/bin/bash
# Edge cases and config variants: the code paths the other suites never reach.
#
# This suite was the last one still built on fixed sleeps and bare pkill, and it
# failed different checks on consecutive runs because of it. Every wait below is
# now a wait for the thing itself: a daemon that answers, a document that has
# landed, a process that has gone. The one remaining sleep is the router's
# completeness cache, which is a constant in the code rather than a race.
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
export DB WDIR
P1=$((PORT+1)); P2=$((PORT+2)); P3=$((PORT+3)); PDASH=$((PORT+10)); PALT=$((PORT+17))
CFG=${CFG:-$WDIR/cline.yaml}
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
NS=tprobe
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-46s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-46s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

# completenessTTL in internal/router/tiers.go: how long the router reuses a
# watermark reading. Declaring a claim is not visible to it until this expires.
CLAIM_TTL=7

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do
    pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0
    sleep 0.5
  done
}
# The router outlives most sections here, so the feeder is stopped on its own.
stopFeeder() {
  pkill -f "cline feeder --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do
    pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || return 0
    sleep 0.5
  done
}
# startFeeder CONFIG LOG: every caller deletes the checkpoint first, so the
# checkpoint reappearing is the feeder saying it has connected and is watching.
# Sleeping a fixed few seconds instead lets the writes that follow land before
# it is listening, and they are then never mirrored.
startFeeder() {
  cline feeder --config "$1" --log-file "$LOGDIR/$2" & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/edge_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin \
      --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT, so nothing below would mean anything"
  tail -5 $LOGDIR/edge_r.log
  exit 2
}
# waitFor WHAT EXPECTED COMMAND: poll until the command prints EXPECTED.
waitFor() {
  local what="$1" want="$2" cmd="$3" got=""
  for _ in $(seq 1 60); do
    got=$(eval "$cmd")
    [ "$got" = "$want" ] && return 0
    sleep 0.5
  done
  echo "  WARN  $what never reached $want, last saw ${got:-none}"
}
# waitMirrored COLLECTION COUNT: block until the archive holds COUNT live
# documents. A fixed sleep here is what made this suite report mirroring
# failures that were only the feeder still catching up.
waitMirrored() {
  waitFor "$DB.$1 in the archive" "$2" \
    "C \"print(db.getSiblingDB(process.env.TC_DB).$1.countDocuments({_tcDeletedAt:{\\\$exists:false}}))\""
}
# Remove documents from the fast tier the way an eviction does, with the feeder
# blind to it, so the archive keeps a live copy.
evictBehindTheFeeder() {
  stopFeeder
  H "$1" >/dev/null
  C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
  startFeeder $CFG edge_f.log
}

echo "== setup =="
# Residency, the warming notices and the warmer marker all have to go, or this
# suite is not independent of its own previous run. The date_field section ends
# with a warm, which pins all 30 documents for five minutes and announces them:
# a rerun inside that window sees the evictor correctly refuse to evict a
# document that was just warmed, and the feeder correctly suppress an echo of a
# document that was just announced. Both look exactly like product bugs.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_DB).dated.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_DB).dated.drop(); db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).residency.deleteMany({}); db.getSiblingDB(process.env.TC_META).warming.deleteMany({}); db.getSiblingDB(process.env.TC_META).markers.deleteOne({_id:'warmerActive'})" >/dev/null
startFeeder $CFG edge_f.log
startRouter

echo "== _id types other than ObjectId =="
R "db.getSiblingDB(process.env.TC_DB).$NS.insertMany([
  {_id:'str-key',v:'s'},{_id:12345,v:'i'},{_id:{a:1,b:2},v:'c'},{_id:new Date('2026-01-01'),v:'d'}])" >/dev/null
waitMirrored $NS 4
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({})"
chk "string _id read miss"    "s" "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:'str-key'}); print(d?d.v:'MISSING')")"
chk "int _id read miss"       "i" "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:12345}); print(d?d.v:'MISSING')")"
chk "document _id read miss"  "c" "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:{a:1,b:2}}); print(d?d.v:'MISSING')")"
chk "date _id read miss"      "d" "$(R "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:new Date('2026-01-01')}); print(d?d.v:'MISSING')")"
# copyUpAsync replies first and copies afterwards, so the fourth read returning
# is not the fourth copy-up landing.
waitFor "copies up" "4" "H \"print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))\""
chk "all four copied up"      "4" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"

# A document-valued _id is a literal, not an operator expression. Reading it as
# an operator meant an update to an archived one matched nothing and reported
# success, losing the write silently.
evictBehindTheFeeder "db.getSiblingDB(process.env.TC_DB).$NS.deleteMany({})"
chk "write miss on a document _id"  "1" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.updateOne({_id:{a:1,b:2}},{\$set:{touched:1}}).matchedCount)")"
waitFor "the touched update in the archive" "1" \
  "C \"const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:{a:1,b:2}}); print(d&&d.touched?d.touched:'no')\""
chk "and it reached the archive"    "1" "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:{a:1,b:2}}); print(d&&d.touched?d.touched:'no')")"
chk "an _id range is still a range" "0" "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.updateOne({_id:{\$gt:999999}},{\$set:{x:1}}).matchedCount)")"

echo "== \$out and \$merge run on incomplete input =="
cline watermark --config $CFG --ns $DB.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep $CLAIM_TTL
chk "\$out runs"              "ok"   "$(R "try{db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$group:{_id:null,n:{\$sum:1}}},{\$out:'e_out'}]).toArray();print('ok')}catch(e){print(e.message.substring(0,50))}")"
# Grouped by a real field, not by null: \$merge refuses a null _id in mongod
# itself, so asking for one here would be testing MongoDB's rule, not ours.
chk "\$merge runs"            "ok"   "$(R "try{db.getSiblingDB(process.env.TC_DB).$NS.aggregate([{\$group:{_id:'\$v',n:{\$sum:1}}},{\$merge:{into:'e_merge'}}]).toArray();print('ok')}catch(e){print(e.message.substring(0,50))}")"
chk "both wrote to the fast tier" "true" "$(H "const n=db.getSiblingDB(process.env.TC_DB).getCollectionNames(); print(n.includes('e_out')&&n.includes('e_merge'))")"
chk "collStats still allowed" "true"    "$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.stats().count >= 0)")"

echo "== dry runs write nothing =="
cline watermark --config $CFG --ns $DB.$NS --complete-all >/dev/null
BEFORE_H=$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")
BEFORE_C=$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")
cline warmer  --config $CFG --dry-run >/dev/null 2>&1
cline evictor --config $CFG --once --dry-run >/dev/null 2>&1
chk "warmer dry run changed the fast tier by"  "$BEFORE_H" "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
chk "evictor dry run changed the archive by"   "$BEFORE_C" "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"

echo "== date_field rules, the non-ObjectId selector =="
python3 - <<'PY'
import os
# The heredoc is quoted so nothing here is expanded by the shell. The database
# and the directory come from the environment, which is what lets a worker edit
# its own config rather than another worker's.
db, wdir = os.environ['DB'], os.environ['WDIR']
p = wdir + '/cline.yaml'; s = open(p).read()
s = s.replace('    - ns: "%s.orders"\n      older_than: 40m' % db,
              '    - ns: "%s.dated"\n      older_than: 40m\n      date_field: "createdAt"' % db)
s = s.replace('    - ns: "%s.orders"\n      back: 40m' % db,
              '    - ns: "%s.dated"\n      back: 40m\n      date_field: "createdAt"' % db)
s = s.replace('    - "%s.tacct"\n' % db, '    - "%s.tacct"\n    - "%s.dated"\n' % (db, db), 1)
open(wdir + '/edge_df.yaml', 'w').write(s)
# The warmer gets its own file, reaching back twelve hours to refill what the
# evictor took. Two files rather than one so neither carries a warm window
# reaching past its own eviction cutoff, which loads but logs a warning, and a
# suite that expects a quiet startup would then report that instead. It is also
# how this runs in practice: the evictor and the warmer are separate daemons.
w = s.replace('    - ns: "%s.dated"\n      back: 40m' % db,
              '    - ns: "%s.dated"\n      back: 12h' % db)
w = w.replace('    - ns: "%s.dated"\n      older_than: 40m' % db,
              '    - ns: "%s.dated"\n      older_than: 12h' % db)
open(wdir + '/edge_dfwarm.yaml', 'w').write(w)
PY
stopFeeder
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder $WDIR/edge_df.yaml edge_df.log
H 'const c=db.getSiblingDB(process.env.TC_DB).dated; const now=Date.now(); const d=[];
   for(let i=0;i<30;i++) d.push({_id:i, createdAt:new Date(now-i*5*60*1000), n:i});
   c.insertMany(d); c.createIndex({createdAt:1,_id:1});' >/dev/null
waitMirrored dated 30
H 'db.getSiblingDB(process.env.TC_DB).dated.updateMany({},{$set:{m:1}})' >/dev/null
# The update has to be mirrored too, or the evictor below runs against an
# archive that is one write behind the fast tier.
waitFor "the m:1 update in the archive" "30" \
  "C 'print(db.getSiblingDB(process.env.TC_DB).dated.countDocuments({m:1}))'"
chk "date_field collection mirrored" "30" "$(C 'print(db.getSiblingDB(process.env.TC_DB).dated.countDocuments({}))')"
cline evictor --config $WDIR/edge_df.yaml --once >/dev/null 2>&1
chk "date_field evictor kept the young"  "8"  "$(H 'print(db.getSiblingDB(process.env.TC_DB).dated.countDocuments({}))')"
chk "and the archive kept everything"    "30" "$(C 'print(db.getSiblingDB(process.env.TC_DB).dated.countDocuments({_tcDeletedAt:{$exists:false}}))')"
cline watermark --config $WDIR/edge_df.yaml --ns $DB.dated --clear >/dev/null
cline warmer --config $WDIR/edge_dfwarm.yaml >/dev/null 2>&1
chk "date_field warmer refilled"         "30" "$(H 'print(db.getSiblingDB(process.env.TC_DB).dated.countDocuments({}))')"

echo "== a warm window reaching past its own eviction cutoff =="
# Named at startup, by every daemon that reads a config, and not refused. It
# costs the same documents being copied out of the archive, written to the fast
# tier and deleted again for as long as both run, which is worth telling an
# operator about and is not a wrong answer.
#
# This was a refusal first, on the strength of eighteen disagreeing answers
# measured on the pilot under a fifteen year warm against a one day cutoff.
# Those belonged to the eviction announcement bug that was tombstoning documents
# at the same time. Measured again with that fixed, under the same two windows
# and with the fast tier emptied by the fight while the questions ran, five
# rounds of 199 questions disagreed nowhere. A refusal would also have been thin
# protection, because the two daemons can be given separate files that are each
# legal alone.
python3 - <<'PY'
import os
db, wdir = os.environ['DB'], os.environ['WDIR']
s = open(wdir + '/cline.yaml').read()
s = s.replace('    - ns: "%s.orders"\n      back: 40m' % db,
              '    - ns: "%s.orders"\n      back: 12h' % db)
open(wdir + '/edge_wide.yaml', 'w').write(s)
PY
chk "the derived config warms past the cutoff"  "1"     "$(grep -c 'back: 12h' $WDIR/edge_wide.yaml)"
rm -f $LOGDIR/edge_wide.log
cline warmer --config $WDIR/edge_wide.yaml --dry-run --log-file $LOGDIR/edge_wide.log >/dev/null 2>&1
chk "it loads rather than refusing"             "true"     "$(grep -q 'reaches past its own eviction cutoff' $LOGDIR/edge_wide.log && echo true || echo false)"
chk "and the warning names the namespace"       "true"     "$(grep -q "$DB.orders: warmer.back is 12h" $LOGDIR/edge_wide.log && echo true || echo false)"
chk "and says it is work rather than a wrong answer" "true"     "$(grep -q 'answers stay correct' $LOGDIR/edge_wide.log && echo true || echo false)"
# Every daemon says it, so an operator reading one log does not have to know
# which component owns the contradiction.
rm -f $LOGDIR/edge_wide_f.log
timeout 12 cline feeder --config $WDIR/edge_wide.yaml --log-file $LOGDIR/edge_wide_f.log >/dev/null 2>&1
chk "the feeder says it too"                    "true"     "$(grep -q 'reaches past its own eviction cutoff' $LOGDIR/edge_wide_f.log && echo true || echo false)"
# And the ordinary config must be quiet, or the warning means nothing.
rm -f $LOGDIR/edge_quiet.log
cline warmer --config $CFG --dry-run --log-file $LOGDIR/edge_quiet.log >/dev/null 2>&1
chk "the shipped config says nothing"           "0"     "$(grep -c 'reaches past its own eviction cutoff' $LOGDIR/edge_quiet.log || true)"

echo "== unannounced delete policies =="
stopFeeder
sed 's/unannounced_deletes: tombstone/unannounced_deletes: ignore/' $CFG > $WDIR/edge_ignore.yaml
sed 's/unannounced_deletes: tombstone/unannounced_deletes: halt/'   $CFG > $WDIR/edge_halt.yaml
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'})" >/dev/null
startFeeder $WDIR/edge_ignore.yaml edge_ig.log
H "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:1,v:'a'})" >/dev/null
waitMirrored $NS 1
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:1})" >/dev/null
# Under ignore the delete changes nothing in the archive, so there is no state
# to wait for. A sentinel written after it is: the change stream is ordered, so
# the sentinel arriving proves the delete was already seen and ignored.
H "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:'sentinel'})" >/dev/null
waitFor "the sentinel in the archive" "true" \
  "C \"print(!!db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:'sentinel'}))\""
chk "ignore leaves the archive readable" "no" "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:1}); print(d?(d._tcDeletedAt?'yes':'no'):'GONE')")"
chk "and the feeder keeps running"       "1"  "$(pgrep -fc "cline feeder --config $WDIR/edge_ignore" || true)"

stopFeeder
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
rm -f $LOGDIR/edge_halt.log
startFeeder $WDIR/edge_halt.yaml edge_halt.log
HP=$FP
H "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:2,v:'b'})" >/dev/null
waitMirrored $NS 1
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:2})" >/dev/null
# Bounded, because a bare wait on a feeder that refuses to halt hangs the suite
# forever with every check above it already passed.
HALTRC=99
for _ in $(seq 1 60); do
  kill -0 $HP 2>/dev/null || { wait $HP 2>/dev/null; HALTRC=$?; break; }
  sleep 0.5
done
chk "halt stops the feeder"              "1"    "$HALTRC"
# The reason has to reach the log file. main prints it to stderr, which is not
# where the logs go once --log-file is set, so a halt used to end the log
# mid-sentence and explain nothing.
chk "and says why in the log file"       "true" "$(grep -qE 'level=ERROR.*feeder stopped.*unannounced' $LOGDIR/edge_halt.log && echo true || echo false)"

echo "== tombstone retention =="
# A delete through the router marks the archive's copy and keeps it. That is an
# audit trail by design, and on a deployment that runs for years it is also an
# archive that grows with every delete it has ever seen, plus a delete that was
# meant to erase and did not. feeder.tombstone_retention is the knob that puts
# an end date on the copy, and these checks are about the knob being honest:
# absent means keep for ever, a value means mongod expires it, and changing the
# value changes what mongod does rather than leaving the first one in place.
stopFeeder
sed '0,/^  batch_size:/s/^  batch_size:/  tombstone_retention: 2m\n  batch_size:/' $CFG > $WDIR/edge_ttl.yaml
sed '0,/^  batch_size:/s/^  batch_size:/  tombstone_retention: 3m\n  batch_size:/' $CFG > $WDIR/edge_ttl3.yaml
sed '0,/^  batch_size:/s/^  batch_size:/  tombstone_retention: 30s\n  batch_size:/' $CFG > $WDIR/edge_ttlshort.yaml
sed '0,/^  batch_size:/s/^  batch_size:/  tombstone_retention: -5m\n  batch_size:/' $CFG > $WDIR/edge_ttlneg.yaml
chk "a retention the TTL monitor cannot honour is refused" "true" \
  "$(cline watermark --config $WDIR/edge_ttlshort.yaml 2>&1 | grep -q 'expires TTL indexes once a minute' && echo true || echo false)"
chk "and a negative retention is refused"       "true" \
  "$(cline watermark --config $WDIR/edge_ttlneg.yaml 2>&1 | grep -q 'cannot be negative' && echo true || echo false)"

ttlSecs() {
  C "const i=db.getSiblingDB(process.env.TC_DB).$NS.getIndexes().find(x=>x.name=='tc_tombstone_ttl'); print(i?i.expireAfterSeconds:'none')"
}

H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop(); db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:'test'})" >/dev/null
rm -f $LOGDIR/edge_ttl.log
startFeeder $WDIR/edge_ttl.yaml edge_ttl.log
H "db.getSiblingDB(process.env.TC_DB).$NS.insertMany([{_id:1,v:'keep'},{_id:2,v:'gone'}])" >/dev/null
waitMirrored $NS 2
chk "no expiry index before anything is deleted" "none" "$(ttlSecs)"
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:2})" >/dev/null
waitMirrored $NS 1
chk "the first tombstone creates the expiry index" "120" "$(ttlSecs)"
chk "and the feeder says so"                    "true" \
  "$(grep -q 'deleted documents expire from the archive' $LOGDIR/edge_ttl.log && echo true || echo false)"
chk "the tombstone is still there to be expired" "yes" \
  "$(C "const d=db.getSiblingDB(process.env.TC_DB).$NS.findOne({_id:2}); print(d?(d._tcDeletedAt?'yes':'no'):'GONE')")"
chk "and the document nothing deleted is untouched" "1" \
  "$(C "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:1,_tcDeletedAt:{\$exists:false}}))")"

# A changed retention has to replace the index. createIndex refuses the same
# name with different options, so a feeder that only created would leave the
# first schedule running for ever while the config said otherwise.
stopFeeder
rm -f $LOGDIR/edge_ttl3.log
startFeeder $WDIR/edge_ttl3.yaml edge_ttl3.log
H "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:3,v:'x'})" >/dev/null
waitMirrored $NS 2
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:3})" >/dev/null
waitFor "the expiry index on the new schedule" "180" "ttlSecs"
chk "a changed retention replaces the index"    "180" "$(ttlSecs)"

# And removing the knob has to stop the expiry, or the config lies in the other
# direction: an operator who wants the audit trail back would keep losing it.
stopFeeder
H "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:4,v:'y'})" >/dev/null
rm -f $LOGDIR/edge_ttloff.log
startFeeder $CFG edge_ttloff.log
waitMirrored $NS 2
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:4})" >/dev/null
waitFor "the expiry index gone" "none" "ttlSecs"
chk "removing the knob drops the index"         "none" "$(ttlSecs)"
stopFeeder
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null

echo "== a command whose field is the wrong type =="
# Two defects, one shape: a command this proxy could not read, acted on anyway.
#
# bson.RawValue.Array panics on anything but an array, so a $merge with `on: 5`,
# a createIndexes with `indexes: 5` and a killCursors with `cursors: 5` closed
# the client's connection and counted a panic. thermocline_panics_total is an
# alerting rule, so a client with a bad query made it fire for a product defect
# that is not one.
#
# The larger one was found by asking mongod the same questions. An aggregate
# with `pipeline: 5`, which mongod refuses with "'pipeline' option must be
# specified as an array", was read as a pipeline with no stages and answered by
# the router with the whole collection. So these check the answer rather than
# the survival: what comes back has to be what mongod says.
#
# The router started at the top outlives every section here, and a second one
# would fail to bind rather than fail loudly. Started only if it has gone.
pgrep -f "cline router --config $WDIR/" >/dev/null 2>&1 || startRouter
EPANIC0=$(curl -s http://127.0.0.1:$PDASH/metrics | awk '/^thermocline_panics_total/ {print $2}')
# ask WHAT [DB]: run it through the router and through mongod, and print agree
# when the two answer the same. Every disagreement here was a defect.
#
# The control below runs against an untiered collection on purpose. A document
# just written to a tiered one is on the fast tier and not yet in the archive,
# and with no feeder running in this section it never will be, so the router
# answering 0 where the fast tier holds 1 is this system working as documented
# rather than a disagreement. Asking where both tiers hold the same thing is
# what makes "still agrees" mean the pipeline was read, not that mirroring won.
ask() {
  local via_r via_h edb=${2:-$DB}
  via_r=$(mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval \
    "try{var r=db.getSiblingDB('$edb').runCommand($1); print('ok:'+(r.cursor?r.cursor.firstBatch.length:'none'))}catch(e){print(/closed|socket|ECONN/i.test(e.message)?'DROPPED':'err:'+e.message.substring(0,70))}" 2>&1 | tail -1)
  via_h=$(mongosh "$HOT" --quiet --eval \
    "try{var r=db.getSiblingDB('$edb').runCommand($1); print('ok:'+(r.cursor?r.cursor.firstBatch.length:'none'))}catch(e){print('err:'+e.message.substring(0,70))}" 2>&1 | tail -1)
  if [ "$via_r" = "$via_h" ]; then echo "agree"; else echo "router=$via_r mongod=$via_h"; fi
}
# One document, in a collection nothing tiers, so both sides see the same one.
R "db.getSiblingDB(process.env.TC_DB2).edgefree.drop(); db.getSiblingDB(process.env.TC_DB2).edgefree.insertOne({_id:'wrongtype',v:1})" >/dev/null
chk "an aggregate with a non-array pipeline"     "agree" "$(ask "{aggregate:'$NS',pipeline:5,cursor:{}}")"
chk "and with the pipeline missing"              "agree" "$(ask "{aggregate:'$NS',cursor:{}}")"
chk "a \$merge with a non-array on"              "agree" "$(ask "{aggregate:'$NS',pipeline:[{\$merge:{into:'$NS',on:5}}],cursor:{}}")"
chk "a \$merge with a non-string into"           "agree" "$(ask "{aggregate:'$NS',pipeline:[{\$merge:{into:5}}],cursor:{}}")"
chk "a \$out with a non-string argument"         "agree" "$(ask "{aggregate:'$NS',pipeline:[{\$out:5}],cursor:{}}")"
chk "createIndexes with a non-array indexes"     "agree" "$(ask "{createIndexes:'$NS',indexes:5}")"
# count and distinct read a query the way find reads a filter, and an unreadable
# one was read as no query at all: `count` with `query: 5` came back with a
# number where mongod says the field is the wrong type.
chk "a count whose query is not a document"      "agree" "$(ask "{count:'$NS',query:5}")"
chk "a distinct whose query is not a document"   "agree" "$(ask "{distinct:'$NS',key:'v',query:5}")"
# A TTL index is one whose expireAfterSeconds is a number. Reading the field's
# presence alone answered a malformed createIndexes with a refusal about this
# system's own TTL policy, which sends an operator looking for a policy.
chk "a non-numeric expireAfterSeconds"           "agree" \
    "$(ask "{createIndexes:'$NS',indexes:[{key:{edgettl:1},name:'edgettl_1',expireAfterSeconds:'x'}]}")"
chk "killCursors with a non-array cursors"       "agree" "$(ask "{killCursors:'$NS',cursors:5}")"
# The controls: the fix must not be a wider refusal.
chk "while an ordinary pipeline still agrees"    "agree" "$(ask "{aggregate:'edgefree',pipeline:[{\$match:{_id:'wrongtype'}}],cursor:{}}" "$DB2")"
chk "and it really did return the document"      "ok:1" \
    "$(R "var r=db.getSiblingDB(process.env.TC_DB2).runCommand({aggregate:'edgefree',pipeline:[{\$match:{_id:'wrongtype'}}],cursor:{}}); print('ok:'+r.cursor.firstBatch.length)")"
chk "and an ordinary \$merge"                    "agree" "$(ask "{aggregate:'$NS',pipeline:[{\$match:{_id:'no-such'}},{\$merge:{into:'$NS',on:'_id'}}],cursor:{}}")"
# And a real TTL index is still refused, or the fix above is a hole rather than
# a narrowing. This one does not agree with mongod on purpose: refusing it is
# the product decision, and the check is that the refusal is ours.
chk "while a numeric one is still refused"       "true" \
    "$(R "try{db.getSiblingDB(process.env.TC_DB).runCommand({createIndexes:'$NS',indexes:[{key:{edgettl:1},name:'edgettl_2',expireAfterSeconds:60}]});print('false')}catch(e){print(/refusing a TTL index/.test(e.message))}")"
chk "and the router counted no panic"            "${EPANIC0:-0}" \
    "$(curl -s http://127.0.0.1:$PDASH/metrics | awk '/^thermocline_panics_total/ {print $2}')"
R "db.getSiblingDB(process.env.TC_DB2).edgefree.drop()" >/dev/null

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
