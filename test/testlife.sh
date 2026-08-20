#!/bin/bash
# The lifecycle of a document, and the operational corners around it.
#
# Everything here was verified by hand at some point and never scripted, which
# means it was verified once and has been assumed ever since. Residency is the
# one that matters most: without it a copy-up and the next eviction sweep undo
# each other forever, and the fast tier thrashes while both tiers stay busy.
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
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
# The collection whose mirror the archive's unique index refuses.
DNS=tdup
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-50s %s
' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-50s got=%s want=%s
' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file $LOGDIR/life_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file $LOGDIR/life_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet       --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}

# Two hours old, so well past the forty minute eviction boundary. Built by hand
# because ObjectId.createFromTime zeroes the bytes after the timestamp, so bulk
# loading fabricated ages collides on _id.
OID=$(printf '%08x%016x' $(( $(date +%s) - 7200 )) 991)
OID2=$(printf '%08x%016x' $(( $(date +%s) - 7200 )) 992)

echo "== setup =="
stopDaemons
for t in orders tttl tdrop tbig tconc $DNS; do
  H "db.getSiblingDB(process.env.TC_DB).$t.drop()" >/dev/null
  C "db.getSiblingDB(process.env.TC_DB).$t.drop()" >/dev/null
done
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).residency.deleteMany({})' >/dev/null
startFeeder
startRouter

echo "== residency protects a document that was just copied up =="
# Archive only: the fast tier has never seen these, which is what an evicted
# document looks like.
C "db.getSiblingDB(process.env.TC_DB).orders.insertMany([
     {_id:ObjectId('$OID'), n:'resident'},
     {_id:ObjectId('$OID2'), n:'untouched'}])" >/dev/null
chk "the fast tier starts empty" "0" "$(H "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({}))")"
chk "a point lookup finds it"    "resident" "$(R "const d=db.getSiblingDB(process.env.TC_DB).orders.findOne({_id:ObjectId('$OID')}); print(d?d.n:'MISSING')")"
sleep 4
chk "and it was copied up"       "1" "$(H "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$OID')}))")"
# Scoped to the document this section is about. Counting every residency record
# in the bookkeeping database made this fail whenever anything else on the same
# worker had copied a document up, which is a true statement about the cluster
# and says nothing about whether the copy-up above recorded its residency.
chk "residency was recorded"     "1" "$(C "print(db.getSiblingDB(process.env.TC_META).residency.countDocuments({ns:process.env.TC_DB+'.orders', doc:ObjectId('$OID')}))")"

# The document is two hours old and the rule evicts past forty minutes, so
# without residency the next sweep would take it straight back out and the copy
# up would have been pure waste.
cline evictor --config $CFG --once >/dev/null 2>&1
chk "the evictor left the resident one alone" "1" "$(H "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$OID')}))")"
chk "the archive still holds it live"         "true" "$(C "const d=db.getSiblingDB(process.env.TC_DB).orders.findOne({_id:ObjectId('$OID')}); print(!!d && !d._tcDeletedAt)")"

echo "== and it is evicted once residency expires =="
# Age the residency record past the 5m window rather than waiting for it.
C "db.getSiblingDB(process.env.TC_META).residency.updateMany({},{\$set:{hotUntil:new Date(Date.now()-3600000)}})" >/dev/null
cline evictor --config $CFG --once >/dev/null 2>&1
chk "the expired resident is evicted now" "0" "$(H "print(db.getSiblingDB(process.env.TC_DB).orders.countDocuments({_id:ObjectId('$OID')}))")"
chk "and the archive kept it live"        "true" "$(C "const d=db.getSiblingDB(process.env.TC_DB).orders.findOne({_id:ObjectId('$OID')}); print(!!d && !d._tcDeletedAt)")"

echo "== a copy-up the fast tier refuses must not fail the read =="
# What a collection looks like after an operator adds a validator to one that
# was already being tiered: the fast tier enforces a rule the archive's copy
# knows nothing about, because the feeder created that copy by upserting
# documents and a validator is not a document.
#
# The archived documents predate the rule and cannot be inserted back. The read
# still has to answer, because the document exists and is readable, and the
# operator has to be told rather than left with a collection that quietly pays
# archive latency on every read for ever.
H "db.getSiblingDB(process.env.TC_DB).tval.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tval.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).createCollection('tval', {validator:{\$jsonSchema:{
     bsonType:'object', required:['email'], properties:{email:{bsonType:'string'}}}}})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tval.insertOne({_id:1, n:'written before the rule'})" >/dev/null
chk "the fast tier does not hold it"          "0" "$(H "print(db.getSiblingDB(process.env.TC_DB).tval.countDocuments({_id:1}))")"
chk "the read is answered anyway"             "written before the rule" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).tval.findOne({_id:1}); print(d?d.n:'MISSING')")"
sleep 2
chk "and the copy-up did not happen"          "0" "$(H "print(db.getSiblingDB(process.env.TC_DB).tval.countDocuments({_id:1}))")"
chk "the archive still holds it live"         "true" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB).tval.findOne({_id:1}); print(!!d && !d._tcDeletedAt)")"
# Silence here would be the real failure: reads keep working and nothing says
# why they are slow.
chk "and the router said why"                 "true" \
    "$(grep -q 'copy up failed' $LOGDIR/life_r.log && echo true || echo false)"

echo "== dropping a tiered collection =="
R "db.getSiblingDB(process.env.TC_DB).tdrop.insertMany([{_id:1,v:'a'},{_id:2,v:'b'},{_id:3,v:'c'}])" >/dev/null
sleep 4
chk "mirrored before the drop" "3" "$(C "print(db.getSiblingDB(process.env.TC_DB).tdrop.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
cline watermark --config $CFG --ns $DB.tdrop --complete-all >/dev/null
sleep 7
R "db.getSiblingDB(process.env.TC_DB).tdrop.drop()" >/dev/null
sleep 4
chk "the feeder survived the drop" "1" "$(pgrep -fc "cline feeder --config $WDIR/" || true)"
chk "the archive kept its copy"    "3" "$(C "print(db.getSiblingDB(process.env.TC_DB).tdrop.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
# The collection is gone from the fast tier but the completeness claim is not.
# If the router believes the claim it answers zero and calls it success, while
# the archive holds three documents.
chk "a read after the drop is not silently empty" "3" "$(R "print(db.getSiblingDB(process.env.TC_DB).tdrop.countDocuments({}))")"
chk "and the watermark no longer claims it"       "false" "$(cline watermark --config $CFG --ns $DB.tdrop 2>&1 | grep -qE 'newer than|all of history' && echo true || echo false)"

echo "== a TTL index on a tiered collection =="
# The document is mirrored before the TTL index exists, and the index is created
# afterwards. Inserting an already-expired document into a collection that
# already had the index is a race against MongoDB's TTL monitor: it sweeps every
# 60 seconds, so roughly one run in fifteen had the document deleted before the
# feeder mirrored it, and the archive then correctly held nothing while the
# check demanded one. Creating the index last removes the race without changing
# what is being tested, which is that a TTL delete is an unannounced delete.
R "db.getSiblingDB(process.env.TC_DB).tttl.insertOne({_id:1,at:new Date(Date.now()-120000),v:'expired'})" >/dev/null
MIRRORED=""
for _ in $(seq 1 60); do
  MIRRORED=$(C "print(db.getSiblingDB(process.env.TC_DB).tttl.countDocuments({_tcDeletedAt:{\$exists:false}}))")
  [ "$MIRRORED" = "1" ] && break
  sleep 0.5
done
chk "mirrored before expiry" "1" "$MIRRORED"
H "db.getSiblingDB(process.env.TC_DB).tttl.createIndex({at:1},{expireAfterSeconds:1})" >/dev/null
echo "  (waiting for the TTL monitor, up to 70s)"
for _ in $(seq 1 35); do
  [ "$(H "print(db.getSiblingDB(process.env.TC_DB).tttl.countDocuments({}))")" = "0" ] && break
  sleep 2
done
chk "the TTL monitor removed it from the fast tier" "0" "$(H "print(db.getSiblingDB(process.env.TC_DB).tttl.countDocuments({}))")"
sleep 5
chk "the feeder is still running"                   "1" "$(pgrep -fc "cline feeder --config $WDIR/" || true)"
chk "the archive tombstoned it, not kept it live"   "0" "$(C "print(db.getSiblingDB(process.env.TC_DB).tttl.countDocuments({_tcDeletedAt:{\$exists:false}}))")"

echo "== a large document survives a copy-up =="
R "db.getSiblingDB(process.env.TC_DB).tbig.insertOne({_id:1, blob:'x'.repeat(8*1024*1024)})" >/dev/null
sleep 6
chk "the big document mirrored" "1" "$(C "print(db.getSiblingDB(process.env.TC_DB).tbig.countDocuments({}))")"
pkill -f "cline feeder --config $WDIR/"; sleep 1
H "db.getSiblingDB(process.env.TC_DB).tbig.deleteOne({_id:1})" >/dev/null
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder
chk "it reads back through the router" "8388608" "$(R "const d=db.getSiblingDB(process.env.TC_DB).tbig.findOne({_id:1}); print(d?d.blob.length:'MISSING')")"
sleep 4
chk "and it was copied up whole"       "8388608" "$(H "const d=db.getSiblingDB(process.env.TC_DB).tbig.findOne({_id:1}); print(d?d.blob.length:'MISSING')")"

echo "== a whole reply of large documents, answered from the archive =="
# One large document is a document; several are a reply. An archive answer is
# assembled here rather than by mongod, so the batch that carries it is this
# router's to bound. Six documents of three megabytes is more than the sixteen a
# single BSON document may occupy and more than one batch can hold, so it can
# only come back as several, and the client must still see all six intact.
#
# Getting this wrong does not look like a wrong answer, it looks like a dead
# connection: a message past the wire protocol's limit is one mongod's own
# drivers refuse to read.
C "db.getSiblingDB(process.env.TC_DB).tbig.deleteMany({_id:{\$gte:101}});
   const blob='y'.repeat(3*1024*1024);
   db.getSiblingDB(process.env.TC_DB).tbig.insertMany(Array.from({length:6},(_,i)=>({_id:101+i, big:1, blob:blob})))" >/dev/null
chk "six large documents are in the archive" "6" "$(C "print(db.getSiblingDB(process.env.TC_DB).tbig.countDocuments({big:1}))")"
chk "and none of them on the fast tier"      "0" "$(H "print(db.getSiblingDB(process.env.TC_DB).tbig.countDocuments({big:1}))")"
BIGREAD=$(R "let n=0, bytes=0; db.getSiblingDB(process.env.TC_DB).tbig.find({big:1}).batchSize(50).forEach(d=>{n++; bytes+=d.blob.length}); print(n+' '+bytes)")
chk "the router returns every one of them"   "6 18874368" "$BIGREAD"
C "db.getSiblingDB(process.env.TC_DB).tbig.deleteMany({_id:{\$gte:101}})" >/dev/null

echo "== deleting a document that is already at the size limit =="
# A document at 16MB has no room for the tombstone field, so marking it deleted
# fails. Left alone that is two failures at once: the archive keeps a live copy
# of a document the application deleted and a later read hands it back, and the
# batch never succeeds, so the feeder reopens its change stream on the same
# event for ever and all mirroring stops. Measured before the fix: thirteen
# identical reopens and a 16MB document returned in full through the router.
#
# Sized by hand because mongosh has no bsonsize. For {_id:<int32>, blob:<str>}
# the envelope is 25 bytes.
H "db.getSiblingDB(process.env.TC_DB).tbig.deleteMany({_id:777})" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tbig.deleteMany({_id:777})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).tbig.insertOne({_id:777, blob:'x'.repeat(16777216-25-2)})" >/dev/null
waitBig() { for _ in $(seq 1 120); do
    [ "$(C "print(db.getSiblingDB(process.env.TC_DB).tbig.countDocuments({_id:777}))")" = "$1" ] && return 0; sleep 0.5; done; }
waitBig 1
chk "the document at the limit mirrored" "1" "$(C "print(db.getSiblingDB(process.env.TC_DB).tbig.countDocuments({_id:777}))")"
H "db.getSiblingDB(process.env.TC_DB).tbig.deleteOne({_id:777})" >/dev/null
for _ in $(seq 1 60); do
  [ "$(C "print(!!db.getSiblingDB(process.env.TC_DB).tbig.findOne({_id:777,_tcDeletedAt:{\$exists:true}}))")" = "true" ] && break
  sleep 0.5
done
chk "the archive marked it deleted"      "true" \
    "$(C "print(!!db.getSiblingDB(process.env.TC_DB).tbig.findOne({_id:777,_tcDeletedAt:{\$exists:true}}))")"
chk "and it says the body was dropped"   "true" \
    "$(C "print(!!db.getSiblingDB(process.env.TC_DB).tbig.findOne({_id:777,_tcBodyDropped:true}))")"
chk "a deleted document does not come back" "MISSING" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).tbig.findOne({_id:777}); print(d?'RESURRECTED':'MISSING')")"
# The point of the repair is that mirroring carries on. A feeder stuck on one
# event stops mirroring everything, which is far worse than the one document.
chk "and the feeder is still mirroring"  "1" "$(pgrep -fc "cline feeder --config $WDIR/" || true)"
R "db.getSiblingDB(process.env.TC_DB).tbig.insertOne({_id:778, after:1})" >/dev/null
waitBig2() { for _ in $(seq 1 60); do
    [ "$(C "print(db.getSiblingDB(process.env.TC_DB).tbig.countDocuments({_id:778}))")" = "1" ] && return 0; sleep 0.5; done; }
waitBig2
chk "a later write still reaches the archive" "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).tbig.countDocuments({_id:778}))")"

echo "== a document the archive's unique index refuses =="
# The other way one document stops everything. The fast tier accepts a value the
# archive already holds, the archive's unique index refuses the mirror, and no
# retry of it can ever succeed because the value belongs to another live archive
# document. Measured before the fix, on an adoption: 85 identical reopens in 85
# seconds, and an order written through the router afterwards never reached the
# archive at all.
#
# Written straight at the fast tier rather than through the router, because the
# router enforces this constraint itself and would refuse it. That is the point:
# this is what reaches the feeder when something else put the duplicate there,
# such as a unique index added to the archive alone, or a writer that does not
# go through the router.
H "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
C "const c=db.getSiblingDB(process.env.TC_DB).$DNS;
   c.createIndex({code:1},{unique:true,name:'code_1'});
   c.insertOne({_id:1, code:'DUP'})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$DNS.insertOne({_id:2, code:'DUP'})" >/dev/null
waitDup() { for _ in $(seq 1 60); do
    [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$DNS.countDocuments({_id:\$1}))")" = "1" ] && return 0; sleep 0.5; done; }
sleep 6
chk "the archive keeps the document it had"   "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$DNS.countDocuments({_id:1}))")"
chk "and does not gain the one it refuses"    "0" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$DNS.countDocuments({_id:2}))")"
# Everything below is the point: one document it cannot mirror must not stop it
# mirroring anything else.
chk "the feeder is still mirroring"           "1" "$(pgrep -fc "cline feeder --config $WDIR/" || true)"
H "db.getSiblingDB(process.env.TC_DB).$DNS.insertOne({_id:3, code:'OTHER'})" >/dev/null
waitDup 3
chk "a later write still reaches the archive" "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$DNS.countDocuments({_id:3}))")"
# And a different namespace, since the change stream is one stream over all of
# them and a jam on this one stops every other one behind it.
H "db.getSiblingDB(process.env.TC_DB).tconc.insertOne({_id:9001, after:1})" >/dev/null
for _ in $(seq 1 60); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).tconc.countDocuments({_id:9001}))")" = "1" ] && break
  sleep 0.5
done
chk "and so does a write to another namespace" "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).tconc.countDocuments({_id:9001}))")"
H "db.getSiblingDB(process.env.TC_DB).tconc.deleteOne({_id:9001})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null

echo "== authentication failures pass through =="
chk "a wrong password is rejected" "true" "$(mongosh --port $PORT -u dba -p wrongpassword --authenticationDatabase admin --quiet --eval 'print(1)' 2>&1 | grep -qiE 'authentication failed|AuthenticationFailed' && echo true || echo false)"
chk "an unknown user is rejected"  "true" "$(mongosh --port $PORT -u nosuchuser -p x --authenticationDatabase admin --quiet --eval 'print(1)' 2>&1 | grep -qiE 'authentication failed|AuthenticationFailed' && echo true || echo false)"

echo "== concurrent clients falling through to the archive =="
R "const c=db.getSiblingDB(process.env.TC_DB).tconc; const d=[]; for(let i=0;i<200;i++) d.push({_id:i,n:i,grp:'g'+(i%5)}); c.insertMany(d);" >/dev/null
sleep 6
cline watermark --config $CFG --ns $DB.tconc --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
rm -f /tmp/conc.*.out
# Wait for these clients only. A bare `wait` also waits for the feeder and the
# router, which are background jobs of this same shell and never exit, so the
# suite hangs forever with every check already passed.
CLIENTS=()
for i in $(seq 1 10); do
  ( mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
      const c = db.getSiblingDB(process.env.TC_DB).tconc;
      let bad = 0;
      for (let r = 0; r < 5; r++) {
        if (c.countDocuments({}) !== 200) bad++;
        if (c.find({}).batchSize(7).itcount() !== 200) bad++;
        if (c.countDocuments({grp:'g2'}) !== 40) bad++;
      }
      print(bad);" > /tmp/conc.$i.out 2>&1 ) &
  CLIENTS+=($!)
done
wait "${CLIENTS[@]}"
chk "ten parallel clients all agree" "0" "$(cat /tmp/conc.*.out | tr -d ' \n' | sed 's/0*//' | grep -q . && echo mismatch || echo 0)"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s
' "${FAILED[*]}"
exit $FAIL
