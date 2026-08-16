#!/bin/bash
# Edge cases and config variants: the code paths the other suites never reach.
#
# This suite was the last one still built on fixed sleeps and bare pkill, and it
# failed different checks on consecutive runs because of it. Every wait below is
# now a wait for the thing itself: a daemon that answers, a document that has
# landed, a process that has gone. The one remaining sleep is the router's
# completeness cache, which is a constant in the code rather than a race.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
NS=tprobe
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-46s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-46s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

# completenessTTL in internal/router/tiers.go: how long the router reuses a
# watermark reading. Declaring a claim is not visible to it until this expires.
CLAIM_TTL=7

stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do
    pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0
    sleep 0.5
  done
}
# The router outlives most sections here, so the feeder is stopped on its own.
stopFeeder() {
  pkill -f 'cline feeder' 2>/dev/null
  for _ in $(seq 1 30); do
    pgrep -f 'cline feeder' >/dev/null 2>&1 || return 0
    sleep 0.5
  done
}
# startFeeder CONFIG LOG: every caller deletes the checkpoint first, so the
# checkpoint reappearing is the feeder saying it has connected and is watching.
# Sleeping a fixed few seconds instead lets the writes that follow land before
# it is listening, and they are then never mirrored.
startFeeder() {
  cline feeder --config "$1" --log-file "/var/log/cline/$2" & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file /var/log/cline/edge_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin \
      --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on 27100, so nothing below would mean anything"
  tail -5 /var/log/cline/edge_r.log
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
  waitFor "shop.$1 in the archive" "$2" \
    "C \"print(db.getSiblingDB('shop').$1.countDocuments({_tcDeletedAt:{\\\$exists:false}}))\""
}
# Remove documents from the fast tier the way an eviction does, with the feeder
# blind to it, so the archive keeps a live copy.
evictBehindTheFeeder() {
  stopFeeder
  H "$1" >/dev/null
  C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
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
H "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('shop').dated.drop()" >/dev/null
C "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('shop').dated.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'}); db.getSiblingDB('_thermocline').watermarks.deleteMany({}); db.getSiblingDB('_thermocline').residency.deleteMany({}); db.getSiblingDB('_thermocline').warming.deleteMany({}); db.getSiblingDB('_thermocline').markers.deleteOne({_id:'warmerActive'})" >/dev/null
startFeeder $CFG edge_f.log
startRouter

echo "== _id types other than ObjectId =="
R "db.getSiblingDB('shop').$NS.insertMany([
  {_id:'str-key',v:'s'},{_id:12345,v:'i'},{_id:{a:1,b:2},v:'c'},{_id:new Date('2026-01-01'),v:'d'}])" >/dev/null
waitMirrored $NS 4
evictBehindTheFeeder "db.getSiblingDB('shop').$NS.deleteMany({})"
chk "string _id read miss"    "s" "$(R "const d=db.getSiblingDB('shop').$NS.findOne({_id:'str-key'}); print(d?d.v:'MISSING')")"
chk "int _id read miss"       "i" "$(R "const d=db.getSiblingDB('shop').$NS.findOne({_id:12345}); print(d?d.v:'MISSING')")"
chk "document _id read miss"  "c" "$(R "const d=db.getSiblingDB('shop').$NS.findOne({_id:{a:1,b:2}}); print(d?d.v:'MISSING')")"
chk "date _id read miss"      "d" "$(R "const d=db.getSiblingDB('shop').$NS.findOne({_id:new Date('2026-01-01')}); print(d?d.v:'MISSING')")"
# copyUpAsync replies first and copies afterwards, so the fourth read returning
# is not the fourth copy-up landing.
waitFor "copies up" "4" "H \"print(db.getSiblingDB('shop').$NS.countDocuments({}))\""
chk "all four copied up"      "4" "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"

# A document-valued _id is a literal, not an operator expression. Reading it as
# an operator meant an update to an archived one matched nothing and reported
# success, losing the write silently.
evictBehindTheFeeder "db.getSiblingDB('shop').$NS.deleteMany({})"
chk "write miss on a document _id"  "1" "$(R "print(db.getSiblingDB('shop').$NS.updateOne({_id:{a:1,b:2}},{\$set:{touched:1}}).matchedCount)")"
waitFor "the touched update in the archive" "1" \
  "C \"const d=db.getSiblingDB('shop').$NS.findOne({_id:{a:1,b:2}}); print(d&&d.touched?d.touched:'no')\""
chk "and it reached the archive"    "1" "$(C "const d=db.getSiblingDB('shop').$NS.findOne({_id:{a:1,b:2}}); print(d&&d.touched?d.touched:'no')")"
chk "an _id range is still a range" "0" "$(R "print(db.getSiblingDB('shop').$NS.updateOne({_id:{\$gt:999999}},{\$set:{x:1}}).matchedCount)")"

echo "== \$out and \$merge run on incomplete input =="
cline watermark --config $CFG --ns shop.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep $CLAIM_TTL
chk "\$out runs"              "ok"   "$(R "try{db.getSiblingDB('shop').$NS.aggregate([{\$group:{_id:null,n:{\$sum:1}}},{\$out:'e_out'}]).toArray();print('ok')}catch(e){print(e.message.substring(0,50))}")"
# Grouped by a real field, not by null: \$merge refuses a null _id in mongod
# itself, so asking for one here would be testing MongoDB's rule, not ours.
chk "\$merge runs"            "ok"   "$(R "try{db.getSiblingDB('shop').$NS.aggregate([{\$group:{_id:'\$v',n:{\$sum:1}}},{\$merge:{into:'e_merge'}}]).toArray();print('ok')}catch(e){print(e.message.substring(0,50))}")"
chk "both wrote to the fast tier" "true" "$(H "const n=db.getSiblingDB('shop').getCollectionNames(); print(n.includes('e_out')&&n.includes('e_merge'))")"
chk "collStats still allowed" "true"    "$(R "print(db.getSiblingDB('shop').$NS.stats().count >= 0)")"

echo "== dry runs write nothing =="
cline watermark --config $CFG --ns shop.$NS --complete-all >/dev/null
BEFORE_H=$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")
BEFORE_C=$(C "print(db.getSiblingDB('shop').$NS.countDocuments({}))")
cline warmer  --config $CFG --dry-run >/dev/null 2>&1
cline evictor --config $CFG --once --dry-run >/dev/null 2>&1
chk "warmer dry run changed the fast tier by"  "$BEFORE_H" "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"
chk "evictor dry run changed the archive by"   "$BEFORE_C" "$(C "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"

echo "== date_field rules, the non-ObjectId selector =="
python3 - <<'PY'
p='/root/cline.yaml'; s=open(p).read()
s=s.replace('    - ns: "shop.orders"\n      older_than: 40m','    - ns: "shop.dated"\n      older_than: 40m\n      date_field: "createdAt"')
s=s.replace('    - ns: "shop.orders"\n      back: 40m','    - ns: "shop.dated"\n      back: 12h\n      date_field: "createdAt"')
s=s.replace('    - "shop.tacct"\n','    - "shop.tacct"\n    - "shop.dated"\n',1)
open('/root/edge_df.yaml','w').write(s)
PY
stopFeeder
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder /root/edge_df.yaml edge_df.log
H 'const c=db.getSiblingDB("shop").dated; const now=Date.now(); const d=[];
   for(let i=0;i<30;i++) d.push({_id:i, createdAt:new Date(now-i*5*60*1000), n:i});
   c.insertMany(d); c.createIndex({createdAt:1,_id:1});' >/dev/null
waitMirrored dated 30
H 'db.getSiblingDB("shop").dated.updateMany({},{$set:{m:1}})' >/dev/null
# The update has to be mirrored too, or the evictor below runs against an
# archive that is one write behind the fast tier.
waitFor "the m:1 update in the archive" "30" \
  "C 'print(db.getSiblingDB(\"shop\").dated.countDocuments({m:1}))'"
chk "date_field collection mirrored" "30" "$(C 'print(db.getSiblingDB("shop").dated.countDocuments({}))')"
cline evictor --config /root/edge_df.yaml --once >/dev/null 2>&1
chk "date_field evictor kept the young"  "8"  "$(H 'print(db.getSiblingDB("shop").dated.countDocuments({}))')"
chk "and the archive kept everything"    "30" "$(C 'print(db.getSiblingDB("shop").dated.countDocuments({_tcDeletedAt:{$exists:false}}))')"
cline watermark --config /root/edge_df.yaml --ns shop.dated --clear >/dev/null
cline warmer --config /root/edge_df.yaml >/dev/null 2>&1
chk "date_field warmer refilled"         "30" "$(H 'print(db.getSiblingDB("shop").dated.countDocuments({}))')"

echo "== unannounced delete policies =="
stopFeeder
sed 's/unannounced_deletes: tombstone/unannounced_deletes: ignore/' $CFG > /root/edge_ignore.yaml
sed 's/unannounced_deletes: tombstone/unannounced_deletes: halt/'   $CFG > /root/edge_halt.yaml
H "db.getSiblingDB('shop').$NS.drop()" >/dev/null
C "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'})" >/dev/null
startFeeder /root/edge_ignore.yaml edge_ig.log
H "db.getSiblingDB('shop').$NS.insertOne({_id:1,v:'a'})" >/dev/null
waitMirrored $NS 1
H "db.getSiblingDB('shop').$NS.deleteOne({_id:1})" >/dev/null
# Under ignore the delete changes nothing in the archive, so there is no state
# to wait for. A sentinel written after it is: the change stream is ordered, so
# the sentinel arriving proves the delete was already seen and ignored.
H "db.getSiblingDB('shop').$NS.insertOne({_id:'sentinel'})" >/dev/null
waitFor "the sentinel in the archive" "true" \
  "C \"print(!!db.getSiblingDB('shop').$NS.findOne({_id:'sentinel'}))\""
chk "ignore leaves the archive readable" "no" "$(C "const d=db.getSiblingDB('shop').$NS.findOne({_id:1}); print(d?(d._tcDeletedAt?'yes':'no'):'GONE')")"
chk "and the feeder keeps running"       "1"  "$(pgrep -fc 'cline feeder --config /root/edge_ignore' || true)"

stopFeeder
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
H "db.getSiblingDB('shop').$NS.drop()" >/dev/null
C "db.getSiblingDB('shop').$NS.drop()" >/dev/null
rm -f /var/log/cline/edge_halt.log
startFeeder /root/edge_halt.yaml edge_halt.log
HP=$FP
H "db.getSiblingDB('shop').$NS.insertOne({_id:2,v:'b'})" >/dev/null
waitMirrored $NS 1
H "db.getSiblingDB('shop').$NS.deleteOne({_id:2})" >/dev/null
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
chk "and says why in the log file"       "true" "$(grep -qE 'level=ERROR.*feeder stopped.*unannounced' /var/log/cline/edge_halt.log && echo true || echo false)"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
