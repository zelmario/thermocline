#!/bin/bash
# The lifecycle of a document, and the operational corners around it.
#
# Everything here was verified by hand at some point and never scripted, which
# means it was verified once and has been assumed ever since. Residency is the
# one that matters most: without it a copy-up and the next eviction sweep undo
# each other forever, and the fast tier thrashes while both tiers stay busy.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-50s %s
' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-50s got=%s want=%s
' "$1" "$3" "$2"; fi
}
R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file /var/log/cline/life_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file /var/log/cline/life_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet       --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on 27100"; exit 2
}

# Two hours old, so well past the forty minute eviction boundary. Built by hand
# because ObjectId.createFromTime zeroes the bytes after the timestamp, so bulk
# loading fabricated ages collides on _id.
OID=$(printf '%08x%016x' $(( $(date +%s) - 7200 )) 991)
OID2=$(printf '%08x%016x' $(( $(date +%s) - 7200 )) 992)

echo "== setup =="
stopDaemons
for t in orders tttl tdrop tbig tconc; do
  H "db.getSiblingDB('shop').$t.drop()" >/dev/null
  C "db.getSiblingDB('shop').$t.drop()" >/dev/null
done
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"}); db.getSiblingDB("_thermocline").watermarks.deleteMany({}); db.getSiblingDB("_thermocline").residency.deleteMany({})' >/dev/null
startFeeder
startRouter

echo "== residency protects a document that was just copied up =="
# Archive only: the fast tier has never seen these, which is what an evicted
# document looks like.
C "db.getSiblingDB('shop').orders.insertMany([
     {_id:ObjectId('$OID'), n:'resident'},
     {_id:ObjectId('$OID2'), n:'untouched'}])" >/dev/null
chk "the fast tier starts empty" "0" "$(H "print(db.getSiblingDB('shop').orders.countDocuments({}))")"
chk "a point lookup finds it"    "resident" "$(R "const d=db.getSiblingDB('shop').orders.findOne({_id:ObjectId('$OID')}); print(d?d.n:'MISSING')")"
sleep 4
chk "and it was copied up"       "1" "$(H "print(db.getSiblingDB('shop').orders.countDocuments({_id:ObjectId('$OID')}))")"
chk "residency was recorded"     "1" "$(C "print(db.getSiblingDB('_thermocline').residency.countDocuments({}))")"

# The document is two hours old and the rule evicts past forty minutes, so
# without residency the next sweep would take it straight back out and the copy
# up would have been pure waste.
cline evictor --config $CFG --once >/dev/null 2>&1
chk "the evictor left the resident one alone" "1" "$(H "print(db.getSiblingDB('shop').orders.countDocuments({_id:ObjectId('$OID')}))")"
chk "the archive still holds it live"         "true" "$(C "const d=db.getSiblingDB('shop').orders.findOne({_id:ObjectId('$OID')}); print(!!d && !d._tcDeletedAt)")"

echo "== and it is evicted once residency expires =="
# Age the residency record past the 5m window rather than waiting for it.
C "db.getSiblingDB('_thermocline').residency.updateMany({},{\$set:{hotUntil:new Date(Date.now()-3600000)}})" >/dev/null
cline evictor --config $CFG --once >/dev/null 2>&1
chk "the expired resident is evicted now" "0" "$(H "print(db.getSiblingDB('shop').orders.countDocuments({_id:ObjectId('$OID')}))")"
chk "and the archive kept it live"        "true" "$(C "const d=db.getSiblingDB('shop').orders.findOne({_id:ObjectId('$OID')}); print(!!d && !d._tcDeletedAt)")"

echo "== dropping a tiered collection =="
R "db.getSiblingDB('shop').tdrop.insertMany([{_id:1,v:'a'},{_id:2,v:'b'},{_id:3,v:'c'}])" >/dev/null
sleep 4
chk "mirrored before the drop" "3" "$(C "print(db.getSiblingDB('shop').tdrop.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
cline watermark --config $CFG --ns shop.tdrop --complete-all >/dev/null
sleep 7
R "db.getSiblingDB('shop').tdrop.drop()" >/dev/null
sleep 4
chk "the feeder survived the drop" "1" "$(pgrep -fc 'cline feeder' || echo 0)"
chk "the archive kept its copy"    "3" "$(C "print(db.getSiblingDB('shop').tdrop.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
# The collection is gone from the fast tier but the completeness claim is not.
# If the router believes the claim it answers zero and calls it success, while
# the archive holds three documents.
chk "a read after the drop is not silently empty" "3" "$(R "print(db.getSiblingDB('shop').tdrop.countDocuments({}))")"
chk "and the watermark no longer claims it"       "false" "$(cline watermark --config $CFG --ns shop.tdrop 2>&1 | grep -qE 'newer than|all of history' && echo true || echo false)"

echo "== a TTL index on a tiered collection =="
H "db.getSiblingDB('shop').tttl.createIndex({at:1},{expireAfterSeconds:1})" >/dev/null
R "db.getSiblingDB('shop').tttl.insertOne({_id:1,at:new Date(Date.now()-120000),v:'expired'})" >/dev/null
sleep 4
chk "mirrored before expiry" "1" "$(C "print(db.getSiblingDB('shop').tttl.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
echo "  (waiting for the TTL monitor, up to 70s)"
for _ in $(seq 1 35); do
  [ "$(H "print(db.getSiblingDB('shop').tttl.countDocuments({}))")" = "0" ] && break
  sleep 2
done
chk "the TTL monitor removed it from the fast tier" "0" "$(H "print(db.getSiblingDB('shop').tttl.countDocuments({}))")"
sleep 5
chk "the feeder is still running"                   "1" "$(pgrep -fc 'cline feeder' || echo 0)"
chk "the archive tombstoned it, not kept it live"   "0" "$(C "print(db.getSiblingDB('shop').tttl.countDocuments({_tcDeletedAt:{\$exists:false}}))")"

echo "== a large document survives a copy-up =="
R "db.getSiblingDB('shop').tbig.insertOne({_id:1, blob:'x'.repeat(8*1024*1024)})" >/dev/null
sleep 6
chk "the big document mirrored" "1" "$(C "print(db.getSiblingDB('shop').tbig.countDocuments({}))")"
pkill -f 'cline feeder'; sleep 1
H "db.getSiblingDB('shop').tbig.deleteOne({_id:1})" >/dev/null
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder
chk "it reads back through the router" "8388608" "$(R "const d=db.getSiblingDB('shop').tbig.findOne({_id:1}); print(d?d.blob.length:'MISSING')")"
sleep 4
chk "and it was copied up whole"       "8388608" "$(H "const d=db.getSiblingDB('shop').tbig.findOne({_id:1}); print(d?d.blob.length:'MISSING')")"

echo "== authentication failures pass through =="
chk "a wrong password is rejected" "true" "$(mongosh --port 27100 -u dba -p wrongpassword --authenticationDatabase admin --quiet --eval 'print(1)' 2>&1 | grep -qiE 'authentication failed|AuthenticationFailed' && echo true || echo false)"
chk "an unknown user is rejected"  "true" "$(mongosh --port 27100 -u nosuchuser -p x --authenticationDatabase admin --quiet --eval 'print(1)' 2>&1 | grep -qiE 'authentication failed|AuthenticationFailed' && echo true || echo false)"

echo "== concurrent clients falling through to the archive =="
R "const c=db.getSiblingDB('shop').tconc; const d=[]; for(let i=0;i<200;i++) d.push({_id:i,n:i,grp:'g'+(i%5)}); c.insertMany(d);" >/dev/null
sleep 6
cline watermark --config $CFG --ns shop.tconc --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
rm -f /tmp/conc.*.out
# Wait for these clients only. A bare `wait` also waits for the feeder and the
# router, which are background jobs of this same shell and never exit, so the
# suite hangs forever with every check already passed.
CLIENTS=()
for i in $(seq 1 10); do
  ( mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "
      const c = db.getSiblingDB('shop').tconc;
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
