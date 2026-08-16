#!/bin/bash
# Mixed traffic through the router, so the dashboard has every case to show.
#
# A dashboard that has only ever seen fast tier hits proves nothing, and one
# built from made-up numbers proves less. This drives real commands down each
# path the router distinguishes: hits, archive fallbacks, point misses that copy
# a document up, a transaction that has to be replayed, and a cross-tier
# duplicate that gets refused.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.3:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
NS=tdash
N=${N:-4000}
LOOPS=${LOOPS:-40}

R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
stopFeeder() {
  pkill -f 'cline feeder' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file /var/log/cline/dash_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
}
startRouter() {
  cline router --config $CFG --log-file /var/log/cline/dash_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "FATAL: the router never came up"; exit 2
}

echo "== setup: $N documents =="
stopDaemons
H "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('shop').${NS}u.drop()" >/dev/null
C "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('shop').${NS}u.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'}); db.getSiblingDB('_thermocline').watermarks.deleteMany({}); db.getSiblingDB('_thermocline').residency.deleteMany({})" >/dev/null
H "db.getSiblingDB('_thermocline').keys.deleteMany({ns:{\$in:['shop.$NS','shop.${NS}u']}})" >/dev/null
startFeeder
startRouter

H "const c=db.getSiblingDB('shop').$NS; const d=[];
   for(let i=0;i<$N;i++) d.push({_id:i, n:i, grp:'g'+(i%20), name:'row-'+i});
   c.insertMany(d); c.createIndex({n:1}); c.createIndex({grp:1});" >/dev/null
H "db.getSiblingDB('shop').${NS}u.createIndex({email:1},{unique:true})" >/dev/null
for _ in $(seq 1 120); do
  [ "$(C "print(db.getSiblingDB('shop').$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")" = "$N" ] && break
  sleep 1
done
C "db.getSiblingDB('shop').$NS.createIndex({n:1}); db.getSiblingDB('shop').$NS.createIndex({grp:1})" >/dev/null
echo "  archive holds $(C "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"

# A tail of the collection lives only in the archive from here on, which is what
# produces read misses rather than hits.
echo "== evicting the oldest quarter behind the feeder =="
stopFeeder
H "db.getSiblingDB('shop').$NS.deleteMany({n:{\$lt:$((N/4))}})" >/dev/null
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder

echo "== driving traffic ($LOOPS rounds) =="
# One unique owner parked in the archive, so a duplicate can be refused later.
R "db.getSiblingDB('shop').${NS}u.insertOne({_id:'owner',email:'taken@x.com'})" >/dev/null
for _ in $(seq 1 60); do
  [ "$(C "print(db.getSiblingDB('shop').${NS}u.countDocuments({_tcDeletedAt:{\$exists:false}}))")" = "1" ] && break
  sleep 0.5
done
stopFeeder
H "db.getSiblingDB('shop').${NS}u.deleteMany({})" >/dev/null
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder

mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "
const c = db.getSiblingDB('shop').$NS;
const u = db.getSiblingDB('shop').${NS}u;
const N = $N, HOT_FROM = Math.floor(N/4);
let hits=0, misses=0, ranges=0, dups=0, txns=0;

for (let round = 0; round < $LOOPS; round++) {
  // Hits: point lookups the fast tier holds.
  for (let i = 0; i < 25; i++) {
    c.findOne({_id: HOT_FROM + ((round*37 + i*11) % (N - HOT_FROM))});
    hits++;
  }
  // Ranges the fast tier cannot prove it holds, so they go to the archive.
  if (round % 3 === 0) {
    c.countDocuments({grp: 'g' + (round % 20)});
    c.aggregate([{\$group: {_id: '\$grp', n: {\$sum: 1}}}]).toArray();
    ranges += 2;
  }
  // Misses: documents that live only in the archive. Each one is copied up, so
  // a repeat of the same id would be a hit.
  for (let i = 0; i < 3; i++) {
    c.findOne({_id: (round * 3 + i) % HOT_FROM});
    misses++;
  }
  // A transaction reaching an archived document, replayed by the driver.
  if (round % 7 === 0) {
    const s = db.getMongo().startSession();
    try {
      s.withTransaction(() => {
        s.getDatabase('shop').$NS.updateOne({_id: (round * 5) % HOT_FROM}, {\$set: {seen: round}});
      });
      txns++;
    } catch (e) {} finally { s.endSession(); }
  }
  // A cross-tier duplicate, refused by the key registry.
  if (round % 5 === 0) {
    try { u.insertOne({_id: 'dup' + round, email: 'taken@x.com'}); }
    catch (e) { dups++; }
  }
  // Ordinary writes.
  c.updateOne({_id: HOT_FROM + (round % 100)}, {\$inc: {hits: 1}});
}
print('  hits ' + hits + '  misses ' + misses + '  archive reads ' + ranges +
      '  txn replays ' + txns + '  duplicates refused ' + dups);
"

echo
echo "Traffic done. The dashboard is on router.dashboard (27110 in test/cline.yaml)"
echo "and keeps five minutes of history."
echo "Daemons are left running on purpose so the page has something live to show."
