#!/bin/bash
# What each kind of query actually costs through the router.
#
# The design rests on a claim about cost: a read the fast tier can answer is
# barely slower than talking to it directly, and a read it cannot answer is
# slower but correct. That claim has never been measured, so this measures it.
#
# Read the numbers as ratios, not as absolutes. Both tiers here are single-node
# replica sets on one machine, so every latency is optimistic and the archive is
# not a big cheap cluster under load. What survives that is the shape: how much
# the proxy adds, and how much falling through to the archive costs.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
NS=bench
N=${N:-20000}

R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1; }
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
  cline feeder --config $CFG --log-file /var/log/cline/bench_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
}
startRouter() {
  # Truncated, because the stale-read count below greps this file. Appending
  # across runs made it report warnings from an hour ago as if this run had
  # produced them.
  rm -f /var/log/cline/bench_r.log
  cline router --config $CFG --log-file /var/log/cline/bench_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "FATAL: the router never came up"; exit 2
}

echo "== setup: $N documents =="
stopDaemons
H "db.getSiblingDB('shop').$NS.drop()" >/dev/null
C "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'}); db.getSiblingDB('_thermocline').watermarks.deleteMany({}); db.getSiblingDB('_thermocline').residency.deleteMany({})" >/dev/null
# The write section's unique collection has to go from the archive and the key
# registry too, not just the fast tier. Dropping it on one tier leaves the
# mirrored copies live in the other, their key claims still point at owners that
# exist there, and every insert in the next run is refused as an archived
# duplicate. Correct behaviour, wrong fixture.
H "db.getSiblingDB('shop').benchuniq.drop()" >/dev/null
C "db.getSiblingDB('shop').benchuniq.drop()" >/dev/null
H "db.getSiblingDB('_thermocline').keys.deleteMany({ns:'shop.benchuniq'})" >/dev/null
startFeeder
startRouter

H "const c=db.getSiblingDB('shop').$NS;
   for (let b=0; b<$N/1000; b++) {
     const d=[];
     for (let i=0;i<1000;i++){const n=b*1000+i; d.push({_id:n, n:n, grp:'g'+(n%50), name:'item-'+n});}
     c.insertMany(d);
   }
   c.createIndex({n:1}); c.createIndex({grp:1});" >/dev/null
echo "  loaded, waiting for mirroring"
for _ in $(seq 1 600); do
  [ "$(C "print(db.getSiblingDB('shop').$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")" = "$N" ] && break
  sleep 1
done
C "db.getSiblingDB('shop').$NS.createIndex({n:1}); db.getSiblingDB('shop').$NS.createIndex({grp:1})" >/dev/null
echo "  archive holds $(C "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"

BENCH='
function bench(label, reps, fn) {
  for (let w = 0; w < 5; w++) fn(w);            // warm up, ignore
  const t = [];
  for (let i = 0; i < reps; i++) {
    const s = Date.now();
    fn(i);
    t.push(Date.now() - s);
  }
  t.sort((a, b) => a - b);
  const sum = t.reduce((a, b) => a + b, 0);
  print("  " + label.padEnd(44) +
        " mean " + (sum / reps).toFixed(2).padStart(8) + " ms" +
        "   p95 " + String(t[Math.floor(reps * 0.95)]).padStart(5) + " ms" +
        "   max " + String(t[reps - 1]).padStart(5) + " ms");
}
const c = db.getSiblingDB("shop").NSNAME;
'

echo
echo "== the fast tier can answer: what the proxy costs =="
cline watermark --config $CFG --ns shop.$NS --complete-all >/dev/null
sleep 7
echo "-- direct to the fast tier, no router --"
mongosh "$HOT" --quiet --eval "${BENCH//NSNAME/$NS}
bench('point lookup by _id', 500, i => c.findOne({_id: i % $N}));
bench('range, 100 documents', 200, i => c.find({n:{\$gte:(i*7)%($N-100), \$lt:((i*7)%($N-100))+100}}).toArray());
bench('countDocuments, whole collection', 50, () => c.countDocuments({}));
bench('aggregate \$group by 50 keys', 50, () => c.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}}]).toArray());
"
echo "-- through the router --"
mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "${BENCH//NSNAME/$NS}
bench('point lookup by _id', 500, i => c.findOne({_id: i % $N}));
bench('range, 100 documents', 200, i => c.find({n:{\$gte:(i*7)%($N-100), \$lt:((i*7)%($N-100))+100}}).toArray());
bench('countDocuments, whole collection', 50, () => c.countDocuments({}));
bench('aggregate \$group by 50 keys', 50, () => c.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}}]).toArray());
"

echo
echo "== the fast tier cannot answer: what falling through costs =="
cline watermark --config $CFG --ns shop.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "${BENCH//NSNAME/$NS}
bench('range, 100 documents, from archive', 200, i => c.find({n:{\$gte:(i*7)%($N-100), \$lt:((i*7)%($N-100))+100}}).toArray());
bench('countDocuments, from archive', 50, () => c.countDocuments({}));
bench('aggregate \$group, from archive', 50, () => c.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}}]).toArray());
"

echo
echo "== the same reads with the freshness barrier off =="
# The numbers above are not the archive's speed. Before answering from the
# archive the router waits for mirroring to reach the instant the read began,
# and the checkpoint it waits on only moves every feeder.checkpoint_every. So a
# fallback read pays up to one checkpoint interval no matter how fast the query
# is. router.archive_wait: 0 turns that barrier off, which gives up the promise
# that a fallback read sees your own last write, and isolates the query cost.
python3 - <<'PY'
s = open('/root/cline.yaml').read()
s = s.replace('  completeness: archive', '  completeness: archive\n  archive_wait: 0s')
open('/root/bench_nowait.yaml', 'w').write(s)
PY
pkill -f 'cline router' 2>/dev/null
for _ in $(seq 1 30); do pgrep -f 'cline router' >/dev/null 2>&1 || break; sleep 0.5; done
cline router --config /root/bench_nowait.yaml --log-file /var/log/cline/bench_nw.log & RP=$!
for _ in $(seq 1 30); do
  mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "${BENCH//NSNAME/$NS}
bench('range, 100 documents, from archive', 200, i => c.find({n:{\$gte:(i*7)%($N-100), \$lt:((i*7)%($N-100))+100}}).toArray());
bench('countDocuments, from archive', 50, () => c.countDocuments({}));
bench('aggregate \$group, from archive', 50, () => c.aggregate([{\$group:{_id:'\$grp',n:{\$sum:1}}}]).toArray());
"
pkill -f 'cline router' 2>/dev/null
for _ in $(seq 1 30); do pgrep -f 'cline router' >/dev/null 2>&1 || break; sleep 0.5; done
startRouter

echo
echo "== and with the shipped default checkpoint_every: 5s =="
# This config uses 1s so the suites run in minutes. The default is 5s, and the
# barrier can only be as precise as the checkpoint it waits on, so this is what
# a fallback read costs out of the box. Few reps, because each one is slow by
# construction: the point is the interval, not the sample size.
python3 - <<'PY'
s = open('/root/cline.yaml').read()
s = s.replace('checkpoint_every: 1s', 'checkpoint_every: 5s')
open('/root/bench_ckpt5.yaml', 'w').write(s)
PY
stopFeeder
cline feeder --config /root/bench_ckpt5.yaml --log-file /var/log/cline/bench_c5.log & FP=$!
for _ in $(seq 1 60); do
  [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && break
  sleep 0.5
done
mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "${BENCH//NSNAME/$NS}
bench('range, 100 documents, from archive', 20, i => c.find({n:{\$gte:(i*7)%($N-100), \$lt:((i*7)%($N-100))+100}}).toArray());
"
# archive_wait defaults to 5s as well, so once the checkpoint interval reaches
# it, some reads give up waiting and answer stale rather than slow. That is the
# barrier failing quietly, and it is worth counting.
# From the counter, not from the log. The warning is rate limited to one line
# every ten seconds, because a feeder that has fallen behind sends every
# fall-through read down that path and the log would be the first casualty of
# the outage it is meant to describe. Counting lines would now undercount by
# whatever the rate limit swallowed; thermocline_stale_reads_total is exact and
# is what the alert reads.
GAVEUP=$(curl -s --max-time 5 "http://127.0.0.1:${PDASH:-27110}/metrics" 2>/dev/null \
         | awk '/^thermocline_stale_reads_total /{print $2}')
if [ -z "$GAVEUP" ]; then
  # Not zero. A scrape that did not answer and a barrier that never fired print
  # the same 0, and this line exists to report the second one.
  echo "  reads that gave up waiting: UNKNOWN, the metrics endpoint did not answer"
else
  echo "  reads that gave up waiting: $GAVEUP"
fi
stopFeeder
startFeeder

echo
echo "== a read miss: the document is only in the archive =="
# Remove a slice from the fast tier without the feeder seeing it, so those
# documents are archived rather than deleted. The first read of each is a miss.
cline watermark --config $CFG --ns shop.$NS --complete-all >/dev/null
# The feeder has to be gone before the delete, not merely asked to go. If it is
# still watching, it tombstones these documents in the archive instead, every
# read below misses in both tiers, and the "miss" numbers are timings of a
# lookup that found nothing.
stopFeeder
H "db.getSiblingDB('shop').$NS.deleteMany({n:{\$lt:1000}})" >/dev/null
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder
GONE=$(H "print(db.getSiblingDB('shop').$NS.countDocuments({n:{\$lt:1000}}))")
LIVE=$(C "print(db.getSiblingDB('shop').$NS.countDocuments({n:{\$lt:1000},_tcDeletedAt:{\$exists:false}}))")
echo "  fast tier holds $GONE of the slice, archive holds $LIVE live (want 0 and 1000)"
mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "${BENCH//NSNAME/$NS}
// Each id is read once, so every one of these is a genuine miss.
let miss = 0;
bench('point lookup, miss then copy-up', 400, () => c.findOne({_id: miss++}));
bench('point lookup, same ids again (now hot)', 400, i => c.findOne({_id: i % 400}));
"

echo
echo "== writes =="
mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "${BENCH//NSNAME/$NS}
const u = db.getSiblingDB('shop').benchuniq;
u.drop();
let k = 0;
bench('insert, no unique index', 500, () => c.insertOne({_id: 'w'+(k++), n: -1}));
u.createIndex({email:1},{unique:true});
let j = 0;
bench('insert, one unique index (key claimed)', 500, () => u.insertOne({email:'u'+(j++)+'@x.com'}));
let m = 0;
bench('update, new key readable from the command', 300, () => u.updateOne({email:'u'+(m)+'@x.com'},{\$set:{email:'v'+(m++)+'@x.com'}}));
// A pipeline update: the new key is an expression, so it cannot be read off
// the command and the router has to evaluate it against the fast tier first.
// This was a \$rename, which emptied the field instead of moving it, and the
// second document to lose it collided with the first on {email: null}: a
// non-sparse unique index holds exactly one missing field. The failure was the
// benchmark's, not the router's, and it stopped this section short.
let q = 0;
bench('update, new key needs the dry run', 300, () => u.updateOne({email:'v'+(q++)+'@x.com'},[{\$set:{email:{\$concat:['\$email','.x']}}}]));
"

stopDaemons
echo
# Say what was actually measured rather than asserting a topology. This claimed
# single-node sets while running against three-member ones, and a benchmark that
# misdescribes its own environment invites its numbers to be quoted somewhere
# they do not apply.
HOTN=$(H 'print(rs.status().members.length)')
COLDN=$(C 'print(rs.status().members.length)')
echo "Measured against rsHot with $HOTN member(s) and rsCold with $COLDN, all on one"
echo "machine, so treat these as ratios rather than as production latencies."
