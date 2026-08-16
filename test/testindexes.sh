#!/bin/bash
# What happens when the two tiers have different indexes, which they always do.
#
# Nothing copies indexes between the tiers, in either direction, and that is
# deliberate: the fast tier carries the application's query plans, and the
# rollout guide tells operators to drop most of the cheap tier's indexes to
# reclaim its storage. So the archive is expected to be able to *find* any
# document and not expected to be able to answer any query efficiently.
#
# The line that matters is between slow and broken. A fallback read that scans
# is fine. A fallback read that errors is an application that worked before
# adoption and does not after, which is the promise this whole system makes.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
NS=tidx
N=${N:-40000}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file /var/log/cline/idx_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file /var/log/cline/idx_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on 27100"; exit 2
}

echo "== setup: $N documents with a kilobyte each =="
stopDaemons
H "db.getSiblingDB('shop').$NS.drop()" >/dev/null
C "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'}); db.getSiblingDB('_thermocline').watermarks.deleteMany({}); db.getSiblingDB('_thermocline').residency.deleteMany({})" >/dev/null
startFeeder
startRouter

# A kilobyte of padding each, so a blocking sort over the collection is well
# past the 32 MB in-memory sort limit. That limit is the thing being probed:
# with an index the sort is free, without one it has to be spilled or refused.
H "const c=db.getSiblingDB('shop').$NS; const pad='x'.repeat(1024);
   for (let b=0; b<$N/2000; b++) {
     const d=[];
     for (let i=0;i<2000;i++){ const n=b*2000+i; d.push({_id:n, sortme:($N-n), grp:'g'+(n%10), body:pad}); }
     c.insertMany(d);
   }" >/dev/null
for _ in $(seq 1 300); do
  [ "$(C "print(db.getSiblingDB('shop').$NS.countDocuments({_tcDeletedAt:{\$exists:false}}))")" = "$N" ] && break
  sleep 1
done
echo "  archive holds $(C "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"

echo "== indexes are not copied, in either direction =="
# Stated as a check rather than a comment, because somebody will eventually
# assume otherwise and this says so out loud.
H "db.getSiblingDB('shop').$NS.createIndex({sortme:1},{name:'fast_sortme'})" >/dev/null
R "db.getSiblingDB('shop').$NS.createIndex({grp:1},{name:'via_router_grp'})" >/dev/null
sleep 4
chk "the fast tier has the index"               "true"  "$(H "print(db.getSiblingDB('shop').$NS.getIndexes().some(i=>i.name=='fast_sortme'))")"
chk "the archive did not get it"                "false" "$(C "print(db.getSiblingDB('shop').$NS.getIndexes().some(i=>i.name=='fast_sortme'))")"
chk "nor one created through the router"        "false" "$(C "print(db.getSiblingDB('shop').$NS.getIndexes().some(i=>i.name=='via_router_grp'))")"

echo "== every read below falls through to the archive =="
cline watermark --config $CFG --ns shop.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "an unsorted fallback read is answered"     "$N" "$(R "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"

echo "== a sort the archive has no index for =="
# PSMDB 8.0 ships allowDiskUseByDefault true and a 100 MB sort limit, so the
# easy case passes for free and proves nothing. These reads are run against a
# deliberately tightened archive: a 1 MB sort limit, which $N kilobyte
# documents cannot possibly fit inside. That is also a real configuration,
# since operators lower these to protect a cluster from expensive queries.
C 'db.adminCommand({setParameter:1, internalQueryMaxBlockingSortMemoryUsageBytes: 1048576})' >/dev/null
# This is the one that matters. The fast tier answers it from an index. The
# archive has to sort $N kilobyte documents with nothing to help it, which is
# far past the 32 MB in-memory limit, so it either spills to disk or fails. A
# query that worked before adoption must not start failing after it.
SORTED=$(R "
  try {
    const c=db.getSiblingDB('shop').$NS.find({}).sort({sortme:1}).limit(3).toArray();
    print(c.length===3 ? c[0].sortme : 'SHORT:'+c.length);
  } catch(e) { print('ERR '+e.message.substring(0,70)); }")
chk "a sorted fallback read is answered"        "1" "$SORTED"
# And the whole thing, not just the first page, since a limit lets the server
# use a top-k sort that fits in memory whatever the collection size.
BIGSORT=$(R "
  try { print(db.getSiblingDB('shop').$NS.find({}).sort({sortme:1}).itcount()); }
  catch(e) { print('ERR '+e.message.substring(0,70)); }")
chk "a full sorted scan is answered"            "$N" "$BIGSORT"

echo "== the same, with the archive refusing to spill =="
# The configuration where this genuinely breaks: no disk use, and a limit the
# sort cannot fit in. If the router does not ask for disk use itself, a query
# the fast tier answers from an index becomes an error on the archive.
C 'db.adminCommand({setParameter:1, allowDiskUseByDefault: false})' >/dev/null
NODISK=$(R "
  try { print(db.getSiblingDB('shop').$NS.find({}).sort({sortme:1}).itcount()); }
  catch(e) { print('ERR '+e.message.substring(0,60)); }")
chk "a sorted read works even with spilling off" "$N" "$NODISK"
C 'db.adminCommand({setParameter:1, allowDiskUseByDefault: true})' >/dev/null

echo "== an aggregation sort, which has its own rules =="
AGGSORT=$(R "
  try { print(db.getSiblingDB('shop').$NS.aggregate([{\$sort:{sortme:1}},{\$limit:1}]).toArray()[0].sortme); }
  catch(e) { print('ERR '+e.message.substring(0,70)); }")
chk "a sorted aggregation is answered"          "1" "$AGGSORT"

echo "== a query whose index exists only on the fast tier =="
chk "a filter on an archive-unindexed field works" "4000" "$(R "print(db.getSiblingDB('shop').$NS.countDocuments({grp:'g3'}))")"

# Put the archive back the way it was found.
C 'db.adminCommand({setParameter:1, internalQueryMaxBlockingSortMemoryUsageBytes: 104857600})' >/dev/null
C 'db.adminCommand({setParameter:1, allowDiskUseByDefault: true})' >/dev/null

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
