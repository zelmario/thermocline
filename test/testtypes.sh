#!/bin/bash
# Collection types that cannot be tiered, and what happens when they are.
#
# The feeder mirrors a change stream, and it decides what a delete means by
# asking whether the evictor announced it. An announced delete is an eviction
# and the archive keeps its copy; an unannounced one came from the application
# and the archive tombstones it, so reads stop returning it.
#
# A capped collection breaks that reasoning at the root. It removes its own
# oldest documents when it fills, nobody announces those, and so the archive
# marks them deleted. The collection rolls, as it is designed to, and the
# archive quietly discards the history it exists to keep.
#
# A time series collection breaks it differently: the documents live in system
# buckets rather than where they are written, so what the feeder mirrors is not
# the collection the application reads.
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
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-54s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-54s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}

echo "== setup =="
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).tcap.drop(); db.getSiblingDB(process.env.TC_DB).tts.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tcap.drop(); db.getSiblingDB(process.env.TC_DB).tts.drop()" >/dev/null
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"}); db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).residency.deleteMany({})' >/dev/null

echo "== a capped collection in feeder.namespaces =="
# Small enough that a few kilobytes of documents force it to roll.
H "db.getSiblingDB(process.env.TC_DB).createCollection('tcap',{capped:true,size:4096})" >/dev/null
chk "the fast tier collection is capped"               "true" \
    "$(H "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'tcap'})[0]; print(!!(c.options&&c.options.capped))")"

# The feeder must not start against a capped namespace: mirroring one destroys
# the archive it is supposed to fill, so this has to fail loudly at startup
# rather than quietly at rollover.
# A timeout, because a feeder that does not refuse simply runs: the check is
# whether it stops with a reason, and without one this would wait forever.
OUT=$(timeout 20 cline feeder --config $CFG 2>&1 | tail -3)
STARTED=$(pgrep -fc "cline feeder --config $WDIR/" 2>/dev/null || true)
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
chk "the feeder refuses to mirror a capped collection"  "true" \
    "$(echo "$OUT" | grep -qi "capped" && echo true || echo "false ($(echo "$OUT" | tail -1 | cut -c1-60))")"
chk "and it did not stay running"                       "0"    "$STARTED"

echo "== a time series collection in feeder.namespaces =="
H "db.getSiblingDB(process.env.TC_DB).createCollection('tts',{timeseries:{timeField:'at'}})" >/dev/null
chk "the fast tier collection is time series"           "true" \
    "$(H "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'tts'})[0]; print(c.type==='timeseries')")"
OUT2=$(timeout 20 cline feeder --config $CFG 2>&1 | tail -3)
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
chk "the feeder refuses to mirror a time series"        "true" \
    "$(echo "$OUT2" | grep -qiE "time series|timeseries" && echo true || echo "false ($(echo "$OUT2" | tail -1 | cut -c1-60))")"

echo "== and an ordinary collection is unaffected =="
H "db.getSiblingDB(process.env.TC_DB).tcap.drop(); db.getSiblingDB(process.env.TC_DB).tts.drop()" >/dev/null
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
cline feeder --config $CFG --log-file $LOGDIR/types_f.log & FP=$!
for _ in $(seq 1 60); do
  [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && break
  sleep 0.5
done
chk "the feeder starts once they are gone"              "1" "$(pgrep -fc "cline feeder --config $WDIR/" || true)"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
