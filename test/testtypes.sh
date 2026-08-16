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
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-54s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-54s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0; sleep 0.5; done
}

echo "== setup =="
stopDaemons
H "db.getSiblingDB('shop').tcap.drop(); db.getSiblingDB('shop').tts.drop()" >/dev/null
C "db.getSiblingDB('shop').tcap.drop(); db.getSiblingDB('shop').tts.drop()" >/dev/null
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"}); db.getSiblingDB("_thermocline").watermarks.deleteMany({}); db.getSiblingDB("_thermocline").residency.deleteMany({})' >/dev/null

echo "== a capped collection in feeder.namespaces =="
# Small enough that a few kilobytes of documents force it to roll.
H "db.getSiblingDB('shop').createCollection('tcap',{capped:true,size:4096})" >/dev/null
chk "the fast tier collection is capped"               "true" \
    "$(H "const c=db.getSiblingDB('shop').getCollectionInfos({name:'tcap'})[0]; print(!!(c.options&&c.options.capped))")"

# The feeder must not start against a capped namespace: mirroring one destroys
# the archive it is supposed to fill, so this has to fail loudly at startup
# rather than quietly at rollover.
# A timeout, because a feeder that does not refuse simply runs: the check is
# whether it stops with a reason, and without one this would wait forever.
OUT=$(timeout 20 cline feeder --config $CFG 2>&1 | tail -3)
STARTED=$(pgrep -fc 'cline feeder' 2>/dev/null || true)
pkill -f 'cline feeder' 2>/dev/null
chk "the feeder refuses to mirror a capped collection"  "true" \
    "$(echo "$OUT" | grep -qi "capped" && echo true || echo "false ($(echo "$OUT" | tail -1 | cut -c1-60))")"
chk "and it did not stay running"                       "0"    "$STARTED"

echo "== a time series collection in feeder.namespaces =="
H "db.getSiblingDB('shop').createCollection('tts',{timeseries:{timeField:'at'}})" >/dev/null
chk "the fast tier collection is time series"           "true" \
    "$(H "const c=db.getSiblingDB('shop').getCollectionInfos({name:'tts'})[0]; print(c.type==='timeseries')")"
OUT2=$(timeout 20 cline feeder --config $CFG 2>&1 | tail -3)
pkill -f 'cline feeder' 2>/dev/null
chk "the feeder refuses to mirror a time series"        "true" \
    "$(echo "$OUT2" | grep -qiE "time series|timeseries" && echo true || echo "false ($(echo "$OUT2" | tail -1 | cut -c1-60))")"

echo "== and an ordinary collection is unaffected =="
H "db.getSiblingDB('shop').tcap.drop(); db.getSiblingDB('shop').tts.drop()" >/dev/null
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
cline feeder --config $CFG --log-file /var/log/cline/types_f.log & FP=$!
for _ in $(seq 1 60); do
  [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && break
  sleep 0.5
done
chk "the feeder starts once they are gone"              "1" "$(pgrep -fc 'cline feeder' || true)"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
