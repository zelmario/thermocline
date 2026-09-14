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

echo "== a queryable encryption collection in feeder.namespaces =="
# The quietest of the three. Its documents copy across the way any binary field
# would, so mirroring would report success; what does not copy is the state
# mongod keeps beside them, in the enxcol_ collections it creates alongside. An
# encrypted read that falls through would then be answered from an archive
# holding the documents without the state that makes their fields searchable.
#
# In its own database, because creating one also creates two enxcol_ collections
# and every other suite that lists a catalog would see them.
QEDB=qe_$DB
export TC_QEDB=$QEDB
H "db.getSiblingDB(process.env.TC_QEDB).dropDatabase()" >/dev/null
H "db.getSiblingDB(process.env.TC_QEDB).createCollection('tqe',{encryptedFields:{fields:[{path:'ssn',bsonType:'string',keyId:UUID('12345678-1234-9876-1234-123456789012')}]}})" >/dev/null
chk "the fast tier collection is encrypted"             "true" \
    "$(H "const c=db.getSiblingDB(process.env.TC_QEDB).getCollectionInfos({name:'tqe'})[0]; print(!!(c&&c.options&&c.options.encryptedFields))")"
chk "and mongod made its state collections"             "2" \
    "$(H "print(db.getSiblingDB(process.env.TC_QEDB).getCollectionInfos().filter(c=>c.name.startsWith('enxcol_')).length)")"
sed 's|    - "'$DB'.tcap"|    - "'$DB'.tcap"\n    - "'$QEDB'.tqe"|' $CFG > $WDIR/withqe.yaml
# tail -20, not tail -3: the capped and time series collections from the sections
# above are still configured, so the refusal names three namespaces and the last
# three lines are not the ones this is about.
OUTQ=$(timeout 20 cline feeder --config $WDIR/withqe.yaml 2>&1 | tail -20)
STARTEDQ=$(pgrep -fc "cline feeder --config $WDIR/" 2>/dev/null || true)
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
chk "the feeder refuses to mirror it"                   "true" \
    "$(echo "$OUTQ" | grep -qi "queryable encryption" && echo true || echo "false ($(echo "$OUTQ" | tail -1 | cut -c1-60))")"
chk "and it did not stay running"                       "0"    "$STARTEDQ"
# And the state collection named directly, which is the other way to configure
# this wrong: it belongs to mongod rather than to the application.
sed 's|    - "'$DB'.tcap"|    - "'$DB'.tcap"\n    - "'$QEDB'.enxcol_.tqe.esc"|' $CFG > $WDIR/withesc.yaml
OUTE=$(timeout 20 cline feeder --config $WDIR/withesc.yaml 2>&1 | tail -20)
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
chk "the state collection is refused as well"           "true" \
    "$(echo "$OUTE" | grep -qi "state collection" && echo true || echo "false ($(echo "$OUTE" | tail -1 | cut -c1-60))")"
H "db.getSiblingDB(process.env.TC_QEDB).dropDatabase()" >/dev/null

echo "== an empty namespace list is checked too =="
# "Mirror everything" used to mean "check nothing": unmirrorable returned early
# on an empty list, so the configuration that mirrors the most did the least
# checking. A capped collection anywhere in the deployment was then mirrored, and
# the archive tombstoned its history at the first rollover.
#
# The capped collection from the section above is recreated for this, because
# what has to be caught is a collection nothing named.
H "db.getSiblingDB(process.env.TC_DB).createCollection('tcap',{capped:true,size:4096})" >/dev/null 2>&1
cat > $WDIR/typesall.yaml <<YEOF
hot:
  uri: "$HOT"
cold:
  uri: "$COLD"
meta:
  db: "$META"
feeder:
  id: "test"
  namespaces: []
YEOF
OUTA=$(timeout 40 cline feeder --config $WDIR/typesall.yaml 2>&1 | tail -20)
STARTEDA=$(pgrep -fc "cline feeder --config $WDIR/" 2>/dev/null || true)
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
chk "the feeder refuses on a collection nobody named" "true" \
    "$(echo "$OUTA" | grep -qi "cannot be mirrored" && echo true || echo "false ($(echo "$OUTA" | tail -1 | cut -c1-60))")"
chk "and names the capped one it found"               "true" \
    "$(echo "$OUTA" | grep -q "$DB.tcap is a capped collection" && echo true || echo false)"
chk "and says what to do about an empty list"        "true" \
    "$(echo "$OUTA" | grep -q 'feeder.namespaces is empty' && echo true || echo false)"
chk "and it did not stay running"                    "0" "$STARTEDA"
# And not over a view or a time series collection, which is the other half of
# this. Nothing of either is ever mirrored, so a deployment that mirrors
# everything and happens to have a view must still start: refusing there would
# make an empty namespace list unusable on almost any real cluster. The suites
# leave views behind on this cluster, so the message above having named only the
# capped one is the check.
chk "and not over a view it found on the way"        "0" \
    "$(echo "$OUTA" | grep -c 'is a view' || true)"
H "db.getSiblingDB(process.env.TC_DB).tcap.drop()" >/dev/null

echo "== and an ordinary collection is unaffected =="
H "db.getSiblingDB(process.env.TC_DB).tcap.drop(); db.getSiblingDB(process.env.TC_DB).tts.drop()" >/dev/null
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
cline feeder --config $CFG --log-file $LOGDIR/types_f.log & FP=$!
for _ in $(seq 1 60); do
  [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && break
  sleep 0.5
done
chk "the feeder starts once they are gone"              "1" "$(pgrep -fc "cline feeder --config $WDIR/" || true)"

echo "== a view in feeder.namespaces =="
# A view holds no documents of its own, only a pipeline over another
# collection, so there is nothing to mirror. Before this was refused the feeder
# accepted one and logged that it was mirroring it, which is what an operator
# gets for following the router's own advice about a stranded namespace: the
# configuration is accepted, reported as working, and cannot work.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).tview.drop()" >/dev/null 2>&1
H "db.getSiblingDB(process.env.TC_DB).createView('tview','tcap',[{\$match:{n:{\$gte:0}}}])" >/dev/null 2>&1
chk "the fast tier has a view"                          "view" \
    "$(H "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'tview'})[0]; print(c?c.type:'none')")"
sed 's|    - "'$DB'.tcap"|    - "'$DB'.tcap"\n    - "'$DB'.tview"|' $CFG > $WDIR/withview.yaml
OUTV=$(timeout 20 cline feeder --config $WDIR/withview.yaml 2>&1 | tail -3)
STARTEDV=$(pgrep -fc "cline feeder --config $WDIR/" 2>/dev/null || true)
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
chk "the feeder refuses to mirror a view"               "true" \
    "$(echo "$OUTV" | grep -qi "is a view" && echo true || echo "false ($(echo "$OUTV" | tail -1 | cut -c1-60))")"
chk "and it did not stay running"                       "0"    "$STARTEDV"
H "db.getSiblingDB(process.env.TC_DB).tview.drop()" >/dev/null 2>&1

echo "== a capped collection created after the feeder started =="
# Every check above asks the question at startup, which is the only time it was
# ever asked. A collection that does not exist yet cannot be looked at, and
# nothing looked again: name a namespace nothing has created, or leave
# feeder.namespaces empty so every namespace is named, and the collection can be
# created capped afterwards with the feeder already running.
#
# Measured before this, on a capped collection of 4KB filled with 400 documents:
# the fast tier kept the 18 it had room for, and the archive held tombstones over
# the rest. Documents nobody deleted, marked deleted in the tier that exists to
# keep them, by a collection doing exactly what capped means.
#
# The remedy is the one the startup check already chose, for the reason written
# at the top of collkind.go: the feeder stops, with the namespace named. A feeder
# that carried on and skipped the namespace would mirror less than its
# configuration says, and the router would answer reads of it from an archive
# holding none of it.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).tcap.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tcap.drop()" >/dev/null
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
chk "neither tier holds it, so the feeder can start"    "false" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().includes('tcap'))")"
# Truncated first. A log file is appended to, so a previous run's refusal is
# still in it, and both checks below then pass against a binary that never
# refused anything: measured, exactly that, against the binary from before this.
: > $LOGDIR/t_types_late.log
cline feeder --config $CFG --log-file $LOGDIR/t_types_late.log &
LATEFP=$!
for _ in $(seq 1 40); do
  [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && break
  sleep 0.5
done
chk "the feeder is running"                             "1" \
    "$(pgrep -fc "cline feeder --config $WDIR/" 2>/dev/null || true)"
# Created out of band, straight at the fast tier, which is how a tool or an
# operator makes one. Through the router it is refused, and that refusal is
# testddl.sh's.
H "db.getSiblingDB(process.env.TC_DB).createCollection('tcap',{capped:true,size:4096})" >/dev/null
H "for(let i=0;i<400;i++){db.getSiblingDB(process.env.TC_DB).tcap.insertOne({_id:i,pad:'x'.repeat(200)})}" >/dev/null
sleep 8
LATEHOT=$(H "print(db.getSiblingDB(process.env.TC_DB).tcap.countDocuments({}))")
LATEDEAD=$(C "print(db.getSiblingDB(process.env.TC_DB).tcap.countDocuments({_tcDeletedAt:{\$exists:true}}))")
echo "  the fast tier kept $LATEHOT of 400, the archive tombstoned $LATEDEAD"
chk "the archive tombstoned none of them"               "0" "$LATEDEAD"
# The refusal message, not the word "capped": the log names the namespace it is
# mirroring at startup, and this collection is called tcap, so a grep for the
# word passes whatever happens. A check that cannot fail is worse than no check.
chk "the feeder stopped, saying why"                    "true" \
    "$(grep -q "cannot be mirrored" $LOGDIR/t_types_late.log && echo true || echo false)"
chk "and it named the collection and the kind"          "true" \
    "$(grep -q "kind=\"a capped collection\"" $LOGDIR/t_types_late.log && echo true || echo false)"
chk "and it is not still mirroring"                     "0" \
    "$(pgrep -fc "cline feeder --config $WDIR/" 2>/dev/null || true)"
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
wait $LATEFP 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).tcap.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tcap.drop()" >/dev/null

echo "== a collection converted to capped while it was being mirrored =="
# The section above closed the door for a collection created capped after the
# feeder started. checkNamespaceKind asks the first time an event for a
# namespace arrives and memoises the answer, forgetting it only on a drop. So
# one door was left: a collection already being mirrored, converted afterwards.
#
# Through the router convertToCapped is refused, and that refusal is
# testddl.sh's. Straight at the fast tier's mongod it is accepted, and nothing
# here asked again.
#
# Measured before this, twenty mirrored documents and then convertToCapped with
# size 4096: mongod accepted it, the feeder went on running, and filling past
# the cap left the archive with sixty tombstones and two of the original twenty
# still live. The history the archive exists to keep, discarded, with nothing
# said anywhere.
#
# Unannounced deletes are what asks the question again. They are the only way
# this does damage and they are its only symptom: a capped collection rolling
# produces nothing else.
stopDaemons
CNS=tcapconv
H "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null 2>&1
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
: > $LOGDIR/t_types_conv.log
cline feeder --config $CFG --log-file $LOGDIR/t_types_conv.log &
CONVFP=$!
for _ in $(seq 1 40); do
  [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && break
  sleep 0.5
done
# An ordinary collection, mirrored the ordinary way, so the kind check has
# already run and said yes.
H "const d=[]; for(let i=1;i<=20;i++) d.push({_id:i, pad:'x'.repeat(200)});
   db.getSiblingDB(process.env.TC_DB).$CNS.insertMany(d)" >/dev/null
for _ in $(seq 1 60); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({}))")" = "20" ] && break
  sleep 0.5
done
chk "twenty documents mirrored before the conversion"   "20" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
H "db.getSiblingDB(process.env.TC_DB).runCommand({convertToCapped:'$CNS', size:4096})" >/dev/null 2>&1
chk "mongod converted it, out of band"                  "true" \
    "$(H "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'$CNS'})[0]; print(!!(c && c.options && c.options.capped))")"
# Filling it past the cap is what makes mongod roll the oldest off, which is
# what produces the deletes nothing announced.
H "for(let i=100;i<160;i++){db.getSiblingDB(process.env.TC_DB).$CNS.insertOne({_id:i,pad:'y'.repeat(200)})}" >/dev/null
sleep 10
CONVDEAD=$(C "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({_tcDeletedAt:{\$exists:true}}))")
CONVLIVE=$(C "print(db.getSiblingDB(process.env.TC_DB).$CNS.countDocuments({_id:{\$lte:20}, _tcDeletedAt:{\$exists:false}}))")
echo "  the archive tombstoned $CONVDEAD, and $CONVLIVE of the original 20 are still live"
chk "the archive kept all twenty originals"             "20" "$CONVLIVE"
chk "and tombstoned none of them"                       "0"  "$CONVDEAD"
chk "the feeder stopped, saying why"                    "true" \
    "$(grep -q "cannot be mirrored" $LOGDIR/t_types_conv.log && echo true || echo false)"
chk "and it named the kind"                             "true" \
    "$(grep -q "kind=\"a capped collection\"" $LOGDIR/t_types_conv.log && echo true || echo false)"
chk "and it is not still mirroring"                     "0" \
    "$(pgrep -fc "cline feeder --config $WDIR/" 2>/dev/null || true)"
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
wait $CONVFP 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$CNS.drop()" >/dev/null 2>&1

echo "== an ordinary collection whose own deletes are unannounced =="
# The control for the section above, and the one that matters: the recheck is
# triggered by deletes nothing announced, and an application deleting its own
# documents produces exactly those. If it stopped the feeder for them it would
# stop for every application that deletes anything, which is most of them.
#
# Same collection kind, same trigger, opposite answer.
stopDaemons
DNS=tdelplain
H "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null 2>&1
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
: > $LOGDIR/t_types_plain.log
cline feeder --config $CFG --log-file $LOGDIR/t_types_plain.log &
PLAINFP=$!
for _ in $(seq 1 40); do
  [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && break
  sleep 0.5
done
H "const d=[]; for(let i=1;i<=20;i++) d.push({_id:i, pad:'x'});
   db.getSiblingDB(process.env.TC_DB).$DNS.insertMany(d)" >/dev/null
for _ in $(seq 1 60); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).$DNS.countDocuments({}))")" = "20" ] && break
  sleep 0.5
done
# The application removing its own documents, which nothing announced because
# no evictor was involved.
H "db.getSiblingDB(process.env.TC_DB).$DNS.deleteMany({_id:{\$lte:10}})" >/dev/null
sleep 8
chk "the feeder read them as unannounced"                "true" \
    "$(grep -q "deletes not announced" $LOGDIR/t_types_plain.log && echo true || echo false)"
chk "and did not stop over them"                         "1" \
    "$(pgrep -fc "cline feeder --config $WDIR/" 2>/dev/null || true)"
chk "nor said this collection cannot be mirrored"        "0" \
    "$(grep -c "cannot be mirrored" $LOGDIR/t_types_plain.log 2>/dev/null || true)"
chk "and the deletes reached the archive"                "10" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$DNS.countDocuments({_tcDeletedAt:{\$exists:true}}))")"
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
wait $PLAINFP 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).$DNS.drop()" >/dev/null 2>&1

echo "== the same collection, capped on the archive instead =="
# Which tier holds the collection depends on where the deployment is in its
# life. Ordinarily the application's collections are on the fast tier and the
# archive's copies are the feeder's. An adoption is the other way round: the
# existing cluster becomes the archive carrying every collection it ever had,
# and the fast tier is new and empty.
#
# So the refusal has to look at both. Measured before it did: the feeder started
# against a capped collection that existed only on the archive and reported it
# was mirroring the namespace, which is the thing this refusal exists to
# prevent.
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).tcap.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tcap.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).createCollection('tcap',{capped:true,size:4096})" >/dev/null
chk "only the archive holds it, and it is capped"       "true" \
    "$(C "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'tcap'})[0]; print(!!(c.options&&c.options.capped))")"
chk "and the fast tier does not hold it at all"         "false" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().includes('tcap'))")"
OUT3=$(timeout 20 cline feeder --config $CFG 2>&1 | tail -3)
STARTED3=$(pgrep -fc "cline feeder --config $WDIR/" 2>/dev/null || true)
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
chk "the feeder refuses it on the archive too"          "true" \
    "$(echo "$OUT3" | grep -qi "capped" && echo true || echo "false ($(echo "$OUT3" | tail -1 | cut -c1-60))")"
chk "and it did not stay running"                       "0"    "$STARTED3"
C "db.getSiblingDB(process.env.TC_DB).tcap.drop()" >/dev/null

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
