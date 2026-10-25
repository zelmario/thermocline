#!/bin/bash
# Closing the hole a stopped feeder leaves.
#
#   bash test/testbackfill.sh
#
# The feeder exits when the fast tier's oplog has rolled past its checkpoint,
# and the operator is told to copy the affected namespaces into the archive and
# clear the checkpoint. Until `cline backfill` there was no command for the
# copying, so it was a script written during an incident, and a script written
# during an incident closes two of the three things that can have happened.
#
# Measured by hand first, which is why this suite exists: one update, one insert
# and one delete against a fast tier with the feeder down, then the remedy
# followed exactly. The update came back right, the insert arrived, and the
# deleted document stayed live in the archive and was served to a client. The
# router counted eleven where the application had left ten.
#
# It runs on the HOST, on two mongods of its own, because it stops the feeder
# and rewrites a completeness claim.
HOTPORT=${HOTPORT:-28100}
COLDPORT=${COLDPORT:-28101}
PORT=${PORT:-28105}
DIR=$(mktemp -d /tmp/tcbackfill.XXXXXX)
BIN=${BIN:-$(pwd)/bin/cline}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%.80s want=%s\n' "$1" "$3" "$2"; fi
}
# stopCline stops the daemons the way an operator does, so each releases its
# lease on the way out. A kill -9 leaves the lease held for lease.Stale, 30s,
# and rollback and backfill refuse beside a feeder that may still be writing.
# Only the sections that mean a crash kill with -9.
stopCline() {
  pkill -TERM -f "$1" 2>/dev/null
  for _ in $(seq 50); do pgrep -f "$1" >/dev/null || return 0; sleep 0.2; done
  pkill -9 -f "$1" 2>/dev/null
}
cleanup() {
  pkill -9 -f "cline.*$DIR" 2>/dev/null
  docker rm -f tc-bf-hot tc-bf-cold >/dev/null 2>&1
  rm -rf "$DIR"
}
trap cleanup EXIT
M() { mongosh --quiet --port "$1" --eval "$2" 2>&1 | tail -1; }
R() { timeout 30 mongosh "mongodb://127.0.0.1:$PORT/?directConnection=true" --quiet --eval "$1" 2>&1 | tail -1; }
claim() { M $COLDPORT "db.getSiblingDB('_thermocline').watermarks.replaceOne({_id:'shop.orders'},{_id:'shop.orders',completeFrom:new Date($1)},{upsert:true})" >/dev/null; }

echo "== setup =="
docker image inspect mongo:8.0 >/dev/null 2>&1 || { echo "  FATAL  docker pull mongo:8.0"; exit 2; }
[ -x "$BIN" ] || { echo "  FATAL  no binary at $BIN; run make build"; exit 2; }
docker rm -f tc-bf-hot tc-bf-cold >/dev/null 2>&1
docker run -d --rm --name tc-bf-hot  -p $HOTPORT:$HOTPORT  mongo:8.0 \
  mongod --replSet bfhot  --port $HOTPORT  --bind_ip_all --wiredTigerCacheSizeGB 0.25 --oplogSize 64 >/dev/null 2>&1
docker run -d --rm --name tc-bf-cold -p $COLDPORT:$COLDPORT mongo:8.0 \
  mongod --replSet bfcold --port $COLDPORT --bind_ip_all --wiredTigerCacheSizeGB 0.25 --oplogSize 64 >/dev/null 2>&1
for p in $HOTPORT $COLDPORT; do
  for _ in $(seq 1 40); do mongosh --quiet --port $p --eval 'db.hello()' >/dev/null 2>&1 && break; sleep 1; done
done
M $HOTPORT  "rs.initiate({_id:'bfhot',members:[{_id:0,host:'127.0.0.1:$HOTPORT'}]})"  >/dev/null
M $COLDPORT "rs.initiate({_id:'bfcold',members:[{_id:0,host:'127.0.0.1:$COLDPORT'}]})" >/dev/null
sleep 6
cat > "$DIR/bf.yaml" <<YAML
hot:
  uri: "mongodb://127.0.0.1:$HOTPORT/?replicaSet=bfhot"
cold:
  uri: "mongodb://127.0.0.1:$COLDPORT/?replicaSet=bfcold"
log:
  file: ""
feeder:
  id: "bf"
  namespaces:
    - "shop.orders"
    # The section on a document the archive will not take, and its control.
    # Tiered because backfill only copies what this deployment tiers.
    - "shop.bfrefused"
    - "shop.bfclean"
  checkpoint_every: 1s
evictor:
  feeder_id: "bf"
  rules:
    - ns: "shop.orders"
      older_than: 30m
      date_field: "at"
router:
  listen: "127.0.0.1:$PORT"
  dashboard: ""
  completeness: archive
YAML

"$BIN" feeder --config "$DIR/bf.yaml" --log-file "$DIR/f1.log" >/dev/null 2>&1 & disown
sleep 3
M $HOTPORT 'const c=db.getSiblingDB("shop").orders;
  for (let i=1;i<=10;i++) c.insertOne({_id:i,v:"first",at:new Date()});' >/dev/null
for _ in $(seq 1 30); do
  [ "$(M $COLDPORT 'print(db.getSiblingDB("shop").orders.countDocuments({}))')" = "10" ] && break
  sleep 1
done
chk "ten documents are mirrored"                      "10" \
    "$(M $COLDPORT 'print(db.getSiblingDB("shop").orders.countDocuments({}))')"
stopCline "cline.*$DIR"

echo "== the window where nothing was mirroring =="
M $HOTPORT 'const c=db.getSiblingDB("shop").orders;
  c.updateOne({_id:1},{$set:{v:"changed in the gap"}});
  c.insertOne({_id:11,v:"inserted in the gap",at:new Date()});
  c.deleteOne({_id:2});' >/dev/null
chk "the fast tier holds ten of its own"              "10" \
    "$(M $HOTPORT 'print(db.getSiblingDB("shop").orders.countDocuments({}))')"
# The feeder's published position has to age out, which in a real incident it
# has long since done: the oplog had to roll past the checkpoint to get here.
sleep 12
claim "Date.now()-1000*60*60*24"

echo "== the guard, before anything is written =="
# A claim the fast tier cannot support is a claim that is wrong, and marking on
# the strength of it would tombstone the archive rather than recover it.
M $HOTPORT 'db.getSiblingDB("shop").orders.renameCollection("orders_aside")' >/dev/null
WRONG=$("$BIN" backfill --config "$DIR/bf.yaml" 2>&1 | grep 'shop.orders')
chk "it refuses a claim the fast tier holds none of"  "true" \
    "$(grep -q 'the claim is wrong rather than the archive' <<<"$WRONG" && echo true || echo false)"
M $HOTPORT 'db.getSiblingDB("shop").orders_aside.renameCollection("orders")' >/dev/null

echo "== reporting, which is the default =="
DRY=$("$BIN" backfill --config "$DIR/bf.yaml" 2>&1)
chk "it says it would copy ten"                       "true" \
    "$(grep -q 'Would copy 10' <<<"$DRY" && echo true || echo false)"
chk "and mark the one that was deleted"               "true" \
    "$(grep -q 'mark 1' <<<"$DRY" && echo true || echo false)"
chk "and wrote nothing while saying so"               "first" \
    "$(M $COLDPORT 'const d=db.getSiblingDB("shop").orders.findOne({_id:1}); print(d?d.v:"absent")')"

echo "== a document that already wears one of this system's own marks =="
# The archive's copy of it is a tombstone: invisible to every read that falls
# through, and counted on the "copied" line as though it had been restored. The
# router refuses writes that would leave the mark and the feeder warns when it
# mirrors one, so a document in this state predates the guard or came from a
# writer that bypassed the router, which is exactly the deployment somebody runs
# a recovery tool on without running preflight first.
#
# Written straight at the fast tier, because through the router it is refused,
# which is the point.
M $HOTPORT 'db.getSiblingDB("shop").orders.insertOne({_id:911,v:"wears the mark",_tcDeletedAt:new Date()})' >/dev/null
MARKED=$("$BIN" backfill --config "$DIR/bf.yaml" 2>&1)
chk "backfill says the document carries the mark"     "true" \
    "$(grep -q 'already carry _tcDeletedAt' <<<"$MARKED" && echo true || echo false)"
chk "and names how many"                              "true" \
    "$(grep -qE '1 document\(s\) already carry' <<<"$MARKED" && echo true || echo false)"
chk "and points at the command that lists them"       "true" \
    "$(grep -q 'cline preflight lists them' <<<"$MARKED" && echo true || echo false)"
# And the control: with it gone the line does not appear, or the check above
# would pass on a message that is always printed.
M $HOTPORT 'db.getSiblingDB("shop").orders.deleteOne({_id:911})' >/dev/null
CLEAN=$("$BIN" backfill --config "$DIR/bf.yaml" 2>&1)
chk "and says nothing once it is gone"                "false" \
    "$(grep -q 'already carry _tcDeletedAt' <<<"$CLEAN" && echo true || echo false)"

echo "== the step after the copy, which had no command behind it =="
# Refused after --ns, because the checkpoint is not per namespace: clearing it
# then would restart mirroring from now for every other namespace too, sealing
# in whatever hole they have.
ONE=$("$BIN" backfill --config "$DIR/bf.yaml" --ns shop.orders --apply --clear-checkpoint 2>&1)
chk "clearing the checkpoint is refused after --ns"   "true" \
    "$(grep -q 'not per namespace' <<<"$ONE" && echo true || echo false)"
chk "and the checkpoint is still there"               "1" \
    "$(M $COLDPORT 'print(db.getSiblingDB("_thermocline").checkpoints.countDocuments({_id:"bf"}))')"

# A namespace this could not do is a failure, not a pass that copied nothing:
# the closing step moves the checkpoint past the gap, and it did so whatever
# had failed, with status 0.
BAD=$("$BIN" backfill --config "$DIR/bf.yaml" --ns orders --apply 2>&1); BADRC=$?
chk "a namespace it could not do fails the command"   "1 true" \
    "$BADRC $(grep -q 'were not backfilled' <<<"$BAD" && echo true || echo false)"
# And a claim the guard refuses is one too: no deletes were marked, and moving
# the checkpoint past the gap with status 0 sealed it in.
M $HOTPORT 'db.getSiblingDB("shop").orders.renameCollection("orders_aside")' >/dev/null
CP0=$(M $COLDPORT 'print(JSON.stringify(db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"bf"})))')
REF=$("$BIN" backfill --config "$DIR/bf.yaml" --ns shop.orders --apply 2>&1); REFRC=$?
CP1=$(M $COLDPORT 'print(JSON.stringify(db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"bf"})))')
M $HOTPORT 'db.getSiblingDB("shop").orders_aside.renameCollection("orders")' >/dev/null
chk "a refused claim fails the command too"           "1 true" \
    "$REFRC $(grep -q 'the claim is wrong rather than the archive' <<<"$REF" && echo true || echo false)"
chk "  and leaves the checkpoint where it was"        "true" \
    "$([ "$CP0" = "$CP1" ] && echo true || echo "false ($CP0 -> $CP1)")"

echo "== and then applied =="
APPLIED=$("$BIN" backfill --config "$DIR/bf.yaml" --apply --clear-checkpoint 2>&1)
chk "it replaced the checkpoint itself"               "true" \
    "$(grep -q 'now starts at' <<<"$APPLIED" && echo true || echo false)"
chk "with a time to replay from and no token"         "true" \
    "$(M $COLDPORT 'const c=db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"bf"}); print(!!(c && c.startAt && !c.token))')"
# The window this used to leave open. Clearing the checkpoint made the feeder
# start from whenever it was next started, so writes made between the copy and
# that moment reached neither: ten seconds of a running shop on the Hetzner
# staging pair on 2026-10-12. Written now, after the copy and before the feeder.
M $HOTPORT 'const c=db.getSiblingDB("shop").orders;
  c.updateOne({_id:3},{$set:{v:"changed after the backfill"}});
  c.insertOne({_id:12,v:"inserted after the backfill",at:new Date()});' >/dev/null
"$BIN" feeder --config "$DIR/bf.yaml" --log-file "$DIR/f3.log" >/dev/null 2>&1 & disown
"$BIN" router --config "$DIR/bf.yaml" --log-file "$DIR/r.log" >/dev/null 2>&1 & disown
for _ in $(seq 1 40); do grep -q 'router listening' "$DIR/r.log" 2>/dev/null && break; sleep 1; done
sleep 2
chk "the update made in the gap is in the archive"    "changed in the gap" \
    "$(M $COLDPORT 'const d=db.getSiblingDB("shop").orders.findOne({_id:1}); print(d?d.v:"absent")')"
chk "the insert made in the gap is too"               "inserted in the gap" \
    "$(M $COLDPORT 'const d=db.getSiblingDB("shop").orders.findOne({_id:11}); print(d?d.v:"absent")')"
# The half copying cannot do: a delete leaves nothing on the fast tier to copy,
# so only the completeness claim can say it happened.
chk "the update made after the backfill is too"      "changed after the backfill" \
    "$(M $COLDPORT 'const d=db.getSiblingDB("shop").orders.findOne({_id:3}); print(d?d.v:"absent")')"
chk "and the insert made after the backfill"          "inserted after the backfill" \
    "$(M $COLDPORT 'const d=db.getSiblingDB("shop").orders.findOne({_id:12}); print(d?d.v:"absent")')"
chk "and the feeder says it replayed"                 "true" \
    "$(grep -q 'replaying from where cline backfill started copying' "$DIR/f3.log" && echo true || echo false)"
chk "and the delete is marked rather than live"       "deleted" \
    "$(M $COLDPORT 'const d=db.getSiblingDB("shop").orders.findOne({_id:2}); print(d ? (d._tcDeletedAt ? "deleted" : d.v) : "absent")')"
chk "so a client does not read the deleted document"  "absent" \
    "$(R 'const d=db.getSiblingDB("shop").orders.findOne({_id:2}); print(d?d.v:"absent")')"
# The number an operator would check first, and the one that was wrong before.
# Eleven: the ten the gap left, and the insert made after the backfill.
chk "and the count is what the application left"      "11" \
    "$(R 'print(db.getSiblingDB("shop").orders.countDocuments({}))')"

echo "== an archive restored from a backup, which is the documented exposure =="
# docs/operations.md calls this the real exposure of the system: documents
# written after the backup are still on the fast tier if they are recent enough
# to be there, and the feeder cannot fetch them, because a change stream reports
# changes from now and mirroring only moves forward.
#
# That was written before this command existed. What the fast tier still holds
# is exactly the set the copy brings back, and with the feeder left running
# there is no window: it carries what happens next.
#
# Measured: ten documents mirrored, a backup, five more written, the archive
# dropped and restored. A client read ten where the application had written
# fifteen. After the copy it read fifteen.
RNS=trestore
# The section above leaves its daemons running on purpose, so they have to go
# first: a second router on the same port never binds, and every read below then
# goes to the old one, which does not tier this namespace. That is what the first
# run of this section measured, and it reported the archive holding nought and
# the client reading all fifteen as though nothing were wrong.
stopCline "cline.*$DIR"
M $HOTPORT "db.getSiblingDB('shop').$RNS.drop()" >/dev/null
M $COLDPORT "db.getSiblingDB('shop').$RNS.drop()" >/dev/null
sed 's|    - "shop.orders"|    - "shop.orders"\n    - "shop.'"$RNS"'"|' "$DIR/bf.yaml" > "$DIR/bf3.yaml"
"$BIN" feeder --config "$DIR/bf3.yaml" --log-file "$DIR/f5.log" >/dev/null 2>&1 & disown
"$BIN" router --config "$DIR/bf3.yaml" --log-file "$DIR/r3.log" >/dev/null 2>&1 & disown
for _ in $(seq 1 40); do grep -q 'router listening' "$DIR/r3.log" 2>/dev/null && break; sleep 1; done
R "for (let i=1;i<=10;i++) db.getSiblingDB('shop').$RNS.insertOne({_id:i,v:'before the backup'})" >/dev/null
for _ in $(seq 1 30); do
  [ "$(M $COLDPORT "print(db.getSiblingDB('shop').$RNS.countDocuments({}))")" = "10" ] && break
  sleep 1
done
docker exec tc-bf-cold mongodump --port $COLDPORT --db shop --collection $RNS -o /tmp/bk$RNS --quiet >/dev/null 2>&1
R "for (let i=11;i<=15;i++) db.getSiblingDB('shop').$RNS.insertOne({_id:i,v:'written after the backup'})" >/dev/null
sleep 3
chk "fifteen documents, mirrored"                      "15" \
    "$(M $COLDPORT "print(db.getSiblingDB('shop').$RNS.countDocuments({}))")"
M $COLDPORT "db.getSiblingDB('shop').$RNS.drop()" >/dev/null
docker exec tc-bf-cold mongorestore --port $COLDPORT --quiet /tmp/bk$RNS >/dev/null 2>&1
chk "the restored archive holds the backup's ten"      "10" \
    "$(M $COLDPORT "print(db.getSiblingDB('shop').$RNS.countDocuments({}))")"
# The exposure, stated as a number before it is closed.
chk "and a client reads ten of the fifteen written"    "10" \
    "$(R "print(db.getSiblingDB('shop').$RNS.countDocuments({}))")"
"$BIN" backfill --config "$DIR/bf3.yaml" --ns "shop.$RNS" --apply >/dev/null 2>&1
sleep 2
chk "after the copy the archive holds fifteen"         "15" \
    "$(M $COLDPORT "print(db.getSiblingDB('shop').$RNS.countDocuments({}))")"
chk "and a client reads all fifteen"                   "15" \
    "$(R "print(db.getSiblingDB('shop').$RNS.countDocuments({}))")"
chk "including one written after the backup"           "written after the backup" \
    "$(R "const d=db.getSiblingDB('shop').$RNS.findOne({_id:13}); print(d?d.v:'MISSING')")"

echo "== a feeder restarted without the namespace its checkpoint names =="
# mongod resumes from an event's token only while that event passes the
# stream's filter. The section above left a checkpoint on shop.trestore, and a
# config without it made the feeder report a rolled oplog and stop, with the
# oplog minutes old: measured on the staging pair on 2026-10-14, with a write
# made while it was down never mirrored. It now resumes from that event's
# cluster time.
DNS=tresdel
stopCline "cline.*$DIR"
M $HOTPORT "db.getSiblingDB('shop').$DNS.drop()" >/dev/null
M $COLDPORT "db.getSiblingDB('shop').$DNS.drop()" >/dev/null
M $HOTPORT "db.getSiblingDB('shop').$DNS.insertOne({_id:1,v:'written while no feeder ran'})" >/dev/null
sed 's|    - "shop.orders"|    - "shop.orders"\n    - "shop.'"$DNS"'"|' "$DIR/bf.yaml" > "$DIR/bf4.yaml"
"$BIN" feeder --config "$DIR/bf4.yaml" --log-file "$DIR/f6.log" >/dev/null 2>&1 & disown
"$BIN" router --config "$DIR/bf4.yaml" --log-file "$DIR/r4.log" >/dev/null 2>&1 & disown
for _ in $(seq 1 40); do grep -q 'router listening' "$DIR/r4.log" 2>/dev/null && break; sleep 1; done
for _ in $(seq 1 30); do
  [ "$(M $COLDPORT "print(db.getSiblingDB('shop').$DNS.countDocuments({}))")" = "1" ] && break
  sleep 1
done
chk "it says why it is not resuming from the token"   "true" \
    "$(grep -q 'no longer mirrors, so mirroring resumes' "$DIR/f6.log" && echo true || echo false)"
chk "and does not call it a rolled oplog"              "false" \
    "$(grep -q 'oplog no longer reaches' "$DIR/f6.log" && echo true || echo false)"
chk "the write made while it was down is mirrored"     "1" \
    "$(M $COLDPORT "print(db.getSiblingDB('shop').$DNS.countDocuments({}))")"

echo "== a delete and an eviction made after an archive backup =="
# The restore section above wrote only inserts after its backup. A delete
# leaves nothing on the fast tier to copy, and with a feeder publishing backfill
# marks nothing, so restoring under a running feeder brings the deleted document
# back. Measured first on the staging pair on 2026-10-14.
#
# The fast tier's oplog still holds the delete, so the procedure is now to
# restore the feeder's checkpoint from the same backup and let it replay. That
# replay also meets every eviction since the backup, whose announcements are
# spent, and read them all as deletes: ten of ten tombstoned on the same pair.
# The fast tier's record of evictions is what tells them apart. See meta.Evicted.
python3 - "$DIR/bf4.yaml" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
s = s.replace("  rules:\n", "  rules:\n    - ns: \"shop.tresdel\"\n      older_than: 30m\n      date_field: \"at\"\n", 1)
s = s.replace("evictor:\n", "evictor:\n  backoff:\n    max_replication_lag: 1h\n    max_cache_dirty: 0.9\n    min_write_tickets_free: 0.01\n    min_oplog_window: 1m\n", 1)
open(p, "w").write(s)
PY
R "const c=db.getSiblingDB('shop').$DNS;
   for (let i=2;i<=10;i++) c.insertOne({_id:i,v:'before the backup'});
   for (let i=21;i<=25;i++) c.insertOne({_id:i,v:'old enough to evict',at:new Date(Date.now()-2*3600e3)})" >/dev/null
for _ in $(seq 1 30); do
  [ "$(M $COLDPORT "print(db.getSiblingDB('shop').$DNS.countDocuments({}))")" = "15" ] && break
  sleep 1
done
sleep 2   # one more checkpoint_every, so the backup's checkpoint is past them
# The checkpoint first: one older than the data only replays more, and one
# newer than the data would skip what the data is missing.
docker exec tc-bf-cold rm -rf /tmp/bk$DNS
docker exec tc-bf-cold mongodump --port $COLDPORT --db _thermocline --collection checkpoints -o /tmp/bk$DNS --quiet >/dev/null 2>&1
docker exec tc-bf-cold mongodump --port $COLDPORT --db shop --collection $DNS -o /tmp/bk$DNS --quiet >/dev/null 2>&1
R "db.getSiblingDB('shop').$DNS.deleteOne({_id:3})" >/dev/null
# And one changed after the backup and evicted after that. Its newest version
# was only ever in the archive being restored, so no replay can bring it back;
# what the feeder can do is say so rather than serve the older one in silence.
M $HOTPORT "db.getSiblingDB('shop').$DNS.updateOne({_id:21},{\$set:{v:'changed after the backup'}})" >/dev/null
for _ in $(seq 1 30); do
  [ "$(M $COLDPORT "print(db.getSiblingDB('shop').$DNS.findOne({_id:21}).v)")" = "changed after the backup" ] && break
  sleep 1
done
"$BIN" evictor --config "$DIR/bf4.yaml" --once --log-file "$DIR/e4.log" >/dev/null 2>&1
sleep 3   # the checkpoint that spends the announcements and records the evictions
chk "the five old ones are evicted from the fast tier" "0" \
    "$(M $HOTPORT "print(db.getSiblingDB('shop').$DNS.countDocuments({_id:{\$gte:21}}))")"
chk "and the fast tier records all five"               "5" \
    "$(M $HOTPORT "print(db.getSiblingDB('_thermocline').evicted.aggregate([{\$unwind:'\$e'},{\$match:{'e.d':/$DNS/}},{\$count:'n'}]).toArray().concat([{n:0}])[0].n)")"
chk "a client reads fourteen before the restore"       "14" \
    "$(R "print(db.getSiblingDB('shop').$DNS.countDocuments({}))")"

# The old order, as the control: restore under a running feeder, then copy.
"$BIN" pause --config "$DIR/bf4.yaml" --reason "restoring the archive" >/dev/null 2>&1
docker exec tc-bf-cold mongorestore --port $COLDPORT --quiet --drop --nsInclude "shop.$DNS" /tmp/bk$DNS >/dev/null 2>&1
"$BIN" backfill --config "$DIR/bf4.yaml" --ns "shop.$DNS" --apply >/dev/null 2>&1
"$BIN" resume --config "$DIR/bf4.yaml" >/dev/null 2>&1
sleep 2
chk "under a running feeder the deleted one is back"   "15" \
    "$(R "print(db.getSiblingDB('shop').$DNS.countDocuments({}))")"

# The procedure: feeder stopped, data and checkpoint restored from the same
# backup, feeder started, and it replays from the backup's instant.
"$BIN" pause --config "$DIR/bf4.yaml" --reason "restoring the archive" >/dev/null 2>&1
pkill -9 -f "feeder --config $DIR/bf4.yaml" 2>/dev/null; sleep 1
docker exec tc-bf-cold mongorestore --port $COLDPORT --quiet --drop --nsInclude "shop.$DNS" /tmp/bk$DNS >/dev/null 2>&1
docker exec tc-bf-cold mongorestore --port $COLDPORT --quiet --drop --nsInclude "_thermocline.checkpoints" /tmp/bk$DNS >/dev/null 2>&1
"$BIN" feeder --config "$DIR/bf4.yaml" --log-file "$DIR/f7.log" >/dev/null 2>&1 & disown
for _ in $(seq 1 30); do
  [ "$(M $COLDPORT "print(db.getSiblingDB('shop').$DNS.countDocuments({_tcDeletedAt:{\$exists:true}}))")" = "1" ] && break
  sleep 1
done
sleep 2
"$BIN" resume --config "$DIR/bf4.yaml" >/dev/null 2>&1
chk "the feeder replays from the backup's checkpoint"  "true" \
    "$(grep -q 'resuming from checkpoint' "$DIR/f7.log" && echo true || echo false)"
chk "a client reads the fourteen the application left" "14" \
    "$(R "print(db.getSiblingDB('shop').$DNS.countDocuments({}))")"
chk "the deleted one stays deleted"                    "MISSING" \
    "$(R "const d=db.getSiblingDB('shop').$DNS.findOne({_id:3}); print(d?d.v:'MISSING')")"
chk "and every evicted one is still read"              "5" \
    "$(R "print(db.getSiblingDB('shop').$DNS.countDocuments({_id:{\$gte:21}}))")"
chk "the archive marked one delete, not six"           "1" \
    "$(M $COLDPORT "print(db.getSiblingDB('shop').$DNS.countDocuments({_tcDeletedAt:{\$exists:true}}))")"
chk "and it names the one that comes back older"      "1" \
    "$(grep -c 'comes back older than the application left it.*'"$DNS"'' "$DIR/f7.log")"

# A backup older than the record would replay evictions nothing recognises any
# more. The feeder refuses rather than tombstone them.
pkill -9 -f "feeder --config $DIR/bf4.yaml" 2>/dev/null; sleep 1
M $COLDPORT "db.getSiblingDB('_thermocline').checkpoints.updateOne({_id:'bf'},{\$set:{eventAt:new Date(Date.now()-9*86400e3)}})" >/dev/null
sleep 31   # the killed feeder's lease
timeout 20 "$BIN" feeder --config "$DIR/bf4.yaml" --log-file "$DIR/f8.log" >/dev/null 2>&1
chk "a checkpoint older than the record is refused"    "true" \
    "$(grep -q 'older than the eviction record reaches' "$DIR/f8.log" && echo true || echo false)"
M $COLDPORT "db.getSiblingDB('_thermocline').checkpoints.updateOne({_id:'bf'},{\$set:{eventAt:new Date()}})" >/dev/null
"$BIN" feeder --config "$DIR/bf4.yaml" --log-file "$DIR/f9.log" >/dev/null 2>&1 & disown
sleep 3
# Left running, because the section below needs a feeder publishing to refuse.

echo "== and it refuses to run underneath a feeder =="
# Tested last, and for a reason worth writing down: the first version of this
# suite started a feeder in the middle of the gap to check the refusal, and that
# feeder mirrored the gap. The scenario the checks after it depended on was gone,
# and three of them failed against a command that was working.
#
# A feeder is running now, from the section above, so the position is fresh.
ALIVE=$("$BIN" backfill --config "$DIR/bf.yaml" --apply 2>&1)
chk "it refuses while a feeder is still publishing"   "true" \
    "$(grep -q 'still publishing' <<<"$ALIVE" && echo true || echo false)"
chk "and says how to get the marking done"             "true" \
    "$(grep -q 'Stop it, or --force, to mark them' <<<"$ALIVE" && echo true || echo false)"
stopCline "cline.*$DIR"

echo "== the other thing this is for: tiering a collection that already has data =="
# Adding a collection to feeder.namespaces is an ordinary operation and it hides
# the collection until the archive has it. Measured on twenty documents living
# on the fast tier untiered, then tiered and one more written: the fast tier held
# twenty one, the archive held the one, and a client read the one. The other
# twenty were invisible the moment somebody edited the config.
#
# The feeder stays running for this, which is what makes it the order with no
# window: it carries what happens next while the copy brings what was already
# there. Marking is held while it runs, and nothing needs marking here anyway.
# The twenty have to be older than the feeder's checkpoint, which is what makes
# them invisible: a collection tiered today has been on the fast tier for months
# and its inserts are long past the oplog. Writing them and then letting a feeder
# that does not tier them checkpoint past is the same state in three seconds.
# Without that the feeder resumes from before the inserts and replays them, which
# is what the first version of this section measured: it reported the client
# seeing all twenty one and called the scenario reproduced.
M $HOTPORT 'const c=db.getSiblingDB("shop").invoices;
  for (let i=1;i<=20;i++) c.insertOne({_id:i,v:"existing",at:new Date()});' >/dev/null
"$BIN" feeder --config "$DIR/bf.yaml" --log-file "$DIR/f3b.log" >/dev/null 2>&1 & disown
sleep 6
stopCline "cline.*$DIR/bf.yaml"
sed 's|    - "shop.orders"|    - "shop.orders"\n    - "shop.invoices"|' "$DIR/bf.yaml" > "$DIR/bf2.yaml"
"$BIN" feeder --config "$DIR/bf2.yaml" --log-file "$DIR/f4.log" >/dev/null 2>&1 & disown
"$BIN" router --config "$DIR/bf2.yaml" --log-file "$DIR/r2.log" >/dev/null 2>&1 & disown
for _ in $(seq 1 40); do grep -q 'router listening' "$DIR/r2.log" 2>/dev/null && break; sleep 1; done
R 'db.getSiblingDB("shop").invoices.insertOne({_id:21,v:"after tiering",at:new Date()})' >/dev/null
sleep 4
# How many are hidden depends on where the previous feeder's checkpoint landed,
# so the count is printed and the claim is the one that does not move: the fast
# tier holds all of them and the client cannot see them.
HID=$(R 'print(db.getSiblingDB("shop").invoices.countDocuments({}))')
echo "      the fast tier holds 21 and a client sees $HID"
chk "tiering it hides what was already there"         "true" \
    "$(awk -v n="$HID" 'BEGIN{print (n+0 < 21) ? "true" : "false (" n ")"}')"
chk "and the fast tier still has all of them"         "21" \
    "$(M $HOTPORT 'print(db.getSiblingDB("shop").invoices.countDocuments({}))')"
chk "and the router said which collection"            "true" \
    "$(grep 'the fast tier holds tiered collections the archive does not' "$DIR/r2.log" | grep -q 'shop.invoices' && echo true || echo false)"
OUT=$("$BIN" backfill --config "$DIR/bf2.yaml" --ns shop.invoices --apply 2>&1)
chk "it copies them with the feeder still running"    "true" \
    "$(grep -q 'copied 21' <<<"$OUT" && echo true || echo false)"
chk "and holds the marking while it is"               "true" \
    "$(grep -q 'no deletes looked for: a feeder is still publishing' <<<"$OUT" && echo true || echo false)"
# The advice has to fit the job. Clearing the checkpoint here would restart
# mirroring from now for every namespace, for no reason at all.
chk "and does not tell you to clear the checkpoint"   "true" \
    "$(grep -q 'no checkpoint to clear' <<<"$OUT" && echo true || echo false)"
sleep 2
chk "the client can see all twenty one now"           "21" \
    "$(R 'print(db.getSiblingDB("shop").invoices.countDocuments({}))')"
echo "== a document the archive will not take, met by the copy =="
# An operator runs this because something is already wrong, so what it says
# about what it did has to be true. Measured before this section existed, on
# three documents with one the archive's unique index refuses: the command said
# "could not be copied", "Copied 0 document(s)", and then its closing line, that
# what was on the fast tier is in the archive now. The copy is unordered, so the
# archive had taken the other two; the driver reports the batch as an error and
# the command threw away both the successes and the detail.
# No feeder for this one, so the copy is the only writer and the counts below
# mean what they say. The section before this leaves one running, and with it up
# the archive filled itself: the check on what the archive holds passed while
# backfill was failing to run at all, which is a check passing for a reason that
# has nothing to do with it.
stopCline "cline.*$DIR"
M $COLDPORT "db.getSiblingDB('shop').bfrefused.drop();
             db.getSiblingDB('shop').bfrefused.createIndex({sku:1},{unique:true});
             db.getSiblingDB('shop').bfrefused.insertOne({_id:9,sku:'DUP',v:'already here'})" >/dev/null
M $HOTPORT "const c=db.getSiblingDB('shop').bfrefused; c.drop();
            c.insertMany([{_id:1,sku:'AAA'},{_id:2,sku:'BBB'},{_id:3,sku:'DUP'}])" >/dev/null
BFOUT=$("$BIN" backfill --config "$DIR/bf.yaml" --ns shop.bfrefused --apply 2>&1)
chk "it copies the documents the archive will take"    "2" \
    "$(echo "$BFOUT" | grep -oE 'copied [0-9]+' | head -1 | awk '{print $2}')"
chk "and names the one it will not"                    "true" \
    "$(echo "$BFOUT" | grep -q '1 document(s) the archive refused' && echo true || echo false)"
chk "with the index and the key"                       "true" \
    "$(echo "$BFOUT" | grep -q 'sku_1 dup key' && echo true || echo false)"
# The line that mattered most: an operator reading only the end of this used to
# be told the gap was closed.
chk "and does not say the gap is closed"               "false" \
    "$(echo "$BFOUT" | grep -q 'on the fast tier is in the archive now' && echo true || echo false)"
# The paragraph rather than the closing sentence, because which closing sentence
# this run gets depends on whether a feeder is publishing and this section stops
# them. The paragraph is the part that says the namespace is still short, and it
# is printed either way.
chk "and says the namespace is still short"            "true" \
    "$(echo "$BFOUT" | grep -q 'still short by them' && echo true || echo false)"
chk "and that the reads are refused"                   "true" \
    "$(echo "$BFOUT" | grep -q 'the router refuses the reads' && echo true || echo false)"
chk "and the archive holds the two it took"            "3" \
    "$(M $COLDPORT "print(db.getSiblingDB('shop').bfrefused.countDocuments({}))")"
# The control, because a report that is only right when something is wrong is
# not a report: an ordinary namespace still ends with the original sentence.
M $COLDPORT "db.getSiblingDB('shop').bfclean.drop()" >/dev/null
M $HOTPORT "const c=db.getSiblingDB('shop').bfclean; c.drop(); c.insertMany([{_id:1},{_id:2}])" >/dev/null
BFOK=$("$BIN" backfill --config "$DIR/bf.yaml" --ns shop.bfclean --apply 2>&1)
chk "an ordinary namespace copies and says so"         "2" \
    "$(echo "$BFOK" | grep -oE 'copied [0-9]+' | head -1 | awk '{print $2}')"
chk "and says nothing about documents left behind"     "false" \
    "$(echo "$BFOK" | grep -q 'still short by them' && echo true || echo false)"

stopCline "cline.*$DIR"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
