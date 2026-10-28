#!/bin/bash
# cline run and the short config, run against an adopted archive.
#
# cline run starts every daemon in one process in the order docs/adoption.md
# gives, and the short config names each tiered collection once. Both replace
# steps an operator used to do by hand, so what is checked here is that each
# step still happens, in its order, and that a step left out would show:
#
#   - the router opens only once the feeder is caught up;
#   - the unique keys the archive already holds are claimed before anything
#     is evicted, so a duplicate of an archived value is refused;
#   - the warmer fills what is newer than the collection's age, and the
#     evictor removes what is older;
#   - a second cline run waits for the first one's feeder and serves;
#   - a router that cannot listen ends the process, and SIGTERM ends it clean.
WDIR=${WDIR:-/root}
LOGDIR=${LOGDIR:-/var/log/cline}
PORT=${PORT:-27100}
DB=${DB:-shop}
META=${META:-_thermocline}
export TC_DB=$DB TC_META=$META
P2=$((PORT+4)); P3=$((PORT+5))
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'}
CFG=$WDIR/run.yaml
LOG1=$LOGDIR/run1-$PORT.log; LOG2=$LOGDIR/run2-$PORT.log; LOG3=$LOGDIR/run3-$PORT.log
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-56s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-56s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
R() { mongosh --port "${2:-$PORT}" -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
up() { mongosh --port "$1" -u dba -p secret --authenticationDatabase admin --quiet --eval 'db.adminCommand({ping:1}).ok' >/dev/null 2>&1; }
# Matched on the config path, which carries this worker's directory, so a
# sweep's other workers are never touched.
# The pattern is anchored at the start of the command line: the bash -c that
# start() wraps around each one carries the same words, and killing it loses
# the exit code it is there to write.
cleanup() { pkill -9 -f "^cline run --config $WDIR/run" 2>/dev/null; true; }
trap cleanup EXIT
mkdir -p "$LOGDIR"

writecfg() { # $1 file, $2 listen port, $3 extra
  cat > "$1" <<YAML
hot: "$HOT"
cold: "$COLD"
listen: "127.0.0.1:$2"
collections:
  $DB.trorders: 30d
  $DB.trusers: {older_than: 30d, date_field: joined}
meta:
  db: "$META"
feeder:
  id: "run$PORT"
evictor:
  backoff: {max_replication_lag: 1h, max_cache_dirty: 0.9, min_write_tickets_free: 0.01, min_oplog_window: 1m}
warmer:
  backoff: {max_replication_lag: 1h, max_cache_dirty: 0.9, min_write_tickets_free: 0.01, min_oplog_window: 1m}
  min_free_disk: 0.01
$3
YAML
}
start() { # $1 config, $2 log, $3 rc file
  rm -f "$3" "$2"
  nohup bash -c "cline run --config $1 --evict-every 3s --warm-every 0 >>$2 2>&1; echo \$? > $3" >/dev/null 2>&1 &
}

echo "== an adopted archive, an empty fast tier =="
cleanup
for t in H C; do $t "db.getSiblingDB(process.env.TC_DB).dropDatabase(); db.getSiblingDB(process.env.TC_META).dropDatabase()" >/dev/null; done
C "const d=db.getSiblingDB(process.env.TC_DB), day=86400;
   const now=Math.floor(Date.now()/1000), docs=[];
   for (let i=0;i<40;i++) docs.push({_id: ObjectId.createFromTime(now-60*day-i), n:i, old:true});
   for (let i=0;i<25;i++) docs.push({_id: ObjectId.createFromTime(now-i), n:100+i, old:false});
   d.trorders.insertMany(docs);
   d.trusers.createIndex({email:1},{unique:true});
   d.trusers.insertMany([{_id:1,email:'ana@x',joined:new Date(Date.now()-90*day*1000)},
                         {_id:2,email:'bo@x', joined:new Date()}]);" >/dev/null
chk "the archive holds 65 orders and 2 users" "65 2" \
    "$(C "const d=db.getSiblingDB(process.env.TC_DB); print(d.trorders.countDocuments()+' '+d.trusers.countDocuments())")"

echo "== one short file is refused when a collection is named twice =="
writecfg $WDIR/run-bad.yaml $P3 ""
sed -i "s|^feeder:|feeder:\n  namespaces: [\"$DB.trorders\"]|" $WDIR/run-bad.yaml
BAD=$(cline run --config $WDIR/run-bad.yaml 2>&1; echo "rc=$?")
chk "cline run refuses it before connecting"             "true" \
    "$(grep -q "$DB.trorders is in collections and in feeder.namespaces" <<<"$BAD" && grep -q 'rc=1' <<<"$BAD" && echo true || echo false)"

echo "== cline run =="
writecfg $CFG $PORT ""
start $CFG $LOG1 $WDIR/run1.rc
for _ in $(seq 1 90); do up $PORT && break; sleep 1; done
chk "the router comes up"                                  "true" "$(up $PORT && echo true || echo false)"
# Once the router is up the key walk and the evictor follow within seconds.
for _ in $(seq 1 30); do grep -q 'msg=running' $LOG1 && break; sleep 1; done
line() { grep -n "$1" $LOG1 | head -1 | cut -d: -f1; }
LPRE=$(line 'preflight passed'); LCAUGHT=$(line 'the feeder is caught up'); LLISTEN=$(line 'router listening')
LKEYS=$(line 'unique keys claimed'); LRUN=$(line 'msg=running')
chk "preflight, then the feeder, then the router"          "true" \
    "$([ -n "$LPRE" ] && [ -n "$LCAUGHT" ] && [ -n "$LLISTEN" ] && [ "$LPRE" -lt "$LCAUGHT" ] && [ "$LCAUGHT" -lt "$LLISTEN" ] && echo true || echo false)"
chk "the keys are claimed after the router, before the rest" "true" \
    "$([ -n "$LKEYS" ] && [ -n "$LRUN" ] && [ "$LLISTEN" -lt "$LKEYS" ] && [ "$LKEYS" -lt "$LRUN" ] && echo true || echo false)"
# What support asks for first, on one line, and never the password the
# connection strings carry.
chk "it starts with one line naming what it tiers"         "true" \
    "$(grep 'msg=starting' $LOG1 | grep -q "collections=\"$DB.trorders=30d by _id $DB.trusers=30d by joined\"" && echo true || echo false)"
chk "and the clusters, without their credentials"         "true" \
    "$(grep 'msg=starting' $LOG1 | grep -q 'fast_tier=rsHot/' && ! grep -q 'secret' $LOG1 && echo true || echo false)"
chk "the router answers for all of history"                 "65" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).trorders.countDocuments())")"

echo "== the unique key the archive holds is enforced =="
# Without the claim this insert is accepted: the fast tier's index cannot see
# ana@x, which only the archive holds. See warnAboutUnclaimedUniqueKeys.
chk "a duplicate of an archived email is refused"           "11000" \
    "$(R "try { db.getSiblingDB(process.env.TC_DB).trusers.insertOne({_id:3,email:'ana@x',joined:new Date()}); print('accepted') } catch(e) { print(e.code) }")"

echo "== the warmer and the evictor =="
for _ in $(seq 1 60); do
  [ "$(H "print(db.getSiblingDB(process.env.TC_DB).trorders.countDocuments({old:false}))")" = 25 ] && break; sleep 1
done
chk "the warmer filled the 25 recent orders"                "25" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).trorders.countDocuments({old:false}))")"
chk "and none of the 40 old ones"                           "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).trorders.countDocuments({old:true}))")"
R "db.getSiblingDB(process.env.TC_DB).trorders.insertOne({_id: ObjectId.createFromTime(Math.floor(Date.now()/1000)-90*86400), n:999, old:true})" >/dev/null
R "db.getSiblingDB(process.env.TC_DB).trorders.insertOne({_id: ObjectId(), n:1000, old:false, fresh:true})" >/dev/null
for _ in $(seq 1 30); do
  [ "$(C "print(db.getSiblingDB(process.env.TC_DB).trorders.countDocuments({n:{\$in:[999,1000]}}))")" = 2 ] && break; sleep 1
done
chk "the feeder mirrors writes into the archive"            "2" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).trorders.countDocuments({n:{\$in:[999,1000]}}))")"
for _ in $(seq 1 90); do
  [ "$(H "print(db.getSiblingDB(process.env.TC_DB).trorders.countDocuments({n:999}))")" = 0 ] && break; sleep 1
done
chk "the evictor removes a 90 day old order from the fast tier" "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).trorders.countDocuments({n:999}))")"
chk "and keeps the new one"                                 "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).trorders.countDocuments({n:1000}))")"
chk "the router still finds the evicted order"              "999" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).trorders.findOne({n:999}).n)")"

echo "== a second cline run =="
writecfg $WDIR/run2.yaml $P2 "log: {format: json}"
start $WDIR/run2.yaml $LOG2 $WDIR/run2.rc
for _ in $(seq 1 90); do up $P2 && break; sleep 1; done
chk "its router serves"                                     "67" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).trorders.countDocuments())" $P2)"
chk "its feeder waits for the first one's lease"            "true" \
    "$(grep -q 'feeder stopped' $LOG2 && grep -qi 'lease\|held' $LOG2 && echo true || echo false)"
# log.format json: every line an object, the same fields as text.
chk "log.format json writes nothing but JSON lines"         "0" "$(grep -vc '^{' $LOG2)"
chk "with the same startup line"                            "true" \
    "$(grep '"msg":"starting"' $LOG2 | grep -q '"component":"run"' && echo true || echo false)"
chk "and the first one's feeder is still the one mirroring" "true" \
    "$(grep -c 'feeder stopped' $LOG1 | grep -qx 0 && echo true || echo false)"
pkill -TERM -f "^cline run --config $WDIR/run2.yaml"

echo "== a router that cannot listen ends the process =="
writecfg $WDIR/run3.yaml $PORT ""
start $WDIR/run3.yaml $LOG3 $WDIR/run3.rc
for _ in $(seq 1 90); do [ -f $WDIR/run3.rc ] && break; sleep 1; done
chk "cline run exits with an error"                         "1" "$(cat $WDIR/run3.rc 2>/dev/null)"
chk "and says the router stopped"                           "true" \
    "$(grep -q 'router stopped' $LOG3 && grep -q 'address already in use' $LOG3 && echo true || echo false)"
chk "the first one is still serving"                        "true" "$(up $PORT && echo true || echo false)"

echo "== SIGTERM =="
pkill -TERM -f "^cline run --config $CFG "
for _ in $(seq 1 30); do [ -f $WDIR/run1.rc ] && break; sleep 1; done
chk "it stops, and exits 0"                                 "0" "$(cat $WDIR/run1.rc 2>/dev/null)"
chk "every daemon was asked to stop, none panicked"         "0" "$(grep -c 'panic' $LOG1)"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
