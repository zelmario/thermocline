#!/bin/bash
# cline adopt, against an archive with one collection of every kind.
#
# adopt reads the cluster being adopted and writes the configuration that
# tiers it, so what is checked here is what an operator would trust it for:
#
#   - each kind of collection is decided the way docs/adoption.md says: an
#     ObjectId collection by _id, an integer _id by its date field, a migrated
#     collection (every ObjectId minted at once) by its date field, and a
#     reference table, a capped collection, an empty one and a view left out
#     with the reason;
#   - it writes to neither tier, refuses to overwrite a file, and refuses to
#     write a file with no collections (empty would tier everything);
#   - the estimate it prints is what the warmer then does;
#   - what it prints runs: the connection string reaches the proxy, and the
#     copy command for an untiered collection puts it where the proxy reads it.
WDIR=${WDIR:-/root}
LOGDIR=${LOGDIR:-/var/log/cline}
PORT=${PORT:-27100}
DB=${DB:-shop}
META=${META:-_thermocline}
ADB=${DB}_adopt
NDB=${DB}_adoptnone
AMETA=${META}_adopt
PA=$((PORT+6))
export ADB NDB
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'}
OUTF=$WDIR/adopt-$PORT.yaml
RUNF=$WDIR/adoptrun-$PORT.yaml
LOG=$LOGDIR/adopt-$PORT.log
TXT=$WDIR/adopt-$PORT.out
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-56s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-56s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
yes_no() { "$@" && echo true || echo false; }
# Anchored at the start of the command line, so neither the bash -c around it
# nor another worker's run is matched.
cleanup() { pkill -9 -f "^cline run --config $RUNF" 2>/dev/null; true; }
dropall() {
  for t in H C; do $t "for (const n of [process.env.ADB, process.env.NDB, '$AMETA']) db.getSiblingDB(n).dropDatabase()" >/dev/null; done
}
trap 'cleanup; dropall' EXIT
mkdir -p "$LOGDIR"
adopt() { cline adopt --hot "$HOT" --cold "$COLD" --listen "127.0.0.1:$PA" --proxy-host 127.0.0.1 "$@"; }

echo "== an archive with one collection of every kind =="
cleanup; dropall; rm -f "$OUTF" "$RUNF"
# Deterministic, so the counts a window keeps are exact: orders one per 0.1
# day over 730 days, events one per 0.1 day over 365, legacy re-inserted now
# with its own dates one per 0.365 day over 1095. At 180 days that is 1800,
# 1800 and 494.
C "const d=db.getSiblingDB(process.env.ADB), day=86400, now=Math.floor(Date.now()/1000);
   let b=[]; for (let i=0;i<7300;i++) b.push({_id: ObjectId.createFromTime(now-i*8640), createdAt: new Date((now-i*8640+5)*1000), n:i});
   d.orders.insertMany(b); b=[];
   for (let i=0;i<3650;i++) b.push({_id: NumberLong(i), ts: new Date((now-i*8640)*1000), updatedAt: new Date()});
   d.events.insertMany(b); b=[];
   for (let i=0;i<3000;i++) b.push({at: new Date((now-Math.floor(i*0.365*day))*1000), note:'migrated'});
   d.legacy.insertMany(b); b=[];
   for (let i=0;i<300;i++) b.push({_id: 'sku-'+i, price: i});
   d.products.insertMany(b);
   d.createCollection('audit', {capped: true, size: 1<<20}); d.audit.insertOne({m:1});
   d.createCollection('empty');
   d.createView('cheap', 'products', [{\$match: {price: {\$lt: 10}}}]);
   db.getSiblingDB(process.env.NDB).products.insertMany([{_id:'a'},{_id:'b'}]);" >/dev/null
COLLS_BEFORE=$(C "db.getSiblingDB(process.env.ADB).getCollectionInfos().filter(c=>!c.name.startsWith('system.')).length")
chk "the archive holds seven collections and views"           "7" "$COLLS_BEFORE"

echo "== cline adopt --keep 180d =="
adopt --db "$ADB" --keep 180d --out "$OUTF" > "$TXT" 2>&1
chk "it exits 0"                                              "0" "$?"
chk "the file is there, readable by its owner only"           "600" "$(stat -c %a "$OUTF" 2>/dev/null)"
chk "preflight ran on it and refused nothing"                 "true" "$(yes_no grep -q 'Nothing above refuses a start' "$TXT")"
chk "orders: tiered by _id, which agrees with createdAt"      "true" "$(yes_no grep -qx "  \"$ADB.orders\": 180d" "$OUTF")"
chk "events: integer _id, tiered by ts"                       "true" \
    "$(grep -A2 -x "  \"$ADB.events\":" "$OUTF" | grep -q 'date_field: "ts"' && echo true || echo false)"
chk "legacy: ObjectIds minted at once, tiered by at"          "true" \
    "$(grep -A2 -x "  \"$ADB.legacy\":" "$OUTF" | grep -q 'date_field: "at"' && echo true || echo false)"
chk "and says why: the documents look re-inserted"            "true" "$(yes_no grep -q 'look re-inserted' "$OUTF")"
chk "three collections tiered, nothing else"                  "3" "$(grep -c '^  "' "$OUTF")"
for c in products audit empty cheap; do
  chk "$c: listed as not tiered"                              "true" "$(yes_no grep -qx "#   $ADB.$c" "$OUTF")"
done
chk "the capped one says why"                                 "true" "$(yes_no grep -q 'capped collection' "$OUTF")"
chk "the index a date_field rule needs is named for ts"       "true" "$(yes_no grep -q "getCollection(\"events\").createIndex({\"ts\": -1, _id: -1})" "$TXT")"
chk "and for at"                                              "true" "$(yes_no grep -q "getCollection(\"legacy\").createIndex({\"at\": -1, _id: -1})" "$TXT")"
EST=$(grep "^  $ADB.orders " "$TXT" | grep -o '[0-9]*% within' | grep -o '^[0-9]*')
chk "orders estimate is 1800/7300 = 25%, within 5 points"     "true" "$([ -n "$EST" ] && [ "$EST" -ge 20 ] && [ "$EST" -le 30 ] && echo true || echo "false($EST)")"

echo "== it wrote to neither tier =="
chk "the fast tier has no trace of the database"              "false" "$(H "db.getMongo().getDBNames().includes(process.env.ADB)")"
chk "the archive holds the same seven"                        "$COLLS_BEFORE" \
    "$(C "db.getSiblingDB(process.env.ADB).getCollectionInfos().filter(c=>!c.name.startsWith('system.')).length")"

echo "== what it refuses =="
adopt --db "$ADB" --keep 180d --out "$OUTF" --no-preflight > /dev/null 2>&1
chk "an existing file without --force"                        "1" "$?"
rm -f "$WDIR/none-$PORT.yaml"
adopt --db "$NDB" --keep 180d --out "$WDIR/none-$PORT.yaml" --no-preflight > "$WDIR/none-$PORT.out" 2>&1
chk "nothing tierable: it exits non-zero"                     "1" "$?"
chk "and writes no file, since empty would tier everything"   "false" "$([ -e "$WDIR/none-$PORT.yaml" ] && echo true || echo false)"
chk "saying so"                                               "true" "$(yes_no grep -q 'would tier everything' "$WDIR/none-$PORT.out")"
adopt --db "$ADB" --keep 90d --fast-size 1GB --out "$WDIR/both-$PORT.yaml" --no-preflight > /dev/null 2>&1
chk "--keep and --fast-size together"                         "1" "$?"
rm -f "$WDIR/small-$PORT.yaml"
adopt --db "$ADB" --fast-size 4KB --out "$WDIR/small-$PORT.yaml" --no-preflight > "$WDIR/small-$PORT.out" 2>&1
chk "a fast tier too small for any window exits non-zero"     "1" "$?"
chk "and says not even the shortest fits"                     "true" "$(yes_no grep -q 'not even 7d' "$WDIR/small-$PORT.out")"
rm -f "$WDIR/small-$PORT.yaml" "$WDIR/both-$PORT.yaml" "$WDIR/none-$PORT."*

echo "== run what it wrote, through what it printed =="
URI=$(grep -A1 'Point the application at the proxy' "$TXT" | tail -1 | sed 's/^ *//; s/<password>/secret/')
chk "the printed connection string has no replicaSet"         "mongodb://dba:secret@127.0.0.1:$PA/?authSource=admin" "$URI"
cp "$OUTF" "$RUNF"
cat >> "$RUNF" <<YAML
meta:
  db: "$AMETA"
feeder:
  id: "adopt$PORT"
evictor:
  backoff: {max_replication_lag: 1h, max_cache_dirty: 0.9, min_write_tickets_free: 0.01, min_oplog_window: 1m}
warmer:
  backoff: {max_replication_lag: 1h, max_cache_dirty: 0.9, min_write_tickets_free: 0.01, min_oplog_window: 1m}
  min_free_disk: 0.01
YAML
rm -f "$LOG"
nohup bash -c "cline run --config $RUNF --evict-every 1h --warm-every 0 >>$LOG 2>&1" >/dev/null 2>&1 &
for _ in $(seq 1 120); do grep -q 'warm complete' "$LOG" 2>/dev/null && break; sleep 1; done
chk "cline run warmed with the file"                          "true" "$(yes_no grep -q 'warm complete' "$LOG")"
P() { mongosh "$URI" --quiet --eval "$1" 2>&1 | tail -1; }
chk "through the printed string, every order"                 "7300" "$(P "db.getSiblingDB(process.env.ADB).orders.countDocuments({})")"
chk "every event"                                             "3650" "$(P "db.getSiblingDB(process.env.ADB).events.countDocuments({})")"
chk "every legacy document"                                   "3000" "$(P "db.getSiblingDB(process.env.ADB).legacy.countDocuments({})")"
chk "the fast tier holds 180 days of orders"                  "1800" "$(H "db.getSiblingDB(process.env.ADB).orders.countDocuments({})")"
chk "of events, by ts"                                        "1800" "$(H "db.getSiblingDB(process.env.ADB).events.countDocuments({})")"
chk "of legacy, by at and not by its _id"                     "494"  "$(H "db.getSiblingDB(process.env.ADB).legacy.countDocuments({})")"

# The copy command, run as printed, with the two URIs it asks for.
CP=$(grep "mongodump .*--collection='products'" "$TXT" | grep -- "--db='$ADB'" | sed 's/^ *//')
chk "it printed a copy command for products"                  "true" "$([ -n "$CP" ] && echo true || echo false)"
chk "before the copy the proxy reads products empty"          "0" "$(P "db.getSiblingDB(process.env.ADB).products.countDocuments({})")"
( export COLD HOT; eval "$CP" ) > /dev/null 2>&1
chk "after it, all 300"                                       "300" "$(P "db.getSiblingDB(process.env.ADB).products.countDocuments({})")"
chk "no daemon logged an error"                               "0" "$(grep -c 'level=ERROR' "$LOG")"
pkill -TERM -f "^cline run --config $RUNF"
rm -f "$OUTF" "$RUNF" "$TXT"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
