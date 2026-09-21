#!/bin/bash
# Losing the fast tier and building it back: the procedure, not the commands.
#
# This one runs on the HOST because it drives the shop binary and the daemons
# together.
#
#   bash test/testrestore.sh
#
# docs/operations.md says what to back up and what comes back without a backup:
# "Collection options and indexes come back from the archive when a router
# starts, the unique key registry from `cline keys --rebuild`, views from the
# archive if a router mirrored them there, and the documents from warming.
# Accounts are the one thing on that tier that nothing rebuilds."
#
# Every clause of that has been tested as a command. Nobody has ever lost a fast
# tier and followed the paragraph. [[rehearse-the-procedure]] is what happens
# when nobody does: `cline rollback` had eight passing checks and the procedure
# it belongs to left an application without its catalogue.
#
# So this takes the backup the document lists, loses the fast tier's data, puts
# it back the way the document says, and then asks the only question that
# matters: does the application find what it was told it had?
#
# The suite is deliberately not a test of mongodump. Each restore step is the
# one an operator would type, and a check that fails here is either a defect or
# a sentence missing from operations.md.
NODE=${NODE:-thermocline-zelmar-node0}
CFG=/root/restore.yaml
DB=${DB:-shopR}
ORDERS=${ORDERS:-2000}
ROUNDS=${ROUNDS:-40}
TICKETS=${TICKETS:-200}
HOT='mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'
COLD='mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'
ROUTER='mongodb://dba:secret@127.0.0.1:27620/?authSource=admin'
DUMP=/root/redump
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-56s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-56s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
N() { docker exec "$NODE" bash -c "$1"; }
ND() { docker exec -d "$NODE" bash -c "$1"; }
R() { docker exec "$NODE" mongosh "$ROUTER" --quiet --eval "$1" 2>&1 | tail -1; }
# The pipe in R hides mongosh's status, so a wait loop needs the command's own.
# See [[status-through-a-pipe]]: every loop that tested R waited for nothing.
routerUp() { docker exec "$NODE" mongosh "$ROUTER" --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1; }
H() { docker exec "$NODE" mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { docker exec "$NODE" mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
stopAll() { N "for p in \$(pgrep -f 'config /root/restor[e]'); do kill -9 \$p 2>/dev/null; done" >/dev/null 2>&1; }
trap stopAll EXIT

echo "== a deployment doing its job =="
docker cp test/store/restore.yaml "$NODE":$CFG >/dev/null
docker cp bin/store "$NODE":/usr/local/bin/store >/dev/null
stopAll; sleep 1
for T in H C; do
  $T "db.getSiblingDB('$DB').dropDatabase(); db.getSiblingDB('_thermocline_restore').dropDatabase()" >/dev/null
done
N "mkdir -p /root/relog && rm -rf $DUMP"
# The feeder first, always: a change stream starts at now, so one started after
# the seed never carries it and the archive is not the complete copy the whole
# procedure rests on.
ND "cline feeder --config $CFG --log-file /root/relog/feeder.log"
sleep 3
N "store -uri '$HOT' -db $DB -orders $ORDERS -days 120 -customers 200 -products 50 -seed-only" >/dev/null 2>&1

# An application account on both tiers, which is the piece with no other source.
# Created on the archive too, because that is what makes the daemons able to say
# the fast tier is missing one.
for T in H C; do
  $T "db.getSiblingDB('admin').createUser({user:'shopuser',pwd:'shoppw',roles:[{role:'readWrite',db:'$DB'}]})" >/dev/null 2>&1
done

for _ in $(seq 1 120); do
  lag=$(C 'const c=db.getSiblingDB("_thermocline_restore").checkpoints.findOne({_id:"restore"}); print(c?Math.round((Date.now()-c.eventAt.getTime())/1000):9999)')
  [ "$lag" -lt 5 ] 2>/dev/null && break
  sleep 2
done
chk "mirroring caught up with the seed"          "true" "$([ "$lag" -lt 5 ] 2>/dev/null && echo true || echo false)"

ND "cline router --config $CFG --log-file /root/relog/router.log"
for _ in $(seq 1 30); do routerUp && break; sleep 1; done
chk "the router came up"                         "true" "$(routerUp && echo true || echo false)"
# A tiered collection with a real unique index on it, so the key registry has
# something to lose, and both tiers have the constraint.
#
# Through the router on purpose. An index created straight at the fast tier's
# mongod never reaches the archive, because no change stream carries index DDL,
# and a rehearsal built that way proves nothing: the constraint is gone with the
# tier that held it and the router is right to stop enforcing what neither tier
# declares. See [[a-fixture-that-mirrors-nothing]].
#
# Ages are fabricated in the ObjectId so the evictor takes them: the id's
# timestamp is what older_than reads, and a bulk load of createFromTime()
# collides because it zeroes everything after the timestamp.
R "const t = db.getSiblingDB('$DB').tickets;
   t.createIndex({code:1},{unique:true});
   const old = Math.floor(Date.now()/1000) - 86400;
   const hex = old.toString(16).padStart(8,'0');
   const docs = [];
   for (let i = 0; i < $TICKETS; i++) {
     docs.push({_id: ObjectId(hex + i.toString(16).padStart(16,'0')), code: 'T'+i, seat: i});
   }
   t.insertMany(docs); print(t.countDocuments({}))" >/dev/null
# Where the constraint actually lives, which is the whole point of what
# follows: the fast tier declares it and the archive does not, because nothing
# copies an index to the archive on purpose. The router says so when it is
# built, and that warning is the last mention the constraint ever gets.
chk "the fast tier declares the unique index"    "true" \
    "$(H "print(db.getSiblingDB('$DB').tickets.getIndexes().some(i => i.name === 'code_1'))")"
chk "and the archive deliberately does not"      "false" \
    "$(C "print(db.getSiblingDB('$DB').tickets.getIndexes().some(i => i.name === 'code_1'))")"
chk "and the router said so when it was built"   "true" \
    "$(N "grep -c 'the archive does not have one over the same fields' /root/relog/router.log" | tr -d '\n' | awk '{print ($1+0>0)?"true":"false"}')"
N "cline keys --rebuild --config $CFG" >/dev/null 2>&1
N "cline warmer --config $CFG" >/dev/null 2>&1
N "store -uri '$ROUTER' -db $DB -rounds $ROUNDS -conc 4 -skip-seed" >/dev/null 2>&1
N "cline evictor --config $CFG --once" >/dev/null 2>&1

# Wait on the feeder's checkpoint rather than on counts. Counts move for reasons
# other than mirroring, an evictor sweep above all, so comparing the two tiers
# while one runs never agrees.
for _ in $(seq 1 60); do
  lag=$(C 'const c=db.getSiblingDB("_thermocline_restore").checkpoints.findOne({_id:"restore"}); print(c?Math.round((Date.now()-c.eventAt.getTime())/1000):9999)')
  [ "$lag" -lt 5 ] 2>/dev/null && break
  sleep 2
done

HOT_ORDERS=$(H "print(db.getSiblingDB('$DB').orders.countDocuments({}))")
COLD_ORDERS=$(C "print(db.getSiblingDB('$DB').orders.countDocuments({_tcDeletedAt:{\$exists:false}}))")
SEEN=$(R "print(db.getSiblingDB('$DB').orders.countDocuments({}))")
chk "the fast tier no longer holds it all"       "true" \
    "$([ "$HOT_ORDERS" -lt "$COLD_ORDERS" ] 2>/dev/null && echo true || echo false)"
chk "and the router answers the whole collection" "$COLD_ORDERS" "$SEEN"
# The catalogue: nothing tiers it, so it has lived on the fast tier alone since
# the cutover and the archive has never held a copy.
chk "the catalogue is on the fast tier alone"    "0" \
    "$(C "print(db.getSiblingDB('$DB').products.countDocuments({}))")"
PRODUCTS=$(H "print(db.getSiblingDB('$DB').products.countDocuments({}))")
chk "and the fast tier does hold it"             "true" \
    "$([ "$PRODUCTS" -gt 0 ] 2>/dev/null && echo true || echo false)"
KEYS=$(C "print(db.getSiblingDB('_thermocline_restore').keys.countDocuments({}))")
KEYSHOT=$(H "print(db.getSiblingDB('_thermocline_restore').keys.countDocuments({}))")
chk "the key registry is on the fast tier"       "true" \
    "$([ "$KEYSHOT" -gt 0 ] 2>/dev/null && echo true || echo false)"
# And nowhere else, which is what makes it the one piece of bookkeeping a
# backup of the archive does not carry.
chk "and not on the archive"                     "0" "$KEYS"
CLAIMS=$(C "print(db.getSiblingDB('_thermocline_restore').watermarks.countDocuments({}))")
chk "there is a completeness claim to lose"      "true" \
    "$([ "$CLAIMS" -gt 0 ] 2>/dev/null && echo true || echo false)"

echo "== the backup, which is the list operations.md gives =="
# Exactly what the document says to take, in the order it says it, and nothing
# else. If the list is short this is where the shortfall is created, silently,
# and it will not show until the restore.
stopAll; sleep 1
N "mongodump --uri='$COLD' --db $DB                     -o $DUMP/archive   --quiet" >/dev/null 2>&1
N "mongodump --uri='$COLD' --db _thermocline_restore    -o $DUMP/archive   --quiet" >/dev/null 2>&1
N "mongodump --uri='$HOT'  --db _thermocline_restore --collection keys -o $DUMP/keys --quiet" >/dev/null 2>&1
chk "the archive's application data is in it"    "true" \
    "$(N "[ -f $DUMP/archive/$DB/orders.bson ] && echo true || echo false")"
chk "the archive's bookkeeping is in it"         "true" \
    "$(N "[ -d $DUMP/archive/_thermocline_restore ] && echo true || echo false")"
chk "the fast tier's key registry is in it"      "true" \
    "$(N "[ -f $DUMP/keys/_thermocline_restore/keys.bson ] && echo true || echo false")"
# And one thing the list does not name, taken separately so the rehearsal can
# show what it buys. The list calls the fast tier's documents optional, "a
# subset of the archive", which is true of everything the feeder mirrors and
# false of everything it does not: a collection nothing tiers has lived on the
# fast tier alone since the cutover and the archive has never held a copy.
for c in customers products store_marker; do
  N "mongodump --uri='$HOT' --db $DB --collection $c -o $DUMP/untiered --quiet" >/dev/null 2>&1
done
chk "the untiered collections are not in that list" "true" \
    "$(N "[ -f $DUMP/untiered/$DB/products.bson ] && [ ! -f $DUMP/archive/$DB/products.bson ] && echo true || echo false")"

echo "== the fast tier is lost =="
# Its data, not its mongods: an anydbver replica set cannot be rebuilt from
# inside a suite. Everything the fast tier held is gone, which is the part the
# paragraph in operations.md is about.
H "db.getSiblingDB('$DB').dropDatabase(); db.getSiblingDB('_thermocline_restore').dropDatabase()" >/dev/null
H "db.getSiblingDB('admin').dropUser('shopuser')" >/dev/null 2>&1
chk "nothing of the shop is left on it"          "0" \
    "$(H "print(db.getSiblingDB('$DB').getCollectionNames().length)")"
chk "the archive still holds everything tiered"  "$COLD_ORDERS" \
    "$(C "print(db.getSiblingDB('$DB').orders.countDocuments({_tcDeletedAt:{\$exists:false}}))")"

echo "== what preflight says before anything is started =="
# The document's own advice: "cline preflight reports it without starting
# anything, which is the check to run after a restore".
PRE=$(N "cline preflight --config $CFG" 2>&1)
chk "preflight names the missing account"        "true" \
    "$(grep -qi 'accounts the fast tier does not\|shopuser' <<<"$PRE" && echo true || echo false)"
chk "preflight says the claim is short"          "true" \
    "$(grep -q 'short of the claim' <<<"$PRE" && echo true || echo false)"

echo "== put it back the way the document says =="
# The registry from the dump, the account by hand, and everything else from a
# router start and a warm.
N "mongorestore --uri='$HOT' $DUMP/keys --quiet" >/dev/null 2>&1
chk "the key registry is back"                   "$KEYSHOT" \
    "$(H "print(db.getSiblingDB('_thermocline_restore').keys.countDocuments({}))")"
H "db.getSiblingDB('admin').createUser({user:'shopuser',pwd:'shoppw',roles:[{role:'readWrite',db:'$DB'}]})" >/dev/null 2>&1
ND "cline feeder --config $CFG --log-file /root/relog/feeder2.log"
sleep 2
ND "cline router --config $CFG --log-file /root/relog/router2.log"
for _ in $(seq 1 30); do routerUp && break; sleep 1; done
chk "the router starts against an empty fast tier" "true" "$(routerUp && echo true || echo false)"
N "cline warmer --config $CFG" >/dev/null 2>&1

echo "== and now the only question that matters =="
# Does the application find what it was told it had? Every number below is read
# through the router, which is the only address the application has.
BACK=$(R "print(db.getSiblingDB('$DB').orders.countDocuments({}))")
chk "the router answers the whole collection"    "$COLD_ORDERS" "$BACK"
# The bounded read is the one the claim decides. operations.md says a claim is
# checked when it is declared and never again, so a claim that survived the tier
# it describes answers from a fast tier that holds nothing.
OIDHEX="$(printf '%08x' $(( $(date +%s) - 600 )))0000000000000000"
BOUNDED=$(R "print(db.getSiblingDB('$DB').orders.countDocuments({_id:{\$gte:ObjectId('$OIDHEX')}}))")
DIRECT=$(C "print(db.getSiblingDB('$DB').orders.countDocuments({_id:{\$gte:ObjectId('$OIDHEX')}, _tcDeletedAt:{\$exists:false}}))")
chk "a bounded read is not short"                "$DIRECT" "$BOUNDED"
chk "the tickets came back"                      "$TICKETS" \
    "$(R "print(db.getSiblingDB('$DB').tickets.countDocuments({}))")"
# The registry constrains the fast tier, so a duplicate has to be refused after
# the restore exactly as it was before it.
# The unique index did not come back and it was never going to: nothing copies
# an index to the archive on purpose, because an index describes the whole
# collection and carrying every one of them gives back most of what tiering
# buys. A unique index is not a plain index, though, and this is what that
# costs: the constraint the application declared is now on neither tier, so a
# value an archived document already holds is accepted.
chk "the unique index did not come back either"  "false" \
    "$(H "print(db.getSiblingDB('$DB').tickets.getIndexes().some(i => i.name === 'code_1'))")"
DUP=$(R "try { db.getSiblingDB('$DB').tickets.insertOne({code:'T7', seat:-1}); print('accepted') }
  catch (e) { print('refused') }")
chk "so a duplicate key is accepted"             "accepted" "$DUP"
# And the part that was missing. The registry's rows are the only thing left
# that remembers the constraint, and until this check nothing read them for it:
# the comparison that notices an index going away is between one listing and the
# one before it, so a router that never saw the index cannot miss it, and a
# router started after a rebuild never saw it.
chk "but the router says the claims are stranded" "true" \
    "$(N "grep -c 'claims for a unique index no tier has' /root/relog/router2.log" | tr -d '\n' | awk '{print ($1+0>0)?"true":"false"}')"
chk "and names the index"                        "true" \
    "$(N "grep -q 'code_1' /root/relog/router2.log && echo true || echo false" | tr -d '\n')"
# The catalogue, and the half this rehearsal was written to find. Everything
# above came back from the archive or from a dump the document's list names.
# Nothing in that list ever held customers, products or the shop's own marker,
# because nothing tiers them, so an operator who took exactly what the document
# asked for has an application with its orders and no catalogue. The same shape
# [[rehearse-the-procedure]] found in rollback, in the other direction.
CAT=$(R "print(db.getSiblingDB('$DB').products.countDocuments({}))")
chk "the catalogue did not come back"            "0" "$CAT"

echo "== which is why the list has to name them =="
N "mongorestore --uri='$HOT' $DUMP/untiered --quiet" >/dev/null 2>&1
chk "restoring them puts the catalogue back"     "$PRODUCTS" \
    "$(R "print(db.getSiblingDB('$DB').products.countDocuments({}))")"
OUT=$(N "store -uri '$ROUTER' -db $DB -rounds 20 -conc 2 -skip-seed" 2>&1)
chk "and the shop runs against the rebuilt tier" "0" \
    "$(grep -oE 'errors: [0-9]+' <<<"$OUT" | grep -oE '[0-9]+' | tail -1)"
chk "and notices nothing wrong"                  "true" \
    "$(grep -q 'noticed nothing wrong' <<<"$OUT" && echo true || echo false)"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
