#!/bin/bash
# Going back: an application repointed at the archive, for real.
#
# This one runs on the HOST because it drives the shop binary and the daemons
# together.
#
#   bash test/testrollback.sh
#
# testall already checks the `cline rollback` command: what it counts, what it
# warns about, that it refuses while a feeder is publishing. What nobody had
# ever done is the procedure the README sells, which is the whole promise of the
# archive being the complete copy: stop everything, clear the delete marks,
# point the application at the archive and carry on.
#
# The gap matters because the command working and the procedure working are
# different claims. The command counts marks. The procedure has to leave an
# application that finds every order it was told it placed, does not find the
# ones it was told it cancelled, and keeps working afterwards against a cluster
# that has never served it before.
NODE=${NODE:-thermocline-zelmar-node0}
CFG=/root/two.yaml
SHOP=${SHOP:-shopA}
ORDERS=${ORDERS:-3000}
ROUNDS=${ROUNDS:-150}
CANCEL=${CANCEL:-40}
HOT='mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'
COLD='mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'
ROUTER='mongodb://dba:secret@127.0.0.1:27600/?authSource=admin'
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
N() { docker exec "$NODE" bash -c "$1"; }
ND() { docker exec -d "$NODE" bash -c "$1"; }
R() { docker exec "$NODE" mongosh "$ROUTER" --quiet --eval "$1" 2>&1 | tail -1; }
H() { docker exec "$NODE" mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { docker exec "$NODE" mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
stopAll() { N "for p in \$(pgrep -f 'config /root/tw[o]'); do kill -9 \$p 2>/dev/null; done" >/dev/null 2>&1; }
trap stopAll EXIT

echo "== a shop running on the tiers =="
docker cp test/store/two.yaml "$NODE":/root/two.yaml >/dev/null
docker cp bin/store "$NODE":/usr/local/bin/store >/dev/null
stopAll; sleep 1
H "db.getSiblingDB('$SHOP').dropDatabase(); db.getSiblingDB('_thermocline_two').dropDatabase()" >/dev/null
C "db.getSiblingDB('$SHOP').dropDatabase(); db.getSiblingDB('_thermocline_two').dropDatabase()" >/dev/null
N "mkdir -p /root/rblog"
# The feeder first, always: a change stream starts at now, so one started after
# the seed never carries it and the archive is not the complete copy this whole
# procedure rests on.
ND "cline feeder --config $CFG --log-file /root/rblog/feeder.log"
sleep 3
N "store -uri '$HOT' -db $SHOP -orders $ORDERS -days 120 -customers 300 -products 100 -seed-only" >/dev/null 2>&1
for _ in $(seq 1 120); do
  lag=$(C 'const c=db.getSiblingDB("_thermocline_two").checkpoints.findOne({_id:"two"}); print(c?Math.round((Date.now()-c.eventAt.getTime())/1000):9999)')
  [ "$lag" -lt 5 ] 2>/dev/null && break
  sleep 2
done
chk "mirroring caught up with the seed"        "true" "$([ "$lag" -lt 5 ] 2>/dev/null && echo true || echo false)"
ND "cline router --config $CFG --log-file /root/rblog/router.log"
for _ in $(seq 1 30); do R 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break; sleep 1; done
N "cline warmer --config $CFG" >/dev/null 2>&1
N "store -uri '$ROUTER' -db $SHOP -rounds $ROUNDS -conc 4 -skip-seed" >/dev/null 2>&1
# An evictor pass, so some of what the application placed lives in the archive
# alone. Rolling back with everything still on the fast tier proves nothing.
N "cline evictor --config $CFG --once" >/dev/null 2>&1
chk "the fast tier no longer holds it all"     "true" \
    "$(python3 -c "print('true' if $(H "print(db.getSiblingDB('$SHOP').orders.countDocuments({}))") < $(C "print(db.getSiblingDB('$SHOP').orders.countDocuments({_tcDeletedAt:{\$exists:false}}))") else 'false')" 2>/dev/null)"

echo "== the application cancels some orders =="
# The store never deletes, and a delete is the one thing that makes the archive
# differ from what the application was told: it is mirrored as a mark, not a
# removal. Without one this rehearsal would not touch the reason rollback needs
# a step at all.
CANCELLED=$(R "const c=db.getSiblingDB('$SHOP').orders;
  const ids = c.find({}, {_id:1}).limit($CANCEL).toArray().map(d=>d._id);
  print(c.deleteMany({_id:{\$in:ids}}).deletedCount)")
chk "the application deleted some"             "$CANCEL" "$CANCELLED"
sleep 4
SAW=$(R "print(db.getSiblingDB('$SHOP').orders.countDocuments({}))")
chk "and the router answers without them"      "true" "$([ "$SAW" -gt 0 ] 2>/dev/null && echo true || echo false)"
# The state that makes rollback necessary: the archive physically holds more
# than the application was ever shown.
RAW=$(C "print(db.getSiblingDB('$SHOP').orders.countDocuments({}))")
chk "while the archive still holds the marks"  "$((SAW + CANCEL))" "$RAW"

echo "== stop everything, then clear the marks =="
# The documented order. A delete that reaches the archive after the clear writes
# another mark, which is why the command refuses while a feeder publishes.
stopAll; sleep 2
COUNTED=$(N "cline rollback --config $CFG --ns $SHOP.orders" 2>&1 | grep -oE '[0-9]+ marked deleted' | grep -oE '^[0-9]+')
chk "rollback counts exactly what was deleted" "$CANCEL" "$COUNTED"
CLEARED=$(N "cline rollback --config $CFG --ns $SHOP.orders --clear" 2>&1 | grep -oE 'cleared [0-9]+' | grep -oE '[0-9]+')
chk "and clears them"                          "$CANCEL" "$CLEARED"

echo "== the archive is now what the application was told =="
# The claim the whole procedure rests on, and the only one that matters to
# whoever is repointing an application at three in the morning.
chk "reading the archive directly agrees"      "$SAW" \
    "$(C "print(db.getSiblingDB('$SHOP').orders.countDocuments({}))")"
chk "and no mark is left anywhere"             "0" \
    "$(C "print(db.getSiblingDB('$SHOP').orders.countDocuments({_tcDeletedAt:{\$exists:true}}))")"
chk "the order events came across too"         "true" \
    "$([ "$(C "print(db.getSiblingDB('$SHOP').order_events.countDocuments({}))")" -gt 0 ] 2>/dev/null && echo true || echo false)"

echo "== but the archive has never held what nothing tiers =="
# The half this rehearsal was written to find, and the reason a rehearsal is
# worth more than a test of the command. feeder.namespaces tiers orders and
# order_events; the shop also has customers, products and its own marker, and
# those have lived on the fast tier alone since the cutover. Repointing without
# them gives an application its orders and not its catalogue.
#
# Measured before rollback said so: the fast tier held five collections, the
# archive two, the shop would not start, and the command reported the accounts
# and finished with "the archive already answers the way the router does".
GAPS=$(N "cline rollback --config $CFG" 2>&1)
for want in customers products store_marker; do
  chk "rollback names $SHOP.$want"             "true" \
      "$(grep -q "$want" <<<"$GAPS" && echo true || echo false)"
done
chk "and does not call the archive ready"      "false" \
    "$(grep -q 'already answers the way the router does' <<<"$GAPS" && echo true || echo false)"

echo "== copy them across, which is what it tells you to do =="
N "rm -rf /root/rbdump && mongodump --uri='$HOT' --db $SHOP --collection customers -o /root/rbdump --quiet &&
   mongodump --uri='$HOT' --db $SHOP --collection products -o /root/rbdump --quiet &&
   mongodump --uri='$HOT' --db $SHOP --collection store_marker -o /root/rbdump --quiet &&
   mongorestore --uri='$COLD' /root/rbdump --quiet" >/dev/null 2>&1
chk "the archive has the catalogue now"        "true" \
    "$([ "$(C "print(db.getSiblingDB('$SHOP').products.countDocuments({}))")" -gt 0 ] 2>/dev/null && echo true || echo false)"

echo "== and the application keeps working on it =="
# Repointed: the same shop, the same invariants, against a cluster that has
# never served it before and with nothing of this system in the way.
OUT=$(N "store -uri '$COLD' -db $SHOP -rounds 60 -conc 4 -skip-seed" 2>&1)
chk "the shop runs against the archive"        "0" "$(grep -oE 'errors: [0-9]+' <<<"$OUT" | grep -oE '[0-9]+' | tail -1)"
chk "and notices nothing wrong"                "true" \
    "$(grep -q 'noticed nothing wrong' <<<"$OUT" && echo true || echo false)"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
