#!/bin/bash
# A load probe, not a suite. Run it by hand against a worker layout.
#
# It reproduces a delete whose cluster time the router never noted: a deleteOne
# naming a document the fast tier does not hold matches nothing there, and that
# first reply is the one the router took the time from. The interception then
# copies the document up and runs the delete again, and that is when the
# document goes. Waiting for the first instant instead of the second let a read
# that fell through be told mirroring had caught up while the tombstone was
# still in flight.
#
# It needs the feeder running and behind, which is why there is a writer filling
# the mirror queue throughout: on a quiet machine mirroring finishes in
# milliseconds and the window never opens. Measured, before the fix, four rounds
# in ninety; after it, none in ninety. stale_reads stayed at zero either way,
# because the barrier believed it had caught up, and that is what made it
# invisible.
#
#   docker exec -e ROUNDS=90 <node> bash /root/staledelete.sh
#
# A suite cannot own this: a four percent per round failure needs hundreds of
# rounds to be reliably red, and each round is several mongosh invocations.
CFG=/root/w0/cline.yaml
PORT=27200
HOT='mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'
COLD='mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'
DB=shop_w0
NS=tprobe

# A writer filling the mirror queue on another tiered collection throughout.
mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval '
  const c = db.getSiblingDB("shop_w0").tacct;
  const pad = "x".repeat(20000);
  for (let i = 0; i < 4000; i++) { c.insertOne({_id: "load-"+i, pad: pad}); }' >/dev/null 2>&1 &
LOAD=$!
sleep 2

BAD=0
for round in $(seq 1 ${ROUNDS:-30}); do
  mongosh "$HOT"  --quiet --eval "db.getSiblingDB('$DB').$NS.drop()" >/dev/null
  mongosh "$COLD" --quiet --eval "
    const c = db.getSiblingDB('$DB').$NS; c.drop();
    c.insertMany(Array.from({length:10},(_,i)=>({_id:i, n:i})));" >/dev/null
  OUT=$(mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
    const c = db.getSiblingDB('$DB').$NS;
    const before = c.countDocuments({});
    const r = c.deleteOne({n: 5});
    const after = c.countDocuments({});
    print(before + ' ' + r.deletedCount + ' ' + after);")
  set -- $OUT
  [ "$3" != "9" ] && { BAD=$((BAD+1)); echo "  round $round: before=$1 deleted=$2 after=$3"; }
done

kill $LOAD 2>/dev/null; wait $LOAD 2>/dev/null
mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval \
  'db.getSiblingDB("shop_w0").tacct.deleteMany({_id:/^load-/})' >/dev/null 2>&1
echo "mismatches under load: $BAD of $ROUNDS"
echo "stale reads admitted:  $(curl -s http://127.0.0.1:27210/api/snapshot | grep -oE '"stale_reads": *[0-9]+' | grep -oE '[0-9]+$')"
