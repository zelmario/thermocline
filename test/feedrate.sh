#!/bin/bash
# How fast mirroring goes, which is a ceiling on how fast the fast tier can take
# writes before every fall-through read waits for the archive to catch up.
#
# Writing and mirroring at the same time cannot answer this: the two fight for
# the same machine and the number that comes out is of the pair. So this writes
# the backlog first with the feeder stopped, then starts it and times the
# catch-up on its own.
#
# Run the feeder with -v to get the split as well. Measured 2026-08-24 on six
# mongods on one WSL2 box, 50,000 documents of 200 bytes:
#
#   mirrored 50000 in 13082ms, 3822/s
#   mirroring pace  events=3000 waiting_ms=0 reading_ms=23 writing_ms=1153
#                   asking_ms=0 bulk_ms=1030 elapsed_ms=1113
#
# Reading and decoding is 1-3%, the archive is never asked anything before the
# write, and the feeder never waits for the stream. The bulk write is all of it.
# Raising batch_size from 500 to 5000 moved 2,973/s to 3,265/s, which is noise
# next to the 70% this machine varies by between runs of the same config.
#
# And the archive is not being used badly. Asked the same upserts directly it
# takes 9,940/s, and 2,713/s while the feeder is mirroring, which is what the
# feeder gets. There is no win in the feeder's code; the limit is the box.
HOT='mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'
COLD='mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'
CFG=/root/cline.yaml
DB=shop; NS=tfeed; N=${N:-50000}
pkill -f "feeder --config $CFG" 2>/dev/null; pkill -f "router --config $CFG" 2>/dev/null
sleep 2
mongosh "$HOT"  --quiet --eval "db.getSiblingDB('$DB').$NS.drop()" >/dev/null
mongosh "$COLD" --quiet --eval "db.getSiblingDB('$DB').$NS.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'})" >/dev/null

# Start it once so a checkpoint exists, then stop it: the backlog has to be
# something it resumes into, not something it never saw.
cline feeder --config $CFG --log-file /root/feedrate.log & FP=$!
for _ in $(seq 1 60); do
  [ "$(mongosh "$COLD" --quiet --eval "print(!!db.getSiblingDB('_thermocline').checkpoints.findOne({_id:'test'}))" | tail -1)" = "true" ] && break
  sleep 0.5
done
kill -9 $FP 2>/dev/null; sleep 2

mongosh "$HOT" --quiet --eval "
  const c = db.getSiblingDB('$DB').$NS;
  const t = Date.now();
  for (let b = 0; b < $N/1000; b++) {
    const d = [];
    for (let i = 0; i < 1000; i++) d.push({at: new Date(), pad: 'x'.repeat(200)});
    c.insertMany(d, {ordered: false});
  }
  print('  wrote ' + c.countDocuments({}) + ' straight to the fast tier in ' + (Date.now()-t) + 'ms');"

T0=$(date +%s%3N)
cline feeder --config $CFG --log-file /root/feedrate.log & FP=$!
for _ in $(seq 1 600); do
  COLDN=$(mongosh "$COLD" --quiet --eval "print(db.getSiblingDB('$DB').$NS.countDocuments({}))" | tail -1)
  [ "$COLDN" = "$N" ] && break
  sleep 0.5
done
T1=$(date +%s%3N)
MS=$((T1-T0))
echo "  mirrored $COLDN in ${MS}ms, $(( COLDN * 1000 / (MS>0?MS:1) ))/s"
kill $FP 2>/dev/null
