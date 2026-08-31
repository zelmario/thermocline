#!/bin/bash
# The pilot, for hours rather than for bursts.
#
# Everything else here runs in minutes, and a burst cannot show what a long run
# shows: cursors that are opened and never closed, memory that only goes up, a
# completeness claim drifting away from what the tiers hold, tombstones
# accumulating. So this asks the same questions the comparison asks, over and
# over, while a warm and an evictor move documents underneath them, and records
# what the router says about itself between rounds.
#
#   HOURS=3 bash test/pilot/soak.sh
#
# What it is looking for is in the last three columns: stale_reads, refused and
# panics are meant to stay at zero, and every other number is meant to be
# explainable. A disagreement in any round is the run failing.
#
# It leaves the pilot running. Stop it early with: touch /tmp/soak.stop
NODE=${NODE:-thermocline-zelmar-node0}
HOURS=${HOURS:-3}
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'}
OUT=${OUT:-/tmp/pilot_soak.tsv}
END=$(( $(date +%s) + HOURS * 3600 ))

N() { docker exec "$NODE" bash -c "$1"; }

# metric NAME: one counter out of the router's own /metrics.
metric() { N "curl -s http://127.0.0.1:27510/metrics" 2>/dev/null | awk -v k="thermocline_$1" '$1==k{print $2}' | tail -1; }

echo "== the pilot, for $HOURS hours =="
bash test/pilot/setup.sh start >/dev/null 2>&1 || { echo "the pilot would not start"; exit 2; }

# The churn loops are short-lived on purpose, so this restarts them each round
# rather than running one long warm that finishes in the first ten minutes.
docker cp test/pilot/churn.sh "$NODE":/root/churn.sh >/dev/null
N "sed -e 's|  residency: 30m|  residency: 5s|' /root/pilot.yaml > /root/pilot_churn.yaml"

printf 'round\telapsed\tdisagree\tstale\trefused\tpanics\tcursors\tcopied\tfrom_archive\ttombstones\trss_mb\tswap_mb\n' | tee "$OUT"

round=0
worst=0
while [ "$(date +%s)" -lt "$END" ]; do
  [ -f /tmp/soak.stop ] && { echo "stopped by /tmp/soak.stop"; break; }
  round=$((round + 1))
  start=$(date +%s)

  # A fresh warm and evictor for this round, so documents keep moving.
  N "pgrep -f 'pilot_churn' >/dev/null || (setsid bash /root/churn.sh >/dev/null 2>&1 &)" >/dev/null 2>&1

  out=$(bash test/pilot/setup.sh compare 2>&1)
  dis=$(grep -oE '[0-9]+ disagreements' <<<"$out" | awk '{s+=$1} END{print s+0}')
  [ "$dis" -gt "$worst" ] && worst=$dis

  cursors=$(docker exec "$NODE" mongosh "$HOT" --quiet --eval \
    'print(db.getSiblingDB("admin").serverStatus().metrics.cursor.open.total)' 2>/dev/null | tail -1)
  tombs=$(docker exec "$NODE" mongosh "$COLD" --quiet --eval \
    'let n=0; ["sample_mflix.comments","sample_supplies.sales","sample_training.trips"].forEach(function(ns){
       const p=ns.split("."); n += db.getSiblingDB(p[0])[p[1]].countDocuments({_tcDeletedAt:{$exists:true}}); });
     print(n)' 2>/dev/null | tail -1)
  rss=$(N "ps -o rss= -C cline --sort=-rss 2>/dev/null | head -1" 2>/dev/null | tr -d ' ')
  rss=$(( ${rss:-0} / 1024 ))
  swap=$(free -m | awk '/Swap:/{print $3}')

  printf '%d\t%ds\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$round" "$(( $(date +%s) - start ))" "${dis:-?}" \
    "$(metric stale_reads_total)" "$(metric refused_total)" "$(metric panics_total)" \
    "${cursors:-?}" "$(metric copied_up_total)" "$(metric from_archive_total)" \
    "${tombs:-?}" "$rss" "${swap:-?}" | tee -a "$OUT"
done

echo
echo "================================"
echo "rounds: $round   worst round: $worst disagreements"
[ "$worst" -gt 0 ] && exit 1
exit 0
