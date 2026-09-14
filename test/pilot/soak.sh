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
# Nothing else may drive the cluster while this runs, and the way that goes
# wrong is not obvious: every suite kills daemons by config *prefix*, `pkill -f
# "cline feeder --config $WDIR/"`, and WDIR is /root when a suite is run by hand
# inside the node. That matches this soak's own feeder, which is
# /root/pilot.yaml. Measured: a testall.sh run in another shell took the feeder
# out at round 26 and the stale column went from 0 to 45 a round for the rest of
# the hour, which is the system behaving exactly as designed with nothing
# mirroring, reported as though the soak had found something.
#
# Three things fail the run, and they are asserted rather than printed for
# somebody to notice:
#
#   a disagreement in any round;
#   a panic, ever, which is a defect with a stack in the log beside it;
#   a refusal the comparison did not itself report.
#
# That last one used to be "refused is meant to stay at zero", which stopped
# being true when a pipeline reading an untiered collection became a refusal:
# two of the nineteen aggregations here are that shape, so the counter climbs by
# two every round by design. A column that grows for a good reason teaches a
# reader to ignore the column, so the round asserts the router's refusals equal
# the ones the comparison printed, and an unexplained one fails.
#
# stale_reads is printed and not asserted, on purpose. A fall-through read that
# waited the full archive_wait during an eviction burst and answered anyway is
# the documented trade rather than a wrong answer; a previous run reached 13 of
# them. The comparison cannot see that kind of staleness either, because it asks
# the archive the same question.
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
[ -n "${TERMINATE_AUTH:-}" ] && echo "  terminate_auth on"

# The churn loops are short-lived on purpose, so this restarts them each round
# rather than running one long warm that finishes in the first ten minutes.
docker cp test/pilot/churn.sh "$NODE":/root/churn.sh >/dev/null
# Derived from whichever config setup.sh drove, so TERMINATE_AUTH=1 reaches the
# warmer and evictor that run underneath every round rather than the router
# alone.
PCFG=${PCFG:-/root/pilot.yaml}
if [ -n "${TERMINATE_AUTH:-}" ]; then PCFG=/root/pilot_ta.yaml; fi
N "sed -e 's|  residency: 30m|  residency: 5s|' $PCFG > /root/pilot_churn.yaml"

printf 'round\telapsed\tdisagree\tstale\trefused\tpanics\tcursors\tcopied\tfrom_archive\ttombstones\trss_mb\tswap_mb\n' | tee "$OUT"

round=0
worst=0
panics=0
unexplained=0
# The counters are cumulative and the pilot may have been used before this run,
# so the first round is measured against wherever they already stood.
refprev=$(metric refused_total); refprev=${refprev:-0}; refprev=${refprev%%.*}
while [ "$(date +%s)" -lt "$END" ]; do
  [ -f /tmp/soak.stop ] && { echo "stopped by /tmp/soak.stop"; break; }
  round=$((round + 1))
  start=$(date +%s)

  # A fresh warm and evictor for this round, so documents keep moving.
  N "pgrep -f 'pilot_churn' >/dev/null || (setsid bash /root/churn.sh >/dev/null 2>&1 &)" >/dev/null 2>&1

  out=$(bash test/pilot/setup.sh compare 2>&1)
  dis=$(grep -oE '[0-9]+ disagreements' <<<"$out" | awk '{s+=$1} END{print s+0}')
  [ "$dis" -gt "$worst" ] && worst=$dis
  # What the comparison says it was refused, so the counter can be held to it.
  saidref=$(grep -oE '[0-9]+ refused' <<<"$out" | awk '{s+=$1} END{print s+0}')

  # Number(), because serverStatus returns a 64 bit integer and mongosh prints
  # one as Long('7'), which is not a number to anything reading this column.
  cursors=$(docker exec "$NODE" mongosh "$HOT" --quiet --eval \
    'print(Number(db.getSiblingDB("admin").serverStatus().metrics.cursor.open.total))' 2>/dev/null | tail -1)
  tombs=$(docker exec "$NODE" mongosh "$COLD" --quiet --eval \
    'let n=0; ["sample_mflix.comments","sample_supplies.sales","sample_training.trips"].forEach(function(ns){
       const p=ns.split("."); n += db.getSiblingDB(p[0])[p[1]].countDocuments({_tcDeletedAt:{$exists:true}}); });
     print(n)' 2>/dev/null | tail -1)
  rss=$(N "ps -o rss= -C cline --sort=-rss 2>/dev/null | head -1" 2>/dev/null | tr -d ' ')
  rss=$(( ${rss:-0} / 1024 ))
  swap=$(free -m | awk '/Swap:/{print $3}')

  refnow=$(metric refused_total); refnow=${refnow:-0}
  pan=$(metric panics_total); pan=${pan:-0}
  # Integers: the exporter prints these as floats, and 2 != 2.0 in a shell test.
  refnow=${refnow%%.*}; pan=${pan%%.*}
  grew=$(( refnow - refprev )); refprev=$refnow

  printf '%d\t%ds\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$round" "$(( $(date +%s) - start ))" "${dis:-?}" \
    "$(metric stale_reads_total)" "$refnow" "$pan" \
    "${cursors:-?}" "$(metric copied_up_total)" "$(metric from_archive_total)" \
    "${tombs:-?}" "$rss" "${swap:-?}" | tee -a "$OUT"

  if [ "$pan" -gt 0 ] 2>/dev/null; then
    echo "  PANIC: the router recovered $pan panic(s); the stack is in /root/pilotlog/router.log"
    panics=$pan
  fi
  if [ "$grew" -ne "$saidref" ] 2>/dev/null; then
    echo "  UNEXPLAINED REFUSAL: the router refused $grew this round and the comparison reported $saidref"
    unexplained=$((unexplained + 1))
  fi
done

echo
echo "================================"
echo "rounds: $round   worst round: $worst disagreements"
echo "panics: $panics   rounds with an unexplained refusal: $unexplained"
[ "$worst" -gt 0 ] && exit 1
[ "$panics" -gt 0 ] && exit 1
[ "$unexplained" -gt 0 ] && exit 1
exit 0
