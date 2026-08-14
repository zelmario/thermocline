#!/bin/bash
# The evictor's documented precondition is a running feeder. Test it both ways.
CFG=/root/cline.yaml
PASS=0; FAIL=0; declare -a FAILED
ok() {
  local name="$1" want="$2" pat="$3"; shift 3; [ "$1" = "--" ] && shift
  local out rc; out=$("$@" 2>&1); rc=$?
  local good=1
  [ -n "$want" ] && [ "$rc" != "$want" ] && good=0
  [ -n "$pat" ] && ! grep -qE "$pat" <<<"$out" && good=0
  if [ $good = 1 ]; then PASS=$((PASS+1)); printf '  PASS  %s\n' "$name"
  else FAIL=$((FAIL+1)); FAILED+=("$name"); printf '  FAIL  %s (exit=%s want=%s)\n    %s\n' "$name" "$rc" "$want" "$(tail -2 <<<"$out"|tr '\n' ' ')"; fi
}

echo "== fail-closed once mirroring goes stale (the safety guard) =="
# Killing the feeder does not make lag stale instantly: the archive is current
# until enough time passes. Wait past max_feeder_lag so the guard has something
# to trip on, which is the condition it exists for.
pkill -f 'cline feeder'; sleep 35
ok "refuses when mirroring lag is unknown/stale" 1 "not healthy enough to evict" \
   -- cline evictor --config $CFG --once --health-timeout 5s

echo "== with the feeder running, as documented =="
cline feeder --config $CFG --log-file /var/log/cline/t_ev_feeder.log &
FP=$!
# The evictor's precondition is mirroring lag inside max_feeder_lag. After the
# feeder has been down it has to catch up first, so wait for that rather than
# racing it.
COLD='mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'
for i in $(seq 1 60); do
  LAG=$(mongosh "$COLD" --quiet --eval 'const c=db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}); print(c?Math.round((Date.now()-c.eventAt.getTime())/1000):9999)' 2>/dev/null | tail -1)
  [ "$LAG" -lt 20 ] 2>/dev/null && { echo "  (feeder caught up, lag ${LAG}s)"; break; }
  sleep 2
done
ok "--once --dry-run"          0 "sweep complete" -- cline evictor --config $CFG --once --dry-run
ok "--once"                    0 "sweep complete" -- cline evictor --config $CFG --once
ok "--once -v"                 0 "sweep complete" -- cline evictor --config $CFG --once -v
ok "--once --health-timeout"   0 "sweep complete" -- cline evictor --config $CFG --once --health-timeout 30s

rm -f /var/log/cline/t_ev.log
cline evictor --config $CFG --every 2s --log-file /var/log/cline/t_ev.log &
EP=$!; sleep 9
N=$(grep -c "sweep complete" /var/log/cline/t_ev.log 2>/dev/null); N=${N:-0}
if [ "$N" -ge 2 ]; then PASS=$((PASS+1)); echo "  PASS  --every loops ($N sweeps)"
else FAIL=$((FAIL+1)); FAILED+=("--every"); echo "  FAIL  --every loops (saw $N)"; fi
kill $EP 2>/dev/null; wait $EP 2>/dev/null

ok "dry run never blocks on health" 0 "dry run" -- cline evictor --config $CFG --once --dry-run

kill $FP 2>/dev/null; wait $FP 2>/dev/null
echo
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
