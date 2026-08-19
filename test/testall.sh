#!/bin/bash
# Exercise every cline subcommand, flag and error path.
# Parallel-safe knobs. Every default reproduces the single-worker layout, so a
# suite run by hand behaves exactly as it always has. psweep.sh sets them per
# worker: its own databases, its own bookkeeping database, its own port block,
# and its own directory for configs and logs. That directory is also what scopes
# the pkills below to this worker's daemons, because every one of them carries
# --config $WDIR/... on its command line.
WDIR=${WDIR:-/root}
LOGDIR=${LOGDIR:-/var/log/cline}
PORT=${PORT:-27100}
DB=${DB:-shop}
DB2=${DB2:-wh}
META=${META:-_thermocline}
# mongosh is node, so the suites reach these through process.env rather than
# shell expansion. Half the eval strings are single-quoted at the shell, where a
# $VAR would not expand and would silently query the wrong database.
export TC_DB=$DB TC_DB2=$DB2 TC_META=$META
P1=$((PORT+1)); P2=$((PORT+2)); P3=$((PORT+3)); PDASH=$((PORT+10)); PALT=$((PORT+17))
CFG=${CFG:-$WDIR/cline.yaml}
HOTU=${HOTU:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLDU=${COLDU:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
PASS=0; FAIL=0
declare -a FAILED

# ok NAME EXPECTED_EXIT PATTERN -- command...
ok() {
  local name="$1" want="$2" pat="$3"; shift 3; [ "$1" = "--" ] && shift
  local out rc
  out=$("$@" 2>&1); rc=$?
  local good=1
  [ -n "$want" ] && [ "$rc" != "$want" ] && good=0
  [ -n "$pat" ] && ! grep -qE "$pat" <<<"$out" && good=0
  if [ $good = 1 ]; then
    PASS=$((PASS+1)); printf '  PASS  %s\n' "$name"
  else
    FAIL=$((FAIL+1)); FAILED+=("$name"); printf '  FAIL  %s (exit=%s want=%s)\n' "$name" "$rc" "$want"
    printf '        %s\n' "$(head -c 200 <<<"$out" | tr '\n' ' ')"
  fi
}

echo "== meta =="
ok "no subcommand exits 1 with usage"   1 "Usage:"            -- cline
ok "help"                               0 "Usage:"            -- cline help
ok "-h"                                 0 "Usage:"            -- cline -h
ok "--help"                             0 "Usage:"            -- cline --help
ok "version"                            0 "cline"             -- cline version
ok "unknown subcommand exits 1"         1 "unknown subcommand" -- cline nonsense

echo "== config errors =="
ok "missing config file"                1 "read config"       -- cline warmer --config $WDIR/nope.yaml
printf 'hot:\n  uri: "["\n  bad yaml here\n' > $WDIR/bad.yaml
ok "unparseable config"                 1 "parse config"      -- cline warmer --config $WDIR/bad.yaml
printf 'cold:\n  uri: "mongodb://x"\n' > $WDIR/nohot.yaml
ok "missing hot.uri"                    1 "hot.uri is required" -- cline warmer --config $WDIR/nohot.yaml
sed 's/completeness: archive/completeness: banana/' $CFG > $WDIR/badcomp.yaml
ok "invalid completeness value"         1 "must be archive, strict or permissive" -- cline router --config $WDIR/badcomp.yaml
# Delete the whole rules block of the evictor, not just its first entry. The
# old range stopped at the first older_than, so a config with two rules left the
# second one orphaned under max_feeder_lag and this checked a parse error rather
# than an empty rules list.
sed '/^evictor:/,/^warmer:/{/^  rules:/,/^  backoff:/{/^  backoff:/!d}}' $CFG > $WDIR/norules.yaml
ok "evictor with no rules"              1 "rules is empty"    -- cline evictor --config $WDIR/norules.yaml --once

echo "== watermark =="
# On a fresh archive there is nothing to list, so declare the claim this section
# reads back. Without it "list all" fails on a clean environment and passes only
# because some earlier run happened to leave a claim behind.
cline watermark --config $CFG --ns $DB.orders --complete-all >/dev/null
ok "list all"                           0 "$DB"              -- cline watermark --config $CFG
ok "single namespace"                   0 "$DB.orders"       -- cline watermark --config $CFG --ns $DB.orders
ok "unknown namespace claims nothing"   0 "claims nothing"    -- cline watermark --config $CFG --ns no.such
ok "--clear without --ns rejected"      1 "ns is required"    -- cline watermark --config $CFG --clear
ok "two actions rejected"               1 "give one"          -- cline watermark --config $CFG --ns $DB.orders --clear --complete-all
ok "bad --complete-from rejected"       1 "RFC3339"           -- cline watermark --config $CFG --ns $DB.orders --complete-from notadate
ok "--complete-from sets"               0 "newer than"        -- cline watermark --config $CFG --ns tc.probe --complete-from 2026-01-01T00:00:00Z
ok "--complete-all sets"                0 "all of history"    -- cline watermark --config $CFG --ns tc.probe --complete-all
ok "--clear withdraws"                  0 "withdrew"          -- cline watermark --config $CFG --ns tc.probe --clear
ok "--clear again is not an error"      0 "no claim"          -- cline watermark --config $CFG --ns tc.probe --clear
ok "--log-file accepted"                0 ""                  -- cline watermark --config $CFG --log-file $LOGDIR/wm.log
test -s $LOGDIR/wm.log && { PASS=$((PASS+1)); echo "  PASS  watermark wrote its log file"; } || { FAIL=$((FAIL+1)); FAILED+=("watermark log file"); echo "  FAIL  watermark wrote its log file"; }

cline watermark --config $CFG --ns $DB.orders --clear >/dev/null

echo "== keys =="
# The key registry only has work to do when a tiered collection carries a unique
# index, so give it one and a document to claim.
mongosh "$HOTU" --quiet --eval '
  const c = db.getSiblingDB(process.env.TC_DB).tacct;
  c.createIndex({email:1},{unique:true});
  c.updateOne({_id:"keyprobe"},{$set:{email:"keyprobe@x.com"}},{upsert:true});' >/dev/null 2>&1
mongosh "$COLDU" --quiet --eval 'db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:process.env.TC_DB+".tacct"})' >/dev/null 2>&1
mongosh "$HOTU"  --quiet --eval 'db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:process.env.TC_DB+".tacct"})' >/dev/null 2>&1
ok "report"                             0 "unique indexes|nothing to enforce" -- cline keys --config $CFG
ok "--rebuild"                          0 "claimed|nothing new" -- cline keys --config $CFG --rebuild
ok "--rebuild is idempotent"            0 "claimed 0|nothing new" -- cline keys --config $CFG --rebuild

echo "== warmer =="
ok "--dry-run"                          0 "warm complete"     -- cline warmer --config $CFG --dry-run
ok "normal run"                         0 "warm complete"     -- cline warmer --config $CFG
ok "-v verbose"                         0 "warm complete"     -- cline warmer --config $CFG -v
sed '/^warmer:/,/^router:/{/  rules:/,/    back:/d}' $CFG > $WDIR/nowarm.yaml
ok "no warmer rules"                    1 "rules is empty"    -- cline warmer --config $WDIR/nowarm.yaml

echo "== evictor =="
# The evictor refuses to work while mirroring is stale, so this section needs a
# feeder that has caught up. Without it these fail for the right reason at the
# wrong time, which looks like a bug in the evictor and is not one.
pkill -f "cline feeder --config $WDIR/"; sleep 1
cline feeder --config $CFG --log-file $LOGDIR/t_pre.log & PRE=$!
for i in $(seq 1 45); do
  LAG=$(mongosh "$COLDU" --quiet --eval 'const c=db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}); print(c?Math.round((Date.now()-c.eventAt.getTime())/1000):9999)' 2>/dev/null | tail -1)
  [ "$LAG" -lt 20 ] 2>/dev/null && break
  sleep 2
done
ok "--once --dry-run"                   0 "sweep complete"    -- cline evictor --config $CFG --once --dry-run
ok "--once"                             0 "sweep complete"    -- cline evictor --config $CFG --once
ok "--once -v"                          0 "sweep complete"    -- cline evictor --config $CFG --once -v
ok "--once --health-timeout"            0 "sweep complete"    -- cline evictor --config $CFG --once --health-timeout 10s

kill $PRE 2>/dev/null; wait $PRE 2>/dev/null

echo "== daemons =="
rm -f $LOGDIR/t_[a-z]*.log
cline feeder --config $CFG --log-file $LOGDIR/t_feeder.log &
FP=$!; sleep 4
grep -q "mirroring" $LOGDIR/t_feeder.log && { PASS=$((PASS+1)); echo "  PASS  feeder starts and mirrors"; } || { FAIL=$((FAIL+1)); FAILED+=("feeder start"); echo "  FAIL  feeder starts and mirrors"; }
kill $FP 2>/dev/null; wait $FP 2>/dev/null

cline feeder --config $CFG -v --log-file $LOGDIR/t_feederv.log &
FP=$!; sleep 4
grep -q "level=DEBUG\|mirroring" $LOGDIR/t_feederv.log && { PASS=$((PASS+1)); echo "  PASS  feeder -v"; } || { FAIL=$((FAIL+1)); FAILED+=("feeder -v"); echo "  FAIL  feeder -v"; }
kill $FP 2>/dev/null; wait $FP 2>/dev/null

cline router --config $CFG --listen 0.0.0.0:$PALT --log-file $LOGDIR/t_router.log &
RP=$!; sleep 4
grep -q "$PALT" $LOGDIR/t_router.log && { PASS=$((PASS+1)); echo "  PASS  router --listen override"; } || { FAIL=$((FAIL+1)); FAILED+=("router --listen"); echo "  FAIL  router --listen override"; }
mongosh --port $PALT -u dba -p secret --authenticationDatabase admin --quiet --eval 'db.getSiblingDB(process.env.TC_DB).orders.findOne()' >/dev/null 2>&1 \
  && { PASS=$((PASS+1)); echo "  PASS  router serves on the overridden port"; } || { FAIL=$((FAIL+1)); FAILED+=("router override serves"); echo "  FAIL  router serves on the overridden port"; }
kill $RP 2>/dev/null; wait $RP 2>/dev/null

cline evictor --config $CFG --every 2s --log-file $LOGDIR/t_evictor.log &
EP=$!; sleep 7
N=$(grep -c "sweep complete\|evicting" $LOGDIR/t_evictor.log 2>/dev/null || true)
[ "$N" -ge 2 ] && { PASS=$((PASS+1)); echo "  PASS  evictor --every loops ($N sweeps)"; } || { FAIL=$((FAIL+1)); FAILED+=("evictor --every"); echo "  FAIL  evictor --every loops (saw $N)"; }
kill $EP 2>/dev/null; wait $EP 2>/dev/null

echo "== SIGHUP rotation =="
cline feeder --config $CFG --log-file $LOGDIR/t_hup.log &
HP=$!; sleep 3
mv $LOGDIR/t_hup.log $LOGDIR/t_hup.log.1
kill -HUP $HP; sleep 2
grep -q "reopened" $LOGDIR/t_hup.log && { PASS=$((PASS+1)); echo "  PASS  SIGHUP reopens the log"; } || { FAIL=$((FAIL+1)); FAILED+=("SIGHUP"); echo "  FAIL  SIGHUP reopens the log"; }
kill $HP 2>/dev/null; wait $HP 2>/dev/null

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
