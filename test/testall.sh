#!/bin/bash
# Exercise every cline subcommand, flag and error path.
CFG=/root/cline.yaml
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
ok "missing config file"                1 "read config"       -- cline warmer --config /root/nope.yaml
printf 'hot:\n  uri: "["\n  bad yaml here\n' > /root/bad.yaml
ok "unparseable config"                 1 "parse config"      -- cline warmer --config /root/bad.yaml
printf 'cold:\n  uri: "mongodb://x"\n' > /root/nohot.yaml
ok "missing hot.uri"                    1 "hot.uri is required" -- cline warmer --config /root/nohot.yaml
sed 's/completeness: archive/completeness: banana/' $CFG > /root/badcomp.yaml
ok "invalid completeness value"         1 "must be archive, strict or permissive" -- cline router --config /root/badcomp.yaml
sed '/^evictor:/,/^warmer:/{/rules:/,/older_than/d}' $CFG > /root/norules.yaml
ok "evictor with no rules"              1 "rules is empty"    -- cline evictor --config /root/norules.yaml --once

echo "== watermark =="
ok "list all"                           0 "shop"              -- cline watermark --config $CFG
ok "single namespace"                   0 "shop.orders"       -- cline watermark --config $CFG --ns shop.orders
ok "unknown namespace claims nothing"   0 "claims nothing"    -- cline watermark --config $CFG --ns no.such
ok "--clear without --ns rejected"      1 "ns is required"    -- cline watermark --config $CFG --clear
ok "two actions rejected"               1 "give one"          -- cline watermark --config $CFG --ns shop.orders --clear --complete-all
ok "bad --complete-from rejected"       1 "RFC3339"           -- cline watermark --config $CFG --ns shop.orders --complete-from notadate
ok "--complete-from sets"               0 "newer than"        -- cline watermark --config $CFG --ns tc.probe --complete-from 2026-01-01T00:00:00Z
ok "--complete-all sets"                0 "all of history"    -- cline watermark --config $CFG --ns tc.probe --complete-all
ok "--clear withdraws"                  0 "withdrew"          -- cline watermark --config $CFG --ns tc.probe --clear
ok "--clear again is not an error"      0 "no claim"          -- cline watermark --config $CFG --ns tc.probe --clear
ok "--log-file accepted"                0 ""                  -- cline watermark --config $CFG --log-file /var/log/cline/wm.log
test -s /var/log/cline/wm.log && { PASS=$((PASS+1)); echo "  PASS  watermark wrote its log file"; } || { FAIL=$((FAIL+1)); FAILED+=("watermark log file"); echo "  FAIL  watermark wrote its log file"; }

echo "== keys =="
ok "report"                             0 "unique indexes|nothing to enforce" -- cline keys --config $CFG
ok "--rebuild"                          0 "claimed|nothing new" -- cline keys --config $CFG --rebuild
ok "--rebuild is idempotent"            0 "claimed 0|nothing new" -- cline keys --config $CFG --rebuild

echo "== warmer =="
ok "--dry-run"                          0 "warm complete"     -- cline warmer --config $CFG --dry-run
ok "normal run"                         0 "warm complete"     -- cline warmer --config $CFG
ok "-v verbose"                         0 "warm complete"     -- cline warmer --config $CFG -v
sed '/^warmer:/,/^router:/{/  rules:/,/    back:/d}' $CFG > /root/nowarm.yaml
ok "no warmer rules"                    1 "rules is empty"    -- cline warmer --config /root/nowarm.yaml

echo "== evictor =="
ok "--once --dry-run"                   0 "sweep complete"    -- cline evictor --config $CFG --once --dry-run
ok "--once"                             0 "sweep complete"    -- cline evictor --config $CFG --once
ok "--once -v"                          0 "sweep complete"    -- cline evictor --config $CFG --once -v
ok "--once --health-timeout"            0 "sweep complete"    -- cline evictor --config $CFG --once --health-timeout 10s

echo "== daemons =="
rm -f /var/log/cline/t_*.log
cline feeder --config $CFG --log-file /var/log/cline/t_feeder.log &
FP=$!; sleep 4
grep -q "mirroring" /var/log/cline/t_feeder.log && { PASS=$((PASS+1)); echo "  PASS  feeder starts and mirrors"; } || { FAIL=$((FAIL+1)); FAILED+=("feeder start"); echo "  FAIL  feeder starts and mirrors"; }
kill $FP 2>/dev/null; wait $FP 2>/dev/null

cline feeder --config $CFG -v --log-file /var/log/cline/t_feederv.log &
FP=$!; sleep 4
grep -q "level=DEBUG\|mirroring" /var/log/cline/t_feederv.log && { PASS=$((PASS+1)); echo "  PASS  feeder -v"; } || { FAIL=$((FAIL+1)); FAILED+=("feeder -v"); echo "  FAIL  feeder -v"; }
kill $FP 2>/dev/null; wait $FP 2>/dev/null

cline router --config $CFG --listen 0.0.0.0:27177 --log-file /var/log/cline/t_router.log &
RP=$!; sleep 4
grep -q "27177" /var/log/cline/t_router.log && { PASS=$((PASS+1)); echo "  PASS  router --listen override"; } || { FAIL=$((FAIL+1)); FAILED+=("router --listen"); echo "  FAIL  router --listen override"; }
mongosh --port 27177 -u dba -p secret --authenticationDatabase admin --quiet --eval 'db.getSiblingDB("shop").orders.findOne()' >/dev/null 2>&1 \
  && { PASS=$((PASS+1)); echo "  PASS  router serves on the overridden port"; } || { FAIL=$((FAIL+1)); FAILED+=("router override serves"); echo "  FAIL  router serves on the overridden port"; }
kill $RP 2>/dev/null; wait $RP 2>/dev/null

cline evictor --config $CFG --every 2s --log-file /var/log/cline/t_evictor.log &
EP=$!; sleep 7
N=$(grep -c "sweep complete\|evicting" /var/log/cline/t_evictor.log 2>/dev/null || echo 0)
[ "$N" -ge 2 ] && { PASS=$((PASS+1)); echo "  PASS  evictor --every loops ($N sweeps)"; } || { FAIL=$((FAIL+1)); FAILED+=("evictor --every"); echo "  FAIL  evictor --every loops (saw $N)"; }
kill $EP 2>/dev/null; wait $EP 2>/dev/null

echo "== SIGHUP rotation =="
cline feeder --config $CFG --log-file /var/log/cline/t_hup.log &
HP=$!; sleep 3
mv /var/log/cline/t_hup.log /var/log/cline/t_hup.log.1
kill -HUP $HP; sleep 2
grep -q "reopened" /var/log/cline/t_hup.log && { PASS=$((PASS+1)); echo "  PASS  SIGHUP reopens the log"; } || { FAIL=$((FAIL+1)); FAILED+=("SIGHUP"); echo "  FAIL  SIGHUP reopens the log"; }
kill $HP 2>/dev/null; wait $HP 2>/dev/null

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
