#!/bin/bash
# The alert rules in docs/monitoring.md, checked against a real Prometheus.
#
# This one runs on the HOST, like testchaos and testtls, because it needs a
# Prometheus and the router's metrics endpoint at the same time.
#
#   bash test/testalerts.sh
#
# Why it exists. docs/monitoring.md carries eleven alert rules and they were written
# down and never run. That is a strange thing to leave unproved: the rules are
# what an operator is told to trust, and a rule that does not fire is
# indistinguishable from a system with nothing wrong. The failure modes are all
# quiet ones. A metric renamed in the exporter and not in the document. A label
# selector that matches nothing. A PromQL typo. None of them shows up until the
# night an alert should have paged somebody and did not.
#
# So the rules are extracted from the document itself rather than copied here.
# A copy would drift, and the copy is not what anyone deploys.
NODE=${NODE:-thermocline-zelmar-node0}
NET=${NET:-thermocline-zelmar-anydbver}
IMAGE=${IMAGE:-prom/prometheus:v2.53.5}
PDASH=${PDASH:-27110}
CFG=${CFG:-/root/cline.yaml}
WORK=$(mktemp -d)
# The Prometheus image runs as nobody, and mktemp -d makes a directory only its
# owner may enter, so every mount into that container fails with a permission
# denied that reads like a missing file. Measured: promtool reported "stat
# /rules/thermocline.rules.yml: permission denied" and Prometheus simply loaded
# zero rules, which the checks below would have called a broken rule file.
chmod 755 "$WORK"
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
cleanup() {
  docker rm -f tc-prom-test >/dev/null 2>&1
  docker exec $NODE sh -c "pkill -f 'cline router --config $CFG'" 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

RULEDOC=${RULEDOC:-docs/monitoring.md}
echo "== the rules $RULEDOC actually carries =="
python3 - "$WORK/thermocline.rules.yml" "$RULEDOC" <<'PY'
import re, sys
s = open(sys.argv[2]).read()
blocks = re.findall(r"```yaml\n(groups:\n.*?)```", s, re.S)
if len(blocks) != 1:
    print("  FATAL  expected exactly one alert rule block in %s, found %d" % (sys.argv[2], len(blocks)))
    sys.exit(2)
open(sys.argv[1], "w").write(blocks[0])
print("  %d alerts extracted from %s" % (blocks[0].count("- alert:"), sys.argv[2]))
PY
[ -s "$WORK/thermocline.rules.yml" ] || { echo "  FATAL  no rules extracted"; exit 2; }
chmod 644 "$WORK/thermocline.rules.yml"
RULES=$(grep -c "^      - alert:" "$WORK/thermocline.rules.yml")

echo "== they are valid PromQL =="
OUT=$(docker run --rm --entrypoint /bin/promtool -v "$WORK":/rules $IMAGE check rules /rules/thermocline.rules.yml 2>&1)
chk "promtool accepts every rule" "SUCCESS: $RULES rules found" "$(echo "$OUT" | grep -o "SUCCESS: .* rules found")"

echo "== every metric they name is exported =="
# The quiet failure this catches: a counter renamed in promexport.go and not in
# the document. The rule then evaluates against nothing and never fires.
docker exec $NODE sh -c "pkill -f 'cline router --config $CFG'" 2>/dev/null; sleep 1
docker exec -d $NODE cline router --config $CFG --log-file /var/log/cline/alerts_r.log
for _ in $(seq 1 30); do
  docker exec $NODE sh -c "curl -sf -o /dev/null http://127.0.0.1:$PDASH/metrics" 2>/dev/null && break
  sleep 1
done
docker exec $NODE sh -c "curl -s http://127.0.0.1:$PDASH/metrics" > "$WORK/metrics.txt"
chk "the router is exporting" "true" "$([ -s "$WORK/metrics.txt" ] && echo true || echo false)"
MISSING=$(python3 - "$WORK/thermocline.rules.yml" "$WORK/metrics.txt" <<'PY'
import re, sys
rules = open(sys.argv[1]).read()
exported = set(re.findall(r"^(thermocline_[a-z_]+)", open(sys.argv[2]).read(), re.M))
# up{} is Prometheus's own, not ours.
named = set(re.findall(r"\b(thermocline_[a-z_]+)", rules))
missing = sorted(n for n in named if n not in exported)
print(",".join(missing) if missing else "none")
PY
)
chk "no rule names a metric nothing exports" "none" "$MISSING"

echo "== and a real Prometheus loads and evaluates them =="
cat > "$WORK/prometheus.yml" <<EOF
global:
  scrape_interval: 5s
  evaluation_interval: 5s
rule_files:
  - /rules/thermocline.rules.yml
scrape_configs:
  # The job name is load bearing: one rule is up{job="thermocline"} == 0.
  - job_name: thermocline
    static_configs:
      - targets: ["$NODE:$PDASH"]
EOF
chmod 644 "$WORK/prometheus.yml"
docker rm -f tc-prom-test >/dev/null 2>&1
docker run -d --name tc-prom-test --network $NET -v "$WORK":/rules \
  -v "$WORK/prometheus.yml":/etc/prometheus/prometheus.yml $IMAGE \
  --config.file=/etc/prometheus/prometheus.yml >/dev/null
PQ() { docker exec tc-prom-test wget -qO- "http://127.0.0.1:9090/api/v1/$1" 2>/dev/null; }
for _ in $(seq 1 30); do PQ rules | grep -q '"state"' && break; sleep 2; done
chk "every rule is loaded and evaluating" "$RULES" \
    "$(PQ rules | python3 -c 'import json,sys; print(sum(len(g["rules"]) for g in json.load(sys.stdin)["data"]["groups"]))' 2>/dev/null)"
for _ in $(seq 1 20); do [ "$(PQ targets | grep -c '"health":"up"')" -ge 1 ] && break; sleep 2; done
chk "and scraping the router"             "1" "$(PQ targets | grep -c '"health":"up"')"

echo "== an alert that should fire, does =="
# The router is stopped, so up{job="thermocline"} goes to 0. Its rule waits two
# minutes, and waiting for it is the whole point: a rule that only ever gets
# read is a rule nobody has seen work.
docker exec $NODE sh -c "pkill -f 'cline router --config $CFG'" 2>/dev/null
for _ in $(seq 1 60); do
  PQ rules | grep -q '"ThermoclineRouterDown","state":"firing"' && break
  sleep 5
done
chk "ThermoclineRouterDown fires with the router down" "firing" \
    "$(PQ rules | python3 -c '
import json,sys
for g in json.load(sys.stdin)["data"]["groups"]:
  for r in g["rules"]:
    if r["name"] == "ThermoclineRouterDown": print(r.get("state"))' 2>/dev/null)"
# And its near neighbour, which must not fire for the same reason. absent() on a
# metric that exists is quiet; these two are easy to write so that both fire on
# everything, and then neither means anything.
chk "and the panic alert stays quiet"     "inactive" \
    "$(PQ rules | python3 -c '
import json,sys
for g in json.load(sys.stdin)["data"]["groups"]:
  for r in g["rules"]:
    if r["name"] == "ThermoclinePanic": print(r.get("state"))' 2>/dev/null)"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
