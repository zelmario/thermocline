#!/bin/bash
# The alert rules in docs/monitoring.md, checked against a real Prometheus.
#
# This one runs on the HOST, like testchaos and testtls, because it needs a
# Prometheus and the router's metrics endpoint at the same time.
#
#   bash test/testalerts.sh
#
# Why it exists. docs/monitoring.md carries the alert rules and they were written
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
PORT=${PORT:-27100}
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

echo "== an alert that fires on a number this system reports =="
# ThermoclineRouterDown, below, fires because the target disappeared. That is
# Prometheus noticing an absence, and it would fire the same way for a rule
# written against a metric this router has never heard of. This one is the
# other kind and the one worth having: the router stays up, publishes a
# counter, and the rule reads it.
#
# A stale read is one command away here. This suite runs a router and no
# feeder, so mirroring has published no position at all, and a read that falls
# through to the archive gives up waiting at once and answers anyway. That is
# exactly what stale_reads_total counts, and it is the one counter in
# docs/monitoring.md whose text says any value above zero is worth
# investigating.
BEFORE=$(docker exec $NODE sh -c "curl -s http://127.0.0.1:$PDASH/metrics" 2>/dev/null \
         | awk '/^thermocline_stale_reads_total /{print $2}')
docker exec $NODE mongosh "mongodb://dba:secret@127.0.0.1:$PORT/?authSource=admin" --quiet \
  --eval 'db.getSiblingDB("shop").tprobe.find({v:{$gt:0}}).limit(5).itcount()' >/dev/null 2>&1
AFTER=$(docker exec $NODE sh -c "curl -s http://127.0.0.1:$PDASH/metrics" 2>/dev/null \
        | awk '/^thermocline_stale_reads_total /{print $2}')
echo "  stale_reads_total went from ${BEFORE:-?} to ${AFTER:-?}"
# The premise, checked before the alert is waited for. Without this a rule that
# never fires and a read that was never stale report the same failure, and the
# wait below would burn two minutes saying the wrong thing.
chk "the read was actually counted as stale" "true" \
    "$([ -n "$AFTER" ] && [ "${AFTER:-0}" -gt "${BEFORE:-0}" ] && echo true || echo false)"
# A second one, so increase() has two rising samples inside its window rather
# than one edge.
docker exec $NODE mongosh "mongodb://dba:secret@127.0.0.1:$PORT/?authSource=admin" --quiet \
  --eval 'db.getSiblingDB("shop").tprobe.find({v:{$gt:1}}).limit(5).itcount()' >/dev/null 2>&1
for _ in $(seq 1 36); do
  PQ rules | grep -q '"ThermoclineStaleRead","state":"firing"' && break
  sleep 5
done
chk "ThermoclineStaleRead fires on a real stale read" "firing" \
    "$(PQ rules | python3 -c '
import json,sys
for g in json.load(sys.stdin)["data"]["groups"]:
  for r in g["rules"]:
    if r["name"] == "ThermoclineStaleRead": print(r.get("state"))' 2>/dev/null)"

# And the rule added on 2026-09-18, which until now was only checked for being
# spelled correctly and naming a metric that exists.
#
# It fires here on the feeder-down state rather than on a feeder that is merely
# behind, and the check is named for what it proves rather than for what the
# rule is for. With no feeder the gauge renders +Inf, which is "this router
# cannot tell", and the rule treats that as worth a warning. A feeder that is
# running and behind is the case this does not reach, and producing it needs a
# feeder held back for two minutes.
#
# Worth having anyway: +Inf is a real production state, it is the one an
# operator meets when a feeder dies, and firing it exercises the gauge, the
# exporter and the expression together. Measured while writing this, with a
# router up and no feeder: mirroring_cluster_time_behind_seconds +Inf while
# mirroring_lag_seconds read 1507, which is the two bars disagreeing exactly as
# docs/limitations.md describes.
for _ in $(seq 1 40); do
  PQ rules | grep -q '"ThermoclineClusterTimeBehind","state":"firing"' && break
  sleep 5
done
chk "ThermoclineClusterTimeBehind fires with no feeder" "firing" \
    "$(PQ rules | python3 -c '
import json,sys
for g in json.load(sys.stdin)["data"]["groups"]:
  for r in g["rules"]:
    if r["name"] == "ThermoclineClusterTimeBehind": print(r.get("state"))' 2>/dev/null)"

echo "== the archive refusing a document the fast tier holds =="
# Two rules for one condition and both were only ever parsed. The pair is worth
# firing together because they are deliberately different shapes: the first is
# an increase over a counter, which reports the moment it happens and then goes
# quiet, and the second is a gauge that stays up for as long as the deployment
# is in that state, which is the whole time an application is seeing refusals.
#
# The state is built the way testkeys builds it: the unique index on the archive
# alone, so the fast tier takes both documents and the archive refuses the
# second.
COLD=${COLD:-'mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'}
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
ANS=tkrefdel
docker exec $NODE mongosh "$HOT"  --quiet --eval "db.getSiblingDB('shop').$ANS.drop()" >/dev/null 2>&1
docker exec $NODE mongosh "$COLD" --quiet --eval "db.getSiblingDB('shop').$ANS.drop(); db.getSiblingDB('_thermocline').unmirrored.deleteMany({})" >/dev/null 2>&1
docker exec $NODE mongosh "$COLD" --quiet --eval "db.getSiblingDB('shop').$ANS.createIndex({code:1},{name:'code_u',unique:true})" >/dev/null 2>&1
docker exec -d $NODE cline feeder --config $CFG --log-file /var/log/cline/alerts_f.log
sleep 4
docker exec $NODE mongosh "$HOT" --quiet --eval "
  const c = db.getSiblingDB('shop').$ANS;
  c.insertOne({_id:1,code:'A'}); c.insertOne({_id:2,code:'A'})" >/dev/null 2>&1
for _ in $(seq 1 40); do
  docker exec $NODE sh -c "curl -s http://127.0.0.1:$PDASH/metrics" | grep -q '^thermocline_not_superset_namespaces 1' && break
  sleep 3
done
chk "the router reports a namespace not a superset" "1" \
    "$(docker exec $NODE sh -c "curl -s http://127.0.0.1:$PDASH/metrics" | awk '/^thermocline_not_superset_namespaces /{print int($2)}')"
for _ in $(seq 1 40); do
  PQ rules | grep -q '"ThermoclineArchiveNotSuperset","state":"firing"' && break
  sleep 5
done
chk "ThermoclineArchiveNotSuperset fires on it" "firing" \
    "$(PQ rules | python3 -c '
import json,sys
for g in json.load(sys.stdin)["data"]["groups"]:
  for r in g["rules"]:
    if r["name"] == "ThermoclineArchiveNotSuperset": print(r.get("state"))' 2>/dev/null)"
chk "and the counter rule fires too"          "firing" \
    "$(PQ rules | python3 -c '
import json,sys
for g in json.load(sys.stdin)["data"]["groups"]:
  for r in g["rules"]:
    if r["name"] == "ThermoclineUnmirrorableDocument": print(r.get("state"))' 2>/dev/null)"
# Cleared by deleting the document the archive would not take, which is what
# testkeys proves lifts the refusal.
docker exec $NODE mongosh "$HOT" --quiet --eval "db.getSiblingDB('shop').$ANS.deleteOne({_id:2})" >/dev/null 2>&1
for _ in $(seq 1 40); do
  docker exec $NODE sh -c "curl -s http://127.0.0.1:$PDASH/metrics" | grep -q '^thermocline_not_superset_namespaces 0' && break
  sleep 3
done
chk "and it clears when the document goes"    "0" \
    "$(docker exec $NODE sh -c "curl -s http://127.0.0.1:$PDASH/metrics" | awk '/^thermocline_not_superset_namespaces /{print int($2)}')"
docker exec $NODE sh -c "pkill -f 'cline feeder --config $CFG'" 2>/dev/null
docker exec $NODE mongosh "$HOT"  --quiet --eval "db.getSiblingDB('shop').$ANS.drop()" >/dev/null 2>&1
docker exec $NODE mongosh "$COLD" --quiet --eval "db.getSiblingDB('shop').$ANS.drop()" >/dev/null 2>&1

echo "== a hold on eviction that nobody lifted =="
# The rule added on 2026-09-21, fired rather than parsed. It is the one signal
# whose metric had to be changed to make it alertable at all: the gauge existed
# only while eviction was paused, so a rule against it had nothing to evaluate
# until the moment it was needed, and it reports zero the rest of the time now.
#
# Backdated rather than waited out, because the rule watches an hour. The gauge
# is the archive's own reading of when the hold was taken, so moving that
# instant is the honest way to make an hour-old hold.
COLD=${COLD:-'mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'}
docker exec $NODE cline pause --config $CFG --reason "testalerts, an hour ago" >/dev/null 2>&1
docker exec $NODE mongosh "$COLD" --quiet --eval '
  print(db.getSiblingDB("_thermocline").markers.updateOne({_id:"evictionPaused"},
    {$set:{since: new Date(Date.now() - 2*3600*1000)}}).modifiedCount)' >/dev/null 2>&1
HELD=$(docker exec $NODE sh -c "curl -s http://127.0.0.1:$PDASH/metrics" | awk '/^thermocline_eviction_paused_seconds /{print int($2)}')
chk "the gauge reads the hold as hours old"   "true" \
    "$([ "${HELD:-0}" -gt 3600 ] 2>/dev/null && echo true || echo false)"
for _ in $(seq 1 40); do
  PQ rules | grep -q '"ThermoclineEvictionHeld","state":"firing"' && break
  sleep 5
done
chk "ThermoclineEvictionHeld fires on it"     "firing" \
    "$(PQ rules | python3 -c '
import json,sys
for g in json.load(sys.stdin)["data"]["groups"]:
  for r in g["rules"]:
    if r["name"] == "ThermoclineEvictionHeld": print(r.get("state"))' 2>/dev/null)"
docker exec $NODE cline resume --config $CFG >/dev/null 2>&1
sleep 8
chk "and resuming takes the gauge back to zero" "0" \
    "$(docker exec $NODE sh -c "curl -s http://127.0.0.1:$PDASH/metrics" | awk '/^thermocline_eviction_paused_seconds /{print int($2)}')"

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
