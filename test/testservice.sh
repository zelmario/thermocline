#!/bin/bash
# The units, run rather than read.
#
# This one runs on the HOST and drives systemd inside node0, which is a real
# systemd 239 and therefore a real check of what these files say.
#
#   bash test/testservice.sh
#
# packaging/systemd/ ships a unit for each component, and a unit file is exactly
# the kind of thing that reads correctly and does nothing:
# [[thermocline-alerts-proved]] is eleven Prometheus rules written down and
# never fired, found by asking what else had only ever been written. These have
# four claims in them worth checking, and none of them is about YAML:
#
#   - three are daemons and the warmer is not, so the warmer is oneshot plus a
#     timer and must not come back by itself;
#   - Restart=always on the feeder is what makes a standby, which rests on the
#     lease refusing the second one;
#   - the router refuses to start without the archive, so its restart loop is
#     the outage procedure;
#   - ProtectSystem=strict makes / read-only, so a daemon told to log to a file
#     cannot write it unless ReadWritePaths says so.
#
# The last one is the reason this suite exists rather than a paragraph. Nothing
# about that failure is visible in the file.
NODE=${NODE:-thermocline-zelmar-node0}
DB=${DB:-shopSvc}
META=_thermocline_svc
HOT='mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'
COLD='mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'
ROUTER='mongodb://dba:secret@127.0.0.1:27640/?authSource=admin'
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-56s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-56s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
N() { docker exec "$NODE" bash -c "$1"; }
H() { docker exec "$NODE" mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { docker exec "$NODE" mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
R() { docker exec "$NODE" mongosh "$ROUTER" --quiet --eval "$1" 2>&1 | tail -1; }
routerUp() { docker exec "$NODE" mongosh "$ROUTER" --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1; }
active() { N "systemctl is-active $1 2>/dev/null" | tr -d '\n'; }
cleanup() {
  N "systemctl disable --now cline-feeder cline-router cline-evictor cline-warmer.timer >/dev/null 2>&1;
     systemctl stop cline-warmer.service >/dev/null 2>&1; true" >/dev/null 2>&1
}
trap cleanup EXIT

echo "== install it the way the document says =="
cleanup
# The installer rather than its steps repeated here, so the procedure an
# operator runs is the one under test. See [[run-the-remedy-you-name]].
# The config this suite wrote last time is still there, and the installer's
# first-install branch is the one about to be checked. Measured: testservice
# passed 30/0 on a machine it had never run on and 29/1 on the next sweep,
# because install.sh correctly reported a config it had left alone. A suite that
# only passes the first time is a suite that reports the second run wrong.
docker exec "$NODE" rm -f /etc/cline/cline.yaml >/dev/null 2>&1
docker exec "$NODE" rm -rf /root/packaging >/dev/null 2>&1
docker cp packaging "$NODE":/root/packaging >/dev/null
INST=$(N "cd /root && bash packaging/install.sh /usr/local/bin/cline 2>&1")
chk "the installer runs clean"                    "0" "$(N "cd /root && bash packaging/install.sh /usr/local/bin/cline >/dev/null 2>&1; echo \$?" | tr -d '\n')"
chk "it creates the account the units run as"     "cline" \
    "$(N "id -un cline 2>/dev/null" | tr -d '\n')"
chk "and installs the units"                      "5" \
    "$(N "ls /etc/systemd/system/cline-* 2>/dev/null | wc -l" | tr -d '\n')"
chk "and says the config is the operator's to write" "true" \
    "$(grep -q 'write /etc/cline/cline.yaml next' <<<"$INST" && echo true || echo false)"
# Its own databases and ports, so this suite cannot disturb another one, and
# log.file set on purpose: a daemon that logs to the journal would never touch
# the filesystem and ProtectSystem=strict would go untested.
N "cat > /etc/cline/cline.yaml <<'YAML'
hot:
  uri: \"$HOT\"
cold:
  uri: \"$COLD\"
log:
  file: \"/var/log/cline/cline.log\"
meta:
  db: \"$META\"
feeder:
  id: \"svc\"
  namespaces:
    - \"$DB.orders\"
  checkpoint_every: 1s
  max_await: 20ms
evictor:
  feeder_id: \"svc\"
  rate: 500
  rules:
    - ns: \"$DB.orders\"
      older_than: 20m
  backoff:
    min_oplog_window: 1m
warmer:
  rate: 500
  rules:
    - ns: \"$DB.orders\"
      back: 15m
  backoff:
    min_oplog_window: 1m
router:
  listen: \"0.0.0.0:27640\"
  dashboard: \"0.0.0.0:27650\"
  completeness: archive
  archive_wait: 5s
YAML
chown cline:cline /etc/cline/cline.yaml && chmod 0640 /etc/cline/cline.yaml" >/dev/null
for T in H C; do $T "db.getSiblingDB('$DB').dropDatabase(); db.getSiblingDB('$META').dropDatabase()" >/dev/null; done
N "systemctl daemon-reload" >/dev/null
chk "the config is readable by the account only"  "640 cline cline" \
    "$(N "stat -c '%a %U %G' /etc/cline/cline.yaml")"
# And now that one exists, the installer must not touch it. This is the branch
# that matters on an upgrade: a config overwritten with an example is a
# deployment pointed at nothing.
AGAIN=$(N "cd /root && bash packaging/install.sh /usr/local/bin/cline 2>&1")
chk "a re-install leaves the config alone"        "true" \
    "$(grep -q 'was left alone' <<<"$AGAIN" && echo true || echo false)"
chk "and the config is unchanged"                 "640 cline cline" \
    "$(N "stat -c '%a %U %G' /etc/cline/cline.yaml")"

echo "== what systemd itself makes of the files =="
# systemd-analyze verify names an unknown directive, which is how a unit written
# against a newer systemd than the target fails: the line is ignored with a log
# line nobody reads, and the protection it was there for is simply absent.
for u in cline-feeder cline-router cline-evictor cline-warmer; do
  BAD=$(N "systemd-analyze verify /etc/systemd/system/$u.service 2>&1 | grep -v '^$' | head -5")
  chk "$u.service verifies clean"                 "" "$BAD"
done
BAD=$(N "systemd-analyze verify /etc/systemd/system/cline-warmer.timer 2>&1 | grep -v '^$' | head -5")
chk "cline-warmer.timer verifies clean"           "" "$BAD"

echo "== the logrotate file, parsed rather than read =="
# It ships beside the units and it depends on them: `create 0640 cline cline`
# names the account they run as, so the file is wrong until that account exists.
# logrotate -d parses and does nothing, which is what makes it safe to run here.
docker cp packaging/logrotate/cline "$NODE":/root/clinelogrotate >/dev/null
N "chown root:root /root/clinelogrotate" >/dev/null
LR=$(N "logrotate -d /root/clinelogrotate 2>&1 | grep -c '^error:'" | tr -d '\n')
chk "the logrotate file parses clean"             "0" "$LR"

echo "== the feeder as a service =="
N "systemctl enable --now cline-feeder" >/dev/null 2>&1
for _ in $(seq 1 30); do [ "$(active cline-feeder)" = "active" ] && break; sleep 1; done
chk "the feeder is active"                        "active" "$(active cline-feeder)"
# The claim under ProtectSystem=strict: the account can write where the config
# sends it. A read-only /var would leave the daemon up and the file empty.
for _ in $(seq 1 20); do
  SZ=$(N "stat -c %s /var/log/cline/cline.log 2>/dev/null || echo 0" | tr -d '\n')
  [ "$SZ" -gt 0 ] 2>/dev/null && break
  sleep 1
done
chk "and can write the log the config names"      "true" \
    "$([ "$SZ" -gt 0 ] 2>/dev/null && echo true || echo false)"
H "db.getSiblingDB('$DB').orders.insertOne({k:1, x:'before'})" >/dev/null
for _ in $(seq 1 30); do
  GOT=$(C "print(db.getSiblingDB('$DB').orders.countDocuments({k:1}))")
  [ "$GOT" = "1" ] && break
  sleep 1
done
chk "and it is really mirroring"                  "1" "$GOT"

# The installer's other half: run over a deployment that is already up, it has
# to say that nothing has been restarted yet and in which order to do it. A
# fresh-install message there would read as "you are done" to somebody upgrading
# at three in the morning.
UP=$(N "cd /root && bash packaging/install.sh /usr/local/bin/cline 2>&1")
chk "installing over a running deployment says so" "true" \
    "$(grep -q 'Nothing here restarted anything' <<<"$UP" && echo true || echo false)"
chk "and puts the routers last"                    "true" \
    "$(grep -q 'routers last' <<<"$UP" && echo true || echo false)"

echo "== Restart=always is what makes a standby =="
# Two halves. A second feeder is refused by the lease, which is what makes it
# safe to enable the unit on two hosts; and systemd brings this one back by
# itself after a kill -9, which is what makes the standby take over.
REFUSED=$(N "timeout 20 runuser -u cline -- /usr/local/bin/cline feeder --config /etc/cline/cline.yaml 2>&1 | head -3")
chk "a second feeder is refused by the lease"     "true" \
    "$(grep -q 'another feeder' <<<"$REFUSED" && echo true || echo false)"
OLDPID=$(N "systemctl show -p MainPID --value cline-feeder" | tr -d '\n')
N "kill -9 $OLDPID" >/dev/null 2>&1
for _ in $(seq 1 40); do
  NEWPID=$(N "systemctl show -p MainPID --value cline-feeder" | tr -d '\n')
  [ -n "$NEWPID" ] && [ "$NEWPID" != "0" ] && [ "$NEWPID" != "$OLDPID" ] && break
  sleep 1
done
chk "systemd brings the feeder back after kill -9" "true" \
    "$([ -n "$NEWPID" ] && [ "$NEWPID" != "0" ] && [ "$NEWPID" != "$OLDPID" ] && echo true || echo false)"
H "db.getSiblingDB('$DB').orders.insertOne({k:2, x:'after'})" >/dev/null
for _ in $(seq 1 40); do
  GOT2=$(C "print(db.getSiblingDB('$DB').orders.countDocuments({k:2}))")
  [ "$GOT2" = "1" ] && break
  sleep 1
done
chk "and the replacement mirrors what came next"  "1" "$GOT2"

echo "== the router as a service =="
N "systemctl enable --now cline-router" >/dev/null 2>&1
for _ in $(seq 1 40); do routerUp && break; sleep 1; done
chk "the router is active"                        "active" "$(active cline-router)"
chk "and answers on router.listen"                "2" \
    "$(R "print(db.getSiblingDB('$DB').orders.countDocuments({}))")"

echo "== a restart with a client connected =="
# The unit says Restart=always and the rolling upgrade says to restart the
# routers last, one host at a time. Both assume a stop that ends.
#
# It did not. A router waits for its connection goroutines, and those are parked
# in a blocking read on sockets an application's pool holds open for ever:
# measured, a router with one idle client was still running 60 seconds after
# SIGTERM, against one second with none. systemd then waits out TimeoutStopSec
# and sends SIGKILL, so every restart took a minute and a half and ended in the
# ungraceful stop the procedure exists to avoid.
#
# The margin here is large on purpose: prompt is about a second, broken is
# ninety, so thirty tells them apart on any machine.
docker exec -d "$NODE" bash -c "mongosh '$ROUTER' --quiet --eval 'db.adminCommand({ping:1}); sleep(120000)' > /root/idleclient.out 2>&1"
sleep 4
chk "a client is connected"                       "true" \
    "$(N "pgrep -f 'mongosh.*27640' >/dev/null && echo true || echo false" | tr -d '\n')"
T0=$(date +%s)
N "systemctl restart cline-router" >/dev/null 2>&1
T1=$(date +%s)
chk "restarting with a client connected is prompt" "true" \
    "$([ $((T1-T0)) -lt 30 ] && echo true || echo false)"
echo "  the restart took $((T1-T0))s"
for _ in $(seq 1 40); do routerUp && break; sleep 1; done
chk "and it is serving again"                     "active" "$(active cline-router)"
N "pkill -f 'mongosh.*2764[0]'" >/dev/null 2>&1

echo "== the warmer is the one that is not a daemon =="
# The claim the unit is built on. A oneshot that has run is inactive with
# Result=success and NRestarts=0; the same file written as the other three would
# be active, or restarting, for ever.
N "systemctl start cline-warmer.service" >/dev/null 2>&1
for _ in $(seq 1 30); do [ "$(active cline-warmer)" != "activating" ] && break; sleep 1; done
sleep 3
chk "the warmer runs and exits"                   "inactive" "$(active cline-warmer)"
chk "it succeeded rather than failed"             "success" \
    "$(N "systemctl show -p Result --value cline-warmer.service" | tr -d '\n')"
chk "and nothing restarted it"                    "0" \
    "$(N "systemctl show -p NRestarts --value cline-warmer.service" | tr -d '\n')"
N "systemctl enable --now cline-warmer.timer" >/dev/null 2>&1
chk "the timer is what schedules it"              "active" "$(active cline-warmer.timer)"
chk "and it has a next run"                       "true" \
    "$(N "systemctl list-timers --all cline-warmer.timer --no-legend | grep -q cline-warmer && echo true || echo false" | tr -d '\n')"

echo "== a restart does not resume eviction =="
N "runuser -u cline -- /usr/local/bin/cline pause --config /etc/cline/cline.yaml --reason 'testservice'" >/dev/null 2>&1
N "systemctl enable --now cline-evictor" >/dev/null 2>&1
for _ in $(seq 1 30); do [ "$(active cline-evictor)" = "active" ] && break; sleep 1; done
chk "the evictor is active"                       "active" "$(active cline-evictor)"
N "systemctl restart cline-evictor" >/dev/null 2>&1
sleep 8
chk "and comes back paused"                       "true" \
    "$(N "grep -c 'eviction is paused by an operator' /var/log/cline/cline.log" | tr -d '\n' | awk '{print ($1+0>0)?"true":"false"}')"
N "runuser -u cline -- /usr/local/bin/cline resume --config /etc/cline/cline.yaml" >/dev/null 2>&1

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
