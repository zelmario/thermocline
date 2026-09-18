#!/bin/bash
# Resilience: kill things while traffic is running and check nothing is lost.
#
# This one runs on the HOST, not inside a node, because it has to stop and start
# mongod on other containers. The other suites run inside the node; this one
# drives them with docker exec.
#
#   bash test/testchaos.sh
#
# Slower than the rest because it waits on real elections.
#
# It runs in either authentication mode, because every check here is stated
# against what the client was told rather than against a fixed number: every
# acknowledged write is present, none is applied twice, a retry finishes the
# job. With authentication proxied, writes issued while the fast tier has no
# primary fail and the client retries. With `terminate_auth` on, the router owns
# its upstream connections and pools them, so those writes are replayed onto a
# fresh primary instead, and this is the only thing that exercises that pool
# during a real election rather than one write at a time:
#
#   docker exec thermocline-zelmar-node0 sh -c \
#     "sed 's/^  listen:/  terminate_auth: true\n  listen:/' /root/cline.yaml > /root/cline_chaos_ta.yaml"
#   CFG=/root/cline_chaos_ta.yaml bash test/testchaos.sh
NODE=thermocline-zelmar-node0
CFG=${CFG:-/root/cline.yaml}
NS=tprobe
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-46s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-46s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { docker exec $NODE mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { docker exec $NODE mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { docker exec $NODE mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
CLINE() { docker exec $NODE cline "$@"; }
nodeFor() {
  for n in 0 1 2 3 4 5; do
    ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' thermocline-zelmar-node$n 2>/dev/null)
    [ "$ip" = "$1" ] && { echo "thermocline-zelmar-node$n"; return; }
  done
}
feederUp() { docker exec -d $NODE cline feeder --config $CFG --log-file /var/log/cline/chaos_f.log; }
routerUp() { docker exec -d $NODE cline router --config $CFG --log-file /var/log/cline/chaos_r.log; }

echo "== setup =="
echo "  config $CFG, terminate_auth $(docker exec $NODE grep -c 'terminate_auth: true' $CFG 2>/dev/null | tail -1)"
docker exec $NODE pkill -f 'cline feeder|cline router' 2>/dev/null; sleep 2
docker exec $NODE sh -c 'rm -f /var/log/cline/chaos_*.log'
H "db.getSiblingDB('shop').$NS.drop()" >/dev/null
C "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('_thermocline').checkpoints.deleteOne({_id:'test'})" >/dev/null
feederUp; routerUp; sleep 7
CLINE watermark --config $CFG --ns shop.$NS --complete-all >/dev/null

echo "== feeder killed and restarted under write load =="
docker exec -d $NODE mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "
for (let i=0;i<300;i++){ try{ db.getSiblingDB('shop').$NS.insertOne({_id:i,seq:i}); }catch(e){} sleep(25); }
print('done');"
sleep 3
docker exec $NODE pkill -f 'cline feeder'
sleep 4
feederUp
sleep 18
chk "every write reached the fast tier" "300" "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({}))")"
chk "none missing from the archive"     "0"   "$(C "let m=0; for(let i=0;i<300;i++){ if(!db.getSiblingDB('shop').$NS.findOne({_id:i})) m++; } print(m)")"

echo "== archive primary killed under read load =="
CLINE watermark --config $CFG --ns shop.$NS --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
CN=$(nodeFor "$(C 'print(rs.status().members.filter(m=>m.stateStr=="PRIMARY")[0].name.split(":")[0])')")
if [ -z "$CN" ]; then echo "  SKIP  archive primary not resolved"; else
  echo "  archive primary is $CN"
  docker exec -d $NODE sh -c "mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval \"
    for (let i=0;i<25;i++){ try{ print(db.getSiblingDB('shop').$NS.countDocuments({})); }catch(e){ print('ERR'); } sleep(1000); }\" > /tmp/coldreads.txt 2>&1"
  sleep 4; docker exec $CN systemctl stop mongod; sleep 16; docker exec $CN systemctl start mongod; sleep 12
  chk "reads survive an archive failover" "0" "$(docker exec $NODE sh -c "grep -cvE '^300\$' /tmp/coldreads.txt || true" 2>/dev/null | tail -1)"
fi

echo "== fast tier primary killed under write load =="
CLINE watermark --config $CFG --ns shop.$NS --complete-all >/dev/null
sleep 3
HN=$(nodeFor "$(H 'print(rs.status().members.filter(m=>m.stateStr=="PRIMARY")[0].name.split(":")[0])')")
if [ -z "$HN" ]; then echo "  SKIP  fast tier primary not resolved"; else
  echo "  fast tier primary is $HN"
  docker exec -d $NODE sh -c "mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval \"
    for (let i=1000;i<1025;i++){ try{ db.getSiblingDB('shop').$NS.insertOne({_id:i,fo:1}); print('ok'); }catch(e){ print('ERR'); } sleep(1000); }\" > /tmp/hotwrites.txt 2>&1"
  sleep 4; docker exec $HN systemctl stop mongod; sleep 18; docker exec $HN systemctl start mongod; sleep 20
  # Writes issued while the fast tier has no primary at all do fail, and that is
  # inherent: the router proxies SCRAM rather than terminating it, so it cannot
  # re-authenticate a fresh upstream and replay the command. A driver talking to
  # the replica set directly would block in server selection instead.
  #
  # What must hold is that the failure is honest. Every write that reported
  # success is present, every write that reported an error is absent, and none
  # is applied twice. Silent loss or phantom success is the thing that matters.
  OKW=$(docker exec $NODE sh -c 'grep -c "^ok$" /tmp/hotwrites.txt || true' 2>/dev/null | tail -1)
  ERRW=$(docker exec $NODE sh -c 'grep -c ERR /tmp/hotwrites.txt || true' 2>/dev/null | tail -1)
  echo "  $OKW acknowledged, $ERRW refused during the election"
  chk "every acknowledged write is present" "$OKW" "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({fo:1}))")"
  chk "no write applied twice"              "$OKW" "$(H "print(db.getSiblingDB('shop').$NS.distinct('_id',{fo:1}).length)")"
  chk "the client can finish by retrying"   "25"   "$(R "for(let i=1000;i<1025;i++){ try{ db.getSiblingDB('shop').$NS.insertOne({_id:i,fo:1}); }catch(e){} } print(db.getSiblingDB('shop').$NS.countDocuments({fo:1}))")"
fi

echo "== an insert while the archive is unreachable =="
# The check that stops an insert duplicating an _id the archive still holds has
# to reach the archive. When it could not, the insert used to be let through:
# measured on a pair with the archive stopped, {_id:500, v:"archived, precious",
# tier:"gold"} came back as {_id:500, v:"clobber"} once the feeder mirrored the
# new document over the archived one. Nothing anywhere said so.
#
# This is the only place that can test it. The router pings both tiers at
# startup and will not start without the archive, so the outage has to happen
# to a router that is already running, which means stopping mongod on other
# containers.
CLDNODES=""
for m in $(C 'print(rs.status().members.map(m=>m.name.split(":")[0]).join(" "))'); do
  n=$(nodeFor "$m")
  [ -n "$n" ] && CLDNODES="$CLDNODES $n"
done
echo "  the archive runs on:$CLDNODES"
if [ -z "$CLDNODES" ]; then echo "  SKIP  archive members not resolved"; else
  C "db.getSiblingDB('shop').$NS.deleteMany({_id:'outage'}); db.getSiblingDB('shop').$NS.insertOne({_id:'outage',v:'archived, precious'})" >/dev/null
  H "db.getSiblingDB('shop').$NS.deleteMany({_id:'outage'})" >/dev/null
  # A second router, identical but for a claim grace and its own two ports. It
  # has to be started before the archive goes away, because a router will not
  # start without it, which is the whole reason that limitation exists.
  docker exec $NODE sh -c "sed -e 's|^  listen: .*|  listen: \"0.0.0.0:27104\"|' \
                               -e 's|^  dashboard: .*|  dashboard: \"0.0.0.0:27114\"|' \
                               -e 's|^  completeness:|  claim_grace: 5m\n  completeness:|' \
                               $CFG > /root/cline_grace.yaml"
  docker exec -d $NODE cline router --config /root/cline_grace.yaml --log-file /var/log/cline/chaos_grace.log
  for _ in $(seq 1 30); do
    docker exec $NODE mongosh --port 27104 -u dba -p secret --authenticationDatabase admin \
      --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
    sleep 1
  done
  # No expected value anywhere here: the fast tier's count is whatever the
  # sections above left, and the claim is that it does not change because the
  # archive went away.
  COUNT() { docker exec $NODE mongosh --port "$1" -u dba -p secret --authenticationDatabase admin --quiet --eval "
    try { print(db.getSiblingDB('shop').$NS.countDocuments({})); }
    catch(e) { print('REFUSED'); }" 2>&1 | tail -1; }
  WANT=$(COUNT 27100)
  chk "the grace router answers as the plain one does, before the outage" "$WANT" "$(COUNT 27104)"
  for n in $CLDNODES; do docker exec $n systemctl stop mongod; done
  sleep 3
  OUT=$(docker exec $NODE mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "
    try { db.getSiblingDB('shop').$NS.insertOne({_id:'outage',v:'clobber'}); print('ACCEPTED'); }
    catch(e) { print(e.message.indexOf('archive could not be reached') >= 0 ? 'REFUSED' : 'OTHER: '+e.message.substring(0,50)); }" 2>&1 | tail -1)
  chk "an insert is refused while the archive is down" "REFUSED" "$OUT"
  chk "and nothing landed on the fast tier"            "0"       "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({_id:'outage'}))")"

  # And a read of what the fast tier owns, which is what router.claim_grace is
  # for. A running router stops answering tiered reads about five seconds into
  # an archive outage, because the completeness claim lives on the archive and
  # the cached copy expires. The grace says how much longer the cached claim may
  # be believed, and the argument for it is that an archive nobody can reach is
  # an archive where nothing can be evicted.
  #
  # Both arms in the one outage window, because that is the only way to be sure
  # they met the same fault: the grace router answers and the default one
  # refuses. Off by default is half of what this checks.
  #
  # Both routers read the claim before the outage, on purpose. A claim never
  # read is nothing to hold over, and the grace deliberately does not invent one.
  sleep 7   # past ClaimCacheTTL, so neither router still holds a claim read before the outage began
  # Timed, because a claim read that cannot be served costs 30.002 seconds of
  # server selection and that used to be paid on every read: an archive outage
  # was a router hanging for half a minute per read and then refusing, not a
  # router refusing. A failure is remembered for the claim cache's own TTL, so
  # the first read here pays it and the two after it must not.
  T0=$(date +%s%N); FIRST=$(COUNT 27104); T1=$(date +%s%N)
  SECOND=$(COUNT 27104); THIRD=$(COUNT 27104); T2=$(date +%s%N)
  echo "  claim reads during the outage: first $(( (T1-T0)/1000000 ))ms, next two $(( (T2-T1)/1000000 ))ms"
  chk "the grace router still answers it, past the claim cache TTL" "$WANT" "$FIRST"
  chk "and again"                                       "$WANT"   "$SECOND"
  chk "and again"                                       "$WANT"   "$THIRD"
  chk "a remembered failure is not paid for twice"      "true"    "$([ $(( (T2-T1)/1000000 )) -lt 5000 ] && echo true || echo false)"
  chk "the same read without a grace is refused"        "REFUSED" "$(COUNT 27100)"
  chk "and the grace router counted what it did"        "true" \
      "$(docker exec $NODE sh -c "curl -s http://127.0.0.1:27114/api/snapshot" 2>/dev/null \
          | python3 -c 'import json,sys; print("true" if json.load(sys.stdin)["counters"]["claim_grace_reads"] > 0 else "false")' 2>/dev/null | tail -1)"

  # The metrics endpoint, in the one state it most has to answer in.
  #
  # Two of its gauges read the archive, and it used to run on the scrape's own
  # context with no bound: the driver waits out server selection, thirty seconds
  # by default, which is longer than Prometheus's ten second scrape timeout. The
  # scrape then fails and every metric on this router is lost, panics_total and
  # stale_reads_total with them. The sharpest part is which one it loses:
  # mirroring_lag_seconds renders +Inf for exactly this state, and reaching the
  # archive to find that out is what hung the scrape.
  #
  # This is the only place that can test it, for the same reason the insert
  # above is: the outage has to happen to a router that is already running.
  MT0=$(date +%s%N)
  MOUT=$(docker exec $NODE sh -c "curl -s --max-time 20 http://127.0.0.1:27114/metrics" 2>/dev/null)
  MT1=$(date +%s%N); MMS=$(( (MT1-MT0)/1000000 ))
  echo "  a metrics scrape during the outage took ${MMS}ms"
  chk "metrics still answer while the archive is down" "true" \
      "$(printf '%s' "$MOUT" | grep -q '^thermocline_panics_total ' && echo true || echo false)"
  chk "inside a Prometheus scrape timeout"            "true" \
      "$([ "${MMS:-99999}" -lt 9000 ] && echo true || echo false)"
  chk "and mirroring lag says +Inf rather than nothing" "true" \
      "$(printf '%s' "$MOUT" | grep -q '^thermocline_mirroring_lag_seconds +Inf' && echo true || echo false)"

  # What the grace must not cover, which is how the split between reads and
  # writes was found. A grace that covered everything let an insert through:
  # the archive's duplicate check is skipped when the fast tier claims all of
  # history, so a claim that has become false lets an _id the archive still
  # holds onto the fast tier, and the feeder then mirrors it over the archived
  # document. Measured before the split: ACCEPTED, and it landed.
  INS=$(docker exec $NODE mongosh --port 27104 -u dba -p secret --authenticationDatabase admin --quiet --eval "
    try { db.getSiblingDB('shop').$NS.insertOne({_id:'graceclobber',v:'x'}); print('ACCEPTED'); }
    catch(e) { print(e.message.indexOf('archive could not be reached') >= 0 ? 'REFUSED' : 'OTHER: '+e.message.substring(0,60)); }" 2>&1 | tail -1)
  chk "an insert on the grace router is still refused"  "REFUSED" "$INS"
  chk "and nothing landed on the fast tier"             "0"       "$(H "print(db.getSiblingDB('shop').$NS.countDocuments({_id:'graceclobber'}))")"
  # A namespace this router was never asked about has no claim to hold over, and
  # the grace deliberately does not invent one.
  chk "a namespace never asked about is refused"        "REFUSED" \
      "$(docker exec $NODE mongosh --port 27104 -u dba -p secret --authenticationDatabase admin --quiet --eval "
          try { print(db.getSiblingDB('shop').tqbound.countDocuments({})); } catch(e) { print('REFUSED'); }" 2>&1 | tail -1)"

  for n in $CLDNODES; do docker exec $n systemctl start mongod; done
  for _ in $(seq 1 60); do
    [ "$(C 'print(rs.status().members.filter(m=>m.stateStr=="PRIMARY").length)' 2>/dev/null)" = "1" ] && break
    sleep 2
  done
  chk "and the archive kept what it had"              "archived, precious"       "$(C "const d=db.getSiblingDB('shop').$NS.findOne({_id:'outage'}); print(d?d.v:'MISSING')")"
  sleep 3
  chk "the claim is read again once the archive answers" "$WANT" "$(COUNT 27104)"
  chk "and so is the one that would not hold it over"    "$WANT" "$(COUNT 27100)"
  H "db.getSiblingDB('shop').$NS.deleteMany({_id:'graceclobber'})" >/dev/null
  docker exec $NODE pkill -f 'config /root/cline_grace.yaml' 2>/dev/null
fi

echo "== every daemon at once under client load =="
docker exec -d $NODE cline evictor --config $CFG --every 3s --log-file /var/log/cline/chaos_ev.log
docker exec -d $NODE sh -c "for i in 1 2 3 4 5 6; do cline warmer --config $CFG --log-file /var/log/cline/chaos_wm.log >/dev/null 2>&1; sleep 3; done"
ERRS=$(R "
let err=0;
for (let i=0;i<60;i++){
  try{
    if (i%3==0) db.getSiblingDB('shop').$NS.insertOne({_id:2000+i, mix:1});
    else if (i%3==1) db.getSiblingDB('shop').$NS.countDocuments({});
    else { let n=0; const c=db.getSiblingDB('shop').$NS.find({}).batchSize(17); while(c.hasNext()&&n<80){c.next();n++} }
  }catch(e){ err++; }
  sleep(150);
}
print(err);")
docker exec $NODE pkill -f 'cline evictor' 2>/dev/null
chk "client errors while everything runs" "0" "$ERRS"
echo "== an index build with one fast tier member away =="
# mongod's default commit quorum for an index build is every voting member, so a
# member that cannot vote means the build never commits and createIndexes never
# returns. No error, no timeout, and everything else on the cluster keeps working,
# which is what makes it unreadable: on this project's own cluster a build sat
# nineteen minutes and looked exactly like a hung proxy.
#
# It matters here because the router builds the archive's indexes on the fast
# tier before it opens its listener, so what hangs is the start of the proxy.
#
# The fast tier's copy is seeded first, and that is the part this check turns on.
# A build on an empty collection skips the two-phase protocol and needs no votes
# at all, so the first version of this section passed against a binary that did
# not send a commit quorum: there was nothing to vote on. Measured separately on
# a three member 8.0.26 set with a thousand documents: 164ms with all members up,
# no answer in ninety seconds with one stopped, 117ms with the quorum set to
# majority.
XNS=tchaosidx
SN=$(nodeFor "$(H 'print(rs.status().members.filter(m=>m.stateStr=="SECONDARY")[0].name.split(":")[0])')")
if [ -z "$SN" ]; then echo "  SKIP  no fast tier secondary resolved"; else
  echo "  stopping fast tier secondary $SN"
  docker exec $NODE pkill -f 'cline [fr]outer|cline [f]eeder' 2>/dev/null; sleep 2
  H "db.getSiblingDB('shop').$XNS.drop()" >/dev/null
  C "db.getSiblingDB('shop').$XNS.drop()" >/dev/null
  C "const c=db.getSiblingDB('shop').$XNS; c.insertOne({_id:1,email:'a@x'});
     c.createIndex({email:1},{unique:true,name:'chaos_email'});" >/dev/null
  # Documents on the fast tier and no index on them, which is what the router
  # finds at startup and what makes the build a two-phase one. Distinct emails,
  # because the index it is about to build is unique.
  H "const c=db.getSiblingDB('shop').$XNS;
     c.insertMany(Array.from({length:2000},(_,i)=>({_id:1000+i,email:'u'+i+'@x'})));" >/dev/null
  chk "the fast tier holds documents and no index"    "1" \
      "$(H "print(db.getSiblingDB('shop').$XNS.getIndexes().length)")"
  docker exec $SN systemctl stop mongod; sleep 10
  # Bounded, or a regression hangs this suite instead of failing it. The router
  # has to be answering, and the index has to exist, inside the bound.
  docker exec -d $NODE cline router --config $CFG --log-file /var/log/cline/chaos_r.log
  UP=false
  for _ in $(seq 1 40); do
    docker exec $NODE mongosh --port 27100 -u dba -p secret --authenticationDatabase admin \
      --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && { UP=true; break; }
    sleep 2
  done
  chk "the router starts with a member away"      "true" "$UP"
  BUILT=false
  for _ in $(seq 1 20); do
    [ "$(H "print(db.getSiblingDB('shop').$XNS.getIndexes().some(i=>i.name=='chaos_email'))")" = "true" ] \
      && { BUILT=true; break; }
    sleep 2
  done
  chk "and the unique index is built on the fast tier" "true" "$BUILT"
  docker exec $SN systemctl start mongod; sleep 15
  H "db.getSiblingDB('shop').$XNS.drop()" >/dev/null
  C "db.getSiblingDB('shop').$XNS.drop()" >/dev/null
  docker exec $NODE pkill -f 'cline [fr]outer' 2>/dev/null; sleep 1
  feederUp; routerUp; sleep 5
fi

# Two ERROR lines here are the product working and are excluded by name.
#
# The feeder logs a reopen when the primary it was tailing goes away, which is
# the recovery. And the router logs the insert it refused while the archive was
# down, three checks above: that refusal is the whole point of that section, so
# counting it here reports a fault for behaviour this suite has just demanded.
# It did, on every run, until it was named.
# The third exclusion is the evictor refusing a namespace whose _id values are
# not ObjectIds, which it says at ERROR on purpose. This suite runs against the
# shared cluster, where other suites leave collections keyed by strings, so it is
# a fixture next door rather than anything chaos did. Every other ERROR and every
# panic still fails this check.
chk "no unexpected daemon error" "0" "$(docker exec $NODE sh -c 'cat /var/log/cline/chaos_*.log 2>/dev/null | grep -iE "level=ERROR|panic" | grep -vEc "change stream ended, reopening|refused an insert because the archive could not be checked|not evicting a namespace this rule cannot address" || true' 2>/dev/null | tail -1)"

docker exec $NODE pkill -f 'cline feeder|cline router' 2>/dev/null
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
