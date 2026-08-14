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
NODE=thermocline-zelmar-node0
CFG=/root/cline.yaml
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
# The feeder logs a reopen at ERROR when the primary it was tailing goes away.
# That is the recovery working, not a fault, so it is excluded by name.
chk "no unexpected daemon error" "0" "$(docker exec $NODE sh -c 'cat /var/log/cline/chaos_*.log 2>/dev/null | grep -iE "level=ERROR|panic" | grep -vc "change stream ended, reopening" || true' 2>/dev/null | tail -1)"

docker exec $NODE pkill -f 'cline feeder|cline router' 2>/dev/null
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
