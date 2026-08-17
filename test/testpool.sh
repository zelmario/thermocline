#!/bin/bash
# Whether idle clients still cost a connection to the fast tier.
#
# One upstream connection per client was correct while authentication was
# proxied: the connection carried the client's identity and could not be used
# for anybody else. It is also the limitation that ends the story at a few
# thousand clients, because a replica set member has a finite number of
# connections and most application clients are idle most of the time.
#
# With authentication terminated the connection belongs to the router, so it can
# be handed to whoever has a command in flight. This measures that rather than
# assuming it: hold a number of client connections open doing nothing, and count
# the connections the primary gains while they sit there.
#
# The same measurement with terminate_auth off is what makes it mean something,
# and it is not a small difference. Eight idle clients cost zero extra
# connections pooled and thirty two proxied, the second number being larger than
# eight because a client opens several connections of its own and every one of
# them became an upstream.
BASE=/root/cline.yaml
POOLCFG=/root/cline_pool.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
CLIENTS=8
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }

cleanup() {
  pkill -9 -f 'cline router' 2>/dev/null
  pkill -9 -f 'mongosh --port 27100' 2>/dev/null
}
trap cleanup EXIT

stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startRouter() { # config
  cline router --config "$1" --log-file /var/log/cline/pool_r.log &
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on 27100"; exit 2
}

# Every connection the primary currently holds.
#
# Counted in total rather than by name, and measured as a delta around holding
# the clients. Counting by appName would only see the pooled router, since a
# proxied connection carries the client's own handshake, and a measurement that
# can only see one of the two things being compared is not a comparison.
conns() { H 'print(db.serverStatus().connections.current)'; }

# Hold CLIENTS connections open, each doing nothing at all.
holdClients() {
  for _ in $(seq 1 $CLIENTS); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'sleep(20000)' >/dev/null 2>&1 &
  done
  sleep 6
}
dropClients() {
  pkill -9 -f 'mongosh --port 27100' 2>/dev/null
  sleep 2
}

echo "== setup =="
stopDaemons
sed 's/^  listen:/  terminate_auth: true\n  listen:/' $BASE > $POOLCFG
chk "the pooled config asks for terminated authentication" "1" "$(grep -c 'terminate_auth: true' $POOLCFG)"

echo "== with authentication terminated, idle clients share =="
startRouter $POOLCFG
BEFORE=$(conns)
holdClients
POOLED=$(( $(conns) - BEFORE ))
dropClients
stopDaemons
echo "      $CLIENTS idle clients cost $POOLED extra connections to the primary"
# Well under one each is the claim. Not zero exactly: a client that happens to be
# mid-command holds one, and the pool keeps a few spares.
chk "idle clients do not each hold a connection"       "true" \
    "$(awk -v n="$POOLED" -v c="$CLIENTS" 'BEGIN{print (n+0 < c+0) ? "true" : "false"}')"

echo "== with authentication proxied, they do not =="
# The control. Without this the numbers above could mean the clients never
# connected, or that the counting query counts nothing.
startRouter $BASE
BEFORE=$(conns)
holdClients
PROXIED=$(( $(conns) - BEFORE ))
dropClients
stopDaemons
echo "      $CLIENTS idle clients cost $PROXIED extra connections to the primary"
# One each, which is the limitation this change exists to lift. Checked as most
# of them rather than exactly all, because the count moves for reasons that have
# nothing to do with this.
chk "proxied clients each hold one"                    "true" \
    "$(awk -v p="$PROXIED" -v c="$CLIENTS" 'BEGIN{print (p+0 >= c+0-1) ? "true" : "false"}')"
chk "and pooling is measurably cheaper"                "true" \
    "$(awk -v p="$PROXIED" -v n="$POOLED" 'BEGIN{print (n+0 < p+0) ? "true" : "false"}')"

echo "== and a transaction keeps the connection it started on =="
startRouter $POOLCFG
chk "a transaction commits through the pool"           "committed" \
    "$(mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval '
        const s=db.getMongo().startSession();
        s.withTransaction(()=>{ const c=s.getDatabase("shop").tpool;
          c.insertOne({_id:new ObjectId(),v:1}); c.find({v:1}).itcount(); });
        s.endSession(); print("committed")' 2>&1 | tail -1)"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
