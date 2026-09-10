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
export DB WDIR
P1=$((PORT+1)); P2=$((PORT+2)); P3=$((PORT+3)); PDASH=$((PORT+10)); PALT=$((PORT+17))
BASE=$WDIR/cline.yaml
POOLCFG=$WDIR/cline_pool.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
CLIENTS=8
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }

cleanup() {
  pkill -9 -f "cline router --config $WDIR/" 2>/dev/null
  pkill -9 -f "mongosh --port $PORT" 2>/dev/null
}
trap cleanup EXIT

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startRouter() { # config
  cline router --config "$1" --log-file $LOGDIR/pool_r.log &
  # conns() counts this pid's sockets, so it has to be captured here. Backgrounded
  # directly rather than through a subshell, so $! is the router itself.
  RPID=$!
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}

# Every connection this worker's router holds to the fast tier.
#
# This was the primary's own connections.current, which counts every client it
# has, and it is read as a delta around holding eight clients open. Under a
# parallel sweep the other three workers' routers, feeders and mongosh
# invocations land in that delta: measured, the pooled arm read 0 and 8 on
# different runs and the proxied arm 50, 32 and 17, so this suite failed most
# sweeps and passed on its own. An absolute threshold fails on the first kind
# of noise and subtracting a separately measured drift fails on the second.
#
# The router's own sockets are the thing the claim is about, and no other
# worker's process owns any of them, so there is no noise to subtract. Read by
# pid, which ss gives, rather than by appName: a proxied connection carries the
# client's own handshake, so a measurement by name can only see the pooled arm,
# and a comparison that can only see one of its arms is not a comparison.
#
# Filtered to the fast tier's addresses, because this process also holds
# sockets to the archive and one to each of the clients it is being measured
# with, and counting those would make every arm look like it gained eight.
HOTADDRS=$(echo "$HOT" | sed -e 's|^mongodb://[^@]*@||' -e 's|/?.*$||' | tr ',' ' ')
conns() {
  [ -n "$RPID" ] || { echo 0; return; }
  ss -tnp 2>/dev/null | awk -v pid="pid=$RPID," -v addrs="$HOTADDRS" '
    BEGIN { n = split(addrs, a, " ") }
    /ESTAB/ && index($0, pid) {
      for (i = 1; i <= n; i++) if ($5 == a[i]) { c++; break }
    }
    END { print c+0 }'
}

# Hold CLIENTS connections open, each doing nothing at all.
holdClients() {
  for _ in $(seq 1 $CLIENTS); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'sleep(20000)' >/dev/null 2>&1 &
  done
  sleep 6
}
dropClients() {
  pkill -9 -f "mongosh --port $PORT" 2>/dev/null
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
# Judged below, against the proxied arm, because that is the claim: the number
# that matters is the ratio between the two arms, not either reading on its own.
# Both are counted the same way, on the same router, at the same two moments.
# See conns() for why it is counted by pid.

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
# The claim, stated as the comparison it is: pooled costs a fraction of proxied,
# not merely one connection less.
#
# The parenthesis is the point. This read `n+0 * 2 < p+0`, and awk binds `*`
# tighter than `+`, so it was `n + (0*2) < p`: the check asked only that pooled
# was lower by anything at all, while its own name claimed a factor of two. A
# check whose arithmetic is weaker than its name is the one kind of failure a
# green suite cannot show you.
chk "and pooled clients cost a fraction of that"       "true" \
    "$(awk -v p="$PROXIED" -v n="$POOLED" 'BEGIN{print ((n+0) * 2 < (p+0)) ? "true" : "false"}')"

echo "== and a transaction keeps the connection it started on =="
startRouter $POOLCFG
chk "a transaction commits through the pool"           "committed" \
    "$(mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval '
        const s=db.getMongo().startSession();
        s.withTransaction(()=>{ const c=s.getDatabase(process.env.TC_DB).tpool;
          c.insertOne({_id:new ObjectId(),v:1}); c.find({v:1}).itcount(); });
        s.endSession(); print("committed")' 2>&1 | tail -1)"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
