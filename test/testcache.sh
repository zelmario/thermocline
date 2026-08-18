#!/bin/bash
# The learned cache, and the advice it gives before anybody turns it on.
#
# The router records three things per read: the shape of the query, a hash of
# its parameters, and a hash of what came back. From those it works out which
# queries repeat, which of those repeat with the same parameters, and how long
# the same answer kept coming back. The suggestion is ten per cent inside that.
#
# What this suite is really checking is that a cache in a system whose promise is
# "no application changes" cannot break the promise:
#
#   - a cached answer is the answer the tiers would have given
#   - a write through the router drops what was learned, so the writer never
#     reads its own stale answer
#   - the measuring half runs whether or not the caching half does
CFG=/root/cline.yaml
CCFG=/root/cline_cache.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
NS=tcache
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-50s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-50s got=%.80s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

cleanup() { pkill -9 -f 'cline router' 2>/dev/null; pkill -9 -f 'cline feeder' 2>/dev/null; }
trap cleanup EXIT
stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startRouter() { # config
  cline router --config "$1" --log-file /var/log/cline/cache_r.log &
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on 27100"; exit 2
}

echo "== setup =="
stopDaemons
# The cache is off in the shared config, so this one is derived rather than
# edited: every other suite runs without it and must keep doing so.
#
# Unless the whole sweep is being run with it on, which is a thing worth doing
# and would otherwise leave two cache blocks in one file. Then the base config
# is already what this suite wants.
if grep -q '^  cache:' $CFG; then
  sed 's/^\( *\)enabled: false/\1enabled: true/' $CFG > $CCFG
else
  sed 's|^  completeness:|  cache:\n    enabled: true\n    max_queries: 200\n    max_ttl: 60s\n  completeness:|' $CFG > $CCFG
fi
chk "the derived config turns the cache on" "1" "$(grep -c 'enabled: true' $CCFG)"
H "db.getSiblingDB('shop').$NS.drop(); db.getSiblingDB('shop').$NS.insertMany(
   Array.from({length:60},(_,i)=>({_id:i, country:['UY','AR','BR'][i%3], n:i})))" >/dev/null
C "db.getSiblingDB('shop').$NS.drop()" >/dev/null
C 'db.getSiblingDB("_thermocline").cacheadvice.deleteMany({})' >/dev/null
cline feeder --config $CCFG --log-file /var/log/cline/cache_f.log &
sleep 3
startRouter $CCFG

echo "== the answer is the answer, cached or not =="
FIRST=$(R "print(db.getSiblingDB('shop').$NS.countDocuments({country:'UY'}))")
SECOND=$(R "print(db.getSiblingDB('shop').$NS.countDocuments({country:'UY'}))")
THIRD=$(R "print(db.getSiblingDB('shop').$NS.countDocuments({country:'UY'}))")
DIRECT=$(H "print(db.getSiblingDB('shop').$NS.countDocuments({country:'UY'}))")
chk "the first answer is the fast tier's"          "$DIRECT" "$FIRST"
chk "and so is the third, whatever served it"      "$DIRECT" "$THIRD"
chk "and the second"                               "$DIRECT" "$SECOND"

echo "== a write drops what was learned =="
# The reader that wrote must never be served its own stale answer, and this is
# what a TTL alone cannot promise. The router sees the write, so it does not
# have to wait for a timer.
BEFORE=$(R "print(db.getSiblingDB('shop').$NS.countDocuments({country:'UY'}))")
AFTER=$(R "db.getSiblingDB('shop').$NS.insertOne({_id:5001,country:'UY',n:5001});
           print(db.getSiblingDB('shop').$NS.countDocuments({country:'UY'}))")
chk "the count rises the moment the write lands"   "$((BEFORE+1))" "$AFTER"
R "db.getSiblingDB('shop').$NS.deleteOne({_id:5001})" >/dev/null
BACK=$(R "print(db.getSiblingDB('shop').$NS.countDocuments({country:'UY'}))")
chk "and falls again on the delete"                "$BEFORE" "$BACK"

echo "== a cached answer still expires =="
# The reply goes back through the same funnel every answer does, and that funnel
# learns from what it sends. Left alone, every hit was recorded as a fresh
# observation of the same value, so the entry's age was reset and a query asked
# steadily was never re-fetched at all: measured with a two second ceiling and
# thirteen seconds of reads, the router asked the fast tier four times and served
# forty one from memory. The TTL was not bounding anything.
#
# What must hold is that a steady stream of reads keeps going back to the tiers.
TCFG=/root/cline_cachettl.yaml
# Whatever the ceiling was, two seconds for this one: the check is that a steady
# read loop keeps going back to the tiers as it expires.
sed 's/^\( *\)max_ttl: .*/\1max_ttl: 2s/' $CCFG > $TCFG
stopDaemons
startRouter $TCFG
STEADY=$(mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const c = db.getSiblingDB('shop').$NS;
  for (let i = 0; i < 30; i++) { c.countDocuments({country:'UY'}); sleep(300); }
  print('done');" 2>&1 | tail -1)
chk "the steady read loop finished"                "done" "$STEADY"
stopDaemons
sleep 2
ASKED=$(cline cache --config $TCFG --top 1 2>&1 | grep -oE '[0-9]+ misses' | grep -oE '^[0-9]+')
# Nine seconds of reads with a two second ceiling has to go back several times.
chk "it went back to the tiers as the ttl expired" "true"     "$(awk -v n="${ASKED:-0}" 'BEGIN{print (n+0 >= 3) ? "true" : "false"}')"
startRouter $CCFG

echo "== a write by somebody else drops it too =="
# A cache that only knows about its own writes has a staleness window for
# everybody else: another application, a migration script, a person in a shell.
# The fast tier publishes every change it makes and this system is built on
# reading that, so the router follows the same stream and drops the answers a
# write made stale. The window becomes the change stream's latency rather than
# the TTL.
#
# The claim makes this a fast tier read, so the write is visible in the truth as
# well as in the cache.
cline watermark --config $CCFG --ns shop.$NS --complete-all >/dev/null 2>&1
sleep 2
OTHER=$(mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const c = db.getSiblingDB('shop').$NS;
  for (let i = 0; i < 5; i++) { c.countDocuments({country:'UY'}); sleep(200); }
  print(c.countDocuments({country:'UY'}));" 2>&1 | tail -1)
H "db.getSiblingDB('shop').$NS.insertOne({_id:6001,country:'UY',n:6001})" >/dev/null
sleep 2
AFTEROTHER=$(R "print(db.getSiblingDB('shop').$NS.countDocuments({country:'UY'}))")
chk "a write straight to the fast tier is seen"    "$((OTHER+1))" "$AFTEROTHER"
H "db.getSiblingDB('shop').$NS.deleteOne({_id:6001})" >/dev/null
sleep 2
cline watermark --config $CCFG --ns shop.$NS --clear >/dev/null 2>&1

echo "== who a cached answer may be given to =="
# The key carries who is asking, or one client would be served another's
# documents. With authentication terminated that is the verified user; with it
# proxied the router does not know who the client is, so the connection is the
# identity and the cache helps a client that repeats itself rather than clients
# that resemble each other.
#
# Every check above used a fresh mongosh, which is a fresh connection, so none
# of them could hit. This one repeats on a single connection, which is what an
# application does.
HITS=$(mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const c = db.getSiblingDB('shop').$NS;
  let answers = {};
  for (let i = 0; i < 8; i++) { answers[c.countDocuments({country:'BR'})] = 1; sleep(150); }
  print(Object.keys(answers).join(','));" 2>&1 | tail -1)
chk "one connection asking eight times agrees with itself" "20" "$HITS"
# The hits are checked below, after the router has stopped: it publishes what it
# learned once a minute and once on the way out, and this suite is much shorter
# than a minute.

echo "== what it learned, and what it suggests =="
# Enough repeats for a stability to be measured at all.
for _ in $(seq 1 6); do
  R "db.getSiblingDB('shop').$NS.countDocuments({country:'AR'})" >/dev/null
  sleep 1
done
stopDaemons
sleep 2
ADVICE=$(cline cache --config $CCFG --ns shop.$NS --top 5 2>&1)
echo "$ADVICE" | sed 's/^/  | /' | head -8
chk "the report names the collection"              "true" \
    "$(echo "$ADVICE" | grep -q "shop.$NS" && echo true || echo false)"
chk "and suggests a ttl for it"                    "true" \
    "$(echo "$ADVICE" | grep -qE '^  [0-9]+(\.[0-9]+)?(ms|s|m)' && echo true || echo false)"
chk "and reports hits"                             "true" \
    "$(echo "$ADVICE" | grep -q 'hits' && echo true || echo false)"
# The repeats on one connection are the only reads here that could hit, and they
# are what proves the cache serves rather than merely measures.
chk "some reads were served from the cache"        "true" \
    "$(echo "$ADVICE" | grep -oE '[0-9]+ hits' | head -1 | grep -qE '^[1-9][0-9]* hits' && echo true || echo false)"

echo "== with the cache off, it still measures =="
# The advice is the reason to turn caching on, so it cannot need caching to be
# on already.
#
# The config is derived with caching explicitly off rather than by assuming the
# shared one has it off, because the whole sweep can be run with it on and then
# this section was testing the opposite of what it says.
OFFCFG=/root/cline_cacheoff.yaml
if grep -q '^  cache:' $CFG; then
  sed 's/^\( *\)enabled: true/\1enabled: false/' $CFG > $OFFCFG
else
  cp $CFG $OFFCFG
fi
C 'db.getSiblingDB("_thermocline").cacheadvice.deleteMany({})' >/dev/null
startRouter $OFFCFG
for _ in $(seq 1 4); do
  R "db.getSiblingDB('shop').$NS.countDocuments({country:'BR'})" >/dev/null
  sleep 1
done
stopDaemons
sleep 2
OFF=$(cline cache --config $OFFCFG --top 5 2>&1)
chk "it publishes with the cache off"              "true" \
    "$(echo "$OFF" | grep -q 'measuring only' && echo true || echo false)"
chk "and still has something to suggest"           "true" \
    "$(echo "$OFF" | grep -q "shop.$NS" && echo true || echo false)"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
