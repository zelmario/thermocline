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
CCFG=$WDIR/cline_cache.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
NS=tcache
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-50s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-50s got=%.80s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

cleanup() { pkill -9 -f "cline router --config $WDIR/" 2>/dev/null; pkill -9 -f "cline feeder --config $WDIR/" 2>/dev/null; }
trap cleanup EXIT
stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startRouter() { # config
  cline router --config "$1" --log-file $LOGDIR/cache_r.log &
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}

# waitMirrored COLLECTION COUNT blocks until the archive holds COUNT live
# documents. A fixed sleep here is the largest single source of false failures
# in this harness: reads that fall through to an archive still filling up return
# a short answer, which reads exactly like the cache serving a stale one.
waitMirrored() {
  local n=""
  for _ in $(seq 1 60); do
    n=$(C "print(db.getSiblingDB(process.env.TC_DB).$1.countDocuments({_tcDeletedAt:{\$exists:false}}))")
    [ "$n" = "$2" ] && return 0
    sleep 0.5
  done
  echo "  WARN  $DB.$1 never reached $2 mirrored documents, last saw ${n:-none}"
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
# The feeder starts before the seed, not after it, and the suite then waits for
# the archive rather than sleeping at it.
#
# Written the other way round this suite only passed by accident. A change
# stream opened after the inserts never sees them, so the archive stayed empty,
# every read fell through to it and returned zero. What hid that was the
# checkpoint an *earlier* suite on the same worker had left: the feeder resumed
# from before the seed and picked it up on the way past. So this passed whenever
# something else had run first and failed whenever it went first, which in a
# queue that hands suites out as workers free up is a coin toss.
H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
C 'db.getSiblingDB(process.env.TC_META).cacheadvice.deleteMany({})' >/dev/null
cline feeder --config $CCFG --log-file $LOGDIR/cache_f.log &
for _ in $(seq 1 60); do
  [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && break
  sleep 0.5
done
H "db.getSiblingDB(process.env.TC_DB).$NS.insertMany(
   Array.from({length:60},(_,i)=>({_id:i, country:['UY','AR','BR'][i%3], n:i})))" >/dev/null
waitMirrored $NS 60
startRouter $CCFG

echo "== the answer is the answer, cached or not =="
FIRST=$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({country:'UY'}))")
SECOND=$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({country:'UY'}))")
THIRD=$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({country:'UY'}))")
DIRECT=$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({country:'UY'}))")
chk "the first answer is the fast tier's"          "$DIRECT" "$FIRST"
chk "and so is the third, whatever served it"      "$DIRECT" "$THIRD"
chk "and the second"                               "$DIRECT" "$SECOND"

echo "== a write drops what was learned =="
# The reader that wrote must never be served its own stale answer, and this is
# what a TTL alone cannot promise. The router sees the write, so it does not
# have to wait for a timer.
BEFORE=$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({country:'UY'}))")
AFTER=$(R "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:5001,country:'UY',n:5001});
           print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({country:'UY'}))")
chk "the count rises the moment the write lands"   "$((BEFORE+1))" "$AFTER"
R "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:5001})" >/dev/null
BACK=$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({country:'UY'}))")
chk "and falls again on the delete"                "$BEFORE" "$BACK"

echo "== a write drops what a view over the same collection learned =="
# The cache hangs an answer from the namespace the command named, and a read of
# a view names the view. Everything that invalidates arrives under the
# collection: the router's own write path names it, and so does the fast tier's
# change stream. The two never meet, so a cached read of a view outlived every
# write to the collection it reads, until its learned TTL ran out.
#
# That is the one thing this cache's design says it does not do. The comment at
# the top of querycache.go is that a write followed by a read of the same
# collection is correct immediately rather than a TTL later, and what the router
# cannot see is another writer.
R "db.getSiblingDB(process.env.TC_DB).vcache.drop()" >/dev/null 2>&1
R "db.getSiblingDB(process.env.TC_DB).createView('vcache','$NS',[])" >/dev/null
# Read it several times first. An entry is not served until it has been observed
# often enough to have a learned TTL, and with only two reads the write below was
# answered from the tiers and the check passed against a binary that never
# dropped anything: measured, the first check passed and only the second failed.
for _ in 1 2 3 4; do
  R "print(db.getSiblingDB(process.env.TC_DB).vcache.countDocuments({country:'UY'}))" >/dev/null
done
VBEFORE=$(R "print(db.getSiblingDB(process.env.TC_DB).vcache.countDocuments({country:'UY'}))")
VAFTER=$(R "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:5002,country:'UY',n:5002});
            print(db.getSiblingDB(process.env.TC_DB).vcache.countDocuments({country:'UY'}))")
chk "the view's count rises the moment the write lands" "$((VBEFORE+1))" "$VAFTER"
R "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:5002})" >/dev/null
VBACK=$(R "print(db.getSiblingDB(process.env.TC_DB).vcache.countDocuments({country:'UY'}))")
chk "  and falls again on the delete"                  "$VBEFORE" "$VBACK"
R "db.getSiblingDB(process.env.TC_DB).vcache.drop()" >/dev/null 2>&1

echo "== a cached answer still expires =="
# The reply goes back through the same funnel every answer does, and that funnel
# learns from what it sends. Left alone, every hit was recorded as a fresh
# observation of the same value, so the entry's age was reset and a query asked
# steadily was never re-fetched at all: measured with a two second ceiling and
# thirteen seconds of reads, the router asked the fast tier four times and served
# forty one from memory. The TTL was not bounding anything.
#
# What must hold is that a steady stream of reads keeps going back to the tiers.
TCFG=$WDIR/cline_cachettl.yaml
# Whatever the ceiling was, two seconds for this one: the check is that a steady
# read loop keeps going back to the tiers as it expires.
sed 's/^\( *\)max_ttl: .*/\1max_ttl: 2s/' $CCFG > $TCFG
stopDaemons
startRouter $TCFG
STEADY=$(mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const c = db.getSiblingDB(process.env.TC_DB).$NS;
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
cline watermark --config $CCFG --ns $DB.$NS --complete-all >/dev/null 2>&1
sleep 2
OTHER=$(mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const c = db.getSiblingDB(process.env.TC_DB).$NS;
  for (let i = 0; i < 5; i++) { c.countDocuments({country:'UY'}); sleep(200); }
  print(c.countDocuments({country:'UY'}));" 2>&1 | tail -1)
H "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:6001,country:'UY',n:6001})" >/dev/null
sleep 2
AFTEROTHER=$(R "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({country:'UY'}))")
chk "a write straight to the fast tier is seen"    "$((OTHER+1))" "$AFTEROTHER"
H "db.getSiblingDB(process.env.TC_DB).$NS.deleteOne({_id:6001})" >/dev/null
sleep 2
cline watermark --config $CCFG --ns $DB.$NS --clear >/dev/null 2>&1

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
HITS=$(mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "
  const c = db.getSiblingDB(process.env.TC_DB).$NS;
  let answers = {};
  for (let i = 0; i < 8; i++) { answers[c.countDocuments({country:'BR'})] = 1; sleep(150); }
  print(Object.keys(answers).join(','));" 2>&1 | tail -1)
chk "one connection asking eight times agrees with itself" "20" "$HITS"
# The hits are checked below, after the router has stopped: it publishes what it
# learned once a minute and once on the way out, and this suite is much shorter
# than a minute.

echo "== a read that asked for a secondary is never cached =="
# cacheableRead refuses any read whose $readPreference is not primary, and the
# rule is load-bearing rather than tidy. With terminate_auth on, such a read is
# sent to a secondary on purpose, so the answer may come from a member behind
# the primary. The cache key deliberately leaves $readPreference out -- two
# clients asking the same question are asking the same question -- so an answer
# held from a secondary would be handed to the next reader who asked for the
# primary, staler than they asked for, which is the one direction a cache must
# not move a read in.
#
# Nothing checked it. The branch could be deleted and every suite would still
# pass, which is how a rule that costs nothing to keep gets dropped as dead
# weight.
#
# Observed through the shape report rather than a hit counter, because the
# counters are per router and every mongosh here makes its own connection and
# its own reads: counting hits globally attributed other queries' hits to this
# one. A shape the cache never admitted is never recorded at all, so its
# absence from the report is the whole answer. Its own field name, so it cannot
# collapse into the shape the section below builds.
for _ in $(seq 1 4); do
  R "db.getSiblingDB(process.env.TC_DB).$NS.find({secpref:'x'}).readPref('secondary').itcount()" >/dev/null
done

echo "== what it learned, and what it suggests =="
# Enough repeats for a stability to be measured at all.
for _ in $(seq 1 6); do
  R "db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({country:'AR'})" >/dev/null
  sleep 1
done
stopDaemons
# No sleep. stopDaemons returns when the process is gone, and a router that has
# exited has already published its last report, because Run waits for it. That
# is the property under test, and it is exactly what a sleep here would hide:
# the report used to be published by a fire-and-forget goroutine racing the
# process exit, so it survived on an idle machine and was lost under load. This
# suite passed for that reason and failed inside a parallel sweep.
ADVICE=$(cline cache --config $CCFG --ns $DB.$NS --top 5 2>&1)
echo "$ADVICE" | sed 's/^/  | /' | head -8
chk "the report names the collection"              "true" \
    "$(echo "$ADVICE" | grep -q "$DB.$NS" && echo true || echo false)"
chk "and suggests a ttl for it"                    "true" \
    "$(echo "$ADVICE" | grep -qE '^  [0-9]+(\.[0-9]+)?(ms|s|m)' && echo true || echo false)"
chk "and reports hits"                             "true" \
    "$(echo "$ADVICE" | grep -q 'hits' && echo true || echo false)"
# The repeats on one connection are the only reads here that could hit, and they
# are what proves the cache serves rather than merely measures.
chk "some reads were served from the cache"        "true" \
    "$(echo "$ADVICE" | grep -oE '[0-9]+ hits' | head -1 | grep -qE '^[1-9][0-9]* hits' && echo true || echo false)"
# And the secondary-preference reads above are nowhere in it. A shape the cache
# refused to admit is never recorded, so the report naming it would mean the
# guard had gone. The control is the line above: this report does name shapes
# and does count hits, so an absence here is an absence rather than an empty
# report.
chk "a secondary read never became a shape"        "false" \
    "$(cline cache --config $CCFG --ns $DB.$NS --top 20 2>&1 | grep -q 'secpref' && echo true || echo false)"

echo "== with the cache off, it still measures =="
# The advice is the reason to turn caching on, so it cannot need caching to be
# on already.
#
# The config is derived with caching explicitly off rather than by assuming the
# shared one has it off, because the whole sweep can be run with it on and then
# this section was testing the opposite of what it says.
OFFCFG=$WDIR/cline_cacheoff.yaml
if grep -q '^  cache:' $CFG; then
  sed 's/^\( *\)enabled: true/\1enabled: false/' $CFG > $OFFCFG
else
  cp $CFG $OFFCFG
fi
C 'db.getSiblingDB(process.env.TC_META).cacheadvice.deleteMany({})' >/dev/null
startRouter $OFFCFG
for _ in $(seq 1 4); do
  R "db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({country:'BR'})" >/dev/null
  sleep 1
done
stopDaemons
OFF=$(cline cache --config $OFFCFG --top 5 2>&1)
chk "it publishes with the cache off"              "true" \
    "$(echo "$OFF" | grep -q 'measuring only' && echo true || echo false)"
chk "and still has something to suggest"           "true" \
    "$(echo "$OFF" | grep -q "$DB.$NS" && echo true || echo false)"

echo "== the cache across an adoption =="
# Nothing had ever run the cache in the shape every deployment starts in: the
# existing cluster is the archive holding every document, the fast tier is new
# and empty, and the tier that answers changes underneath the application as the
# fast tier fills. A cached answer that outlives one of those changes is the
# failure this looks for, so the same question is asked at every stage and
# compared with the archive's own answer, which is the truth because the archive
# holds everything.
ANS=tcadopt
stopDaemons
H "db.getSiblingDB(process.env.TC_DB).$ANS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$ANS.drop()" >/dev/null
C "const c=db.getSiblingDB(process.env.TC_DB).$ANS; const now=Date.now(); const d=[];
   // Inside the 40m warm window, so the warm below really does fill the fast
   // tier and the stage that says so is a fact.
   for (let i=0;i<60;i++) d.push({_id:new ObjectId(), grp:'g'+(i%4), state:(i%3===0?'done':'open'), at:new Date(now-i*20000)});
   c.insertMany(d);" >/dev/null
chk "the archive holds them, the fast tier does not" "60/0" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$ANS.countDocuments({}))")/$(H "print(db.getSiblingDB(process.env.TC_DB).$ANS.countDocuments({}))")"
cline feeder --config $CCFG --log-file $LOGDIR/cache_f2.log &
for _ in $(seq 1 60); do
  [ "$(C 'print(!!db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}))')" = "true" ] && break
  sleep 0.5
done
startRouter $CCFG

asked() { R "print(db.getSiblingDB(process.env.TC_DB).$ANS.countDocuments({grp:'g1',state:'open'}))"; }
# Asked twice, so the second answer is the one a cache would give if it had one.
#
# Compared with a count this suite knows by construction, not with the archive.
# The archive is behind by the mirroring lag, so immediately after a write it
# legitimately disagrees with the router and reading it as the truth fails the
# router for being right. That is what the first version of this did.
stage() { asked >/dev/null; chk "$1" "$2" "$(asked)"; }

OPEN=$(C "print(db.getSiblingDB(process.env.TC_DB).$ANS.countDocuments({grp:'g1',state:'open',_tcDeletedAt:{\$exists:false}}))")
stage "day one, the archive answers" "$OPEN"
cline warmer --config $CCFG >/dev/null 2>&1
# Say that the tier really did flip, so the label above is a fact rather than an
# intention: a stage that never changed which tier answers proves nothing about
# a cached answer surviving the change.
chk "the warm put them on the fast tier"            "60" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$ANS.countDocuments({}))")"
stage "after the warm, the fast tier answers" "$OPEN"
R "db.getSiblingDB(process.env.TC_DB).$ANS.insertOne({_id:new ObjectId(), grp:'g1', state:'open', at:new Date()})" >/dev/null
stage "immediately after a write" "$((OPEN + 1))"
R "db.getSiblingDB(process.env.TC_DB).$ANS.updateOne({grp:'g1',state:'open'},{\$set:{state:'done'}})" >/dev/null
stage "immediately after an update" "$OPEN"
H "db.getSiblingDB(process.env.TC_DB).$ANS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$ANS.drop()" >/dev/null
stopDaemons

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
