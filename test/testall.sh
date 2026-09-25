#!/bin/bash
# Exercise every cline subcommand, flag and error path.
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
HOTU=${HOTU:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLDU=${COLDU:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
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

# chkrb compares a value rather than running a command, which is what the
# sections below need where ok() wants an exit code and an output pattern.
chkrb() { # chkrb NAME WANT GOT
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %s\n' "$1"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %s got=%s want=%s\n' "$1" "$3" "$2"; fi
}

echo "== meta =="
ok "no subcommand exits 1 with usage"   1 "Usage:"            -- cline
ok "help"                               0 "Usage:"            -- cline help
ok "-h"                                 0 "Usage:"            -- cline -h
ok "--help"                             0 "Usage:"            -- cline --help
ok "version"                            0 "cline"             -- cline version
ok "unknown subcommand exits 1"         1 "unknown subcommand" -- cline nonsense

echo "== config errors =="
ok "missing config file"                1 "read config"       -- cline warmer --config $WDIR/nope.yaml
printf 'hot:\n  uri: "["\n  bad yaml here\n' > $WDIR/bad.yaml
ok "unparseable config"                 1 "parse config"      -- cline warmer --config $WDIR/bad.yaml
printf 'cold:\n  uri: "mongodb://x"\n' > $WDIR/nohot.yaml
ok "missing hot.uri"                    1 "hot.uri is required" -- cline warmer --config $WDIR/nohot.yaml
sed 's/completeness: archive/completeness: banana/' $CFG > $WDIR/badcomp.yaml
ok "invalid completeness value"         1 "must be archive, strict or permissive" -- cline router --config $WDIR/badcomp.yaml
# Delete the whole rules block of the evictor, not just its first entry. The
# old range stopped at the first older_than, so a config with two rules left the
# second one orphaned under max_feeder_lag and this checked a parse error rather
# than an empty rules list.
sed '/^evictor:/,/^warmer:/{/^  rules:/,/^  backoff:/{/^  backoff:/!d}}' $CFG > $WDIR/norules.yaml
ok "evictor with no rules"              1 "rules is empty"    -- cline evictor --config $WDIR/norules.yaml --once

echo "== watermark =="
# On a fresh archive there is nothing to list, so declare the claim this section
# reads back. Without it "list all" fails on a clean environment and passes only
# because some earlier run happened to leave a claim behind.
#
# On tc.probe rather than a tiered collection, because --complete-all checks the
# claim against both tiers now and refuses one the counts contradict. Claiming
# $DB.orders here therefore fails whenever an earlier suite left documents in
# the archive and none on the fast tier, which is a state this section neither
# creates nor cares about: it read "the fast tier holds 0 of everything in
# shop_w0.orders and the archive holds 2" in a sweep on 2026-08-31. No rule ages
# tc.probe, so its claim is recorded unchecked, which is all a listing needs.
cline watermark --config $CFG --ns tc.probe --complete-all >/dev/null
ok "list all"                           0 "tc.probe"         -- cline watermark --config $CFG
ok "single namespace"                   0 "$DB.orders"       -- cline watermark --config $CFG --ns $DB.orders
ok "unknown namespace claims nothing"   0 "claims nothing"    -- cline watermark --config $CFG --ns no.such
ok "--clear without --ns rejected"      1 "ns is required"    -- cline watermark --config $CFG --clear
ok "two actions rejected"               1 "give one"          -- cline watermark --config $CFG --ns $DB.orders --clear --complete-all
ok "bad --complete-from rejected"       1 "RFC3339"           -- cline watermark --config $CFG --ns $DB.orders --complete-from notadate
# Counting both tiers is bounded, and past the bound a claim is recorded
# unchecked rather than refused. The bound is a flag now, because on the
# collection big enough for a minute to run out, "recorded unchecked" is the one
# outcome nobody asked for.
ok "--count-for is accepted"            0 "all of history"    -- cline watermark --config $CFG --ns tc.probe --complete-all --count-for 5s
ok "a nonsense duration is refused"     2 ""                  -- cline watermark --config $CFG --ns tc.probe --complete-all --count-for banana
ok "--complete-from sets"               0 "newer than"        -- cline watermark --config $CFG --ns tc.probe --complete-from 2026-01-01T00:00:00Z
# A window that selects nothing is not the tiers agreeing, and it used to print
# as though it were: checkClaim says nothing when the counts bear the claim out,
# and nothing was also what it said when neither tier held a document in the
# window. The operator reads silence as "checked".
#
# tintid is the plain way to reach it. Its rule carries no date_field, so the
# age comes from the _id timestamp and the window is {_id: {$gte: ObjectId(T)}};
# the documents carry integer _ids, MongoDB brackets the predicate by type, and
# an integer is never in that range however many there are. Both tiers count
# nought while the collection is not empty at all.
mongosh "$HOTU"  --quiet --eval "db.getSiblingDB(process.env.TC_DB).tintid.drop();
   db.getSiblingDB(process.env.TC_DB).tintid.insertMany([{_id:1,n:'a'},{_id:2,n:'b'}])" >/dev/null 2>&1
ok "a window that selects nothing says so" 0 "bear out nothing" \
   -- cline watermark --config $CFG --ns $DB.tintid --complete-from 2026-01-01T00:00:00Z
# The control, on the same two documents: --complete-all counts the whole
# collection rather than a window, so the counts do bear something out and the
# note must not appear. Without this the check above passes on a command that
# prints it every time.
ALLOUT=$(cline watermark --config $CFG --ns $DB.tintid --complete-all 2>&1)
if grep -q "bear out nothing" <<<"$ALLOUT"; then
  FAIL=$((FAIL+1)); FAILED+=("the note is not printed when the counts do say something")
  echo "  FAIL  the note is not printed when the counts do say something"
else
  PASS=$((PASS+1)); echo "  PASS  the note is not printed when the counts do say something"
fi
# Both tiers, and the archive is the half that matters. A feeder running beside
# this mirrors the two documents down, and dropping the fast tier's copy is not
# a delete: the archive keeps what the fast tier stops holding, which is the
# whole point of it. Left there, the warmer finds documents in shop.tintid it
# cannot age on every start and says so at ERROR -- which is the refusal
# testwarm.sh relies on, and which testchaos counts as an unexpected daemon
# error. Measured: two documents left behind here failed testchaos twice, 25 of
# 26, in a suite that has nothing to do with this one.
mongosh "$HOTU"  --quiet --eval "db.getSiblingDB(process.env.TC_DB).tintid.drop()" >/dev/null 2>&1
mongosh "$COLDU" --quiet --eval "db.getSiblingDB(process.env.TC_DB).tintid.drop()" >/dev/null 2>&1
cline watermark --config $CFG --ns $DB.tintid --clear >/dev/null 2>&1

# The refusal itself, which nothing checked. checkClaim exists because
# declaring a claim by hand is the one operator action that can make every
# unbounded read return short answers, silently, for as long as it stands: it
# counts both tiers and refuses a claim the numbers contradict. Replacing that
# comparison with `true` left every test in the repository passing.
#
# Its own namespace, and not one the feeder tiers, so the counts are this
# section's to arrange and no daemon moves them underneath it. The archive holds
# two and the fast tier none, which is exactly the state the refusal is for.
mongosh "$COLDU" --quiet --eval "db.getSiblingDB('tc').refuse.drop();
   db.getSiblingDB('tc').refuse.insertMany([{_id:1},{_id:2}])" >/dev/null 2>&1
ok "a claim the counts contradict is refused"  1 "short of the claim" \
   -- cline watermark --config $CFG --ns tc.refuse --complete-all
# And the escape hatch works, or the refusal is a wall rather than a check: a
# warm still running is the case it is there for.
ok "--force records it anyway, saying so"      0 "because --force was given" \
   -- cline watermark --config $CFG --ns tc.refuse --complete-all --force
# Both tiers and the claim. Dropping the fast tier's copy is not a delete and
# this one was only ever on the archive, so the archive is the half that counts.
mongosh "$COLDU" --quiet --eval "db.getSiblingDB('tc').refuse.drop()" >/dev/null 2>&1
mongosh "$HOTU"  --quiet --eval "db.getSiblingDB('tc').refuse.drop()" >/dev/null 2>&1
cline watermark --config $CFG --ns tc.refuse --clear >/dev/null 2>&1
ok "--complete-all sets"                0 "all of history"    -- cline watermark --config $CFG --ns tc.probe --complete-all
ok "--clear withdraws"                  0 "withdrew"          -- cline watermark --config $CFG --ns tc.probe --clear
ok "--clear again is not an error"      0 "no claim"          -- cline watermark --config $CFG --ns tc.probe --clear
ok "--log-file accepted"                0 ""                  -- cline watermark --config $CFG --log-file $LOGDIR/wm.log
test -s $LOGDIR/wm.log && { PASS=$((PASS+1)); echo "  PASS  watermark wrote its log file"; } || { FAIL=$((FAIL+1)); FAILED+=("watermark log file"); echo "  FAIL  watermark wrote its log file"; }

cline watermark --config $CFG --ns $DB.orders --clear >/dev/null

echo "== preflight =="
# The one command meant to be pointed at a cluster nobody has decided about yet,
# so the property that matters is that it writes nothing. A router start is not
# read-only -- it creates tiered collections on the fast tier and builds the
# archive's indexes on them -- and until this existed those findings could only
# be had by starting one.
ok "preflight runs against the pair"        0 "Nothing will be written" \
   -- cline preflight --config $CFG
ok "and says nothing refuses a start"       0 "Nothing above refuses a start" \
   -- cline preflight --config $CFG
# Both lines above are sentences the command prints. This is the property they
# claim, which nothing checked: the bookkeeping on both tiers is where a write
# would land, so it is counted either side of a run and has to be identical.
# Greping a command's output proves it looks right.
snapMeta() {
  mongosh "$1" --quiet --eval '
    const d = db.getSiblingDB(process.env.TC_META);
    print(d.getCollectionNames().sort().map(c => c + "=" + d.getCollection(c).countDocuments({})).join(","));' 2>&1 | tail -1
}
BEFORE_H=$(snapMeta "$HOTU"); BEFORE_C=$(snapMeta "$COLDU")
cline preflight --config $CFG >/dev/null 2>&1
AFTER_H=$(snapMeta "$HOTU"); AFTER_C=$(snapMeta "$COLDU")
if [ "$BEFORE_H" = "$AFTER_H" ]; then PASS=$((PASS+1)); echo "  PASS  and wrote nothing to the fast tier's bookkeeping"
else FAIL=$((FAIL+1)); FAILED+=("preflight wrote to the fast tier"); echo "  FAIL  preflight changed the fast tier: $BEFORE_H -> $AFTER_H"; fi
if [ "$BEFORE_C" = "$AFTER_C" ]; then PASS=$((PASS+1)); echo "  PASS  and wrote nothing to the archive's bookkeeping"
else FAIL=$((FAIL+1)); FAILED+=("preflight wrote to the archive"); echo "  FAIL  preflight changed the archive: $BEFORE_C -> $AFTER_C"; fi
# And that line has to be true of the feeder as well as the router, which it was
# not. preflight ran the router's checks only, so a capped collection named in
# feeder.namespaces printed a clean report here and stopped the feeder dead
# several steps into docs/adoption.md. The kind check is the feeder's, and this
# is where it is proved to be reached.
PKNS=trcapped
mongosh "$HOTU"  --quiet --eval "db.getSiblingDB(process.env.TC_DB).$PKNS.drop()" >/dev/null 2>&1
mongosh "$COLDU" --quiet --eval "db.getSiblingDB(process.env.TC_DB).$PKNS.drop()" >/dev/null 2>&1
mongosh "$HOTU"  --quiet --eval "db.getSiblingDB(process.env.TC_DB).createCollection('$PKNS',{capped:true,size:4096})" >/dev/null 2>&1
ok "a capped collection in the list refuses" 1 "cannot be mirrored" \
   -- cline preflight --config $CFG
ok "and names the collection and the reason" 1 "$PKNS is a capped collection" \
   -- cline preflight --config $CFG
mongosh "$HOTU"  --quiet --eval "db.getSiblingDB(process.env.TC_DB).$PKNS.drop()" >/dev/null 2>&1
mongosh "$COLDU" --quiet --eval "db.getSiblingDB(process.env.TC_DB).$PKNS.drop()" >/dev/null 2>&1
# The cleanup is verified rather than assumed: a capped collection left behind
# here refuses every later preflight in this suite, and the failures would read
# as the guard firing on nothing.
ok "and it is quiet again once it is gone"  0 "Nothing above refuses a start" \
   -- cline preflight --config $CFG
# The adoption shape, which is the case it exists for and the one it used to be
# silent about: a tiered collection the archive has and the fast tier has never
# held. warnAboutOptionDrift skips a collection only one tier has, because by
# the time a started router reaches it the indexer has already made the other
# side; in a survey the indexer has not run, so that reasoning does not hold.
PFNS=zprobe
mongosh "$COLDU" --quiet --eval "db.getSiblingDB(process.env.TC_DB).$PFNS.drop();
   db.getSiblingDB(process.env.TC_DB).$PFNS.insertOne({_id:1,email:'a@b.c'});
   db.getSiblingDB(process.env.TC_DB).$PFNS.createIndex({email:1},{unique:true,name:'pf_u'})" >/dev/null 2>&1
mongosh "$HOTU" --quiet --eval "db.getSiblingDB(process.env.TC_DB).$PFNS.drop()" >/dev/null 2>&1
ok "it names what a start would build"      0 "would create these on the fast tier" \
   -- cline preflight --config $CFG
# And the whole point: the fast tier is exactly as it was. A router start here
# would have created the collection and built both indexes, one of them unique.
PFHOT=$(mongosh "$HOTU" --quiet --eval "try{print(db.getSiblingDB(process.env.TC_DB).$PFNS.getIndexes().length)}catch(e){print('none')}" 2>&1 | tail -1)
if [ "$PFHOT" = "none" ]; then
  PASS=$((PASS+1)); echo "  PASS  and built nothing on the fast tier"
else
  FAIL=$((FAIL+1)); FAILED+=("preflight wrote to the fast tier")
  echo "  FAIL  and built nothing on the fast tier                got=$PFHOT indexes want=none"
fi
mongosh "$HOTU"  --quiet --eval "db.getSiblingDB(process.env.TC_DB).$PFNS.drop()" >/dev/null 2>&1
mongosh "$COLDU" --quiet --eval "db.getSiblingDB(process.env.TC_DB).$PFNS.drop()" >/dev/null 2>&1
cline watermark --config $CFG --ns $DB.$PFNS --clear >/dev/null 2>&1

# A claim the fast tier no longer covers, which is what a restore leaves behind.
#
# The completeness claim is checked when it is declared and never again, and it
# lives on the archive. So restoring the fast tier from a backup, or rebuilding
# it, leaves a claim describing the tier that backup replaced: every read
# bounded inside the claimed range is then answered from the fast tier alone and
# comes back short, with no error and nothing in any log.
#
# Measured on a shop with 6,187 orders: forty removed from the fast tier out of
# band with no feeder running, and the same bounded read answered 147 through
# the router where the archive holds 187. preflight is the command the runbook
# already says to run after a restore, so it is what should say so.
PCNS=zclaim
mongosh "$HOTU"  --quiet --eval "db.getSiblingDB(process.env.TC_DB).$PCNS.drop()" >/dev/null 2>&1
mongosh "$COLDU" --quiet --eval "db.getSiblingDB(process.env.TC_DB).$PCNS.drop()" >/dev/null 2>&1
# Both tiers hold five, and the claim is true when it is made.
mongosh "$COLDU" --quiet --eval "db.getSiblingDB(process.env.TC_DB).$PCNS.insertMany(
   Array.from({length:5},(_,i)=>({_id:i,when:new Date()})))" >/dev/null 2>&1
mongosh "$HOTU"  --quiet --eval "db.getSiblingDB(process.env.TC_DB).$PCNS.insertMany(
   Array.from({length:5},(_,i)=>({_id:i,when:new Date()})))" >/dev/null 2>&1
cline watermark --config $CFG --ns $DB.$PCNS --complete-all >/dev/null 2>&1
ok "a claim both tiers bear out is quiet"   0 "borne out by the counts" \
   -- cline preflight --config $CFG
# The restore: the fast tier loses three and the archive keeps them.
mongosh "$HOTU" --quiet --eval "db.getSiblingDB(process.env.TC_DB).$PCNS.deleteMany({_id:{\$lt:3}})" >/dev/null 2>&1
ok "and one the fast tier is short of is named" 0 "short of the claim" \
   -- cline preflight --config $CFG
ok "with what it costs the application"     0 "documents where there are" \
   -- cline preflight --config $CFG
mongosh "$HOTU"  --quiet --eval "db.getSiblingDB(process.env.TC_DB).$PCNS.drop()" >/dev/null 2>&1
mongosh "$COLDU" --quiet --eval "db.getSiblingDB(process.env.TC_DB).$PCNS.drop()" >/dev/null 2>&1
cline watermark --config $CFG --ns $DB.$PCNS --clear >/dev/null 2>&1

# The other two things a start changes, which it also has to name rather than
# do: the collections it would remove from the fast tier, and the views it would
# give the archive. A start removing a collection is worth seeing beforehand
# even when the collection is this system's own litter.
mongosh "$HOTU" --quiet --eval "
  const old = Math.floor(Date.now()-3*3600*1000)*1000000;
  db.getSiblingDB(process.env.TC_DB).createCollection('tmp.thermocline.out.'+old);
  db.getSiblingDB(process.env.TC_DB).$PFNS.insertOne({_id:1,n:1});
  db.getSiblingDB(process.env.TC_DB).createView('${PFNS}view','$PFNS',[])" >/dev/null 2>&1
ok "it names what a start would remove"     0 "would remove these from the fast tier" \
   -- cline preflight --config $CFG
ok "and the views it would give the archive" 0 "would give the archive these views" \
   -- cline preflight --config $CFG
# Neither happened. The scratch collection is the sharper half: it is the one
# thing a start deletes.
PFLEFT=$(mongosh "$HOTU" --quiet --eval "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().filter(function (n) { return n.indexOf('tmp.thermocline') === 0 }).length)" 2>&1 | tail -1)
if [ "$PFLEFT" = "1" ]; then
  PASS=$((PASS+1)); echo "  PASS  and removed nothing"
else
  FAIL=$((FAIL+1)); FAILED+=("preflight removed a scratch collection")
  echo "  FAIL  and removed nothing                              got=$PFLEFT want=1"
fi
PFVIEW=$(mongosh "$COLDU" --quiet --eval "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().indexOf('${PFNS}view') >= 0)" 2>&1 | tail -1)
if [ "$PFVIEW" = "false" ]; then
  PASS=$((PASS+1)); echo "  PASS  and created nothing on the archive"
else
  FAIL=$((FAIL+1)); FAILED+=("preflight created a view on the archive")
  echo "  FAIL  and created nothing on the archive               got=$PFVIEW want=false"
fi
for U in "$HOTU" "$COLDU"; do
  mongosh "$U" --quiet --eval "
    const d = db.getSiblingDB(process.env.TC_DB);
    d.getCollectionNames().filter(function (n) { return n.indexOf('tmp.thermocline') === 0 }).forEach(function (n) { d.getCollection(n).drop() });
    try { d.${PFNS}view.drop() } catch (e) {}
    d.$PFNS.drop()" >/dev/null 2>&1
done

echo "== keys =="
# The key registry only has work to do when a tiered collection carries a unique
# index, so give it one and a document to claim.
mongosh "$HOTU" --quiet --eval '
  const c = db.getSiblingDB(process.env.TC_DB).tacct;
  c.createIndex({email:1},{unique:true});
  c.updateOne({_id:"keyprobe"},{$set:{email:"keyprobe@x.com"}},{upsert:true});' >/dev/null 2>&1
mongosh "$COLDU" --quiet --eval 'db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:process.env.TC_DB+".tacct"})' >/dev/null 2>&1
mongosh "$HOTU"  --quiet --eval 'db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:process.env.TC_DB+".tacct"})' >/dev/null 2>&1
ok "report"                             0 "unique indexes|nothing to enforce" -- cline keys --config $CFG
ok "--rebuild"                          0 "claimed|nothing new" -- cline keys --config $CFG --rebuild
ok "--rebuild is idempotent"            0 "claimed 0|nothing new" -- cline keys --config $CFG --rebuild

echo "== warmer =="
ok "--dry-run"                          0 "warm complete"     -- cline warmer --config $CFG --dry-run
ok "normal run"                         0 "warm complete"     -- cline warmer --config $CFG
ok "-v verbose"                         0 "warm complete"     -- cline warmer --config $CFG -v
# Delete the whole rules block, not one rule. The old range ended at the first
# `back:`, so a second warm rule in the shared config left a dangling list item
# and the config would not parse at all, which reads as the warmer rejecting a
# valid file. The evictor fixture above was fixed for the same reason.
sed '/^warmer:/,/^router:/{/^  rules:/,/^  backoff:/{/^  backoff:/!d}}' $CFG > $WDIR/nowarm.yaml
ok "no warmer rules"                    1 "rules is empty"    -- cline warmer --config $WDIR/nowarm.yaml

echo "== evictor =="
# The evictor refuses to work while mirroring is stale, so this section needs a
# feeder that has caught up. Without it these fail for the right reason at the
# wrong time, which looks like a bug in the evictor and is not one.
pkill -f "cline feeder --config $WDIR/"; sleep 1
cline feeder --config $CFG --log-file $LOGDIR/t_pre.log & PRE=$!
for i in $(seq 1 45); do
  LAG=$(mongosh "$COLDU" --quiet --eval 'const c=db.getSiblingDB(process.env.TC_META).checkpoints.findOne({_id:"test"}); print(c?Math.round((Date.now()-c.eventAt.getTime())/1000):9999)' 2>/dev/null | tail -1)
  [ "$LAG" -lt 20 ] 2>/dev/null && break
  sleep 2
done
ok "--once --dry-run"                   0 "sweep complete"    -- cline evictor --config $CFG --once --dry-run
ok "--once"                             0 "sweep complete"    -- cline evictor --config $CFG --once
ok "--once -v"                          0 "sweep complete"    -- cline evictor --config $CFG --once -v
ok "--once --health-timeout"            0 "sweep complete"    -- cline evictor --config $CFG --once --health-timeout 10s

kill $PRE 2>/dev/null; wait $PRE 2>/dev/null

echo "== daemons =="
rm -f $LOGDIR/t_[a-z]*.log
cline feeder --config $CFG --log-file $LOGDIR/t_feeder.log &
FP=$!; sleep 4
grep -q "mirroring" $LOGDIR/t_feeder.log && { PASS=$((PASS+1)); echo "  PASS  feeder starts and mirrors"; } || { FAIL=$((FAIL+1)); FAILED+=("feeder start"); echo "  FAIL  feeder starts and mirrors"; }
kill $FP 2>/dev/null; wait $FP 2>/dev/null

cline feeder --config $CFG -v --log-file $LOGDIR/t_feederv.log &
FP=$!; sleep 4
grep -q "level=DEBUG\|mirroring" $LOGDIR/t_feederv.log && { PASS=$((PASS+1)); echo "  PASS  feeder -v"; } || { FAIL=$((FAIL+1)); FAILED+=("feeder -v"); echo "  FAIL  feeder -v"; }
kill $FP 2>/dev/null; wait $FP 2>/dev/null

cline router --config $CFG --listen 0.0.0.0:$PALT --log-file $LOGDIR/t_router.log &
# Waited on rather than slept. A router's startup reads both tiers' catalogues
# and says what differs, and under a four-worker sweep that took 4.35 seconds on
# 2026-09-25, 1.9 of them on the pilot's sample datasets. A fixed four seconds
# passed or failed by the box's load, not by whether --listen works.
RP=$!
for _ in $(seq 1 60); do grep -q "router listening.*:$PALT" $LOGDIR/t_router.log 2>/dev/null && break; sleep 0.5; done
grep -q "$PALT" $LOGDIR/t_router.log && { PASS=$((PASS+1)); echo "  PASS  router --listen override"; } || { FAIL=$((FAIL+1)); FAILED+=("router --listen"); echo "  FAIL  router --listen override"; }
mongosh --port $PALT -u dba -p secret --authenticationDatabase admin --quiet --eval 'db.getSiblingDB(process.env.TC_DB).orders.findOne()' >/dev/null 2>&1 \
  && { PASS=$((PASS+1)); echo "  PASS  router serves on the overridden port"; } || { FAIL=$((FAIL+1)); FAILED+=("router override serves"); echo "  FAIL  router serves on the overridden port"; }
kill $RP 2>/dev/null; wait $RP 2>/dev/null

# Two finished sweeps, waited for rather than slept at. The check used to sleep
# seven seconds and count the lines one sweep writes: "evicting" and "sweep
# complete" are two of them, so a single sweep already met the bar, and under a
# four-worker sweep the window closed between those two lines and it failed for
# saying 1. What it means to assert is that --every comes round again.
rm -f $LOGDIR/t_evictor.log
cline evictor --config $CFG --every 2s --log-file $LOGDIR/t_evictor.log &
EP=$!
N=0
for _ in $(seq 1 60); do
  N=$(grep -c "sweep complete" $LOGDIR/t_evictor.log 2>/dev/null || true)
  [ "$N" -ge 2 ] && break
  sleep 0.5
done
[ "$N" -ge 2 ] && { PASS=$((PASS+1)); echo "  PASS  evictor --every loops ($N sweeps)"; } || { FAIL=$((FAIL+1)); FAILED+=("evictor --every"); echo "  FAIL  evictor --every loops (saw $N in 30s)"; }
kill $EP 2>/dev/null; wait $EP 2>/dev/null

echo "== two of one daemon =="
# Two feeders with one id watch the same namespaces and both see every delete,
# and an eviction is announced once. The first to reach it takes the
# announcement down; the second finds nothing, reads a real eviction as the
# application deleting the document, and tombstones the archive's copy. The
# document is then gone from both tiers with nothing said anywhere, and the
# default policy is what makes that the outcome.
#
# Nothing exotic arranges it. Starting the daemon on a second host for
# redundancy does, and so does a unit file that starts one before the old one
# has exited. This harness has known it since mkworker.sh was written, which
# isolates every worker's namespaces for exactly this reason; the product said
# nothing and did nothing until the lease.
cline feeder --config $CFG --log-file $LOGDIR/t_lease.log &
LP=$!
for _ in $(seq 1 30); do grep -q "mirroring" $LOGDIR/t_lease.log 2>/dev/null && break; sleep 0.5; done
ok "a second feeder with the same id refuses to start" 1 "another feeder with id" \
   -- cline feeder --config $CFG --log-file $LOGDIR/t_lease2.log
# And it says what to do about it, because the message is the whole of what an
# operator gets.
ok "and it says which process holds it"       1 "pid [0-9]+.*last seen" \
   -- cline feeder --config $CFG --log-file $LOGDIR/t_lease3.log
# A sweep by hand while the evictor daemon is mid-pass is the same hazard one
# step milder: both announce, one deletes, and a count is left standing that
# the application's own delete of that _id then answers for. The dry run is
# deliberately let through, because it announces nothing and removes nothing.
ok "a dry-run sweep is never blocked"         0 "sweep complete" \
   -- cline evictor --config $CFG --once --dry-run
kill $LP 2>/dev/null; wait $LP 2>/dev/null
# Released on a clean exit, so the next one starts at once rather than waiting
# the whole window out. This is the check that fails if the lease is only ever
# taken: with no release the feeder below would refuse for thirty seconds.
rm -f $LOGDIR/t_lease4.log
cline feeder --config $CFG --log-file $LOGDIR/t_lease4.log &
LP=$!
FREE=false
for _ in $(seq 1 16); do grep -q "mirroring" $LOGDIR/t_lease4.log 2>/dev/null && { FREE=true; break; }; sleep 0.5; done
chkrb "the lease is free again once the holder stops" "true" "$FREE"
kill $LP 2>/dev/null; wait $LP 2>/dev/null

echo "== SIGHUP rotation =="
cline feeder --config $CFG --log-file $LOGDIR/t_hup.log &
HP=$!; sleep 3
mv $LOGDIR/t_hup.log $LOGDIR/t_hup.log.1
kill -HUP $HP; sleep 2
grep -q "reopened" $LOGDIR/t_hup.log && { PASS=$((PASS+1)); echo "  PASS  SIGHUP reopens the log"; } || { FAIL=$((FAIL+1)); FAILED+=("SIGHUP"); echo "  FAIL  SIGHUP reopens the log"; }
kill $HP 2>/dev/null; wait $HP 2>/dev/null

echo "== rollback =="
# Going back is repointing the application at the archive, and a delete was
# mirrored as a mark rather than a removal, so every document deleted since the
# cutover is still physically there. A client reading the cluster directly sees
# them; the router does not.
RBNS=trollback
C() { mongosh "$COLDU" --quiet --eval "$1" 2>&1 | tail -1; }
C "db.getSiblingDB(process.env.TC_DB).$RBNS.drop();
   db.getSiblingDB(process.env.TC_DB).$RBNS.insertMany([
     {_id:1, n:'live'},
     {_id:2, n:'deleted through the router', _tcDeletedAt:new Date()},
     {_id:3, n:'also deleted', _tcDeletedAt:new Date()}])" >/dev/null
# Stop anything publishing a position, because the guard below is about that and
# a leftover feeder would make this section report it instead.
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
for _ in $(seq 1 20); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || break; sleep 0.5; done
C "db.getSiblingDB(process.env.TC_META).positions.deleteMany({})" >/dev/null
ok "counting names the namespace and the number" 0 "$RBNS +2 marked deleted" \
    -- cline rollback --config $CFG --ns $DB.$RBNS
ok "and says what a direct reader would see"     0 "reading this cluster directly sees" \
    -- cline rollback --config $CFG --ns $DB.$RBNS
chkrb "counting changed nothing" "3" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$RBNS.countDocuments({}))")"

# What the archive is missing, which is the half an operator cannot fix
# afterwards. Neither of these is a document, so nothing mirrored it: an account
# cannot be mirrored at all, because MongoDB derives SCRAM credentials per
# cluster, and a view holds no documents for the feeder to carry. Repointing an
# application at an archive without its accounts fails to log in at all, and
# MongoDB answers that with an ordinary authentication failure naming nothing.
HRB() { mongosh "$HOTU" --quiet --eval "$1" 2>&1 | tail -1; }
HRB "db.getSiblingDB(process.env.TC_DB).${RBNS}v.drop();
     db.getSiblingDB(process.env.TC_DB).createView('${RBNS}v','$RBNS',[{\$match:{n:'live'}}])" >/dev/null
HRB "try{db.getSiblingDB('admin').createUser({user:'rbprobe',pwd:'x',roles:[]})}catch(e){}" >/dev/null
# The archive must actually lack it at this moment, and a router starting is
# enough to give it one: mirrorViewsToArchive carries every view the fast tier
# has and the archive does not, on purpose, and this suite starts routers all
# through it. Without this line the check passes or fails on that race, and once
# the archive has a copy the teardown below never took it away, so it failed
# every run on that worker from then on. Found on 2026-09-14 with the view
# sitting in one worker's archive and three without. See [[clean-both-tiers]].
CRB() { mongosh "$COLDU" --quiet --eval "$1" 2>&1 | tail -1; }
CRB "db.getSiblingDB(process.env.TC_DB).${RBNS}v.drop()" >/dev/null
RBOUT=$(cline rollback --config $CFG --ns $DB.$RBNS 2>&1)
chkrb "rollback names the view the archive lacks" "true" \
    "$(grep -q "${RBNS}v" <<<"$RBOUT" && echo true || echo false)"
chkrb "and the account the archive lacks"        "true" \
    "$(grep -q 'rbprobe@admin' <<<"$RBOUT" && echo true || echo false)"
chkrb "and says credentials could not be copied" "true" \
    "$(grep -q 'credentials are per cluster' <<<"$RBOUT" && echo true || echo false)"
HRB "db.getSiblingDB(process.env.TC_DB).${RBNS}v.drop(); db.getSiblingDB('admin').dropUser('rbprobe')" >/dev/null
# Both tiers. Dropping the fast tier's copy is not a delete, and the archive's
# is what outlived the run and broke the next one.
CRB "db.getSiblingDB(process.env.TC_DB).${RBNS}v.drop()" >/dev/null
# The ordering guard, which is the part nobody was being told about.
C "db.getSiblingDB(process.env.TC_META).positions.updateOne({_id:'test'},
     {\$set:{at:new Date(), savedAt:new Date(), everyMS:1000}},{upsert:true})" >/dev/null
ok "clearing refuses while a feeder publishes"   1 "still mirroring" \
    -- cline rollback --config $CFG --ns $DB.$RBNS --clear
chkrb "and it cleared nothing"                   "3" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$RBNS.countDocuments({}))")"
ok "--force clears anyway"                       0 "cleared 2" \
    -- cline rollback --config $CFG --ns $DB.$RBNS --clear --force
chkrb "and the archive answers as the router did" "1" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).$RBNS.countDocuments({}))")"
C "db.getSiblingDB(process.env.TC_META).positions.deleteMany({})" >/dev/null
ok "with nothing left it says so"                0 "Nothing is marked deleted" \
    -- cline rollback --config $CFG --ns $DB.$RBNS
C "db.getSiblingDB(process.env.TC_DB).$RBNS.drop()" >/dev/null

# The fixture this suite leaves behind, dropped last and then checked.
#
# tintid's two documents are inserted on the fast tier near the top, and both
# tiers are dropped there. That is not enough: the feeder this suite starts
# afterwards resumes from a checkpoint older than the drop and replays those
# inserts into the archive, and a collection drop is not mirrored, so nothing
# takes them out again. The warmer then finds documents in shop.tintid it cannot
# age on every start and says so at ERROR, which is the refusal testwarm.sh
# relies on and which testchaos counts as an unexpected daemon error. Measured:
# two documents left here failed testchaos twice, 25 of 26, in a suite that has
# nothing to do with this one, and the second time was a month after the first.
#
# So it goes after the last feeder has stopped, and the suite says whether it
# worked rather than leaving the next one to find out.
pkill -f "cline feeder --config $WDIR/" 2>/dev/null
for _ in $(seq 1 20); do pgrep -f "cline feeder --config $WDIR/" >/dev/null 2>&1 || break; sleep 0.5; done
LEFT=""
for _ in $(seq 1 10); do
  C "db.getSiblingDB(process.env.TC_DB).tintid.drop()" >/dev/null
  LEFT=$(C "print(db.getSiblingDB(process.env.TC_DB).tintid.countDocuments({}))")
  [ "$LEFT" = "0" ] && break
  sleep 0.5
done
chkrb "the tintid fixture left nothing in the archive" "0" "$LEFT"
cline watermark --config $CFG --ns $DB.tintid --clear >/dev/null 2>&1

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
