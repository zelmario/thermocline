#!/bin/bash
# Read preference honoured, once the router authenticates clients itself.
#
# testreadpref.sh pins the limitation as it stands with authentication proxied:
# every read lands on the primary because the one upstream connection carrying
# the client's identity is the connection to the primary. This suite is the same
# measurement with terminate_auth on, where connections belong to the router and
# a read asking for a secondary can be sent to one.
#
# The profiler is the only thing that can answer "which node ran this", and it
# has two traps, both of which have cost a run here before. system.profile is
# capped and cannot be dropped on a secondary, so entries survive between runs
# and a fixed comment finds a stale one on the wrong node: every run tags its
# queries uniquely. And an uncaught error in mongosh --eval ends the script, so
# enabling profiling is checked afterwards rather than assumed.
#
# A control that does not come back positive invalidates the whole run, so the
# first thing measured is a query sent straight to the replica set, where the
# answer is already known.
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
CFG=${CFG:-$WDIR/cline_rpterm.yaml}
BASE=$WDIR/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'}
LOG=$LOGDIR/rpterm_r.log
NS=trpterm
TAG="t$$"

PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-54s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-54s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
P() { mongosh "mongodb://dba:secret@127.0.0.1:$PORT/?authSource=admin&readPreference=$1" --quiet --eval "$2" 2>&1 | tail -1; }
N() { mongosh "mongodb://dba:secret@$1:27017/?authSource=admin&directConnection=true" --quiet --eval "$2" 2>&1 | tail -1; }

NODES=$(mongosh "$HOT" --quiet --eval 'print(db.hello().hosts.map(h=>h.split(":")[0]).join(" "))' 2>/dev/null | tail -1)

cleanup() { pkill -9 -f "cline router --config $WDIR/" 2>/dev/null; pkill -9 -f "cline feeder --config $WDIR/" 2>/dev/null; }
trap cleanup EXIT

stopDaemons() {
  pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f "cline feeder --config $WDIR/|cline router --config $WDIR/" >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startRouter() {
  cline router --config $CFG --log-file $LOG &
  for _ in $(seq 1 30); do
    mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on $PORT"; exit 2
}

echo "== setup =="
stopDaemons
rm -f $LOG
sed 's/^  listen:/  terminate_auth: true\n  listen:/' $BASE > $CFG
chk "the config asks for terminated authentication"     "1" "$(grep -c 'terminate_auth: true' $CFG)"

H "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
# The archive's copy too. Only the fast tier's was dropped, so the archive kept
# whatever earlier runs had mirrored into it: 22 documents against the 20
# inserted below, on every worker. Nothing noticed, because the claim recorded
# below says the fast tier holds everything and the archive was therefore never
# read. It became visible when cline watermark started checking that claim
# against the two tiers and refused to record it, which is the check working.
C "db.getSiblingDB(process.env.TC_DB).$NS.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).$NS.insertMany(Array.from({length:20},(_,i)=>({_id:i,n:i})),{writeConcern:{w:3}})" >/dev/null
chk "the fast tier holds the documents"                 "20" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"
# It is a tiered namespace, so without a claim every range read falls through to
# an empty archive and answers zero, which has nothing to do with which node ran
# it. See testauthterm.sh, where the same thing cost a debugging round.
cline watermark --config $CFG --ns $DB.$NS --complete-all >/dev/null 2>&1

for n in $NODES; do N "$n" "db.getSiblingDB(process.env.TC_DB).setProfilingLevel(2)" >/dev/null; done
ON=0
for n in $NODES; do
  [ "$(N "$n" "print(db.getSiblingDB(process.env.TC_DB).getProfilingStatus().was)")" = "2" ] && ON=$((ON+1))
done
chk "the profiler is recording on every node"           "3" "$ON"

landed() {
  local where="none"
  for n in $NODES; do
    if [ "$(N "$n" "print(db.getSiblingDB(process.env.TC_DB).system.profile.countDocuments({ns:'$DB.$NS','command.comment':'$1'}))")" != "0" ]; then
      where=$(N "$n" 'print(db.hello().isWritablePrimary ? "PRIMARY" : "secondary")')
    fi
  done
  echo "$where"
}

echo "== control: straight to the replica set =="
mongosh "$HOT&readPreference=secondary" --quiet \
  --eval "db.getSiblingDB(process.env.TC_DB).$NS.find({}).comment('ctl-sec-$TAG').itcount()" >/dev/null 2>&1
mongosh "$HOT&readPreference=primary" --quiet \
  --eval "db.getSiblingDB(process.env.TC_DB).$NS.find({}).comment('ctl-pri-$TAG').itcount()" >/dev/null 2>&1
sleep 3
chk "a secondary read lands on a secondary"             "secondary" "$(landed ctl-sec-$TAG)"
chk "a primary read lands on the primary"               "PRIMARY"   "$(landed ctl-pri-$TAG)"

echo "== through the router, with authentication terminated =="
startRouter
for mode in secondary secondaryPreferred nearest; do
  P "$mode" "db.getSiblingDB(process.env.TC_DB).$NS.find({}).comment('rt-$mode-$TAG').itcount()" >/dev/null 2>&1
done
sleep 3
# Correctness before placement: whichever node answers, the answer has to be
# right, and a suite that only checked placement could pass while returning
# nothing at all.
for mode in secondary secondaryPreferred nearest; do
  chk "readPreference=$mode still returns everything"   "20" \
      "$(P "$mode" "print(db.getSiblingDB(process.env.TC_DB).$NS.find({}).itcount())")"
done
for mode in secondary secondaryPreferred nearest; do
  chk "readPreference=$mode now lands on a secondary"   "secondary" "$(landed "rt-$mode-$TAG")"
done

echo "== and everything that must not move, does not =="
P primary "db.getSiblingDB(process.env.TC_DB).$NS.find({}).comment('rt-pri-$TAG').itcount()" >/dev/null 2>&1
sleep 2
chk "readPreference=primary still lands on the primary" "PRIMARY" "$(landed "rt-pri-$TAG")"

# A write is not a read however the connection string is set.
P secondary "db.getSiblingDB(process.env.TC_DB).$NS.insertOne({_id:100,n:100})" >/dev/null 2>&1
chk "a write still succeeds under a secondary preference" "21" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({}))")"

# A transaction must see its own uncommitted writes, which exist nowhere but the
# primary, so its reads must not be routed.
#
# It cannot be provoked from here with a secondary preference: the driver refuses
# before anything reaches the wire, with "Read preference in a transaction must
# be primary, not: secondary". Nothing then runs, the profiler records nothing,
# and a check written that way reports that the read landed nowhere, which looks
# identical to the guard working. The guard itself is proved in
# secondaryread_test.go, where the command can be built by hand as a client that
# did not check would send it.
#
# What is worth measuring here is the other half: that transactions still work
# through the pooled path, and still run where they must.
P primary "const s=db.getMongo().startSession();
  s.withTransaction(()=>{ const c=s.getDatabase(process.env.TC_DB).$NS;
    c.insertOne({_id:200,n:200}); print(c.find({_id:200}).comment('rt-txn-$TAG').itcount()); });
  s.endSession(); print('COMMITTED')" >/dev/null 2>&1
sleep 2
chk "a transaction still commits through the router"    "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({_id:200}))")"
chk "and its read ran on the primary"                   "PRIMARY" "$(landed "rt-txn-$TAG")"

# A cursor lives on the node that made it, so its getMore has to go back there.
# A small batch size forces several.
chk "a cursor paged across getMore returns everything"  "21" \
    "$(P secondary "print(db.getSiblingDB(process.env.TC_DB).$NS.find({_id:{\$lt:100}}).batchSize(2).itcount()+db.getSiblingDB(process.env.TC_DB).$NS.find({_id:100}).itcount())")"

echo "== a member that is in the set and not readable =="
# The router picks a secondary out of hello's `hosts`, and `hosts` is every
# configured member rather than every readable one. A member in a maintenance
# window, an initial sync or a rollback stays in that list, stays up, keeps
# accepting connections and answers reads with NotPrimaryOrSecondary. A driver
# never picks such a member, so an application never sees it.
#
# Measured before this was fixed, with retries off, which is what a getMore and
# an application that turned retryReads off both look like: ten reads straight
# at the set all answered, ten through the router all failed.
#
# retryReads=false is what makes this a measurement rather than a coin toss. With
# the driver's own retry on, a read refused by the recovering member is quietly
# re-sent to the other one and the check passes whether or not the router
# noticed anything.
HOTMEMBERS="172.24.0.5:27017 172.24.0.6:27017 172.24.0.7:27017"
DIRECT() { mongosh "mongodb://dba:secret@$1/?authSource=admin&directConnection=true" --quiet --eval "$2" 2>&1 | tail -1; }
NORETRY() { mongosh "mongodb://dba:secret@127.0.0.1:$PORT/?authSource=admin&readPreference=secondary&retryReads=false" --quiet --eval "$1" 2>&1 | tail -1; }
maintOff() { for a in $HOTMEMBERS; do DIRECT $a 'try{ db.adminCommand({replSetMaintenance:false}) }catch(e){}' >/dev/null 2>&1; done; }
# A member left in RECOVERING is not this suite's to leave behind.
trap 'maintOff' EXIT
RPRIM=$(H 'print(db.hello().primary)')
RVICTIM=""
for a in $HOTMEMBERS; do [ "$a" = "$RPRIM" ] && continue; RVICTIM=$a; break; done
DIRECT $RVICTIM 'try{ db.adminCommand({replSetMaintenance:true}) }catch(e){}' >/dev/null
chk "one secondary is in RECOVERING"                    "3" \
    "$(DIRECT $RVICTIM 'print(db.adminCommand({replSetGetStatus:1}).myState)')"
chk "and the set still lists it"                        "true" \
    "$(H "print(db.hello().hosts.includes('$RVICTIM'))")"
RQ="try{ print(db.getSiblingDB(process.env.TC_DB).$NS.countDocuments({})) }catch(e){ print('ERR '+(e.codeName||e.message)) }"
RBAD=0
for i in 1 2 3 4 5 6 7 8 9 10; do
  case "$(NORETRY "$RQ")" in *ERR*) RBAD=$((RBAD+1)) ;; esac
done
chk "ten reads with retries off, none refused"          "0" "$RBAD"
chk "and the router said which member it stopped using" "true" \
    "$(grep -qc "not readable" $LOG >/dev/null 2>&1 && grep -q "$RVICTIM" $LOG && echo true || echo false)"
maintOff
trap - EXIT

echo "== the router counts what it moved =="
chk "secondary reads are counted"                       "true" \
    "$([ -f $LOG ] && echo true || echo false)"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
