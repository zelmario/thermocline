#!/bin/bash
# The router authenticating clients itself, instead of passing SCRAM through.
#
# With terminate_auth on, the upstream connection belongs to the router and is
# authenticated as the router's own account, so the fast tier will do whatever it
# is asked. The router's own check is then the only thing between a client and
# somebody else's data, which makes this suite the one that decides whether
# terminating authentication was safe to do at all.
#
# The shape of the danger is already known. Command surface item 0 was a
# privilege bypass through exactly this route, for count, distinct and aggregate,
# because those are judged before they run so the fast tier never saw them and
# the answer came from the archive over the router's own connection. Those three
# are checked here by name, along with find, and with a user who has read on one
# database and nothing else.
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
CFG=${CFG:-$WDIR/cline_auth.yaml}
BASE=$WDIR/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }
# As a given user, through the router.
AS() { mongosh --port $PORT -u "$1" -p "$2" --authenticationDatabase admin --quiet --eval "$3" 2>&1 | tail -1; }
# Reduce any authorization failure to one word, so a check states what it means.
denied() { case "$1" in *"not authorized"*) echo DENIED ;; *) echo "$1" ;; esac; }
refused() { case "$1" in *"thermocline: refusing"*) echo REFUSED ;; *) echo "$1" ;; esac; }
# The same command as the same user, sent straight to the fast tier. mongod is
# the reference: anything the router allows that mongod refuses is a privilege
# bypass. Defined here rather than halfway down the file, because a function is
# not in scope above the line that defines it.
HOTAS() { local uri=${HOT/dba:secret/$1:$2}; mongosh "$uri" --quiet --eval "$3" 2>&1 | tail -1; }

cleanup() { pkill -9 -f "cline router --config $WDIR/" 2>/dev/null; pkill -9 -f "cline feeder --config $WDIR/" 2>/dev/null; }
trap cleanup EXIT
pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/" 2>/dev/null; sleep 2

echo "== setup =="
# terminate_auth is not in the shared config, because every other suite runs
# with it off. Derived here so the two cannot drift apart.
#
# And router.freshness: permissive. No feeder runs here, and the one section that
# needs the archive writes there directly and deletes the fast tier's copy behind
# the router's back; a feeder would mirror that delete and the fixture with it.
# With nothing mirroring, the default since 2026-10-12, strict, refuses every
# read that needs the archive, which is testfresh's subject, not this suite's.
sed 's/^  listen:/  terminate_auth: true\n  freshness: permissive\n  listen:/' $BASE > $CFG
chk "the config asks for terminated authentication" "1" "$(grep -c 'terminate_auth: true' $CFG)"

# Both tiers, not just the fast one. A collection dropped on the fast tier alone
# leaves the archive holding the previous run's documents, and the router then
# merges them into every answer, which reads as a count that is simply wrong.
H "db.getSiblingDB('payroll').salaries.drop();
   db.getSiblingDB('payroll').salaries.insertMany([{_id:1,who:'a',amount:100},{_id:2,who:'b',amount:200}]);
   db.getSiblingDB(process.env.TC_DB).tatrm.drop();
   db.getSiblingDB(process.env.TC_DB).tatrm.insertMany([{_id:1,v:'one'},{_id:2,v:'two'}])" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tatrm.drop()" >/dev/null
# Users are server-wide, not per worker, so the name is this suite's own.
# testauth.sh creates a 'limited' too, with read on its worker's database; on
# 2026-10-23 the sweep's new order ran the two at once and 17 checks here failed
# with "Authentication failed" as each recreated the other's user.
H "try { db.getSiblingDB('admin').dropUser('atlimited') } catch(e) {}
   db.getSiblingDB('admin').createUser({user:'atlimited',pwd:'limpass',roles:[{role:'read',db:'$DB'}]})" >/dev/null
H "try { db.getSiblingDB('admin').dropUser('writer') } catch(e) {}
   db.getSiblingDB('admin').createUser({user:'writer',pwd:'writepass',roles:[{role:'readWrite',db:'$DB'}]})" >/dev/null

# shop.tatrm is a tiered namespace, so without a completeness claim every range
# read falls through to an archive that has nothing in it and answers zero. That
# is the router working as designed and it has nothing to do with authentication,
# so the claim is stated here and the one section that needs the archive
# withdraws it again.
cline watermark --config $CFG --ns $DB.tatrm --complete-all >/dev/null 2>&1

cline router --config $CFG --log-file $LOGDIR/authterm_r.log &
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
if ! mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1; then
  echo "  FATAL  the router never came up on $PORT"; exit 2
fi

echo "== the router verifies the password itself =="
chk "a correct password authenticates"          "1"  "$(AS dba secret 'print(db.adminCommand({ping:1}).ok)')"
chk "and can read"                              "2"  "$(AS dba secret 'print(db.getSiblingDB(process.env.TC_DB).tatrm.countDocuments({}))')"
out=$(AS dba WRONG 'print("GOT IN")')
chk "a wrong password does not"                 "Authentication failed." "$(echo "$out" | sed 's/.*MongoServerError: //')"
out=$(AS nosuchuser whatever 'print("GOT IN")')
chk "and neither does a user that does not exist" "Authentication failed." "$(echo "$out" | sed 's/.*MongoServerError: //')"

echo "== a user reaches its own database and no other =="
chk "limited reads the database it was granted" "2"      "$(AS atlimited limpass 'print(db.getSiblingDB(process.env.TC_DB).tatrm.countDocuments({}))')"
chk "find on another database is refused"       "DENIED" "$(denied "$(AS atlimited limpass 'print(db.getSiblingDB("payroll").salaries.find().toArray().length)')")"
chk "count on another database is refused"      "DENIED" "$(denied "$(AS atlimited limpass 'print(db.getSiblingDB("payroll").salaries.count())')")"
chk "distinct on another database is refused"   "DENIED" "$(denied "$(AS atlimited limpass 'print(db.getSiblingDB("payroll").salaries.distinct("who"))')")"
chk "aggregate on another database is refused"  "DENIED" "$(denied "$(AS atlimited limpass 'print(db.getSiblingDB("payroll").salaries.aggregate([{$group:{_id:null,t:{$sum:"$amount"}}}]).toArray().length)')")"

echo "== read does not imply write =="
chk "a read only user cannot insert"            "DENIED" "$(denied "$(AS atlimited limpass 'db.getSiblingDB(process.env.TC_DB).tatrm.insertOne({_id:99}); print("INSERTED")')")"
chk "a read only user cannot delete"            "DENIED" "$(denied "$(AS atlimited limpass 'print(db.getSiblingDB(process.env.TC_DB).tatrm.deleteOne({_id:1}).deletedCount)')")"
chk "and the document is still there"           "2"      "$(H 'print(db.getSiblingDB(process.env.TC_DB).tatrm.countDocuments({}))')"
chk "a readWrite user can insert"               "3"      "$(AS writer writepass 'db.getSiblingDB(process.env.TC_DB).tatrm.insertOne({_id:3,v:"three"}); print(db.getSiblingDB(process.env.TC_DB).tatrm.countDocuments({}))')"

echo "== a command whose privilege is unknown is refused, not guessed =="
chk "an unknown command is refused by name"     "REFUSED" "$(refused "$(AS dba secret 'print(JSON.stringify(db.getSiblingDB(process.env.TC_DB).runCommand({thisCommandDoesNotExist:1})))')")"

echo "== the diagnostic surface a driver and a shell actually send =="
# None of this was judged, and an unjudged command is refused, so with
# terminate_auth on the router refused all of it: measured, fourteen out of
# fourteen. A monitoring agent got nothing, rs.status() failed, and the Go
# driver's own connection setup sends getParameter, so every client connection
# produced a refusal nobody asked for.
#
# Each action was established against mongod rather than remembered: a custom
# role granting exactly one action makes exactly one of these allowed for a user
# who otherwise holds read on one database. The pairs below are what that means
# from outside, and the second half of each pair is the one that matters. A
# mapping that is too weak is a privilege bypass, not a refusal.
chk "a root user can read server status"        "1"      "$(AS dba secret 'print(db.adminCommand({serverStatus:1}).ok)')"
chk "and a parameter"                           "1"      "$(AS dba secret 'print(db.adminCommand({getParameter:1,featureCompatibilityVersion:1}).ok)')"
chk "and the startup warnings mongosh asks for" "1"      "$(AS dba secret 'print(db.adminCommand({getLog:"startupWarnings"}).ok)')"
chk "and the replica set status"                "1"      "$(AS dba secret 'print(db.adminCommand({replSetGetStatus:1}).ok)')"
chk "a read only user is refused server status" "DENIED" "$(denied "$(AS atlimited limpass 'print(db.adminCommand({serverStatus:1}).ok)')")"
chk "and mongod refuses the same user the same" "DENIED" "$(denied "$(HOTAS atlimited limpass 'print(db.adminCommand({serverStatus:1}).ok)')")"
chk "and it is refused the user list"           "DENIED" "$(denied "$(AS atlimited limpass 'print(db.getSiblingDB("admin").runCommand({usersInfo:1}).ok)')")"
chk "and refused what is running"               "DENIED" "$(denied "$(AS atlimited limpass 'print(db.adminCommand({currentOp:1}).ok)')")"
# connectionStatus reports the caller's own authentication state and mongod
# asks no privilege for it. mongosh sends it on connect.
chk "connectionStatus needs no privilege"       "1"      "$(AS atlimited limpass 'print(db.adminCommand({connectionStatus:1}).ok)')"
# The sharpest of these: the same command, allowed on the database this user can
# read and refused on the one it cannot. A blanket exemption would pass the
# first and fail the second.
#
# Named collections rather than the whole database, and untiered ones, because
# dbHash over a tiered collection the fast tier holds part of is refused for a
# reason that has nothing to do with authorisation: it would answer a hash of
# that part. The first version of this asked for the whole database and started
# failing the day that refusal was added, on tiering grounds, which says nothing
# about the privilege being tested here.
H "db.getSiblingDB(process.env.TC_DB).tazhash.drop();
   db.getSiblingDB(process.env.TC_DB).tazhash.insertOne({_id:1,v:'x'});
   db.getSiblingDB('payroll').tazhash.drop();
   db.getSiblingDB('payroll').tazhash.insertOne({_id:1,v:'x'})" >/dev/null
chk "dbHash on the granted database"            "1"      "$(AS atlimited limpass 'print(db.getSiblingDB(process.env.TC_DB).runCommand({dbHash:1,collections:["tazhash"]}).ok)')"
chk "and refused on another"                    "DENIED" "$(denied "$(AS atlimited limpass 'print(db.getSiblingDB("payroll").runCommand({dbHash:1,collections:["tazhash"]}).ok)')")"
chk "and mongod agrees about both"              "DENIED" "$(denied "$(HOTAS atlimited limpass 'print(db.getSiblingDB("payroll").runCommand({dbHash:1,collections:["tazhash"]}).ok)')")"
# atlasVersion is how mongosh asks whether it is talking to Atlas, and mongod
# does not have the command. Forwarded, so the client gets mongod's own answer
# rather than a refusal this proxy invented, and so the refusal counter is not
# stepped twice by every shell that connects.
chk "atlasVersion answers as mongod does"       "CommandNotFound" \
    "$(AS dba secret 'try { db.adminCommand({atlasVersion:1}); print("ANSWERED") } catch (e) { print(e.codeName || e.message.substring(0,40)) }')"

echo "== tiering still works with authentication terminated =="
# The point of the whole system has to survive the change: a document only the
# archive holds must still be found, by a user allowed to see it.
# Withdraw the claim first: while the fast tier says it holds everything, the
# router is right not to look in the archive at all.
cline watermark --config $CFG --ns $DB.tatrm --complete-from 2030-01-01T00:00:00Z >/dev/null 2>&1
sleep 6
C "db.getSiblingDB(process.env.TC_DB).tatrm.updateOne({_id:1},{\$set:{v:'one'}},{upsert:true})" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).tatrm.deleteOne({_id:1})" >/dev/null
chk "an archived document is still served"      "one"    "$(AS dba secret 'const d=db.getSiblingDB(process.env.TC_DB).tatrm.findOne({_id:1}); print(d?d.v:"MISSING")')"
chk "and refused to a user without the grant"   "DENIED" "$(denied "$(AS atlimited limpass 'const d=db.getSiblingDB("payroll").salaries.findOne({_id:1}); print(d?d.who:"MISSING")')")"

# A command names more collections than the one it is sent to.
#
# The gate judges an aggregate by the collection in the `aggregate` field, and
# that is not the only namespace an aggregation touches. $lookup, $unionWith and
# $graphLookup read another collection; $out and $merge write one. mongod
# requires find on every collection read and insert, update or remove on every
# collection written, and it checks each of them.
#
# With authentication proxied none of this mattered, because the fast tier saw
# the command with the client's own identity on it and applied its own rules.
# With terminate_auth on, the upstream connection is the router's and the fast
# tier will do whatever it is asked, so a namespace the gate does not judge is a
# namespace with no check on it at all.
#
# The reference is the same user sending the same command straight to the fast
# tier. Anything the router allows that mongod refuses is a privilege bypass,
# and for the writing stages it is a destructive one.

echo "== the collections a command names besides its own =="
H "db.getSiblingDB(process.env.TC_DB).tazopen.drop();
   db.getSiblingDB(process.env.TC_DB).tazopen.insertMany([{_id:1,v:'open'},{_id:2,v:'open2'}]);
   db.getSiblingDB(process.env.TC_DB).tazsecret.drop();
   db.getSiblingDB(process.env.TC_DB).tazsecret.insertMany([{_id:1,pay:100},{_id:2,pay:200}])" >/dev/null
H "try { db.getSiblingDB('admin').dropUser('narrow') } catch(e) {}
   try { db.getSiblingDB(process.env.TC_DB).dropRole('tazreader') } catch(e) {}
   db.getSiblingDB(process.env.TC_DB).createRole({role:'tazreader',
     privileges:[{resource:{db:'$DB',collection:'tazopen'},actions:['find','insert','update','remove']}], roles:[]});
   db.getSiblingDB('admin').createUser({user:'narrow',pwd:'narrowpass',roles:[{role:'tazreader',db:'$DB'}]})" >/dev/null
seedTaz() {
  H "db.getSiblingDB(process.env.TC_DB).tazsecret.deleteMany({});
     db.getSiblingDB(process.env.TC_DB).tazsecret.insertMany([{_id:1,pay:100},{_id:2,pay:200}])" >/dev/null
}
secretNow() { H "print(db.getSiblingDB(process.env.TC_DB).tazsecret.find().sort({_id:1}).toArray().map(d=>d._id+':'+d.pay).join(','))"; }

chk "the narrow user reads its own collection"      "2" \
    "$(AS narrow narrowpass 'print(db.getSiblingDB(process.env.TC_DB).tazopen.countDocuments({}))')"
chk "and cannot read the other one"                 "DENIED" \
    "$(denied "$(AS narrow narrowpass 'print(db.getSiblingDB(process.env.TC_DB).tazsecret.countDocuments({}))')")"

LKP='print(db.getSiblingDB(process.env.TC_DB).tazopen.aggregate([{$lookup:{from:"tazsecret",localField:"_id",foreignField:"_id",as:"j"}}]).toArray().map(d=>d.j.length).join(","))'
chk "a \$lookup joining it matches mongod"           "$(denied "$(HOTAS narrow narrowpass "$LKP")")" \
    "$(denied "$(AS narrow narrowpass "$LKP")")"

UNI='print(db.getSiblingDB(process.env.TC_DB).tazopen.aggregate([{$unionWith:"tazsecret"}]).toArray().length)'
chk "a \$unionWith of it matches mongod"             "$(denied "$(HOTAS narrow narrowpass "$UNI")")" \
    "$(denied "$(AS narrow narrowpass "$UNI")")"

GRP='print(db.getSiblingDB(process.env.TC_DB).tazopen.aggregate([{$graphLookup:{from:"tazsecret",startWith:"$_id",connectFromField:"_id",connectToField:"_id",as:"j"}}]).toArray().map(d=>d.j.length).join(","))'
chk "a \$graphLookup into it matches mongod"         "$(denied "$(HOTAS narrow narrowpass "$GRP")")" \
    "$(denied "$(AS narrow narrowpass "$GRP")")"

seedTaz
MRG='db.getSiblingDB(process.env.TC_DB).tazopen.aggregate([{$project:{pay:{$literal:-1}}},{$merge:{into:"tazsecret",whenMatched:"replace",whenNotMatched:"insert"}}]).toArray(); print("WROTE")'
REFMRG=$(denied "$(HOTAS narrow narrowpass "$MRG")")
REFSTATE=$(secretNow)
seedTaz
chk "a \$merge into it matches mongod"               "$REFMRG" "$(denied "$(AS narrow narrowpass "$MRG")")"
chk "and leaves the collection as mongod left it"   "$REFSTATE" "$(secretNow)"

seedTaz
OUT='db.getSiblingDB(process.env.TC_DB).tazopen.aggregate([{$project:{pay:{$literal:-2}}},{$out:"tazsecret"}]).toArray(); print("WROTE")'
REFOUT=$(denied "$(HOTAS narrow narrowpass "$OUT")")
REFOSTATE=$(secretNow)
seedTaz
chk "an \$out over it matches mongod"                "$REFOUT" "$(denied "$(AS narrow narrowpass "$OUT")")"
chk "and it still holds what mongod left"           "$REFOSTATE" "$(secretNow)"
echo "== commands that carry another command inside them =="
# explain runs nothing itself, so judging it by name refuses every explain a
# driver sends, and treating it as harmless hands a user the shape and the index
# bounds of a collection they may not read. It needs what the command inside it
# needs, which is what mongod does.
EXPOPEN='print(db.getSiblingDB(process.env.TC_DB).tazopen.find({_id:1}).explain().queryPlanner.namespace)'
chk "an explain of a permitted find is allowed"     "$(HOTAS narrow narrowpass "$EXPOPEN")" \
    "$(denied "$(AS narrow narrowpass "$EXPOPEN")")"
EXPSEC='print(db.getSiblingDB(process.env.TC_DB).tazsecret.find({_id:1}).explain().queryPlanner.namespace)'
chk "an explain of a forbidden find matches mongod" "$(denied "$(HOTAS narrow narrowpass "$EXPSEC")")" \
    "$(denied "$(AS narrow narrowpass "$EXPSEC")")"

# bulkWrite is the 8.0 write path. It is sent to admin and names its collections
# in nsInfo, so there is no collection where authorization would look for one.
H 'db.getSiblingDB(process.env.TC_DB).tazopen.deleteMany({_id:{$gte:900}})' >/dev/null
BWOPEN='print(db.getSiblingDB("admin").runCommand({bulkWrite:1, ops:[{insert:0, document:{_id:900,v:"bw"}}], nsInfo:[{ns:process.env.TC_DB+".tazopen"}]}).ok)'
chk "a permitted bulkWrite is allowed"              "1" \
    "$(denied "$(AS narrow narrowpass "$BWOPEN")")"
BWSEC='print(db.getSiblingDB("admin").runCommand({bulkWrite:1, ops:[{insert:0, document:{_id:901,pay:-3}}], nsInfo:[{ns:process.env.TC_DB+".tazsecret"}]}).ok)'
chk "a forbidden bulkWrite matches mongod"          "$(denied "$(HOTAS narrow narrowpass "$BWSEC")")" \
    "$(denied "$(AS narrow narrowpass "$BWSEC")")"
chk "and it wrote nothing there"                    "0" \
    "$(H 'print(db.getSiblingDB(process.env.TC_DB).tazsecret.countDocuments({_id:901}))')"
echo "== renaming, which mongod judges by the database =="
# A same-database rename needs renameCollectionSameDB on the database, which
# readWrite carries and read does not. Across databases mongod wants privileges
# on both sides that this does not know how to check, so that stays refused by
# name rather than guessed at.
chk "a read only user cannot rename"                "DENIED"     "$(denied "$(AS atlimited limpass 'db.adminCommand({renameCollection:process.env.TC_DB+".tazopen",to:process.env.TC_DB+".tazmoved"}); print("RENAMED")')")"
chk "a readWrite user can"                          "RENAMED"     "$(denied "$(AS writer writepass 'db.adminCommand({renameCollection:process.env.TC_DB+".tazopen",to:process.env.TC_DB+".tazmoved"}); print("RENAMED")')")"
H 'db.adminCommand({renameCollection:process.env.TC_DB+".tazmoved",to:process.env.TC_DB+".tazopen"})' >/dev/null 2>&1
chk "a rename across databases is refused by name"  "REFUSED"     "$(refused "$(AS dba secret 'db.adminCommand({renameCollection:process.env.TC_DB+".tazopen",to:"payroll.tazopen"}); print("RENAMED")')")"

echo "== a write that asks for no acknowledgement is still a write =="
# An unacknowledged write, w:0, is sent with the wire protocol's moreToCome flag
# and the client never waits for an answer. There is therefore nowhere to put a
# refusal, and that is a reason to not perform the write, not a reason to skip
# asking whether it is allowed. Forwarding it would be the quietest privilege
# bypass in the system: no error anywhere, and the document lands.
UNACK='db.getSiblingDB("payroll").salaries.insertOne({_id:777,who:"unack"},{writeConcern:{w:0}}); print("sent")'
H 'db.getSiblingDB("payroll").salaries.deleteMany({_id:777})' >/dev/null
HOTAS atlimited limpass "$UNACK" >/dev/null 2>&1
sleep 2
REFUNACK=$(H 'print(db.getSiblingDB("payroll").salaries.countDocuments({_id:777}))')
H 'db.getSiblingDB("payroll").salaries.deleteMany({_id:777})' >/dev/null
AS atlimited limpass "$UNACK" >/dev/null 2>&1
sleep 2
chk "an unacknowledged forbidden write matches mongod" "$REFUNACK" \
    "$(H 'print(db.getSiblingDB("payroll").salaries.countDocuments({_id:777}))')"
H 'db.getSiblingDB("payroll").salaries.deleteMany({_id:777})' >/dev/null

# And the permitted one still has to work, because refusing every unacknowledged
# write would be a fix that broke the feature.
H 'db.getSiblingDB(process.env.TC_DB).tatrm.deleteMany({_id:778})' >/dev/null
AS writer writepass 'db.getSiblingDB(process.env.TC_DB).tatrm.insertOne({_id:778,v:"unack"},{writeConcern:{w:0}}); print("sent")' >/dev/null 2>&1
sleep 2
chk "an unacknowledged permitted write still lands"   "1" \
    "$(H 'print(db.getSiblingDB(process.env.TC_DB).tatrm.countDocuments({_id:778}))')"
H 'db.getSiblingDB(process.env.TC_DB).tatrm.deleteMany({_id:778})' >/dev/null
echo "== the privilege a stage asks for itself =="
# mongod judges an aggregation stage by stage. A pipeline that starts by
# describing the collection never reads it, so find is not what it needs:
# $indexStats needs indexStats, $collStats needs collStats, $planCacheStats
# needs planCacheRead, and a change stream needs changeStream as well as find.
# Judging all of them as "find on the collection" is wrong in both directions,
# and one of them hands a user metadata mongod would have refused.
H "try { db.getSiblingDB('admin').dropUser('finder') } catch(e) {}
   try { db.getSiblingDB(process.env.TC_DB).dropRole('tazfinder') } catch(e) {}
   db.getSiblingDB(process.env.TC_DB).createRole({role:'tazfinder',
     privileges:[{resource:{db:'$DB',collection:'tazopen'},actions:['find']}], roles:[]});
   db.getSiblingDB('admin').createUser({user:'finder',pwd:'findpass',roles:[{role:'tazfinder',db:'$DB'}]})" >/dev/null
H "try { db.getSiblingDB('admin').dropUser('statser') } catch(e) {}
   try { db.getSiblingDB(process.env.TC_DB).dropRole('tazstats') } catch(e) {}
   db.getSiblingDB(process.env.TC_DB).createRole({role:'tazstats',
     privileges:[{resource:{db:'$DB',collection:'tazopen'},actions:['collStats']}], roles:[]});
   db.getSiblingDB('admin').createUser({user:'statser',pwd:'statspass',roles:[{role:'tazstats',db:'$DB'}]})" >/dev/null

IDX='print(db.getSiblingDB(process.env.TC_DB).tazopen.aggregate([{$indexStats:{}}]).toArray().length>0)'
chk "\$indexStats on find alone matches mongod"      "$(denied "$(HOTAS finder findpass "$IDX")")" \
    "$(denied "$(AS finder findpass "$IDX")")"
CS='const cs=db.getSiblingDB(process.env.TC_DB).tazopen.watch(); cs.close(); print("OPENED")'
chk "a change stream on find alone matches mongod"  "$(denied "$(HOTAS finder findpass "$CS")")" \
    "$(denied "$(AS finder findpass "$CS")")"
STATS='print(db.getSiblingDB(process.env.TC_DB).tazopen.aggregate([{$collStats:{storageStats:{}}}]).toArray().length)'
chk "\$collStats without find matches mongod"        "$(denied "$(HOTAS statser statspass "$STATS")")" \
    "$(denied "$(AS statser statspass "$STATS")")"
chk "and find alone cannot ask for it"              "$(denied "$(HOTAS finder findpass "$STATS")")" \
    "$(denied "$(AS finder findpass "$STATS")")"

echo "== a \$merge asks only for what it can do =="
# whenNotMatched: discard means the merge can never insert, and mongod asks only
# for update. Asking for both refuses a user who may update the collection and
# may not insert into it.
H "try { db.getSiblingDB('admin').dropUser('updater') } catch(e) {}
   try { db.getSiblingDB(process.env.TC_DB).dropRole('tazupdater') } catch(e) {}
   db.getSiblingDB(process.env.TC_DB).createRole({role:'tazupdater',
     privileges:[{resource:{db:'$DB',collection:'tazopen'},actions:['find']},
                 {resource:{db:'$DB',collection:'tazsecret'},actions:['update']}], roles:[]});
   db.getSiblingDB('admin').createUser({user:'updater',pwd:'updpass',roles:[{role:'tazupdater',db:'$DB'}]})" >/dev/null
seedTaz
MRGU='db.getSiblingDB(process.env.TC_DB).tazopen.aggregate([{$project:{pay:{$literal:9}}},{$merge:{into:"tazsecret",whenMatched:"merge",whenNotMatched:"discard"}}]).toArray(); print("WROTE")'
REFU=$(denied "$(HOTAS updater updpass "$MRGU")")
REFUSTATE=$(secretNow)
seedTaz
chk "a merge that can only update matches mongod"   "$REFU" "$(denied "$(AS updater updpass "$MRGU")")"
chk "and left the collection the same way"          "$REFUSTATE" "$(secretNow)"

echo "== skipping a collection's own validation rules =="
# bypassDocumentValidation asks the server to write what the collection says is
# invalid, and mongod requires a privilege for it that write access does not
# imply.
BYP='print(db.getSiblingDB(process.env.TC_DB).tazopen.insertOne({_id:950,v:"byp"},{bypassDocumentValidation:true}).acknowledged)'
H 'db.getSiblingDB(process.env.TC_DB).tazopen.deleteMany({_id:950})' >/dev/null
REFBYP=$(denied "$(HOTAS narrow narrowpass "$BYP")")
H 'db.getSiblingDB(process.env.TC_DB).tazopen.deleteMany({_id:950})' >/dev/null
chk "bypassing validation matches mongod"           "$REFBYP" "$(denied "$(AS narrow narrowpass "$BYP")")"
H 'db.getSiblingDB(process.env.TC_DB).tazopen.deleteMany({_id:950})' >/dev/null
echo "== the router says which mode it is in, every start =="
# The startup line named the address, the fast tier, the completeness mode and
# whether the connections are encrypted, and said nothing about which of the two
# authentication modes was running. They are not the same product: proxied, the
# fast tier judges every command against the account the client authenticated
# as, and this proxy cannot grant what mongod would refuse; terminated, the
# router's own authorisation gate is the only thing between a client and
# somebody else's data.
#
# An operator reading the log could not tell which one they had.
chk "the listening line names the mode"                "true" \
    "$(grep 'router listening' $LOGDIR/authterm_r.log | grep -q 'auth=terminated' && echo true || echo false)"
chk "and says what it means, every start"              "true" \
    "$(grep -q 'authenticating clients itself, rather than letting the fast tier do it' $LOGDIR/authterm_r.log && echo true || echo false)"
# The control, because a line that appears in both modes says nothing.
#
# On its own port, and this is the whole of why the control was worthless
# before. The router started at the top of this suite holds $PORT for the length
# of it, so a second one here could never bind: it died with "address already in
# use" and wrote no log at all. The wait below then succeeded against the *first*
# router, which was answering on that port perfectly well, so the section walked
# on as though its control had started. The check that reads the log failed, and
# the one beside it, "the warning is not printed", passed against an empty file.
# A control nothing proved, asserting a mode it never ran in.
# The dashboard goes with it. It is "empty for off" in the config, and leaving
# the running router's dashboard port in this file is the same collision one
# line further down.
sed -e 's|^  terminate_auth: true|  terminate_auth: false|' \
    -e "s|^  listen:.*|  listen: \"0.0.0.0:$PALT\"|" \
    -e 's|^  dashboard:.*|  dashboard: ""|' $CFG > $WDIR/cline_authoff.yaml
rm -f $LOGDIR/authoff_r.log
cline router --config $WDIR/cline_authoff.yaml --log-file $LOGDIR/authoff_r.log & AOP=$!
for _ in $(seq 1 30); do
  mongosh --port $PALT -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
# Asserted rather than assumed, for the same reason: a control that did not come
# up must say so here rather than three checks later in words about auth modes.
chk "the control router came up on its own port"       "true" \
    "$(mongosh --port $PALT -u dba -p secret --authenticationDatabase admin --quiet \
        --eval 'print(db.adminCommand({ping:1}).ok===1)' 2>&1 | tail -1)"
kill $AOP 2>/dev/null; wait $AOP 2>/dev/null
chk "with it off the line says proxied"                "true" \
    "$(grep 'router listening' $LOGDIR/authoff_r.log | grep -q 'auth=proxied' && echo true || echo false)"
chk "and the warning is not printed"                   "false" \
    "$(grep -q 'authenticating clients itself' $LOGDIR/authoff_r.log && echo true || echo false)"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
