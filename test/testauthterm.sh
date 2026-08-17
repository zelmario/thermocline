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
CFG=/root/cline_auth.yaml
BASE=/root/cline.yaml
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
AS() { mongosh --port 27100 -u "$1" -p "$2" --authenticationDatabase admin --quiet --eval "$3" 2>&1 | tail -1; }
# Reduce any authorization failure to one word, so a check states what it means.
denied() { case "$1" in *"not authorized"*) echo DENIED ;; *) echo "$1" ;; esac; }
refused() { case "$1" in *"thermocline: refusing"*) echo REFUSED ;; *) echo "$1" ;; esac; }

cleanup() { pkill -9 -f 'cline router' 2>/dev/null; pkill -9 -f 'cline feeder' 2>/dev/null; }
trap cleanup EXIT
pkill -f 'cline feeder|cline router' 2>/dev/null; sleep 2

echo "== setup =="
# terminate_auth is not in the shared config, because every other suite runs
# with it off. Derived here so the two cannot drift apart.
sed 's/^  listen:/  terminate_auth: true\n  listen:/' $BASE > $CFG
chk "the config asks for terminated authentication" "1" "$(grep -c 'terminate_auth: true' $CFG)"

# Both tiers, not just the fast one. A collection dropped on the fast tier alone
# leaves the archive holding the previous run's documents, and the router then
# merges them into every answer, which reads as a count that is simply wrong.
H "db.getSiblingDB('payroll').salaries.drop();
   db.getSiblingDB('payroll').salaries.insertMany([{_id:1,who:'a',amount:100},{_id:2,who:'b',amount:200}]);
   db.getSiblingDB('shop').tatrm.drop();
   db.getSiblingDB('shop').tatrm.insertMany([{_id:1,v:'one'},{_id:2,v:'two'}])" >/dev/null
C "db.getSiblingDB('shop').tatrm.drop()" >/dev/null
H "try { db.getSiblingDB('admin').dropUser('limited') } catch(e) {}
   db.getSiblingDB('admin').createUser({user:'limited',pwd:'limpass',roles:[{role:'read',db:'shop'}]})" >/dev/null
H "try { db.getSiblingDB('admin').dropUser('writer') } catch(e) {}
   db.getSiblingDB('admin').createUser({user:'writer',pwd:'writepass',roles:[{role:'readWrite',db:'shop'}]})" >/dev/null

# shop.tatrm is a tiered namespace, so without a completeness claim every range
# read falls through to an archive that has nothing in it and answers zero. That
# is the router working as designed and it has nothing to do with authentication,
# so the claim is stated here and the one section that needs the archive
# withdraws it again.
cline watermark --config $CFG --ns shop.tatrm --complete-all >/dev/null 2>&1

cline router --config $CFG --log-file /var/log/cline/authterm_r.log &
for _ in $(seq 1 30); do
  mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
if ! mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1; then
  echo "  FATAL  the router never came up on 27100"; exit 2
fi

echo "== the router verifies the password itself =="
chk "a correct password authenticates"          "1"  "$(AS dba secret 'print(db.adminCommand({ping:1}).ok)')"
chk "and can read"                              "2"  "$(AS dba secret 'print(db.getSiblingDB("shop").tatrm.countDocuments({}))')"
out=$(AS dba WRONG 'print("GOT IN")')
chk "a wrong password does not"                 "Authentication failed." "$(echo "$out" | sed 's/.*MongoServerError: //')"
out=$(AS nosuchuser whatever 'print("GOT IN")')
chk "and neither does a user that does not exist" "Authentication failed." "$(echo "$out" | sed 's/.*MongoServerError: //')"

echo "== a user reaches its own database and no other =="
chk "limited reads the database it was granted" "2"      "$(AS limited limpass 'print(db.getSiblingDB("shop").tatrm.countDocuments({}))')"
chk "find on another database is refused"       "DENIED" "$(denied "$(AS limited limpass 'print(db.getSiblingDB("payroll").salaries.find().toArray().length)')")"
chk "count on another database is refused"      "DENIED" "$(denied "$(AS limited limpass 'print(db.getSiblingDB("payroll").salaries.count())')")"
chk "distinct on another database is refused"   "DENIED" "$(denied "$(AS limited limpass 'print(db.getSiblingDB("payroll").salaries.distinct("who"))')")"
chk "aggregate on another database is refused"  "DENIED" "$(denied "$(AS limited limpass 'print(db.getSiblingDB("payroll").salaries.aggregate([{$group:{_id:null,t:{$sum:"$amount"}}}]).toArray().length)')")"

echo "== read does not imply write =="
chk "a read only user cannot insert"            "DENIED" "$(denied "$(AS limited limpass 'db.getSiblingDB("shop").tatrm.insertOne({_id:99}); print("INSERTED")')")"
chk "a read only user cannot delete"            "DENIED" "$(denied "$(AS limited limpass 'print(db.getSiblingDB("shop").tatrm.deleteOne({_id:1}).deletedCount)')")"
chk "and the document is still there"           "2"      "$(H 'print(db.getSiblingDB("shop").tatrm.countDocuments({}))')"
chk "a readWrite user can insert"               "3"      "$(AS writer writepass 'db.getSiblingDB("shop").tatrm.insertOne({_id:3,v:"three"}); print(db.getSiblingDB("shop").tatrm.countDocuments({}))')"

echo "== a command whose privilege is unknown is refused, not guessed =="
chk "an unknown command is refused by name"     "REFUSED" "$(refused "$(AS dba secret 'print(JSON.stringify(db.getSiblingDB("shop").runCommand({thisCommandDoesNotExist:1})))')")"

echo "== tiering still works with authentication terminated =="
# The point of the whole system has to survive the change: a document only the
# archive holds must still be found, by a user allowed to see it.
# Withdraw the claim first: while the fast tier says it holds everything, the
# router is right not to look in the archive at all.
cline watermark --config $CFG --ns shop.tatrm --complete-from 2030-01-01T00:00:00Z >/dev/null 2>&1
sleep 6
C "db.getSiblingDB('shop').tatrm.updateOne({_id:1},{\$set:{v:'one'}},{upsert:true})" >/dev/null
H "db.getSiblingDB('shop').tatrm.deleteOne({_id:1})" >/dev/null
chk "an archived document is still served"      "one"    "$(AS dba secret 'const d=db.getSiblingDB("shop").tatrm.findOne({_id:1}); print(d?d.v:"MISSING")')"
chk "and refused to a user without the grant"   "DENIED" "$(denied "$(AS limited limpass 'const d=db.getSiblingDB("payroll").salaries.findOne({_id:1}); print(d?d.who:"MISSING")')")"

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
if [ $FAIL -gt 0 ]; then echo "failed: ${FAILED[*]}"; exit $FAIL; fi
