#!/bin/bash
# Query features that depend on something other than the documents.
#
# Most of what the router does assumes the two tiers differ only in which
# documents they hold. That is not quite true. A collection carries a default
# collation, and an index can be the only thing that makes a query answerable at
# all, and neither of those crosses to the archive by itself.
#
# The dangerous half is collation, because it changes which documents match
# rather than how fast they are found. A case-insensitive collection answers one
# way on the fast tier and another on an archive that has no collation, and both
# answers look like success.
CFG=/root/cline.yaml
HOT=${HOT:-'mongodb://dba:secret@172.24.0.6:27017,172.24.0.7:27017,172.24.0.5:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.4:27017,172.24.0.2:27017,172.24.0.3:27017/?replicaSet=rsCold&authSource=admin'}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-52s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-52s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

stopDaemons() {
  pkill -f 'cline feeder|cline router' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder|cline router' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
startFeeder() {
  cline feeder --config $CFG --log-file /var/log/cline/q_f.log & FP=$!
  for _ in $(seq 1 60); do
    [ "$(C 'print(!!db.getSiblingDB("_thermocline").checkpoints.findOne({_id:"test"}))')" = "true" ] && return 0
    sleep 0.5
  done
  echo "  WARN  the feeder never wrote a checkpoint"
}
startRouter() {
  cline router --config $CFG --log-file /var/log/cline/q_r.log & RP=$!
  for _ in $(seq 1 30); do
    mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  FATAL  the router never came up on 27100"; exit 2
}
waitMirrored() {
  local n=""
  for _ in $(seq 1 60); do
    n=$(C "print(db.getSiblingDB('shop').$1.countDocuments({_tcDeletedAt:{\$exists:false}}))")
    [ "$n" = "$2" ] && return 0
    sleep 0.5
  done
  echo "  WARN  shop.$1 never reached $2 mirrored documents, last saw ${n:-none}"
}

echo "== setup =="
stopDaemons
for t in tqcoll tqtext; do
  H "db.getSiblingDB('shop').$t.drop()" >/dev/null
  C "db.getSiblingDB('shop').$t.drop()" >/dev/null
done
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"}); db.getSiblingDB("_thermocline").watermarks.deleteMany({}); db.getSiblingDB("_thermocline").residency.deleteMany({})' >/dev/null
startFeeder
startRouter

echo "== a collection with a case-insensitive default collation =="
# strength 2 ignores case. Three documents, two of which differ only in case, so
# the collation decides whether a query for one finds one document or two.
H "db.getSiblingDB('shop').createCollection('tqcoll', {collation: {locale: 'en', strength: 2}})" >/dev/null
R "db.getSiblingDB('shop').tqcoll.insertMany([{_id:1,name:'Alice'},{_id:2,name:'alice'},{_id:3,name:'BOB'}])" >/dev/null
waitMirrored tqcoll 3
chk "the fast tier collection has a collation"   "en" "$(H "const c=db.getSiblingDB('shop').getCollectionInfos({name:'tqcoll'})[0]; print(c.options.collation?c.options.collation.locale:'none')")"
chk "the archive's copy has none"                "none" "$(C "const c=db.getSiblingDB('shop').getCollectionInfos({name:'tqcoll'})[0]; print(c&&c.options&&c.options.collation?c.options.collation.locale:'none')")"

# With the fast tier answering, the collation applies: 'alice' matches both
# spellings. This is the answer the application already gets today.
cline watermark --config $CFG --ns shop.tqcoll --complete-all >/dev/null
sleep 7
HOTANS=$(R "print(db.getSiblingDB('shop').tqcoll.countDocuments({name:'alice'}))")
chk "the fast tier matches case-insensitively"   "2" "$HOTANS"

# Now the same query, answered by the archive. It must not change its mind about
# which documents match.
cline watermark --config $CFG --ns shop.tqcoll --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
COLDANS=$(R "print(db.getSiblingDB('shop').tqcoll.countDocuments({name:'alice'}))")
chk "and so does the archive"                    "$HOTANS" "$COLDANS"

# An explicit collation on the query is a different matter: it travels with the
# command, so it should work on either tier.
chk "an explicit collation crosses too"          "2" "$(R "print(db.getSiblingDB('shop').tqcoll.find({name:'alice'}).collation({locale:'en',strength:2}).itcount())")"

echo "== a query that needs an index the archive cannot have =="
# A text query is not slow without a text index, it is impossible: mongod
# refuses it outright. The archive is not expected to carry one, so this is a
# case the router cannot make transparent. What it must do is fail clearly
# rather than return nothing and call it success.
H "db.getSiblingDB('shop').tqtext.createIndex({body:'text'})" >/dev/null
R "db.getSiblingDB('shop').tqtext.insertMany([{_id:1,body:'thermocline tiering'},{_id:2,body:'mongodb storage'}])" >/dev/null
waitMirrored tqtext 2
cline watermark --config $CFG --ns shop.tqtext --complete-all >/dev/null
sleep 7
chk "a text search works on the fast tier"       "1" "$(R "print(db.getSiblingDB('shop').tqtext.find({\$text:{\$search:'thermocline'}}).itcount())")"
cline watermark --config $CFG --ns shop.tqtext --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
TEXTANS=$(R "
  try { print('ANSWERED:'+db.getSiblingDB('shop').tqtext.find({\$text:{\$search:'thermocline'}}).itcount()); }
  catch(e) { print('REFUSED'); }")
# Either it is answered correctly or it is refused. Returning zero matches
# quietly would be the wrong answer this whole system exists to prevent.
chk "a text search does not silently return none" "true" \
    "$([ "$TEXTANS" = "REFUSED" ] || [ "$TEXTANS" = "ANSWERED:1" ] && echo true || echo "false ($TEXTANS)")"

echo "== a view over a tiered collection =="
# A view is a saved pipeline over another collection. The router decides whether
# to guard a query by the namespace the query names, and that name is the
# view's, which is not in feeder.namespaces and never could be: a view holds no
# documents of its own to mirror. So the completeness check is skipped and the
# view answers from whatever the fast tier happens to be holding, which is the
# short answer this system exists to prevent.
H "db.getSiblingDB('shop').tqview.drop()" >/dev/null
C "db.getSiblingDB('shop').tqview.drop()" >/dev/null
R "db.getSiblingDB('shop').tqview.insertMany(Array.from({length:10},(_,i)=>({_id:i,n:i,grp:'g'})))" >/dev/null
waitMirrored tqview 10
H "db.getSiblingDB('shop').createView('vqview','tqview',[{\$match:{grp:'g'}}])" >/dev/null
cline watermark --config $CFG --ns shop.tqview --complete-all >/dev/null
sleep 7
chk "the view reads correctly while the tier is whole" "10" "$(R "print(db.getSiblingDB('shop').vqview.countDocuments({}))")"

# Now take the documents out from under it. The view still resolves to the same
# collection, which no longer holds them.
stopFeeder2() {
  pkill -f 'cline feeder' 2>/dev/null
  for _ in $(seq 1 30); do pgrep -f 'cline feeder' >/dev/null 2>&1 || return 0; sleep 0.5; done
}
stopFeeder2
H "db.getSiblingDB('shop').tqview.deleteMany({_id:{\$lt:7}})" >/dev/null
C 'db.getSiblingDB("_thermocline").checkpoints.deleteOne({_id:"test"})' >/dev/null
startFeeder
chk "the fast tier now holds three"                    "3"  "$(H "print(db.getSiblingDB('shop').tqview.countDocuments({}))")"

# The watermark has to stop claiming the range, or the router is right to trust
# the fast tier and every query returns three, view or not. With the claim
# withdrawn, a query on the collection itself falls through to the archive and
# answers ten. The view has to reach the same place.
cline watermark --config $CFG --ns shop.tqview --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
chk "the collection itself answers in full"            "10" "$(R "print(db.getSiblingDB('shop').tqview.countDocuments({}))")"
VIEWANS=$(R "
  try { print(db.getSiblingDB('shop').vqview.countDocuments({})); }
  catch(e) { print('REFUSED'); }")
# Ten or a refusal are both acceptable. Three is the silent short answer.
chk "the view does not answer short"                   "true" \
    "$([ "$VIEWANS" = "10" ] || [ "$VIEWANS" = "REFUSED" ] && echo true || echo "false (got $VIEWANS)")"

stopDaemons
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
