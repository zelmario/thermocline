#!/bin/bash
# Functional tests of every router behaviour, through the wire protocol.
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
# A database with nothing tiered in it, for the section that checks it is named.
LOSTDB=${DB}_lost; export TC_LOSTDB=$LOSTDB
# Tiered end to end and used by nothing else, so the catalog section can drop
# the whole database on the fast tier.
DB3=${DB3:-catdb}; export TC_DB3=$DB3
P1=$((PORT+1)); P2=$((PORT+2)); P3=$((PORT+3)); PDASH=$((PORT+10)); PALT=$((PORT+17))
CFG=${CFG:-$WDIR/cline.yaml}
HOT=${HOT:-'mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'}
COLD=${COLD:-'mongodb://dba:secret@172.24.0.3:27017,172.24.0.4:27017,172.24.0.2:27017/?replicaSet=rsCold&authSource=admin'}
PASS=0; FAIL=0; declare -a FAILED
chk() { # chk NAME EXPECTED ACTUAL
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-46s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-46s got=%s want=%s\n' "$1" "$3" "$2"; fi
}
R() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }

# routerUp is the same ping R makes, without the pipe, and it exists because a
# wait loop has to test the command's own status.
#
# R ends in `| tail -1`, so a pipeline's status is tail's and is zero however
# badly mongosh failed. Every `do R 'ping' && break` loop in this file therefore
# broke on its first iteration and waited for nothing at all: the router was
# given no time to come up, and whatever ran next met a closed port. On an idle
# box the router wins that race and nothing shows; under a parallel sweep it
# does not, and the result is a write failing into /dev/null and a later check
# reporting something else entirely. Measured: testindexes reported "carol
# reached the archive got=2 want=3" for a router that was still starting.
routerUp() { mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
  --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1; }
H() { mongosh "$HOT" --quiet --eval "$1" 2>&1 | tail -1; }
C() { mongosh "$COLD" --quiet --eval "$1" 2>&1 | tail -1; }

H 'db.getSiblingDB(process.env.TC_DB).tprobe_out.drop()' >/dev/null 2>&1
echo "== setup: clean collections, feeder + router running =="
pkill -f "cline feeder --config $WDIR/|cline router --config $WDIR/"; sleep 2
H 'db.getSiblingDB(process.env.TC_DB).tprobe.drop(); db.getSiblingDB(process.env.TC_DB).tacct.drop()' >/dev/null
C 'db.getSiblingDB(process.env.TC_DB).tprobe.drop(); db.getSiblingDB(process.env.TC_DB).tacct.drop()' >/dev/null
C 'db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({})' >/dev/null
H 'db.getSiblingDB(process.env.TC_META).keys.deleteMany({})' >/dev/null
cline feeder --config $CFG --log-file $LOGDIR/f.log &
FP=$!
cline router --config $CFG --log-file $LOGDIR/r.log &
RP=$!
sleep 6

echo "== hello / handshake =="
chk "presents as mongos (isdbgrid)" "isdbgrid" "$(R 'print(db.hello().msg)')"
chk "hides the replica set name"    "undefined" "$(R 'print(typeof db.hello().setName)')"

echo "== writes and mirroring =="
R 'db.getSiblingDB(process.env.TC_DB).tprobe.insertMany([{_id:1,v:"a"},{_id:2,v:"b"},{_id:3,v:"c"}])' >/dev/null
sleep 4
chk "insert reaches the fast tier"  "3" "$(H 'print(db.getSiblingDB(process.env.TC_DB).tprobe.countDocuments({}))')"
chk "feeder mirrors to the archive" "3" "$(C 'print(db.getSiblingDB(process.env.TC_DB).tprobe.countDocuments({}))')"

echo "== read miss and copy-up =="
H 'db.getSiblingDB(process.env.TC_DB).tprobe.deleteOne({_id:2})' >/dev/null   # feeder will tombstone; use an archive-only doc instead
C 'db.getSiblingDB(process.env.TC_DB).tprobe.insertOne({_id:9,v:"archived-only"})' >/dev/null
chk "point lookup finds an archived doc" "archived-only" "$(R 'const d=db.getSiblingDB(process.env.TC_DB).tprobe.findOne({_id:9}); print(d?d.v:"MISSING")')"
sleep 2
chk "and it was copied up"          "archived-only" "$(H 'const d=db.getSiblingDB(process.env.TC_DB).tprobe.findOne({_id:9}); print(d?d.v:"MISSING")')"

echo "== a point read is the whole query, not just the _id =="
# An _id equality names at most one document, which is why a point lookup skips
# the completeness guard. It never justified answering a different question, and
# the miss used to be resolved by reading the document by its _id and handing it
# back: the rest of the filter, the projection and the skip all went missing.
C 'db.getSiblingDB(process.env.TC_DB).tprobe.insertOne({_id:11,v:"untouched"})' >/dev/null
chk "a predicate the document fails returns nothing" "null" "$(R 'const d=db.getSiblingDB(process.env.TC_DB).tprobe.findOne({_id:11,v:"nosuchvalue"}); print(d?d.v:null)')"
sleep 2
# And nothing was promoted. The copy-up exists because the client just read the
# document, so it is hot; a read that returned no rows leaves nothing hot, and
# copying up anyway writes into the fast tier for nobody and announces it on
# every change stream watching the collection.
chk "and the fast tier was left alone"               "MISSING" "$(H 'const d=db.getSiblingDB(process.env.TC_DB).tprobe.findOne({_id:11}); print(d?d.v:"MISSING")')"
chk "a projection is applied to it"                  "1"       "$(R 'const d=db.getSiblingDB(process.env.TC_DB).tprobe.findOne({_id:11},{_id:0,v:1}); print(d?Object.keys(d).length:"MISSING")')"
C 'db.getSiblingDB(process.env.TC_DB).tprobe.insertOne({_id:12,v:"skipme"})' >/dev/null
chk "a skip past the only match returns nothing"     "0"       "$(R 'print(db.getSiblingDB(process.env.TC_DB).tprobe.find({_id:12}).skip(1).toArray().length)')"
# batchSize zero asks for a cursor and no documents yet, which drivers do. The
# hand-built reply put the document in the first batch regardless.
C 'db.getSiblingDB(process.env.TC_DB).tprobe.insertOne({_id:13,v:"cursored"})' >/dev/null
# Both commands go through one explicit session, and that is not tidiness. Two
# separate runCommand calls each take an implicit session from the driver's
# pool, and a getMore from a different session than the one that opened the
# cursor is refused -- by mongod and by the router alike. The pool usually hands
# back the same session, so this passed for months and then failed under a
# parallel sweep with "cursor id ... was not created by the authenticated user
# or was created in another session", which reads exactly like a router defect
# and is the driver doing what it says it does.
chk "a cursor asked for with no first batch"         "0/cursored" "$(R 'const s=db.getMongo().startSession();
  const d=s.getDatabase(process.env.TC_DB);
  const c=d.runCommand({find:"tprobe",filter:{_id:13},batchSize:0});
  const n=c.cursor.firstBatch.length;
  const m=d.runCommand({getMore:c.cursor.id,collection:"tprobe"});
  print(n+"/"+((m.cursor&&m.cursor.nextBatch.length)?m.cursor.nextBatch[0].v:"NOTHING"));
  s.endSession()')"

echo "== write miss and copy-up =="
C 'db.getSiblingDB(process.env.TC_DB).tprobe.insertOne({_id:10,v:"old"})' >/dev/null
chk "update on an archived doc matches 1" "1" "$(R 'print(db.getSiblingDB(process.env.TC_DB).tprobe.updateOne({_id:10},{$set:{v:"new"}}).matchedCount)')"
chk "and the fast tier now holds it"      "new" "$(H 'const d=db.getSiblingDB(process.env.TC_DB).tprobe.findOne({_id:10}); print(d?d.v:"MISSING")')"

echo "== tombstones are not resurrected =="
R 'db.getSiblingDB(process.env.TC_DB).tprobe.deleteOne({_id:1})' >/dev/null
sleep 4
chk "archive keeps a deleted doc"   "true"  "$(C 'print(!!db.getSiblingDB(process.env.TC_DB).tprobe.findOne({_id:1}))')"
chk "but marks it deleted"          "true"  "$(C 'print(!!db.getSiblingDB(process.env.TC_DB).tprobe.findOne({_id:1})._tcDeletedAt)')"
chk "and the router will not return it" "null" "$(R 'print(db.getSiblingDB(process.env.TC_DB).tprobe.findOne({_id:1}))')"

echo "== completeness: archive fallback =="
cline watermark --config $CFG --ns $DB.tprobe --complete-from 2030-01-01T00:00:00Z >/dev/null
sleep 7
LIVE=$(C 'print(db.getSiblingDB(process.env.TC_DB).tprobe.countDocuments({_tcDeletedAt:{$exists:false}}))')
chk "countDocuments matches the archive" "$LIVE" "$(R 'print(db.getSiblingDB(process.env.TC_DB).tprobe.countDocuments({}))')"
chk "find itcount matches"               "$LIVE" "$(R 'print(db.getSiblingDB(process.env.TC_DB).tprobe.find({}).itcount())')"
chk "sort+skip+limit works"              "1"     "$(R 'print(db.getSiblingDB(process.env.TC_DB).tprobe.find({}).sort({_id:1}).skip(1).limit(1).itcount())')"

echo "== cursor paging through the archive =="
chk "batched find returns every doc" "$LIVE" "$(R 'let n=0; const c=db.getSiblingDB(process.env.TC_DB).tprobe.find({}).batchSize(2); while(c.hasNext()){c.next();n++} print(n)')"

echo "== whose cursor is it =="
# A cursor over an archive answer belongs to this router, and it is served from a
# registry shared by every connection, because a real MongoDB cursor is
# server-scoped and a driver's pool sends the getMore from whichever connection
# is free. What has to scope it is what scopes it in mongod: the user, the
# session, and the collection named.
#
# The ids used to run in sequence from 2^62, so the first was always 2^62+1, and
# nothing else was checked. A second connection could name it and be handed the
# first client's documents. See test/cursorowner for the client that found it.
CID=$(R 'const r=db.getSiblingDB(process.env.TC_DB).runCommand({find:"tprobe",filter:{},batchSize:2}); print(r.cursor.id.toString())')
chk "the router owns this cursor"          "true"     "$(awk -v id="$CID" 'BEGIN{print (id+0 > 4611686018427387904) ? "true" : "false"}')"
# A second mongosh is a second connection and a second session, which is exactly
# what mongod refuses: "for cursors created inside a session, you cannot call
# getMore outside the session".
# mongosh throws on ok:0 rather than returning the reply, so the code has to be
# read off the exception. A check written without this catch can never pass, and
# reads as the product failing.
STEAL=$(R "try{ const r=db.getSiblingDB(process.env.TC_DB).runCommand({getMore:NumberLong('$CID'),collection:'tprobe'});
             print(r.ok===1?'READ '+r.cursor.nextBatch.length:r.code+' '+r.codeName) }
           catch(e){ print(e.code+' '+e.codeName) }")
chk "another session cannot read it"       "13 Unauthorized" "$STEAL"
# And the collection has to be the cursor's. mongod has refused a mismatch since
# at least 6.0, measured.
WRONGNS=$(R "const r=db.getSiblingDB(process.env.TC_DB).runCommand({find:'tprobe',filter:{},batchSize:2});
             try{ const g=db.getSiblingDB(process.env.TC_DB).runCommand({getMore:r.cursor.id,collection:'tprobeXX'});
                  print(g.ok===1?'READ':g.code+' '+g.codeName) }
             catch(e){ print(e.code+' '+e.codeName) }")
chk "a getMore naming another collection"  "13 Unauthorized" "$WRONGNS"
# A cursor id nobody minted: an expired one, or one another router opened. The
# router cannot tell those apart, because a cursor id says nothing about who
# minted it, so it says both and how to rule one out. The limitation this
# closes was written as "it cannot tell you more than that", and what it could
# not tell was *which*, not *anything*.
LOST=$(R "try{ const r=db.getSiblingDB(process.env.TC_DB).runCommand({getMore:NumberLong('1234567890123456'),collection:'tprobe'});
             print(r.code + ' ' + ((r.errmsg||'').indexOf('another router opened') >= 0 ? 'EXPLAINED' : 'BARE')) }
           catch(e){ print(e.code + ' ' + ((e.message||'').indexOf('another router opened') >= 0 ? 'EXPLAINED' : 'BARE')) }")
chk "an unknown cursor id is explained"    "43 EXPLAINED" "$LOST"
# The control: one client, one session, its own cursor, paged to the end. This is
# the case the three checks above must not have broken.
# Says what went wrong rather than -1. A paging failure here is worth
# distinguishing from a wrong count, and the first version of this collapsed
# every error to the same number, which said nothing about whether the cursor
# had been refused, had expired, or the router had gone away.
# In one explicit session, not two implicit ones. find and getMore issued as
# separate runCommand calls each get their own implicit session from the
# driver's pool, and the pool usually hands back the same one, which is why this
# passed for months and failed twice under a parallel sweep. mongod enforces the
# same rule the router does: a getMore must come from the session that created
# the cursor. Two runCommands do not guarantee that, so the check was asking for
# something no driver promises.
OWN=$(R "let n=0; const s=db.getMongo().startSession();
         const db2=s.getDatabase(process.env.TC_DB);
         let r=db2.runCommand({find:'tprobe',filter:{},batchSize:2});
         n+=r.cursor.firstBatch.length;
         let id=r.cursor.id;
         let err='';
         while(id.toString()!=='0'){ let g;
           try{ g=db2.runCommand({getMore:id,collection:'tprobe',batchSize:2}) }
           catch(e){ err='threw after '+n+': '+String(e.message).substring(0,60); break }
           if(g.ok!==1){ err='not ok after '+n+': '+g.code+' '+g.codeName; break }
           n+=g.cursor.nextBatch.length; id=g.cursor.id }
         s.endSession();
         print(err || n)")
chk "its own session pages to the end"     "$LIVE" "$OWN"

echo "== the archive's sense of time must not follow the reply out =="
# $clusterTime and operationTime describe the cluster that produced them, and
# the two tiers are different clusters with different signing keys. A driver
# gossips the $clusterTime it is given back on its next command, so an archive
# reply passed through whole leaves the session speaking the archive's time to
# the fast tier, which answers "No keys found for HMAC that is valid for time".
#
# The failure lands on the *next* command, which is what makes it hard to see:
# the count is correct and then an unrelated command fails. count and distinct
# were the two that forwarded the archive's reply verbatim; find and aggregate
# build their own, which is why only these two ever did it.
cline watermark --config $CFG --ns $DB.tprobe --clear >/dev/null 2>&1
LEAK=$(R "const d=db.getSiblingDB(process.env.TC_DB);
  function after(label, f) {
    try { f() } catch(e) { return label+':fallback-failed' }
    try { d.tprobe.find({_id:'no-such'}).itcount(); return '' }
    catch(e) { return label+':'+String(e.message).slice(0,40) }
  }
  const bad = [
    after('count', ()=>d.tprobe.estimatedDocumentCount()),
    after('distinct', ()=>d.tprobe.distinct('n').length),
    after('find', ()=>d.tprobe.find({n:{\$gte:0}}).itcount()),
    after('aggregate', ()=>d.tprobe.countDocuments({})),
  ].filter(x=>x);
  print(bad.length ? bad.join(' | ') : 'none')")
chk "no fallback leaves the session broken"        "none" "$LEAK"

echo "== unique index across tiers =="
H 'db.getSiblingDB(process.env.TC_DB).tacct.drop(); db.getSiblingDB(process.env.TC_DB).tacct.createIndex({email:1},{unique:true})' >/dev/null
H 'db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:process.env.TC_DB+".tacct"})' >/dev/null
R 'db.getSiblingDB(process.env.TC_DB).tacct.insertOne({_id:1,email:"z@x.com"})' >/dev/null
sleep 4
# Simulate an eviction rather than a delete. Deleting through the router would
# make the feeder tombstone the archive copy, which frees the key correctly and
# tests the opposite thing. A real eviction leaves the archive copy live, so the
# feeder must not see this removal at all.
kill $FP 2>/dev/null; wait $FP 2>/dev/null
H 'db.getSiblingDB(process.env.TC_DB).tacct.deleteOne({_id:1})' >/dev/null
C 'db.getSiblingDB(process.env.TC_META).checkpoints.deleteOne({_id:"test"})' >/dev/null
cline feeder --config $CFG --log-file $LOGDIR/f.log & FP=$!
sleep 5
chk "archive still holds the original"     "true"  "$(C 'const d=db.getSiblingDB(process.env.TC_DB).tacct.findOne({_id:1}); print(!!d && !d._tcDeletedAt)')"
chk "duplicate of an evicted key rejected" "11000" "$(R 'try{db.getSiblingDB(process.env.TC_DB).tacct.insertOne({_id:2,email:"z@x.com"});print("ACCEPTED")}catch(e){print(e.code||e.message.substring(0,20))}')"
chk "a fresh key is still accepted"        "ok"    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tacct.insertOne({_id:3,email:"new@x.com"});print("ok")}catch(e){print(e.code)}')"
# And the other half: once the original is genuinely deleted the key frees up.
C 'db.getSiblingDB(process.env.TC_DB).tacct.updateOne({_id:1},{$set:{_tcDeletedAt:new Date()}})' >/dev/null
chk "a deleted original frees its key"     "ok"    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tacct.insertOne({_id:4,email:"z@x.com"});print("ok")}catch(e){print(e.code)}')"

echo "== bulk insert keeps mongod semantics =="
H 'db.getSiblingDB(process.env.TC_DB).tacct.drop(); db.getSiblingDB(process.env.TC_DB).tacct.createIndex({email:1},{unique:true})' >/dev/null
H 'db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:process.env.TC_DB+".tacct"})' >/dev/null
R 'db.getSiblingDB(process.env.TC_DB).tacct.insertOne({_id:"seed",email:"taken@x.com"})' >/dev/null
R 'try{db.getSiblingDB(process.env.TC_DB).tacct.insertMany([{_id:"b1",email:"n1@x.com"},{_id:"b2",email:"taken@x.com"},{_id:"b3",email:"n3@x.com"}],{ordered:false})}catch(e){}' >/dev/null
chk "unordered inserts the good documents" "true,false,true" "$(H 'print(["b1","b2","b3"].map(i=>!!db.getSiblingDB(process.env.TC_DB).tacct.findOne({_id:i})).join(","))')"
H 'db.getSiblingDB(process.env.TC_DB).tacct.deleteMany({_id:{$in:["b1","b3"]}})' >/dev/null
H 'db.getSiblingDB(process.env.TC_META).keys.deleteMany({ns:process.env.TC_DB+".tacct"})' >/dev/null
R 'db.getSiblingDB(process.env.TC_DB).tacct.insertOne({_id:"seed2",email:"taken2@x.com"})' >/dev/null
R 'try{db.getSiblingDB(process.env.TC_DB).tacct.insertMany([{_id:"o1",email:"p1@x.com"},{_id:"o2",email:"taken2@x.com"},{_id:"o3",email:"p3@x.com"}],{ordered:true})}catch(e){}' >/dev/null
chk "ordered stops at the first clash" "true,false,false" "$(H 'print(["o1","o2","o3"].map(i=>!!db.getSiblingDB(process.env.TC_DB).tacct.findOne({_id:i})).join(","))')"

echo "== transactions =="
C 'db.getSiblingDB(process.env.TC_DB).tprobe.insertOne({_id:20,v:"txn"})' >/dev/null
chk "transaction replays after a copy-up" "1" "$(R 'const s=db.getMongo().startSession(); let a=0; try{ s.withTransaction(()=>{a++; const r=s.getDatabase(process.env.TC_DB).tprobe.updateOne({_id:20},{$set:{v:"txn2"}}); if(r.matchedCount!==1) throw new Error("miss");}); print(a>1?1:0);}catch(e){print("FAILED")} s.endSession()')"

echo "== \$out runs against the archive =="
# The input reaches past what the fast tier holds, so it is read from the
# archive and the write stage is performed on the fast tier, where the
# application will look for the result.
chk "\$out on incomplete input runs"    "ok"    "$(R 'try{db.getSiblingDB(process.env.TC_DB).tprobe.aggregate([{$group:{_id:null,n:{$sum:1}}},{$out:"tprobe_out"}]).toArray(); print("ok")}catch(e){print(e.message.substring(0,50))}')"
chk "and the result is on the fast tier" "true"  "$(H 'print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().includes("tprobe_out"))')"
# Recomputed here, not reused from earlier: the transaction section above adds
# a document to the archive, so the count taken before it is already stale.
NOWLIVE=$(C 'print(db.getSiblingDB(process.env.TC_DB).tprobe.countDocuments({_tcDeletedAt:{$exists:false}}))')
chk "and it counted every document"      "$NOWLIVE" "$(H 'const d=db.getSiblingDB(process.env.TC_DB).tprobe_out.findOne({}); print(d?d.n:"MISSING")')"

kill $RP 2>/dev/null; wait $RP 2>/dev/null

echo "== a view the archive has never heard of =="
# A view created through the router is given to the archive as it is created. Two
# kinds are not: one that predates that being true, and one an operator made
# straight on the fast tier, which is what the adoption runbook tells them to do.
# Both matter twice over. Repointing the application at the archive is what
# rollback is, and a view the archive has never heard of is an error there. And a
# read through a view the fast tier cannot answer goes to the archive only when
# the archive holds the same view.
VNS=tprobe
H "db.getSiblingDB(process.env.TC_DB).vstart.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).vstart.drop()" >/dev/null
H "db.getSiblingDB(process.env.TC_DB).createView('vstart','$VNS',[{\$match:{n:{\$exists:true}}}])" >/dev/null
chk "the archive does not have it"        "none" \
    "$(C "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'vstart'})[0]; print(c?c.type:'none')")"
# A router starting is what notices. Its own port, so the one this suite has been
# using stays up for everything after this.
cline router --config $CFG --listen 0.0.0.0:$P3 --log-file $LOGDIR/rv.log &
VP=$!; sleep 6
chk "a router starting gives it to the archive" "view" \
    "$(C "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'vstart'})[0]; print(c?c.type:'none')")"
chk "and reads the same collection it did here" "$VNS" \
    "$(C "const c=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'vstart'})[0]; print(c&&c.options?c.options.viewOn:'none')")"
kill $VP 2>/dev/null; wait $VP 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).vstart.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).vstart.drop()" >/dev/null

echo "== completeness: strict =="
sed 's/completeness: archive/completeness: strict/' $CFG > $WDIR/strict.yaml
cline router --config $WDIR/strict.yaml --listen 0.0.0.0:$P1 --log-file $LOGDIR/rs.log &
SP=$!; sleep 5
S() { mongosh --port $P1 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
chk "strict refuses an incomplete find" "refused" "$(S 'try{db.getSiblingDB(process.env.TC_DB).tprobe.find({}).itcount(); print("ALLOWED")}catch(e){print(e.message.match(/refusing/)?"refused":"other")}')"
kill $SP 2>/dev/null; wait $SP 2>/dev/null

echo "== completeness: permissive =="
sed 's/completeness: archive/completeness: permissive/' $CFG > $WDIR/perm.yaml
cline router --config $WDIR/perm.yaml --listen 0.0.0.0:$P2 --log-file $LOGDIR/rp.log &
PP=$!; sleep 5
P() { mongosh --port $P2 -u dba -p secret --authenticationDatabase admin --quiet --eval "$1" 2>&1 | tail -1; }
HOTN=$(H 'print(db.getSiblingDB(process.env.TC_DB).tprobe.countDocuments({}))')
chk "permissive returns the fast tier answer" "$HOTN" "$(P 'print(db.getSiblingDB(process.env.TC_DB).tprobe.find({}).itcount())')"
kill $PP 2>/dev/null; wait $PP 2>/dev/null

echo "== a collection nothing tiers is the fast tier's alone =="
# Adoption puts every collection on the archive, because the archive is the
# cluster the application already had, and only the tiered ones are ever filled
# into the new fast tier. tuntier is deliberately absent from feeder.namespaces,
# so it is what one of those left-behind collections looks like.
#
# The point is that all reads agree. A lookup by exact id skips the completeness
# guard, because a point read has no completeness question, and it used to skip
# the tiering question with it: measured, findOne returned an archived document
# from an untiered collection and copied it up, while countDocuments on the same
# collection answered zero. The copy-up was the worse half, writing into a
# collection nothing manages and the feeder will never mirror, so the two tiers
# disagreed about it from then on.
# The collection has to exist on the archive before the router starts, because
# the warning is a startup reading of what each tier holds. Written the other
# way round the check failed for a reason that had nothing to do with the
# product.
H "db.getSiblingDB(process.env.TC_DB).tuntier.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tuntier.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).tuntier.insertMany([{_id:'p1',name:'a kettle'},{_id:'p2',name:'a toaster'}])" >/dev/null
cline router --config $CFG --log-file $LOGDIR/unt_r.log & UP=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
chk "a count sees the fast tier's answer"     "0" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).tuntier.countDocuments({}))")"
chk "and a lookup by _id agrees with it"      "NOTHING" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB).tuntier.findOne({_id:'p1'}); print(d?d.name:'NOTHING')")"
sleep 2
chk "nothing was copied into it"              "0" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).tuntier.countDocuments({}))")"
chk "and the archive still has it"            "2" \
    "$(C "print(db.getSiblingDB(process.env.TC_DB).tuntier.countDocuments({}))")"
C "db.getSiblingDB(process.env.TC_DB).tuntier.drop()" >/dev/null
kill $UP 2>/dev/null; wait $UP 2>/dev/null
# Correct and silent is the wrong pair: the application sees an empty collection
# where its catalogue used to be, so the router names it at startup.
chk "the router named it at startup"          "true" \
    "$(grep -q 'the archive holds collections the fast tier does not' $LOGDIR/unt_r.log && echo true || echo false)"

echo "== a database nothing tiers is not empty, it is absent =="
# The stranded check above works within a database that already has something
# tiered in it. It used to derive the whole list of databases to compare from
# feeder.namespaces, so a database with nothing tiered was never looked at, and
# that is the worse case rather than a rarer one: every collection in it is
# invisible, and listDatabases and listCollections are answered by the fast tier
# too, so it does not show up as empty, it does not show up.
#
# Found on MongoDB's own sample datasets adopted as an archive. Four collections
# tiered across four databases: startup named eleven stranded collections and
# said nothing at all about five whole databases holding 44,000 documents.
C "db.getSiblingDB(process.env.TC_LOSTDB).catalogue.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_LOSTDB).catalogue.insertMany([{_id:'k1',name:'a kettle'},{_id:'k2',name:'a toaster'}])" >/dev/null
rm -f $LOGDIR/lost_r.log
cline router --config $CFG --log-file $LOGDIR/lost_r.log & LP=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
chk "the application sees the collection empty" "0" \
    "$(R "print(db.getSiblingDB(process.env.TC_LOSTDB).catalogue.countDocuments({}))")"
chk "and the database is not even listed"       "false" \
    "$(R "print(db.adminCommand({listDatabases:1}).databases.some(function(d){return d.name===process.env.TC_LOSTDB}))")"
chk "while the archive still holds it"          "2" \
    "$(C "print(db.getSiblingDB(process.env.TC_LOSTDB).catalogue.countDocuments({}))")"
kill $LP 2>/dev/null; wait $LP 2>/dev/null
chk "the router named the stranded collection"  "true" \
    "$(grep -q "$LOSTDB.catalogue" $LOGDIR/lost_r.log && echo true || echo false)"
chk "and named the database as having no trace" "true" \
    "$(grep "no trace of these databases" $LOGDIR/lost_r.log | grep -q "$LOSTDB" && echo true || echo false)"
C "db.getSiblingDB(process.env.TC_LOSTDB).dropDatabase()" >/dev/null

echo "== another deployment's bookkeeping, with one of MongoDB's own beside it =="
# The exclusion above is for another Thermocline's bookkeeping database, which
# must not be reported as an application's data: the advice that follows is to
# tier it, and following it would mean tiering somebody's checkpoints.
#
# It is recognised by content, and the test was every collection in the
# database being one this system writes. MongoDB's own collections are not, so
# one of them appearing was enough to lose the exclusion. Profiling is all it
# takes: system.profile is created the moment it is switched on, and the router
# then names another deployment's checkpoints, watermarks and residency as
# stranded application data. The same loop skips system collections four lines
# further down, which is what makes this an inconsistency rather than a
# judgement call.
BKDB=${TC_LOSTDB}bk
C "db.getSiblingDB('$BKDB').dropDatabase()" >/dev/null
C "db.getSiblingDB('$BKDB').checkpoints.insertOne({_id:'other'});
   db.getSiblingDB('$BKDB').watermarks.insertOne({_id:'other.ns'});
   db.getSiblingDB('$BKDB').residency.insertOne({_id:'other.ns|1'})" >/dev/null
rm -f $LOGDIR/bk_r.log
cline router --config $CFG --log-file $LOGDIR/bk_r.log & BKP=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
kill $BKP 2>/dev/null; wait $BKP 2>/dev/null
chk "plain bookkeeping is left alone"           "false" \
    "$(grep -q "$BKDB" $LOGDIR/bk_r.log && echo true || echo false)"
# Profiling on, which is the whole difference.
C "db.getSiblingDB('$BKDB').setProfilingLevel(2)" >/dev/null
chk "system.profile is there now"               "true" \
    "$(C "print(db.getSiblingDB('$BKDB').getCollectionNames().indexOf('system.profile') >= 0)")"
rm -f $LOGDIR/bk2_r.log
cline router --config $CFG --log-file $LOGDIR/bk2_r.log & BKQ=$!
for _ in $(seq 1 30); do
  mongosh --port $PORT -u dba -p secret --authenticationDatabase admin --quiet \
    --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
  sleep 1
done
kill $BKQ 2>/dev/null; wait $BKQ 2>/dev/null
chk "and it is still left alone once profiled"  "false" \
    "$(grep -q "$BKDB" $LOGDIR/bk2_r.log && echo true || echo false)"
C "db.getSiblingDB('$BKDB').setProfilingLevel(0)" >/dev/null
C "db.getSiblingDB('$BKDB').dropDatabase()" >/dev/null

echo "== accounts and roles the fast tier does not have =="
# The router authenticates clients against the FAST tier, and that replica set
# was new at adoption: its accounts were created by hand and come from nowhere
# else. An account the archive has and the fast tier does not cannot log in, and
# what the application sees is an ordinary authentication failure naming
# nothing.
#
# This is also the check docs/operations.md now sends an operator to after
# restoring a backup, because accounts are the one thing on the fast tier that
# nothing rebuilds: collection options and indexes come back from the archive,
# the key registry from `cline keys --rebuild`, the documents from warming.
# Nothing covered the warning at all -- removing it outright left every test in
# the repository passing.
C "db.getSiblingDB('admin').dropUser('tcrestore')" >/dev/null 2>&1
C "db.getSiblingDB('admin').dropRole('tcrestorerole')" >/dev/null 2>&1
C "db.getSiblingDB('admin').createRole({role:'tcrestorerole', privileges:[], roles:[]})" >/dev/null 2>&1
C "db.getSiblingDB('admin').createUser({user:'tcrestore', pwd:'tcrestore', roles:[{role:'read', db:process.env.TC_DB}]})" >/dev/null 2>&1
rm -f $LOGDIR/acct_r.log
cline router --config $CFG --log-file $LOGDIR/acct_r.log & ARP=$!
for _ in $(seq 1 30); do routerUp && break; sleep 1; done
kill $ARP 2>/dev/null; wait $ARP 2>/dev/null
chk "the router names the account by name"      "true" \
    "$(grep -q 'tcrestore@admin' $LOGDIR/acct_r.log && echo true || echo false)"
chk "and says the application sees a login failure" "true" \
    "$(grep -q 'accounts the fast tier does not' $LOGDIR/acct_r.log && echo true || echo false)"
chk "the role is named too"                     "true" \
    "$(grep -q 'tcrestorerole' $LOGDIR/acct_r.log && echo true || echo false)"
# And preflight says the same without starting anything, which is what the
# restore procedure asks for.
chk "preflight reports it without a router"     "true" \
    "$(cline preflight --config $CFG 2>&1 | grep -q 'tcrestore@admin' && echo true || echo false)"
C "db.getSiblingDB('admin').dropUser('tcrestore')" >/dev/null 2>&1
C "db.getSiblingDB('admin').dropRole('tcrestorerole')" >/dev/null 2>&1
# Quiet once the tiers agree again, or the warning is noise on every start.
rm -f $LOGDIR/acct2_r.log
cline router --config $CFG --log-file $LOGDIR/acct2_r.log & ARQ=$!
for _ in $(seq 1 30); do routerUp && break; sleep 1; done
kill $ARQ 2>/dev/null; wait $ARQ 2>/dev/null
chk "and says nothing about it once it is gone" "false" \
    "$(grep -q 'tcrestore' $LOGDIR/acct2_r.log && echo true || echo false)"

echo "== a read preference on cold.uri, which does not do what it looks like =="
# The natural thing to reach for when adopting a large cluster: during the
# adoption most reads fall through to the archive, which is the production
# cluster being relieved, so sending them to its secondaries looks obvious.
#
# It is ignored, because archive reads are issued as commands and the driver
# runs a command against the primary whatever the client's preference says.
# Measured with secondaryPreferred on cold.uri: the fallback read was served by
# the archive's primary anyway. Keeping it ignored is deliberate, because the
# freshness barrier waits for mirroring to reach the archive's primary. Being
# silent about it was not.
sed 's|\(uri: "mongodb://[^"]*rsCold[^"]*\)"|\1\&readPreference=secondaryPreferred"|' $CFG > $WDIR/coldpref.yaml
chk "the derived config asks for secondaries"      "1" \
    "$(grep -c 'readPreference=secondaryPreferred' $WDIR/coldpref.yaml)"
rm -f $LOGDIR/coldpref.log
cline router --config $WDIR/coldpref.yaml --log-file $LOGDIR/coldpref.log & CP=$!
for _ in $(seq 1 30); do grep -q "router listening" $LOGDIR/coldpref.log 2>/dev/null && break; sleep 1; done
kill $CP 2>/dev/null; wait $CP 2>/dev/null
chk "the router says it is ignored"                "true" \
    "$(grep -q 'read preference on cold.uri is ignored' $LOGDIR/coldpref.log && echo true || echo false)"
chk "and says a secondary could miss a write"      "true" \
    "$(grep -q 'miss a write that has already been acknowledged' $LOGDIR/coldpref.log && echo true || echo false)"
# And it must not say it about an ordinary config, or it becomes noise.
rm -f $LOGDIR/coldplain.log
cline router --config $CFG --log-file $LOGDIR/coldplain.log & CQ=$!
for _ in $(seq 1 30); do grep -q "router listening" $LOGDIR/coldplain.log 2>/dev/null && break; sleep 1; done
kill $CQ 2>/dev/null; wait $CQ 2>/dev/null
chk "and stays quiet when nothing asks for it"     "0" \
    "$(grep -c 'read preference on cold.uri' $LOGDIR/coldplain.log || true)"


echo "== a tiered collection the fast tier does not hold is still in the catalog =="
# listCollections is answered by the fast tier. For a tiered collection that is
# a partial view: the fast tier holds the recent part, the archive holds the
# rest, and a read is answered from whichever has the documents. The catalog is
# not merged that way, so a tiered collection with no copy on the fast tier is
# reported as not existing, while reads of it work perfectly.
#
# That is not an exotic state, it is what an adoption starts in. Found on the
# pilot: sample_supplies.sales answered countDocuments 5000 and findOne
# correctly while "show collections" came back empty. Anything that finds its
# work by asking what exists finds nothing, and it reads as data loss.
CATNS=tcatalog
H "db.getSiblingDB(process.env.TC_DB).$CATNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$CATNS.drop()" >/dev/null
cline router --config $CFG >/dev/null 2>&1 & CATR=$!
for _ in $(seq 1 30); do routerUp && break; sleep 1; done
# In the archive only, which is where an unwarmed tiered collection lives.
C "db.getSiblingDB(process.env.TC_DB).$CATNS.insertMany([{_id:1,n:'a'},{_id:2,n:'b'},{_id:3,n:'c'}])" >/dev/null
chk "the fast tier does not have it"               "false" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().indexOf('$CATNS') >= 0)")"
# The control. Without this a listing check passes on a router that cannot read
# it either, and says nothing about the catalog.
chk "the router reads it anyway"                   "3" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$CATNS.countDocuments({}))")"
chk "and names it in the catalog"                  "true" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().indexOf('$CATNS') >= 0)")"
chk "with a filter on the name too"                "1" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).runCommand({listCollections:1,filter:{name:'$CATNS'}}).cursor.firstBatch.length)")"
chk "and under nameOnly"                           "1" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).runCommand({listCollections:1,nameOnly:true,filter:{name:'$CATNS'}}).cursor.firstBatch.length)")"
chk "it is not listed twice once both tiers have it" "1" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).runCommand({listCollections:1,filter:{name:'tprobe'}}).cursor.firstBatch.length)")"

# Seeing the gap is also what repairs it: the router makes the collection on the
# fast tier with the archive's options and indexes, which is what lets it be
# asked for its stats. Bounded well past the indexer's own retry interval.
for _ in $(seq 1 40); do
  [ "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().indexOf('$CATNS') >= 0)")" = "true" ] && break
  R "db.getSiblingDB(process.env.TC_DB).getCollectionNames()" >/dev/null 2>&1
  sleep 3
done
chk "the fast tier ends up with the collection"    "true" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().indexOf('$CATNS') >= 0)")"
chk "so stats answers, and for the whole thing"    "3" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$CATNS.stats().count)")"

echo "== the commands that describe a collection, not the ones that read it =="
# Reads of a tiered collection the fast tier has never held work, because they
# fall through to the archive. The commands that ask *about* the collection go to
# the fast tier alone, and it answers that the collection does not exist.
#
# Measured before this, with the router running throughout and nothing having
# listed the catalog: countDocuments 6, find 6, distinct 6, and then
# db.coll.stats() "Unable to retrieve storageStats in $collStats",
# getIndexes() "ns does not exist", validate() "does not exist to validate",
# collMod "ns does not exist". The collStats *command* answered, because mongod
# is lenient about that one form and replies with a count of nought, which this
# router then corrects. So two spellings of one question and the one an operator
# types by hand was the one that failed.
#
# Its own collection, because a catalog listing is what used to repair this and
# the section above has just done one.
CAT2=tcatalog2
H "db.getSiblingDB(process.env.TC_DB).$CAT2.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$CAT2.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).createCollection('$CAT2',{collation:{locale:'en',strength:2}});
   db.getSiblingDB(process.env.TC_DB).$CAT2.createIndex({n:1});
   db.getSiblingDB(process.env.TC_DB).$CAT2.insertMany([{_id:1,n:'a'},{_id:2,n:'b'},{_id:3,n:'c'}])" >/dev/null
chk "the fast tier does not have it"               "false" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().indexOf('$CAT2') >= 0)")"
# The control again: a check that it describes the collection says nothing if the
# router cannot read it either.
chk "the router reads it"                          "3" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).$CAT2.countDocuments({}))")"
chk "getIndexes answers rather than denying it"    "_id_" \
    "$(R "try{print(db.getSiblingDB(process.env.TC_DB).$CAT2.getIndexes().map(function (i) { return i.name })[0])}catch(e){print('ERROR '+e.message.substring(0,40))}")"
chk "db.coll.stats() answers, for the whole thing" "3" \
    "$(R "try{print(db.getSiblingDB(process.env.TC_DB).$CAT2.stats().count)}catch(e){print('ERROR '+e.message.substring(0,40))}")"
chk "validate runs"                                "true" \
    "$(R "try{print(db.getSiblingDB(process.env.TC_DB).$CAT2.validate().ok === 1)}catch(e){print('ERROR '+e.message.substring(0,40))}")"
chk "collMod runs"                                 "1" \
    "$(R "try{print(db.getSiblingDB(process.env.TC_DB).runCommand({collMod:'$CAT2',validationLevel:'off'}).ok)}catch(e){print('ERROR '+e.message.substring(0,40))}")"
# Asking is what repairs it, and the repair is the archive's collection rather
# than a bare one: the collation decides which documents match and cannot be
# added afterwards, and the indexes are what the application's queries use.
chk "the fast tier has the archive's collation"    "en/2" \
    "$(H "const o=db.getSiblingDB(process.env.TC_DB).getCollectionInfos({name:'$CAT2'})[0];
          print(o&&o.options&&o.options.collation?o.options.collation.locale+'/'+o.options.collation.strength:'NONE')")"
for _ in $(seq 1 40); do
  [ "$(H "print(db.getSiblingDB(process.env.TC_DB).$CAT2.getIndexes().length)")" = "2" ] && break
  sleep 1
done
chk "and the archive's indexes, once built"        "_id_,n_1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).$CAT2.getIndexes().map(function (i) { return i.name }).sort().join(','))")"
H "db.getSiblingDB(process.env.TC_DB).$CAT2.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$CAT2.drop()" >/dev/null

echo "== and a database the fast tier has no trace of =="
# The same gap one level up, and it reads worse. A database whose tiered
# collections are all in the archive is not in listDatabases at all, so it does
# not show up as empty, it does not show up; and "use" it and the collections
# and the documents are all there.
H "db.getSiblingDB(process.env.TC_DB3).dropDatabase()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB3).tcatdb.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB3).tcatdb.insertMany([{_id:1},{_id:2}])" >/dev/null
chk "the fast tier has no trace of the database"   "false" \
    "$(H "print(db.adminCommand({listDatabases:1,nameOnly:true}).databases.some(function (d) { return d.name === process.env.TC_DB3 }))")"
chk "the router reads it anyway"                   "2" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB3).tcatdb.countDocuments({}))")"
chk "and names it in listDatabases"                "true" \
    "$(R "print(db.adminCommand({listDatabases:1}).databases.some(function (d) { return d.name === process.env.TC_DB3 }))")"
chk "under nameOnly as well"                       "true" \
    "$(R "print(db.adminCommand({listDatabases:1,nameOnly:true}).databases.some(function (d) { return d.name === process.env.TC_DB3 }))")"
chk "and it reports no size on the fast tier"      "0" \
    "$(R "print(Number(db.adminCommand({listDatabases:1}).databases.filter(function (d) { return d.name === process.env.TC_DB3 })[0].sizeOnDisk))")"
# The fast tier holds the bookkeeping database itself, so it is in the list
# either way. What must not happen is the merge adding the archive's copy on top
# of it, which would show the operator two of them.
chk "the bookkeeping database is listed once"      "1" \
    "$(R "print(db.adminCommand({listDatabases:1}).databases.filter(function (d) { return d.name === process.env.TC_META }).length)")"
chk "and it is not listed twice"                   "1" \
    "$(R "print(db.adminCommand({listDatabases:1}).databases.filter(function (d) { return d.name === process.env.TC_DB }).length)")"

# Dropping the whole database has to withdraw every claim in it. The feeder does
# that for a single collection, because the drop event names one. dropDatabase
# names none, so every claim in the database used to stand exactly as it was:
# the router went on answering bounded reads from a fast tier holding nothing
# and returning empty, while the archive still held the documents.
H "db.getSiblingDB(process.env.TC_DB3).dropDatabase()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB3).tcatdb.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB3).tcatdb.insertMany([{_id:1},{_id:2},{_id:3}])" >/dev/null
H "db.getSiblingDB(process.env.TC_DB3).tcatdb.insertMany([{_id:1},{_id:2},{_id:3}])" >/dev/null
cline watermark --config $CFG --ns $DB3.tcatdb --complete-all >/dev/null
chk "the claim covers all of it"                   "3"     "$(R "print(db.getSiblingDB(process.env.TC_DB3).tcatdb.countDocuments({}))")"
R "db.getSiblingDB(process.env.TC_DB3).dropDatabase()" >/dev/null
# The feeder sees the event and withdraws; give it the time it needs rather than
# a fixed sleep, since what is being tested is that it happens at all.
for _ in $(seq 1 40); do
  cline watermark --config $CFG --ns $DB3.tcatdb 2>&1 | grep -q 'claims nothing' && break
  sleep 0.5
done
chk "the archive still holds them after the drop"  "3"     "$(C "print(db.getSiblingDB(process.env.TC_DB3).tcatdb.countDocuments({_tcDeletedAt:{\$exists:false}}))")"
chk "and the router reads them rather than none"   "3"     "$(R "print(db.getSiblingDB(process.env.TC_DB3).tcatdb.countDocuments({}))")"
C "db.getSiblingDB(process.env.TC_DB3).tcatdb.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB3).tcatdb.insertMany([{_id:1},{_id:2}])" >/dev/null

# A view is a different case again and must stay out. It holds no documents, the
# feeder refuses to tier one, and a read of it goes to the fast tier and finds
# nothing, so listing it would be the same lie. With no namespaces configured
# everything is tiered, and a view would otherwise be listed and then handed to
# the indexer to be built as a collection.
H "db.getSiblingDB(process.env.TC_DB).${CATNS}_view.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).${CATNS}_view.drop()" >/dev/null 2>&1
C "db.getSiblingDB(process.env.TC_DB).createView('${CATNS}_view', '$CATNS', [])" >/dev/null 2>&1
chk "a view the fast tier does not have is not listed" "false" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().indexOf('${CATNS}_view') >= 0)")"
# With namespaces configured that follows from the view not being one of them,
# so it is asked again with none configured, which means every namespace is
# tiered and the view is no longer excluded by name. That is the config where a
# view would otherwise be listed and then handed to the indexer to be built as
# a collection.
awk '/^  namespaces:/{skip=1; next} skip && /^    (-|#)/{next} {skip=0; print}' $CFG > $WDIR/router_all.yaml
chk "the derived config tiers everything"          "0" \
    "$(grep -c '^  namespaces:' $WDIR/router_all.yaml)"
kill $CATR 2>/dev/null; wait $CATR 2>/dev/null
cline router --config $WDIR/router_all.yaml >/dev/null 2>&1 & CATR2=$!
for _ in $(seq 1 30); do routerUp && break; sleep 1; done
chk "the collection is still named"                "true" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().indexOf('$CATNS') >= 0)")"
chk "and the view is still not"                    "false" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().indexOf('${CATNS}_view') >= 0)")"
chk "and nothing built the view as a collection"   "false" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().indexOf('${CATNS}_view') >= 0)")"
kill $CATR2 2>/dev/null; wait $CATR2 2>/dev/null
cline router --config $CFG >/dev/null 2>&1 & CATR=$!
for _ in $(seq 1 30); do routerUp && break; sleep 1; done
C "db.getSiblingDB(process.env.TC_DB).${CATNS}_view.drop()" >/dev/null 2>&1
# On the fast tier too. A binary without the guard builds the view there as a
# real collection, and left behind it fails this section on the next run for a
# reason that has nothing to do with that run.
H "db.getSiblingDB(process.env.TC_DB).${CATNS}_view.drop()" >/dev/null 2>&1

# An untiered collection the archive holds is a different case and must stay
# out. Reads of it go to the fast tier and come back empty, so listing it would
# be the lie this section removes.
C "db.getSiblingDB(process.env.TC_LOSTDB).notiering.insertOne({_id:1})" >/dev/null
chk "an untiered archive collection is not listed" "false" \
    "$(R "print(db.getSiblingDB(process.env.TC_LOSTDB).getCollectionNames().indexOf('notiering') >= 0)")"
# And the collections this router makes for itself, which are the application's
# database's only by necessity: a tiered $out is assembled in one and renamed
# over its target, and a rename cannot cross databases. A client that
# enumerates collections and acts on what it finds would act on that, and after
# a router died mid-aggregation it would go on doing so.
SCRATCHNS="tmp.thermocline.out.$(( $(date +%s) * 1000000000 ))"
H "db.getSiblingDB(process.env.TC_DB).getCollection('$SCRATCHNS').insertOne({v:1})" >/dev/null
chk "the fast tier does list the scratch collection" "1" \
    "$(H "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().filter(n=>n.startsWith('tmp.thermocline')).length)")"
chk "and the router does not"                        "0" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().filter(n=>n.startsWith('tmp.thermocline')).length)")"
chk "while still listing the real ones"              "true" \
    "$(R "print(db.getSiblingDB(process.env.TC_DB).getCollectionNames().length > 0)")"
# And a listing long enough to page. Only the first batch is filtered where the
# archive's half is added to it, so the batches after it used to come straight
# from the fast tier: measured with batchSize 1, one scratch collection appeared
# in a later batch of a listing whose first batch was clean.
chk "and not in a later batch of a paged listing"    "0" \
    "$(R "const d=db.getSiblingDB(process.env.TC_DB);
          let r=d.runCommand({listCollections:1,nameOnly:true,cursor:{batchSize:1}});
          let names=r.cursor.firstBatch.map(x=>x.name); let id=r.cursor.id, n=0;
          while (String(id) !== '0' && n < 400) {
            const g=d.runCommand({getMore:id, collection:'\$cmd.listCollections', batchSize:1});
            if (g.ok !== 1) break;
            names=names.concat(g.cursor.nextBatch.map(x=>x.name)); id=g.cursor.id; n++;
          }
          print(names.filter(x=>x.startsWith('tmp.thermocline')).length)")"
H "db.getSiblingDB(process.env.TC_DB).getCollection('$SCRATCHNS').drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_LOSTDB).notiering.drop()" >/dev/null
kill $CATR 2>/dev/null; wait $CATR 2>/dev/null
H "db.getSiblingDB(process.env.TC_DB).$CATNS.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB3).tcatdb.drop()" >/dev/null
C "db.getSiblingDB(process.env.TC_DB).$CATNS.drop()" >/dev/null

echo "== the claim is the field, not the row =="
# "No completeness watermarks recorded" is the first thing an operator reads
# when the router starts refusing range queries, and it used to be a count of
# rows in that collection rather than of claims.
#
# Mark.Claimed came from the row existing at all, so a row carrying nothing but
# its _id read as complete from the zero time: the most permissive claim this
# system can hold, every range query on that namespace answered from the fast
# tier alone however far back it reaches. Measured through `cline watermark` on
# such a row before the fix: "complete for all of history". Nothing here writes
# one, which is why it was never seen and exactly why it was worth closing: an
# $unset makes one, so does a hand-edited row, a backup restored from an older
# schema, or the next feature to $set some other field here with upsert:true.
#
# Last in this file and restored afterwards, because it empties the collection
# every other section depends on.
WMSAVE=$(C 'print(JSON.stringify(db.getSiblingDB(process.env.TC_META).watermarks.find().toArray()))')
wmArm() {
  pkill -f "config $CFG --log-file $LOGDIR/wm_r.log" 2>/dev/null
  sleep 1
  rm -f $LOGDIR/wm_r.log
  cline router --config $CFG --listen 0.0.0.0:$PALT --log-file $LOGDIR/wm_r.log &
  WMR=$!
  for _ in $(seq 1 30); do
    mongosh --port $PALT -u dba -p secret --authenticationDatabase admin --quiet \
      --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1 && break
    sleep 1
  done
  sleep 1
  local got
  got=$(grep -c 'no completeness watermarks recorded' $LOGDIR/wm_r.log 2>/dev/null | tail -1)
  kill $WMR 2>/dev/null; wait $WMR 2>/dev/null
  chk "$1" "$2" "$got"
}
C 'db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({})' >/dev/null
wmArm "nothing recorded is named at startup" "1"
C 'db.getSiblingDB(process.env.TC_META).watermarks.insertOne({_id:process.env.TC_DB+".tprobe"})' >/dev/null
wmArm "and so is a row that is not a claim"  "1"
chk "which claims nothing rather than everything" "true" \
    "$(cline watermark --config $CFG --ns $DB.tprobe 2>&1 | grep -qc 'claims nothing' && echo true || echo false)"
C 'db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); db.getSiblingDB(process.env.TC_META).watermarks.insertOne({_id:process.env.TC_DB+".tprobe", completeFrom:new Date(0)})' >/dev/null
wmArm "a real claim is quiet"                "0"
chk "and that one reads as a claim from its date" "true" \
    "$(cline watermark --config $CFG --ns $DB.tprobe 2>&1 | grep -qc 'newer than 1970-01-01' && echo true || echo false)"
C "db.getSiblingDB(process.env.TC_META).watermarks.deleteMany({}); const s=$WMSAVE; if (s.length) db.getSiblingDB(process.env.TC_META).watermarks.insertMany(s.map(function(r){ if (r.completeFrom) r.completeFrom=new Date(r.completeFrom); if (r.updatedAt) r.updatedAt=new Date(r.updatedAt); return r; }))" >/dev/null

kill $FP 2>/dev/null; wait $FP 2>/dev/null
echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
