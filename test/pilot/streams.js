// What a reactive application sees through the router.
//
// Run against the router. A change stream is served from the fast tier, and the
// fast tier gains documents in ways the application did not ask for: a read of
// an archived document copies it up, and a warm copies up thousands. Each of
// those is an insert in the fast tier's oplog. An application watching a
// collection must not be told that a comment from 1994 was just created, and
// must still be told about the writes it did make.
//
// The events are what is checked, not counts, so nothing here depends on how
// much of the collection happens to be on which tier.
const R = db.getSiblingDB("sample_mflix").comments;
const cold = new Mongo(process.env.TC_COLD).getDB("sample_mflix").comments;
const hot = new Mongo(process.env.TC_HOT).getDB("sample_mflix").comments;

let pass = 0, fail = 0, skip = 0;
function chk(name, want, got) {
  if (String(want) === String(got)) { pass++; print("  PASS  " + name.padEnd(56) + got); }
  else { fail++; print("  FAIL  " + name.padEnd(56) + "got=" + got + " want=" + want); }
}
function skipped(name, why) { skip++; print("  SKIP  " + name.padEnd(56) + why); }

// A live comment the archive holds and the fast tier does not.
function archiveOnly(exclude) {
  const cur = cold.find({date: {$lt: new Date("1995-01-01")}, _tcDeletedAt: {$exists: false}})
                  .sort({_id: 1}).limit(300);
  while (cur.hasNext()) {
    const d = cur.next();
    if (exclude && String(d._id) === String(exclude)) continue;
    if (hot.countDocuments({_id: d._id}) === 0) return d;
  }
  return null;
}

const readOnly = archiveOnly(null);
const writeOnly = archiveOnly(readOnly ? readOnly._id : null);
if (!readOnly || !writeOnly) {
  print("  SKIP  archived comments to work with"
        + "                    the fast tier holds every candidate already");
  print("");
  print("PASS 0   FAIL 0   SKIP 1");
  quit(0);
}

print("== what the application did ==");
print("  reads an archived comment  " + readOnly._id + "  (a copy-up, which it did not ask for)");
print("  updates an archived one    " + writeOnly._id);
const fresh = new ObjectId();
print("  inserts a new one          " + fresh);

// Open the stream before anything happens, which is what an application does.
const stream = R.watch([], {fullDocument: "updateLookup"});

// 1. A read that copies a document up. No event should reach the client.
R.findOne({_id: readOnly._id});
// 2. A write to an archived document. One update event, and no insert, even
//    though the fast tier gained the document a moment earlier.
R.updateOne({_id: writeOnly._id}, {$set: {streamMark: "one"}});
// 3. An ordinary insert.
R.insertOne({_id: fresh, name: "pilot", text: "watched", date: new Date()});
// 4. An ordinary delete.
R.deleteOne({_id: fresh});

const seen = [];
const deadline = Date.now() + 20000;
while (Date.now() < deadline && seen.length < 12) {
  const ev = stream.tryNext();
  if (ev) {
    seen.push({op: ev.operationType, id: String(ev.documentKey && ev.documentKey._id)});
    continue;
  }
  sleep(200);
}
stream.close();

print("== what the stream delivered ==");
seen.forEach(e => print("  " + e.op.padEnd(10) + e.id));

const of = (id) => seen.filter(e => e.id === String(id)).map(e => e.op).join(",");

chk("a copy-up caused by a read is not an event", "", of(readOnly._id));
chk("the update is delivered",                    "update", of(writeOnly._id));
chk("the insert and delete both are",             "insert,delete", of(fresh));
chk("and nothing else arrived",                   3 - 1, seen.length - 1);

print("== putting the data back ==");
R.updateMany({streamMark: {$exists: true}}, {$unset: {streamMark: ""}});
chk("no marks left", 0, R.countDocuments({streamMark: {$exists: true}}));

print("");
print("PASS " + pass + "   FAIL " + fail + "   SKIP " + skip);
if (fail > 0) quit(1);
