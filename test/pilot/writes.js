// Writing to documents that are not on the fast tier.
//
// Run against the router. Everything it checks it checks against the archive,
// which it opens a second connection to, so nothing here asserts a value chosen
// by hand. The interesting documents are the ones nobody put on the fast tier:
// sample_mflix.comments runs back to 1970 and the hot window is ten years, so a
// comment from 1994 exists only in the archive, and every write below has to
// find it there.
//
// It picks its subjects from the state it finds rather than assuming one,
// because its own writes change that state: a write to an archived document
// copies it up, which is the design, so the second run of this file meets a
// fast tier holding what the first run pulled into it. A check that cannot be
// run is reported as skipped rather than passed, and rather than failed.
const cold = new Mongo(process.env.TC_COLD);
const R = db.getSiblingDB("sample_mflix").comments;
const C = cold.getDB("sample_mflix").comments;
const HOT = new Mongo(process.env.TC_HOT).getDB("sample_mflix").comments;

let pass = 0, fail = 0, skip = 0;
function chk(name, want, got) {
  if (String(want) === String(got)) { pass++; print("  PASS  " + name.padEnd(52) + got); }
  else { fail++; print("  FAIL  " + name.padEnd(52) + "got=" + got + " want=" + want); }
}
function skipped(name, why) { skip++; print("  SKIP  " + name.padEnd(52) + why); }

// Mirroring is asynchronous, so a check on the archive is allowed to wait for
// it. A fixed sleep would be too short on a slow run and wasted on every fast
// one, and one that gave up early would report a lost write.
function eventually(f, want, tries) {
  for (let i = 0; i < (tries || 40); i++) {
    if (String(f()) === String(want)) return want;
    sleep(250);
  }
  return f();
}

// A live comment the archive holds and the fast tier does not. Tombstoned ones
// are excluded: the archive keeps what the application deleted, and a previous
// run of this file deleted one.
function archiveOnly(exclude) {
  const cur = C.find({date: {$lt: new Date("1995-01-01")}, _tcDeletedAt: {$exists: false}})
               .sort({_id: 1}).limit(200);
  while (cur.hasNext()) {
    const d = cur.next();
    if (exclude && String(d._id) === String(exclude)) continue;
    if (HOT.countDocuments({_id: d._id}) === 0) return d;
  }
  return null;
}

print("== a document that is only in the archive ==");
const old = archiveOnly(null);
if (!old) {
  skipped("an archived comment to write to", "the fast tier holds every candidate already");
} else {
  chk("the fast tier does not hold it", 0, HOT.countDocuments({_id: old._id}));
  chk("the router finds it anyway", old.name, (R.findOne({_id: old._id}) || {}).name);

  print("== updating it ==");
  chk("one document modified", 1, R.updateOne({_id: old._id}, {$set: {pilotMark: "one"}}).modifiedCount);
  chk("the router reads the new value", "one", (R.findOne({_id: old._id}) || {}).pilotMark);
  chk("and it kept everything else", old.name, (R.findOne({_id: old._id}) || {}).name);
  chk("the archive has it too", "one",
      eventually(() => (C.findOne({_id: old._id}) || {}).pilotMark, "one"));
  // Writing to an archived document brings it up, which is the design rather
  // than a side effect: the next read of it should not pay for the archive
  // again. Saying so here is what stops the next run being surprised by it.
  chk("and the write brought it up to the fast tier", 1, HOT.countDocuments({_id: old._id}));
}

print("== a write across the tier boundary ==");
// A year the fast tier holds some of and not all of. Which year that is depends
// on the hot window and on what earlier runs pulled up, so it is searched for
// rather than named.
let span = null, total = 0, onHot = 0;
for (let y = 2017; y >= 2010 && !span; y--) {
  const s = {date: {$gte: new Date(y + "-01-01"), $lt: new Date((y + 1) + "-01-01")}};
  const t = C.countDocuments({$and: [s, {_tcDeletedAt: {$exists: false}}]});
  const h = HOT.countDocuments(s);
  if (t > 0 && h > 0 && h < t) { span = s; total = t; onHot = h; }
}
if (!span) {
  skipped("a range that straddles the boundary", "no year has documents on both sides of it");
} else {
  print("  " + total + " comments in that year, of which the fast tier holds " + onHot);
  chk("updateMany reached every one", total, R.updateMany(span, {$set: {pilotMark: "many"}}).modifiedCount);
  chk("and the router can see them all marked", total, R.countDocuments({$and: [span, {pilotMark: "many"}]}));
  chk("the archive agrees", total,
      eventually(() => C.countDocuments({$and: [span, {pilotMark: "many"}, {_tcDeletedAt: {$exists: false}}]}), total));
}

print("== deleting one the fast tier never held ==");
const victim = archiveOnly(old ? old._id : null);
if (!victim) {
  skipped("an archived comment to delete", "the fast tier holds every candidate already");
} else {
  const before = R.countDocuments({});
  chk("the fast tier does not hold it either", 0, HOT.countDocuments({_id: victim._id}));
  chk("one document deleted", 1, R.deleteOne({_id: victim._id}).deletedCount);
  chk("the router cannot find it", null, R.findOne({_id: victim._id}));
  chk("and the count went down by one", before - 1, R.countDocuments({}));
  chk("the archive keeps it as a tombstone", 1,
      eventually(() => C.countDocuments({_id: victim._id, _tcDeletedAt: {$exists: true}}), 1));
  chk("which the router does not return", 0, R.countDocuments({_id: victim._id}));
}

print("== inserting into a tiered collection ==");
const fresh = {_id: new ObjectId(), name: "pilot", text: "written through the router",
               date: new Date(), pilotMark: "new"};
chk("inserted", 1, R.insertOne(fresh).acknowledged ? 1 : 0);
chk("the router reads it back", "pilot", (R.findOne({_id: fresh._id}) || {}).name);
chk("the fast tier has it", 1, HOT.countDocuments({_id: fresh._id}));
chk("and it reaches the archive", 1, eventually(() => C.countDocuments({_id: fresh._id}), 1));

print("== putting the data back ==");
R.deleteOne({_id: fresh._id});
R.updateMany({pilotMark: {$exists: true}}, {$unset: {pilotMark: ""}});
chk("no marks left through the router", 0, R.countDocuments({pilotMark: {$exists: true}}));
chk("and none in the archive", 0,
    eventually(() => C.countDocuments({pilotMark: {$exists: true}, _tcDeletedAt: {$exists: false}}), 0));
// A deleted comment stays deleted. Putting it back would mean writing a
// document the application never wrote, and the tombstone is the honest record
// of what this run did.
if (victim) print("  the deleted comment " + victim._id + " stays deleted, by design");

print("");
print("PASS " + pass + "   FAIL " + fail + "   SKIP " + skip);
if (fail > 0) quit(1);
