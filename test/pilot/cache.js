// The query cache, over data that lives mostly in the archive.
//
// Run against a router started with router.cache.enabled. Everything is checked
// against the archive, which this opens a second connection to.
//
// While authentication is proxied the router does not know who a client is, so
// a held answer is keyed by the connection that asked for it and never shared
// with another. That is why this runs every repeat on one connection: two
// mongosh invocations are two clients and share nothing, and a test that opened
// a new one each time would measure a cache that never hits.
//
// The case worth the most here is a query the fast tier cannot answer. Its
// result comes from the archive, and the change stream the cache listens to is
// on the fast tier, so the thing that makes the answer stale and the thing that
// announces it are on different clusters.
const cold = new Mongo(process.env.TC_COLD).getDB("sample_mflix").comments;
const hot = new Mongo(process.env.TC_HOT).getDB("sample_mflix").comments;
const R = db.getSiblingDB("sample_mflix").comments;

let pass = 0, fail = 0;
function chk(name, want, got) {
  if (String(want) === String(got)) { pass++; print("  PASS  " + name.padEnd(54) + got); }
  else { fail++; print("  FAIL  " + name.padEnd(54) + "got=" + got + " want=" + want); }
}
function eventually(f, want, tries) {
  for (let i = 0; i < (tries || 40); i++) {
    if (String(f()) === String(want)) return want;
    sleep(250);
  }
  return f();
}
function stats() {
  const s = db.getSiblingDB("admin").runCommand({ping: 1});  // keep the connection warm
  return s.ok;
}

// A window reaching back before the hot one, so the answer must come from the
// archive every time it is not served from the cache.
const OLD = {date: {$lt: new Date("1990-01-01")}};
const live = q => ({$and: [q, {_tcDeletedAt: {$exists: false}}]});

print("== a query the fast tier cannot answer ==");
const truth = cold.countDocuments(live(OLD));
const onHot = hot.countDocuments(OLD);
print("  the archive holds " + truth + " of them, the fast tier " + onHot);
// Not zero, and it does not need to be: earlier probes copy documents up, which
// is the design. What the query needs is a fast tier that cannot answer it
// alone, so the answer comes from the archive whenever the cache does not
// provide it. Asserting zero was a precondition about the order tests had run
// in rather than about the router.
chk("the fast tier cannot answer it alone", "true",
    String(onHot < truth && truth > 0));

// Ten rounds on this one connection. Whether any of them is served from the
// cache is the router's business; that every one of them is right is not.
let wrong = 0;
for (let i = 0; i < 10; i++) {
  if (R.countDocuments(OLD) !== truth) wrong++;
  sleep(100);
}
chk("ten repeats all give the archive's number", 0, wrong);

print("== a write inside that range, from this same client ==");
const victim = cold.find(live(OLD)).sort({_id: 1}).limit(1).next();
chk("the router agrees before the write", truth, R.countDocuments(OLD));
R.updateOne({_id: victim._id}, {$set: {cacheMark: 1}});
// The count does not move, so ask something whose answer does.
chk("a marked document is visible at once", 1, R.countDocuments({$and: [OLD, {cacheMark: 1}]}));

print("== a write that changes a held answer ==");
// Delete one of them. The count must drop, and the cache must not go on
// answering with the number it held a moment ago.
const before = R.countDocuments(OLD);
R.deleteOne({_id: victim._id});
chk("the count drops immediately", before - 1, R.countDocuments(OLD));
chk("and keeps saying so", before - 1, R.countDocuments(OLD));
chk("the archive tombstoned it", 1,
    eventually(() => cold.countDocuments({_id: victim._id, _tcDeletedAt: {$exists: true}}), 1));
chk("the router still hides it", 0, R.countDocuments({_id: victim._id}));

print("== a write by somebody else ==");
// Another connection entirely, which is the case the change stream exists for:
// this client's held answers have to be dropped by a write it did not make.
const other = new Mongo("mongodb://dba:secret@127.0.0.1:27500/?authSource=admin")
                .getDB("sample_mflix").comments;
const n0 = R.countDocuments(OLD);
const second = cold.find(live(OLD)).sort({_id: 1}).limit(1).next();
other.deleteOne({_id: second._id});
chk("this client sees the other client's delete", n0 - 1,
    eventually(() => R.countDocuments(OLD), n0 - 1));

print("== putting the data back ==");
R.updateMany({cacheMark: {$exists: true}}, {$unset: {cacheMark: ""}});
chk("no marks left", 0, R.countDocuments({cacheMark: {$exists: true}}));
print("  the two deleted comments stay deleted, by design");

print("");
print("PASS " + pass + "   FAIL " + fail);
if (fail > 0) quit(1);
