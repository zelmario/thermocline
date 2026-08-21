// Transactions over documents that are not on the fast tier.
//
// A transaction runs on one cluster. The fast tier is where it runs, and the
// documents it needs may be in the archive, so they have to arrive before it
// starts touching them: a copy-up inside a transaction would be a write to
// another cluster from inside one this one cannot see.
//
// Run against the router. Everything is checked against the archive, which this
// opens a second connection to, so no value here is chosen by hand. The
// subjects are picked from the state found rather than assumed, because a
// transaction that touches an archived document copies it up and the next run
// meets a different fast tier.
const cold = new Mongo(process.env.TC_COLD).getDB("sample_mflix").comments;
const hot = new Mongo(process.env.TC_HOT).getDB("sample_mflix").comments;
const R = db.getSiblingDB("sample_mflix").comments;

let pass = 0, fail = 0, skip = 0;
function chk(name, want, got) {
  if (String(want) === String(got)) { pass++; print("  PASS  " + name.padEnd(54) + got); }
  else { fail++; print("  FAIL  " + name.padEnd(54) + "got=" + got + " want=" + want); }
}
function skipped(name, why) { skip++; print("  SKIP  " + name.padEnd(54) + why); }
function eventually(f, want, tries) {
  for (let i = 0; i < (tries || 40); i++) {
    if (String(f()) === String(want)) return want;
    sleep(250);
  }
  return f();
}

function archiveOnly(exclude) {
  const cur = cold.find({date: {$lt: new Date("1995-01-01")}, _tcDeletedAt: {$exists: false}})
                  .sort({_id: 1}).limit(300);
  while (cur.hasNext()) {
    const d = cur.next();
    if (exclude && exclude.some(x => String(x) === String(d._id))) continue;
    if (hot.countDocuments({_id: d._id}) === 0) return d;
  }
  return null;
}

const a = archiveOnly([]);
const b = archiveOnly(a ? [a._id] : []);
const c = archiveOnly(a && b ? [a._id, b._id] : []);
if (!a || !b || !c) {
  skipped("archived comments to work with", "the fast tier holds every candidate already");
  print("");
  print("PASS " + pass + "   FAIL " + fail + "   SKIP " + skip);
  quit(0);
}

// withTransaction, because that is what an application uses and what this
// design rests on.
//
// A transaction runs on the fast tier and cannot reach into the archive from
// inside itself, so when it touches an archived document the router copies that
// document up and aborts with TransientTransactionError. The driver's helper
// retries, the document is there the second time, and the application sees a
// transaction that took slightly longer. Driving it by hand with startTransaction
// and commitTransaction sees the abort instead, which is the contract working
// and not a failure; the last section here checks that label directly.
function txn(f) {
  const s = db.getMongo().startSession();
  try {
    let out;
    s.withTransaction(() => { out = f(s.getDatabase("sample_mflix").comments, s); });
    return out;
  } finally {
    s.endSession();
  }
}

print("== reading an archived document inside a transaction ==");
chk("the fast tier does not hold it", 0, hot.countDocuments({_id: a._id}));
let name;
try { name = txn(t => (t.findOne({_id: a._id}) || {}).name); }
catch (e) { name = "ERR " + (e.codeName || "") + " " + String(e.message).slice(0, 90); }
chk("the transaction finds it anyway", a.name, name);

print("== writing to one inside a transaction ==");
chk("the fast tier does not hold it either", 0, hot.countDocuments({_id: b._id}));
let modified;
try {
  modified = txn(t => {
    const r = t.updateOne({_id: b._id}, {$set: {txnMark: "committed"}});
    // Read your own write, inside the transaction, which is the promise a
    // transaction makes and the one a copy-up could break.
    const back = t.findOne({_id: b._id});
    return r.modifiedCount + ":" + (back ? back.txnMark : "MISSING");
  });
} catch (e) { modified = "ERR " + (e.codeName || "") + " " + String(e.message).slice(0, 90); }
chk("one modified, and readable inside", "1:committed", modified);
chk("the router sees it after the commit", "committed", (R.findOne({_id: b._id}) || {}).txnMark);
chk("and it reaches the archive", "committed",
    eventually(() => (cold.findOne({_id: b._id}) || {}).txnMark, "committed"));

print("== an abort must leave both tiers alone ==");
// The document is brought up first, on its own, so the transaction below has
// nothing to be replayed for and the abort is the only thing under test.
R.findOne({_id: c._id});
for (let i = 0; i < 40 && hot.countDocuments({_id: c._id}) === 0; i++) sleep(250);
chk("it is on the fast tier before the transaction", 1, hot.countDocuments({_id: c._id}));
let aborted = "no error";
const s = db.getMongo().startSession();
s.startTransaction();
try {
  s.getDatabase("sample_mflix").comments.updateOne({_id: c._id}, {$set: {txnMark: "rolled back"}});
  s.abortTransaction();
} catch (e) {
  aborted = "ERR " + (e.codeName || "") + " " + String(e.message).slice(0, 90);
} finally { s.endSession(); }
chk("the abort itself was clean", "no error", aborted);
chk("the router shows no mark", "none", (R.findOne({_id: c._id}) || {}).txnMark || "none");
sleep(2000);
chk("and the archive shows none either", "none", (cold.findOne({_id: c._id}) || {}).txnMark || "none");

print("== what a transaction driven by hand is told ==");
// The label is the whole contract. Without TransientTransactionError the
// driver's helper would not retry, and every transaction touching an archived
// document would fail instead of taking slightly longer.
const d = archiveOnly([a._id, b._id, c._id]);
if (!d) {
  skipped("an archived comment for the label check", "the fast tier holds every candidate already");
} else {
  const s2 = db.getMongo().startSession();
  s2.startTransaction();
  let label = "no error at all", code = "";
  try {
    s2.getDatabase("sample_mflix").comments.findOne({_id: d._id});
    s2.commitTransaction();
  } catch (e) {
    code = e.codeName;
    label = (typeof e.hasErrorLabel === "function" && e.hasErrorLabel("TransientTransactionError"))
            ? "TransientTransactionError" : JSON.stringify(e.errorLabels || null);
    try { s2.abortTransaction(); } catch (ig) {}
  }
  s2.endSession();
  chk("the abort is labelled for the driver to retry", "TransientTransactionError", label);
  chk("and it says so as a write conflict", "WriteConflict", code);
  chk("the document it was waiting for is now up", 1, hot.countDocuments({_id: d._id}));
}

print("== a transaction that spans the tier boundary ==");
// A year the fast tier holds some of and not all of, so the transaction has to
// reach documents on both sides of it.
let span = null, total = 0, onHot = 0;
for (let y = 2017; y >= 2010 && !span; y--) {
  const q = {date: {$gte: new Date(y + "-01-01"), $lt: new Date((y + 1) + "-01-01")}};
  const t = cold.countDocuments({$and: [q, {_tcDeletedAt: {$exists: false}}]});
  const h = hot.countDocuments(q);
  if (t > 0 && h > 0 && h < t) { span = q; total = t; onHot = h; }
}
if (!span) {
  skipped("a range that straddles the boundary", "no year has documents on both sides");
} else {
  print("  " + total + " comments in that year, of which the fast tier holds " + onHot);
  let n;
  try { n = txn(t => t.updateMany(span, {$set: {txnMark: "spanned"}}).modifiedCount); }
  catch (e) { n = "ERR " + (e.codeName || "") + " " + String(e.message).slice(0, 90); }
  chk("every document in the range was modified", total, n);
  chk("the router can see them all", total, R.countDocuments({$and: [span, {txnMark: "spanned"}]}));
  chk("the archive agrees", total,
      eventually(() => cold.countDocuments({$and: [span, {txnMark: "spanned"},
                                                   {_tcDeletedAt: {$exists: false}}]}), total));
}

print("== putting the data back ==");
R.updateMany({txnMark: {$exists: true}}, {$unset: {txnMark: ""}});
chk("no marks left through the router", 0, R.countDocuments({txnMark: {$exists: true}}));
chk("and none in the archive", 0,
    eventually(() => cold.countDocuments({txnMark: {$exists: true},
                                          _tcDeletedAt: {$exists: false}}), 0));

print("");
print("PASS " + pass + "   FAIL " + fail + "   SKIP " + skip);
if (fail > 0) quit(1);
