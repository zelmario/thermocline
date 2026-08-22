// Ask the same question of the router and of the archive, and print every
// disagreement.
//
// This is the drop-in test done over somebody else's data: MongoDB's published
// sample datasets, twenty-three collections nobody here designed, adopted as an
// archive. Nothing here asserts an expected value. The archive is the control,
// and the only claim being checked is that a client cannot tell the difference.
// The archive keeps what the application deleted, carrying a tombstone, and the
// router filters those out of every answer. Read raw, the archive is therefore
// not a control at all once anything has been deleted: it answers with rows the
// router is right to withhold. writes.js deletes one comment per run, so this
// matters after the very first one.
const LIVE = process.env.TC_TARGET === "archive" ? {_tcDeletedAt: {$exists: false}} : {};
// TC_REPEAT asks everything more than once on this one connection and reports
// the last pass. With the query cache on, that last pass is the one the cache
// answered, and comparing it against the archive is how a held answer is
// checked rather than assumed. A held answer is keyed by connection while
// authentication is proxied, so repeating inside one process is the only way to
// reach it.
const REPEAT = parseInt(process.env.TC_REPEAT || "1", 10);
let out = [];

// How many records the archive is keeping for documents the application
// deleted. Zero against the router, which never returns them.
function buried(c) {
  return process.env.TC_TARGET === "archive" ? c.countDocuments({_tcDeletedAt: {$exists: true}}) : 0;
}

function ask(ns, label, f) {
  let v;
  try { v = f(); } catch (e) { v = "ERR " + (e.codeName || "") + " " + String(e.message).slice(0, 120); }
  out.push({ ns: ns, q: label, v: v });
}

// The question list comes from one side, not from each. Asking each target what
// it holds and then questioning it about that lets the two answer different
// question sets, and the comparison reports a question one of them was never
// asked as a blank answer: not obviously a missing question, and easy to read
// as a wrong one.
//
// That is exactly what a real defect looked like here. The router did not name
// a tiered collection the fast tier had no copy of, so nine questions about
// sample_supplies.sales were asked of the archive alone, and the report showed
// nine disagreements with the router's side empty while the router in fact
// answered every one of them correctly when asked directly.
//
// So the archive names the work, because it holds everything, and both sides
// answer the same list. A collection the router cannot see is then a real
// failure on a real question rather than a gap in the report.
// Carried as the value rather than a path: mongosh has no way to read a file
// from a script, and the list is a few kilobytes.
const PLAN = process.env.TC_PLAN;
let dbs, colls = null;
if (PLAN) {
  const plan = JSON.parse(PLAN);
  dbs = Object.keys(plan).sort();
  colls = plan;
} else {
  dbs = db.adminCommand({ listDatabases: 1 }).databases
    .map(d => d.name).filter(n => /^sample_/.test(n)).sort();
}

// Emitted by the archive pass so the router pass can be given the same list.
if (process.env.TC_PLAN_OUT) {
  const plan = {};
  for (const dbName of dbs) {
    plan[dbName] = db.getSiblingDB(dbName).getCollectionNames().sort();
  }
  print(JSON.stringify(plan));
  quit(0);
}

for (let round = 0; round < REPEAT; round++) {
  out = [];
  for (const dbName of dbs) {
  const d = db.getSiblingDB(dbName);
  for (const cName of (colls ? colls[dbName] : d.getCollectionNames().sort())) {
    const ns = dbName + "." + cName;
    const c = d[cName];

    ask(ns, "countDocuments",     () => c.countDocuments(LIVE));
    // These two take no filter, so the archive cannot leave its tombstones out
    // of them the way every other question here does. It keeps a record for
    // each document the application deleted and the router counts only live
    // ones, so the archive's raw answer is larger by exactly that many. Taking
    // them off is what makes the two comparable; leaving them on would report a
    // difference that is the archive doing its job.
    ask(ns, "estimatedCount",     () => c.estimatedDocumentCount() - buried(c));
    ask(ns, "stats.count",        () => c.stats().count - buried(c));
    ask(ns, "first 5 _id",        () => c.find(LIVE, {_id: 1}).sort({_id: 1}).limit(5).toArray().map(x => String(x._id)).join(","));
    ask(ns, "last 5 _id",         () => c.find(LIVE, {_id: 1}).sort({_id: -1}).limit(5).toArray().map(x => String(x._id)).join(","));
    // Sorted, because findOne with no sort returns whatever the storage engine
    // reaches first, and the two tiers are different clusters that need not
    // agree on that. Documents here have optional fields, so an arbitrary
    // document has an arbitrary field set and the check reported a difference
    // that was two different documents rather than two different answers.
    const first = () => c.find(LIVE).sort({_id: 1}).limit(1).toArray()[0] || {};
    ask(ns, "field set of the first document", () => Object.keys(first()).sort().join(","));
    // A group-by over the whole collection, which is the shape a report has and
    // the shape that goes wrong quietly if half the documents are elsewhere.
    const one = first();
    const key = Object.keys(one).filter(k => k !== "_id" && typeof one[k] === "string")[0];
    if (key) {
      ask(ns, "group by " + key, () =>
        c.aggregate([{$match: LIVE}, {$group: {_id: "$" + key, n: {$sum: 1}}}, {$sort: {n: -1, _id: 1}}, {$limit: 5}])
          .toArray().map(r => r._id + ":" + r.n).join(" | "));
      ask(ns, "distinct " + key, () => { const v = c.distinct(key, LIVE); return v.length + " values"; });
    }
    ask(ns, "count with sort+skip", () => c.find(LIVE).sort({_id: 1}).skip(7).limit(3).toArray().length);
  }
}
}
print(JSON.stringify(out));
