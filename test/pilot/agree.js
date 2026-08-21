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
const out = [];

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

const dbs = db.adminCommand({ listDatabases: 1 }).databases
  .map(d => d.name).filter(n => /^sample_/.test(n)).sort();

for (const dbName of dbs) {
  const d = db.getSiblingDB(dbName);
  for (const cName of d.getCollectionNames().sort()) {
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
    ask(ns, "distinct field set", () => Object.keys(c.findOne(LIVE) || {}).sort().join(","));
    // A group-by over the whole collection, which is the shape a report has and
    // the shape that goes wrong quietly if half the documents are elsewhere.
    const one = c.findOne(LIVE) || {};
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
print(JSON.stringify(out));
