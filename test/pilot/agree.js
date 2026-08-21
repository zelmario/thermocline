// Ask the same question of the router and of the archive, and print every
// disagreement.
//
// This is the drop-in test done over somebody else's data: MongoDB's published
// sample datasets, twenty-three collections nobody here designed, adopted as an
// archive. Nothing here asserts an expected value. The archive is the control,
// and the only claim being checked is that a client cannot tell the difference.
const target = process.env.TC_TARGET;               // "router" or "archive"
const out = [];

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

    ask(ns, "countDocuments",     () => c.countDocuments({}));
    ask(ns, "estimatedCount",     () => c.estimatedDocumentCount());
    ask(ns, "stats.count",        () => c.stats().count);
    ask(ns, "first 5 _id",        () => c.find({}, {_id: 1}).sort({_id: 1}).limit(5).toArray().map(x => String(x._id)).join(","));
    ask(ns, "last 5 _id",         () => c.find({}, {_id: 1}).sort({_id: -1}).limit(5).toArray().map(x => String(x._id)).join(","));
    ask(ns, "distinct field set", () => Object.keys(c.findOne({}) || {}).sort().join(","));
    // A group-by over the whole collection, which is the shape a report has and
    // the shape that goes wrong quietly if half the documents are elsewhere.
    const one = c.findOne({}) || {};
    const key = Object.keys(one).filter(k => k !== "_id" && typeof one[k] === "string")[0];
    if (key) {
      ask(ns, "group by " + key, () =>
        c.aggregate([{$group: {_id: "$" + key, n: {$sum: 1}}}, {$sort: {n: -1, _id: 1}}, {$limit: 5}])
          .toArray().map(r => r._id + ":" + r.n).join(" | "));
      ask(ns, "distinct " + key, () => { const v = c.distinct(key); return v.length + " values"; });
    }
    ask(ns, "count with sort+skip", () => c.find({}).sort({_id: 1}).skip(7).limit(3).toArray().length);
  }
}
print(JSON.stringify(out));
