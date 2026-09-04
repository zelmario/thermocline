// Randomized differential testing of the router, run inside one mongosh.
//
// Every hand-written suite here asks the questions somebody thought of. That
// works until it does not: the shapes we can list all agree, and the defects
// still being found are shapes nobody listed. So this one does not list them.
// It generates operations from a grammar, asks the same operation of an
// untiered reference collection and of a tiered one, and reports every
// disagreement.
//
// shop.tcref is untiered, so the router forwards it and it answers as MongoDB
// does. shop.tcfuzz holds the same documents with half of them only in the
// archive and a completeness claim that says the fast tier is short, so every
// answer has to be resolved across both tiers. The reference is the oracle.
// Nothing here asserts a value by hand.
//
// Three outcomes, and the difference between them is the whole point:
//
//   agreed    both sides gave the same answer, or both failed the same way
//   refused   the tiered side returned a "thermocline:" error where the
//             reference answered. A limitation, not a wrong answer, and the
//             census of them is worth having.
//   DISAGREED both sides answered and the answers differ, or the tiered side
//             failed for a reason that is not a refusal. That is a bug.
//
// A disagreement is asked again a second later before it is called one, because
// the archive is written by the feeder asynchronously and a stale answer that
// settles is a freshness gap rather than a wrong answer for ever. Both get
// reported, under their own names.
//
// One thing the agreed count does not mean: an operation both sides reject the
// same way is counted there, and it proved nothing about tiering. The grammar
// generates plenty of those on purpose, because the error has to match too, so
// read the number as operations compared rather than tiering exercised.

const DB   = process.env.TC_DB || 'shop';
const OPS  = Number(process.env.TC_OPS || 60);
// Reads only. The suite uses this for its own negative control, where the point
// is to see a divergence that has been put there deliberately and writes would
// paper over it by copying documents up.
const RO   = process.env.TC_READONLY === '1';
const SEED = Number(process.env.TC_SEED || 1);
const d    = db.getSiblingDB(DB);
const REF  = d.tcref;
const TIER = d.tcfuzz;
// A second pair, so a join can name a collection other than the one it is
// running against. A self join is the narrowest version of the case: two tiered
// collections carry two separate completeness claims, and the guard that judges
// a joined collection looks at the other one's.
const REF2  = d.tcref2;
const TIER2 = d.tcfuzz2;
function otherOf(c) { return c.getName() === REF.getName() ? REF2.getName() : TIER2.getName(); }

// xorshift32, so a seed reproduces a run exactly. Math.random cannot: a
// disagreement nobody can ask for again is not a bug report.
let _s = (SEED >>> 0) || 1;
function rnd() {
  _s ^= _s << 13; _s >>>= 0;
  _s ^= _s >>> 17;
  _s ^= _s << 5;  _s >>>= 0;
  return _s / 4294967296;
}
function ri(n)   { return Math.floor(rnd() * n); }
function pick(a) { return a[ri(a.length)]; }
function maybe(p){ return rnd() < p; }

// The fields the seed documents carry, each with a generator that produces a
// value of the right type. Generating {n: "alpha"} is legal MongoDB and matches
// nothing, which tests the comparison and not the router, so predicates are
// built from the field's own domain and only sometimes from another's.
const FIELDS = [
  { p: 'n',           t: 'num',  v: () => ri(70) },
  { p: 'f',           t: 'num',  v: () => ri(200) / 4 },
  { p: 'name',        t: 'str',  v: () => 'user' + String(1 + ri(60)).padStart(2, '0') },
  { p: 'grp',         t: 'str',  v: () => pick(['alpha', 'beta', 'gamma']) },
  { p: 'tags',        t: 'arr',  v: () => pick(['t1', 't2', 't3', 'common', 'none']) },
  { p: 'items.qty',   t: 'num',  v: () => ri(70) },
  { p: 'items.sku',   t: 'str',  v: () => pick(['s0', 's1', 's2', 's3', 's9']) },
  { p: 'meta.score',  t: 'num',  v: () => ri(13) },
  { p: 'meta.active', t: 'bool', v: () => pick([true, false]) },
  { p: 'when',        t: 'date', v: () => new Date(Date.UTC(2024, ri(12), 1 + ri(27))) },
  { p: 'opt',         t: 'str',  v: () => pick(['yes', 'no']) },
  { p: '_id',         t: 'num',  v: () => 1 + ri(70) },
];
const SORTABLE = FIELDS.map(f => f.p);

// One predicate on one field. $nor and $not are in here on purpose: the router
// reads a lower time bound out of a filter to decide whether the fast tier can
// answer it whole, and a negation is the shape where reading one would be
// wrong.
function predicate(f) {
  const v = f.v(), w = f.v();
  switch (ri(14)) {
    case 0:  return v;
    case 1:  return { $ne: v };
    case 2:  return { $gt: v };
    case 3:  return { $gte: v };
    case 4:  return { $lt: v };
    case 5:  return { $lte: v };
    case 6:  return { $in: [v, w] };
    case 7:  return { $nin: [v, w] };
    case 8:  return { $exists: maybe(0.7) };
    case 9:  return { $gte: v, $lt: w };
    case 10: return { $not: { $gt: v } };
    case 11: return f.t === 'num' ? { $mod: [1 + ri(4), 0] } : { $ne: v };
    case 12: return f.t === 'arr' ? { $all: [v] } : { $eq: v };
    default: return { $type: pick(['int', 'double', 'string', 'array', 'object', 'date', 'bool', 'null']) };
  }
}

function leaf() {
  const out = {};
  const howMany = 1 + ri(2);
  for (let i = 0; i < howMany; i++) {
    const f = pick(FIELDS);
    out[f.p] = predicate(f);
  }
  return out;
}

function filter(depth) {
  if (depth <= 0) return leaf();
  const r = rnd();
  if (r < 0.14) return { $and: [filter(depth - 1), filter(depth - 1)] };
  if (r < 0.30) return { $or:  [filter(depth - 1), filter(depth - 1)] };
  if (r < 0.36) return { $nor: [filter(depth - 1)] };
  if (r < 0.42) return { items: { $elemMatch: { qty: predicate(FIELDS[5]), sku: predicate(FIELDS[6]) } } };
  if (r < 0.46) return { $expr: { $gt: [ '$n', ri(70) ] } };
  return leaf();
}

// Sorts always end in _id so the order is total. MongoDB does not promise an
// order for ties and neither does this, so a generated sort that left them
// would report the router's tie-breaking as a wrong answer.
function sortSpec() {
  const s = {};
  const howMany = ri(2);
  for (let i = 0; i < howMany; i++) s[pick(SORTABLE)] = maybe(0.5) ? 1 : -1;
  s._id = 1;
  return s;
}

function projection() {
  if (maybe(0.4)) return undefined;
  const p = {};
  const inclusive = maybe(0.7);
  for (let i = 0; i < 1 + ri(3); i++) p[pick(SORTABLE).replace('_id', 'n')] = inclusive ? 1 : 0;
  if (inclusive && maybe(0.5)) p._id = 0;
  return p;
}

// Three modifiers with a history of being dropped on the way to the archive.
// A hint on a sparse or partial index decides which documents are looked at; a
// collation decides which strings are one value; a read concern decides which
// writes are visible. Each is generated sometimes and applied to both sides, so
// a tier that forwards it and a tier that forgets it give different answers.
const INDEXES = ['n_1', 'grp_1', 'when_1', 'meta.score_1', 'tags_1'];
function modifiers() {
  const m = {};
  if (maybe(0.15)) m.hint = pick(INDEXES);
  if (maybe(0.15)) m.collation = { locale: 'en', strength: 1 + ri(3) };
  if (maybe(0.15)) m.readConcern = pick(['local', 'majority']);
  return m;
}
function applyMods(q, m) {
  if (m.hint) q = q.hint(m.hint);
  if (m.collation) q = q.collation(m.collation);
  if (m.readConcern) q = q.readConcern(m.readConcern.level || m.readConcern);
  return q;
}

function updateSpec() {
  const f = pick(FIELDS.filter(x => x.p !== '_id'));
  switch (ri(10)) {
    case 0: return { $set:   { [f.p]: f.v() } };
    case 1: return { $unset: { [f.p]: '' } };
    case 2: return { $inc:   { n: 1 + ri(5) } };
    case 3: return { $mul:   { f: 2 } };
    case 4: return { $min:   { 'meta.score': ri(13) } };
    case 5: return { $max:   { 'meta.score': ri(13) } };
    case 6: return { $push:  { tags: 'p' + ri(4) } };
    case 7: return { $pull:  { tags: pick(['t1', 't2', 'common']) } };
    case 8: return { $addToSet: { tags: 'a' + ri(3) } };
    default: return { $set: { [f.p]: f.v() }, $inc: { n: 1 } };
  }
}

// Set per side before a pipeline is generated. The two sides pick the same
// branch, because the generator is replayed from the same seed; only the name
// it lands on differs.
let SELF = 'tcref';
let OTHER = 'tcref2';
function pipeline() {
  const stages = [];
  if (maybe(0.8)) stages.push({ $match: filter(1) });
  switch (ri(10)) {
    case 0:
      stages.push({ $group: { _id: '$' + pick(['grp', 'opt', 'meta.active']),
                              c: { $sum: 1 }, s: { $sum: '$n' }, mx: { $max: '$f' } } });
      break;
    case 1:
      stages.push({ $unwind: '$tags' }, { $group: { _id: '$tags', c: { $sum: 1 } } });
      break;
    case 2:
      stages.push({ $sort: sortSpec() }, { $limit: 1 + ri(20) });
      break;
    case 3:
      stages.push({ $project: { n: 1, grp: 1, dbl: { $multiply: ['$n', 2] } } });
      break;
    case 4:
      stages.push({ $count: 'c' });
      break;
    case 5:
      stages.push({ $addFields: { band: { $cond: [{ $gt: ['$n', 30] }, 'high', 'low'] } } },
                  { $group: { _id: '$band', c: { $sum: 1 } } });
      break;
    case 6:
      // A join, of itself or of the other collection. Either way the tiered
      // side has to resolve a tiered namespace across both tiers as the joined
      // side, which is not where the pipeline's own filter reaches.
      stages.push({ $lookup: { from: pick([SELF, OTHER]), localField: 'grp', foreignField: 'grp', as: 'sib' } },
                  { $project: { n: 1, grp: 1, sibs: { $size: '$sib' } } });
      break;
    case 7:
      stages.push({ $unionWith: { coll: pick([SELF, OTHER]), pipeline: [{ $match: filter(0) }] } },
                  { $group: { _id: '$grp', c: { $sum: 1 } } });
      break;
    case 8:
      stages.push({ $graphLookup: { from: pick([SELF, OTHER]), startWith: '$grp',
                                    connectFromField: 'grp', connectToField: 'grp', as: 'tree' } },
                  { $project: { grp: 1, t: { $size: '$tree' } } });
      break;
    case 9:
      // A lookup with its own pipeline, which is the correlated form and a
      // different code path from the field one.
      stages.push({ $lookup: { from: pick([SELF, OTHER]), let: { g: '$grp' },
                               pipeline: [{ $match: { $expr: { $eq: ['$grp', '$$g'] } } }], as: 'sib' } },
                  { $project: { grp: 1, sibs: { $size: '$sib' } } });
      break;
    default:
      stages.push({ $sortByCount: '$grp' });
  }
  return stages;
}

// Canonical form. Keys are sorted recursively, because field order is not
// something either tier promises and comparing it would report noise. Numbers
// are serialized strictly: an int that became a double is a real difference and
// relaxed JSON prints both as 1.
function canon(x) {
  if (Array.isArray(x)) return x.map(canon);
  if (x && typeof x === 'object' && !(x instanceof Date) && x._bsontype === undefined) {
    const o = {};
    Object.keys(x).sort().forEach(k => { o[k] = canon(x[k]); });
    return o;
  }
  return x;
}
function ser(x) { return EJSON.stringify(canon(x), null, 0, { relaxed: false }); }
// A multiset, for anything whose order MongoDB does not promise.
function bag(a) { return Array.isArray(a) ? a.map(ser).sort().join('|') : ser(a); }

function attempt(fn, c) {
  try { return { ok: true, v: fn(c) }; }
  catch (e) { return { ok: false, code: e.code || 0, msg: String(e.message || e).slice(0, 200) }; }
}

let agreed = 0, refused = 0, settled = 0;
const refusals = [], bugs = [], stale = [];
// What each shape actually did. Without this a round can report a clean
// hundred agreements without having exercised a shape at all, or having
// exercised it only through operations both sides rejected, which compares the
// error message and nothing else.
const shapes = {};
function note(what, kind) {
  const t = shapes[what] || (shapes[what] = { n: 0, vacuous: 0 });
  t.n++;
  if (kind === 'vacuous') t.vacuous++;
}

function ask(what, op, fn, how) {
  const norm = how === 'bag' ? bag : ser;
  const a = attempt(fn, REF), b = attempt(fn, TIER);

  if (!a.ok && !b.ok) {
    if (a.code === b.code) { agreed++; note(what, 'vacuous'); return; }
    // Both failed differently. Only interesting when the tiered side is not
    // simply saying no.
    if (/^thermocline:/.test(b.msg)) { refused++; note(what, 'real'); refusals.push([what, op, b.msg]); return; }
    note(what, 'real');
    bugs.push([what, op, 'ref failed ' + a.code + ': ' + a.msg, 'tier failed ' + b.code + ': ' + b.msg]);
    return;
  }
  if (a.ok && !b.ok) {
    if (/^thermocline:/.test(b.msg)) { refused++; note(what, 'real'); refusals.push([what, op, b.msg]); return; }
    note(what, 'real');
    bugs.push([what, op, 'ref answered', 'tier failed ' + b.code + ': ' + b.msg]);
    return;
  }
  if (!a.ok && b.ok) {
    note(what, 'real');
    bugs.push([what, op, 'ref failed ' + a.code + ': ' + a.msg, 'tier answered']);
    return;
  }

  let ra = norm(a.v), rb = norm(b.v);
  if (ra === rb) { agreed++; note(what, 'real'); return; }

  // The archive is written by the feeder after the fact, so a difference can be
  // a stale answer that is about to settle. Ask again before calling it a bug.
  sleep(1500);
  const a2 = attempt(fn, REF), b2 = attempt(fn, TIER);
  note(what, 'real');
  if (a2.ok && b2.ok && norm(a2.v) === norm(b2.v)) {
    settled++; stale.push([what, op, ra.slice(0, 160), rb.slice(0, 160)]);
    return;
  }
  bugs.push([what, op, ra.slice(0, 400), rb.slice(0, 400)]);
}

// After a write the two collections have to hold the same documents, which is a
// stronger statement than the write's own reported counts and catches a write
// that landed on one document too many.
function contentsMatch(after, op) {
  ask(after, op, c => c.find({}).sort({ _id: 1 }).toArray(), 'seq');
}

const ids = () => REF.find({}, { _id: 1 }).sort({ _id: 1 }).toArray().map(x => x._id);
// n distinct ids drawn from what the collection holds.
function someIds(known, n) {
  const out = [];
  for (let i = 0; i < n && known.length; i++) {
    const v = known[ri(known.length)];
    if (out.indexOf(v) < 0) out.push(v);
  }
  return out.length ? out : [1];
}

function oneOp(i) {
  const F = filter(1 + ri(2));
  const S = sortSpec();
  const P = projection();
  const U = updateSpec();
  const bs = pick([0, 0, 1, 2, 3, 7]);
  const known = ids();
  const anId = known.length ? pick(known) : 1;

  // Reads outnumber writes about two to one. Every write copies documents up
  // into the fast tier, so a write-heavy round warms the collection and the
  // comparison it is there to make gets weaker with every operation.
  // Aggregate is weighted up because the join shapes live inside it, four of
  // its ten branches, and a defect has already come out of one of them. At an
  // even weight a round of fifty operations generated two joins.
  const READS = [0, 1, 2, 3, 3, 3, 4, 5, 3, 4];
  const WRITES = [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 16];
  const choice = RO ? pick(READS) : (maybe(0.65) ? pick(READS) : pick(WRITES));
  switch (choice) {
    case 0: {
      const sk = ri(3), lm = maybe(0.5) ? 1 + ri(25) : 0;
      const M = modifiers();
      const op = 'find ' + ser(F) + ' sort ' + ser(S) + (P ? ' proj ' + ser(P) : '') +
                 ' skip ' + sk + ' limit ' + lm + ' batch ' + bs +
                 (Object.keys(M).length ? ' with ' + ser(M) : '');
      ask('find', op, c => {
        let q = P ? c.find(F, P) : c.find(F);
        q = applyMods(q.sort(S).skip(sk), M);
        if (lm) q = q.limit(lm);
        if (bs) q = q.batchSize(bs);
        return q.toArray();
      }, 'seq');
      break;
    }
    case 1: {
      const M = modifiers();
      ask('countDocuments', 'count ' + ser(F) + (Object.keys(M).length ? ' with ' + ser(M) : ''),
          c => c.countDocuments(F, M), 'seq');
      break;
    }
    case 2: {
      const f = pick(SORTABLE);
      const M = modifiers();
      ask('distinct', 'distinct ' + f + ' ' + ser(F) + (Object.keys(M).length ? ' with ' + ser(M) : ''),
          c => c.distinct(f, F, M.collation ? { collation: M.collation } : {}).map(ser).sort(), 'seq');
      break;
    }
    case 3: {
      // The pipeline is generated once and rendered per side, because a self
      // join names its own collection and the two sides have different names.
      SELF = REF.getName(); OTHER = REF2.getName();
      const shape = _s;
      const pl = pipeline();
      ask('aggregate', 'aggregate ' + ser(pl) + ' batch ' + bs, c => {
        SELF = c.getName();
        OTHER = otherOf(c);
        _s = shape;
        const mine = pipeline();
        return c.aggregate(mine, bs ? { batchSize: bs } : {}).toArray();
      }, 'bag');
      break;
    }
    case 4:
      ask('find one, sorted', 'first of ' + ser(F) + ' by ' + ser(S),
          c => c.find(F).sort(S).limit(1).toArray(), 'seq');
      break;
    case 5:
      ask('total', 'countDocuments {}', c => c.countDocuments({}), 'seq');
      break;
    case 6: {
      const op = 'updateMany ' + ser(F) + ' ' + ser(U);
      ask('updateMany', op, c => { const r = c.updateMany(F, U); return [r.matchedCount, r.modifiedCount]; }, 'seq');
      contentsMatch('contents after updateMany', op);
      break;
    }
    case 7: {
      // Bounded to a handful of ids on purpose. A generated filter can match
      // most of the collection, and two of those in a round leave nothing
      // archive-only for the rest of it to disagree about: the harness stops
      // testing anything and still reports agreement. The ids come from what
      // the collection actually holds, which includes the archive-only half, so
      // deleting a document the fast tier does not have is still reached.
      const f = { $and: [ { _id: { $in: someIds(known, 3) } }, F ] };
      const op = 'deleteMany ' + ser(f);
      ask('deleteMany', op, c => c.deleteMany(f).deletedCount, 'seq');
      contentsMatch('contents after deleteMany', op);
      break;
    }
    case 8: {
      // One-document writes are anchored on an _id. deleteOne on a filter that
      // matches several picks whichever document it likes, on both sides
      // independently, and comparing that reports MongoDB disagreeing with
      // itself.
      const f = { _id: anId };
      const op = 'updateOne ' + ser(f) + ' ' + ser(U);
      ask('updateOne', op, c => { const r = c.updateOne(f, U); return [r.matchedCount, r.modifiedCount]; }, 'seq');
      contentsMatch('contents after updateOne', op);
      break;
    }
    case 9: {
      const f = { _id: anId };
      const op = 'deleteOne ' + ser(f);
      ask('deleteOne', op, c => c.deleteOne(f).deletedCount, 'seq');
      contentsMatch('contents after deleteOne', op);
      break;
    }
    case 10: {
      const doc = { _id: anId, n: ri(70), grp: pick(['alpha', 'beta']), tags: ['r' + ri(3)],
                    meta: { score: ri(13), active: maybe(0.5) }, when: new Date(Date.UTC(2024, ri(12), 1 + ri(27))) };
      const op = 'replaceOne ' + ser(doc);
      ask('replaceOne', op, c => { const r = c.replaceOne({ _id: anId }, doc); return [r.matchedCount, r.modifiedCount]; }, 'seq');
      contentsMatch('contents after replaceOne', op);
      break;
    }
    case 11: {
      const doc = { _id: 1000 + i, n: ri(70), grp: pick(['alpha', 'beta', 'gamma']),
                    name: 'new' + i, tags: ['t' + ri(3), 'common'],
                    items: [{ sku: 's' + ri(4), qty: ri(70) }],
                    meta: { score: ri(13), active: maybe(0.5) },
                    when: new Date(Date.UTC(2024, ri(12), 1 + ri(27))) };
      const op = 'insertOne ' + ser(doc);
      ask('insertOne', op, c => c.insertOne(doc).insertedId, 'seq');
      contentsMatch('contents after insertOne', op);
      break;
    }
    case 12: {
      // An upsert whose filter carries the _id, so the document it creates when
      // nothing matches is the same document on both sides. Without that each
      // side invents its own ObjectId.
      const f = { _id: 2000 + ri(8), grp: pick(['alpha', 'beta']) };
      const op = 'upsert ' + ser(f) + ' ' + ser(U);
      ask('upsert', op, c => { const r = c.updateOne(f, U, { upsert: true }); return [r.matchedCount, r.modifiedCount, ser(r.upsertedId)]; }, 'seq');
      contentsMatch('contents after upsert', op);
      break;
    }
    case 13: {
      const newDoc = maybe(0.5);
      const op = 'findOneAndUpdate ' + ser(F) + ' ' + ser(U) + ' by ' + ser(S) + (newDoc ? ' returning new' : '');
      ask('findOneAndUpdate', op,
          c => c.findOneAndUpdate(F, U, { sort: S, returnNewDocument: newDoc }), 'seq');
      contentsMatch('contents after findOneAndUpdate', op);
      break;
    }
    case 14: {
      const op = 'findOneAndDelete ' + ser(F) + ' by ' + ser(S);
      ask('findOneAndDelete', op, c => c.findOneAndDelete(F, { sort: S }), 'seq');
      contentsMatch('contents after findOneAndDelete', op);
      break;
    }
    case 16: {
      // A transaction of reads and writes. The untiered reference runs it on
      // one cluster and needs nothing special; the tiered collection cannot
      // send a transaction to the other cluster without leaving the caller's
      // snapshot behind, so the router has to move the documents it reads to
      // the tier the transaction is on and replay. That is the promise being
      // compared, and it is the hardest one here.
      //
      // withTransaction, not startTransaction, because a WriteConflict is a
      // legitimate outcome that the driver is expected to retry: reporting it
      // would be reporting MongoDB rather than the router. The body is
      // rebuilt on every attempt, so nothing accumulates across a retry.
      const f2 = { _id: { $in: someIds(known, 2) } };
      const op = 'txn: count ' + ser(F) + ', updateMany ' + ser(f2) + ' ' + ser(U) +
                 ', find sorted ' + ser(S);
      ask('transaction', op, c => {
        const s = db.getMongo().startSession();
        let out = null;
        try {
          s.withTransaction(() => {
            const tc = s.getDatabase(DB).getCollection(c.getName());
            const acc = [];
            acc.push(tc.countDocuments(F));
            acc.push(tc.updateMany(f2, U).modifiedCount);
            acc.push(tc.find(F).sort(S).limit(3).toArray().length);
            out = acc;
          });
        } finally { s.endSession(); }
        return out;
      }, 'seq');
      contentsMatch('contents after a transaction', op);
      break;
    }
    default: {
      const idA = anId, idB = pick(known.length ? known : [1]);
      const ops = [
        { updateOne: { filter: { _id: idA }, update: U } },
        { deleteOne: { filter: { _id: idB } } },
        { insertOne: { document: { _id: 3000 + i, n: ri(70), grp: 'alpha', tags: ['b'] } } },
      ];
      const op = 'bulkWrite ' + ser(ops);
      ask('bulkWrite', op, c => {
        const r = c.bulkWrite(ops, { ordered: false });
        return [r.matchedCount, r.modifiedCount, r.deletedCount, r.insertedCount];
      }, 'seq');
      contentsMatch('contents after bulkWrite', op);
    }
  }
}

for (let i = 0; i < OPS; i++) {
  try { oneOp(i); }
  catch (e) {
    // A generator that produced something mongosh itself cannot send is a
    // harness fault, not a finding, and it must not be silent.
    print('  HARNESS  op ' + i + ': ' + String(e.message || e).slice(0, 160));
  }
}

print('AGREED ' + agreed + '  REFUSED ' + refused + '  SETTLED ' + settled + '  DISAGREED ' + bugs.length);
// One line naming every shape this round compared, as done/vacuous. A shape
// missing from it was never generated; a shape whose two numbers are equal
// compared nothing but error messages.
print('SHAPES ' + Object.keys(shapes).sort()
  .map(k => k.replace(/ /g, '_') + '=' + shapes[k].n + '/' + shapes[k].vacuous).join(' '));
for (const [w, op, ref, got] of bugs) {
  print('DISAGREE\t' + w + '\t' + op + '\n\t  ref=' + ref + '\n\t  got=' + got);
}
for (const [w, op, msg] of refusals) print('REFUSAL\t' + w + '\t' + op + '\n\t  ' + msg);
for (const [w, op, ref, got] of stale) print('STALE\t' + w + '\t' + op + '\n\t  ref=' + ref + '\n\t  got=' + got);
