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
const TRACE = process.env.TC_TRACE === '1';
// The strict section asks a different question: with a fast tier holding every
// document there is nothing the router cannot answer, so every refusal there is
// the completeness guard being more cautious than the data requires. A TTL index
// on a tiered collection is refused by a different guard, for a reason that has
// nothing to do with completeness: it would delete the newest documents and keep
// the oldest. Generating one there would make that section fail for the guard
// working.
const NO_TTL = process.env.TC_NO_TTL === '1';
// Set by a section that seeds both tiers itself and runs no feeder. See
// waitForMirroring.
const NO_FEEDER = process.env.TC_NO_FEEDER === '1';
// Compare what a change stream on each collection delivered for the round's own
// writes. Off by default because it holds two cursors open for the whole round.
const STREAM = process.env.TC_STREAM === '1';
// Read the same collections through views. Off by default because it creates
// four of them and drops them again, which is DDL the other sections would
// otherwise have to work around.
const VIEWS = process.env.TC_VIEWS === '1';
const VIEW_OPS = Number(process.env.TC_VIEW_OPS || 12);
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
    // Ordered, because the two values are drawn independently and about half
    // of them came out with the start after the end. An empty range matches
    // nothing on either side, so the operation agreed without having asked
    // anything about tiering, and this is the one predicate shape that puts a
    // lower bound and an upper bound on the same field at once, which is how an
    // application asks for a period. Measured on one 200 operation round: 9 of
    // the generated `when` predicates carried a $gte and the ones that were
    // ranges were mostly backwards.
    case 9:  {
      const lo = (v <= w) ? v : w, hi = (v <= w) ? w : v;
      return lo < hi ? { $gte: lo, $lt: hi } : { $gte: lo };
    }
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

// AGE is the field the tiered collections here are aged by, and ageBound builds
// a lower bound on it at the top level of a filter, where the router will read
// one.
//
// Deliberate, because left to chance it was almost never generated, and it is
// the centre of the system: the router reads a lower bound out of a filter to
// decide whether the fast tier can answer the query alone, and everything the
// completeness claim is for turns on that. `when` is one of ten fields, a leaf
// takes one or two of them, five of the fourteen predicate shapes yield a bound
// at all, and a fifth of filters wrap the leaf in $or or $nor where reading one
// would be wrong. Measured against a claim deliberately made false: 600
// generated operations over three seeds produced no disagreement at all, while
// a direct query on the same pair was short by twenty.
const AGE = 'when';
function ageBound() {
  const f = FIELDS.find(x => x.p === AGE);
  const v = f.v(), w = f.v();
  switch (ri(3)) {
    case 0: return { $gte: v };
    case 1: return { $gt: v };
    default: {
      const lo = (v <= w) ? v : w, hi = (v <= w) ? w : v;
      return lo < hi ? { $gte: lo, $lt: hi } : { $gte: lo };
    }
  }
}

function filter(depth) {
  if (depth <= 0) return leaf();
  const r = rnd();
  if (r < 0.14) return { $and: [filter(depth - 1), filter(depth - 1)] };
  if (r < 0.30) return { $or:  [filter(depth - 1), filter(depth - 1)] };
  if (r < 0.36) return { $nor: [filter(depth - 1)] };
  if (r < 0.42) return { items: { $elemMatch: { qty: predicate(FIELDS[5]), sku: predicate(FIELDS[6]) } } };
  if (r < 0.46) return { $expr: { $gt: [ '$n', ri(70) ] } };
  // The age field last, so a leaf that happened to pick it too does not
  // overwrite the bound with a predicate that yields none.
  if (r < 0.58) return Object.assign(leaf(), { [AGE]: ageBound() });
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

// setValue is what $set writes for a field, which is the field's own generated
// value except for tags.
//
// tags is the field the array operators work on, and $set writing a bare string
// to it turns a later $push, $pull or $addToSet into an error for that document
// alone. An updateMany is not atomic across documents: it applies them one at a
// time and stops at the first error, so such a write leaves a partial result
// whose extent depends on the order the documents were visited, and the two
// collections need not visit them in the same order.
//
// Measured: a $pull over a collection where one document's tags had been $set to
// a string threw on both sides, was correctly counted as agreed, and left the
// reference with the pull applied to a document the tiered side had not reached.
// The two collections then differed for the rest of the round, 38 disagreements,
// none of them the router. Guarding the contents comparison on the write having
// succeeded stops it being reported at the write; it does not put the
// collections back.
//
// So the type of tags is left alone. Every other field is still $set to whatever
// its generator produces, types and all, which is where the type coverage was.
function setValue(f) {
  if (f.p !== 'tags') return f.v();
  return [f.v(), f.v()];
}

function updateSpec() {
  const f = pick(FIELDS.filter(x => x.p !== '_id'));
  switch (ri(10)) {
    case 0: return { $set:   { [f.p]: setValue(f) } };
    case 1: return { $unset: { [f.p]: '' } };
    case 2: return { $inc:   { n: 1 + ri(5) } };
    case 3: return { $mul:   { f: 2 } };
    case 4: return { $min:   { 'meta.score': ri(13) } };
    case 5: return { $max:   { 'meta.score': ri(13) } };
    case 6: return { $push:  { tags: 'p' + ri(4) } };
    case 7: return { $pull:  { tags: pick(['t1', 't2', 'common']) } };
    case 8: return { $addToSet: { tags: 'a' + ri(3) } };
    default: return { $set: { [f.p]: setValue(f) }, $inc: { n: 1 } };
  }
}

// multiSafe is an update spec with the one thing taken out that must not appear
// in a write over many documents.
//
// `items` is an array in these documents, so $set of items.sku or items.qty
// fails with code 28, "Cannot create field 'sku' in element {items: [...]}",
// on every document that has one. Both sides fail identically, so the
// comparison is vacuous -- and in an updateMany it is worse than vacuous. The
// update applies to the documents without an array and stops at the first one
// with, so the two collections end up holding different things and the round
// stops there. Measured on seed 777001: four rounds in ten stopped early, one
// after nine of a hundred and fifty operations, and code 28 was the commonest
// cause of it.
//
// Left alone for single-document writes, where a failure is atomic and the
// error parity is still worth comparing: an update of an archive-only document
// has to be copied up and applied, and it has to fail the same way there.
// Rewritten rather than redrawn, so the generated stream is unchanged.
function multiSafe(u) {
  const touchesArray = Object.keys(u).some(op =>
    Object.keys(u[op]).some(f => f.indexOf('items.') === 0));
  // opt, because nothing in IDX_SPECS indexes it. The first substitute here was
  // $inc on n, which every one of those specs does index: a constant increment
  // across many documents collides on a unique one and fails partway, which is
  // the thing this function exists to avoid. A fix that reintroduces the defect
  // it removes is worth catching before it is measured rather than after.
  return touchesArray ? { $set: { opt: 'yes' } } : u;
}

// Set per side before a pipeline is generated. The two sides pick the same
// branch, because the generator is replayed from the same seed; only the name
// it lands on differs.
let SELF = 'tcref';
let OTHER = 'tcref2';

// One sub-pipeline for a $facet, and every one of them ends in a deterministic
// order.
//
// That is the whole difficulty of generating this stage. A $facet returns one
// document whose fields are arrays, and the comparison normalises the top level
// as a bag; the arrays inside are compared in order. MongoDB promises no order
// without a $sort, so a sub-pipeline that merely filters can hand the two sides
// the same documents in different orders and be reported as a disagreement
// about the router. Each shape here either ends in a $sort or collapses to a
// single value.
//
// What may not appear inside a $facet, per MongoDB: $out, $merge, $geoNear,
// $collStats, $indexStats, $planCacheStats, and another $facet. $lookup,
// $unionWith and $graphLookup are allowed, and they are the interesting ones
// here: a sub-pipeline reading a tiered collection of its own is exactly what
// the outer pipeline's filter does not reach.
function facetBranch() {
  switch (ri(5)) {
    case 0: return [{ $match: filter(0) }, { $count: 'c' }];
    case 1: return [{ $group: { _id: '$grp', c: { $sum: 1 }, s: { $sum: '$n' } } },
                    { $sort: { _id: 1 } }];
    case 2: return [{ $match: filter(0) }, { $sort: { _id: 1 } }, { $limit: 1 + ri(5) },
                    { $project: { n: 1, grp: 1 } }];
    case 3: return [{ $lookup: { from: pick([SELF, OTHER]), localField: 'grp',
                                 foreignField: 'grp', as: 'sib' } },
                    { $sort: { _id: 1 } }, { $limit: 1 + ri(4) },
                    { $project: { grp: 1, sibs: { $size: '$sib' } } }];
    default: return [{ $unionWith: { coll: pick([SELF, OTHER]),
                                     pipeline: [{ $match: filter(0) }] } },
                     { $group: { _id: '$grp', c: { $sum: 1 } } },
                     { $sort: { _id: 1 } }];
  }
}

function pipeline() {
  const stages = [];
  if (maybe(0.8)) stages.push({ $match: filter(1) });
  switch (ri(16)) {
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
    case 10:
      // Sub-pipelines over one input, which the router has to reason about
      // without a filter in front of them. It guards $facet by recursing into
      // every field, in the completeness pass and again in the rewrite, and
      // nothing generated was reaching either: the hand-written suites cover
      // the stage and the grammar never produced one.
      stages.push({ $facet: { a: facetBranch(), b: facetBranch() } });
      break;
    case 11:
      // A range expressed as boundaries rather than as a predicate, which is
      // the shape this project keeps getting wrong. MongoDB brackets a range
      // by type, and $bucket puts the boundaries in a stage rather than in a
      // filter: the router has to decide completeness for a query whose bounds
      // are not in its $match at all. `when` boundaries are dates and `n`
      // boundaries are numbers, so both the aged field and an ordinary one are
      // reached, and the default catches everything outside them, which is
      // what makes two tiers comparable rather than one of them erroring.
      stages.push(maybe(0.5)
        ? { $bucket: { groupBy: '$n', boundaries: [0, 10, 25, 50, 100],
                       default: 'outside', output: { c: { $sum: 1 } } } }
        : { $bucket: { groupBy: '$' + AGE,
                       boundaries: [new Date(Date.UTC(2024, 0, 1)), new Date(Date.UTC(2024, 4, 1)),
                                    new Date(Date.UTC(2024, 8, 1)), new Date(Date.UTC(2025, 0, 1))],
                       default: 'outside', output: { c: { $sum: 1 } } } });
      stages.push({ $sort: { _id: 1 } });
      break;
    case 12:
      // The same question asked the other way: the boundaries are chosen by
      // the server from the data it was given, so the two tiers have to have
      // been given the same documents for the answer to match at all.
      stages.push({ $bucketAuto: { groupBy: '$n', buckets: 1 + ri(4) } },
                  { $sort: { '_id.min': 1 } });
      break;
    case 13:
      // A window function, which computes over neighbouring documents rather
      // than over one. If the two tiers were given different documents the
      // windows differ and every running total is wrong, which a $group would
      // not show: a sum over the whole partition is the same however the
      // documents are split. Sorted by two fields so ties cannot make the
      // running total ambiguous and turn agreement into luck.
      stages.push({ $setWindowFields: { partitionBy: '$grp', sortBy: { n: 1, _id: 1 },
                                        output: { run: { $sum: '$n',
                                                         window: { documents: ['unbounded', 'current'] } } } } },
                  { $sort: { _id: 1 } },
                  { $project: { grp: 1, n: 1, run: 1 } });
      break;
    case 14:
      // A stage that invents documents. $densify fills the gaps in a sequence,
      // so what it emits depends on which documents it was given, and a tier
      // holding half of them produces a different set of gaps rather than half
      // the answer.
      stages.push({ $densify: { field: 'n', range: { step: 10, bounds: 'full' } } },
                  { $group: { _id: null, c: { $sum: 1 } } });
      break;
    case 15:
      // A sample, compared by how many came back rather than by which ones,
      // because which ones is random and the size is not. It says whether the
      // pool sampled from was the whole collection.
      stages.push({ $sample: { size: 1 + ri(30) } }, { $count: 'c' });
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
// The writes that touch more than one document, and so can stop half done. A
// failed updateOne or replaceOne is one document and cannot.
// bulkWrite is deliberately not here. Every operation this grammar puts in one
// is single-document -- an updateOne and a deleteOne by _id and an insertOne --
// and with ordered:false each is attempted independently, so two collections
// holding the same documents apply exactly the same subset and cannot diverge.
// Voiding for it cost a round in four measured on seed 777001 and protected
// nothing. Revisit if a multi-document operation is ever generated inside one.
const MULTI_WRITE = /^(updateMany|deleteMany|contents after (updateMany|deleteMany))/;
// Set when the two collections stop holding the same documents through no fault
// of the router. Declared here with the rest of the run's state, and not beside
// contentsMatch where it is explained, because ask reads it: a `let` further
// down the file is in the temporal dead zone for anything that runs before it.
let contentsVoid = null;
function note(what, kind) {
  const t = shapes[what] || (shapes[what] = { n: 0, vacuous: 0 });
  t.n++;
  if (kind === 'vacuous') t.vacuous++;
}

// Returns true when both sides answered, which is the only case where comparing
// what they left behind means anything. See contentsMatch.
function ask(what, op, fn, how) {
  // TC_TRACE prints every operation as it runs, in order. The seed makes a round
  // reproducible and the report names the operation that disagreed, but not what
  // ran before it, and a disagreement is usually about state something earlier
  // left behind. Off by default because it is one line per comparison.
  if (TRACE) print('  op\t' + what + '\t' + op);
  const norm = how === 'bag' ? bag : ser;
  const a = attempt(fn, REF), b = attempt(fn, TIER);

  if (!a.ok && !b.ok) {
    if (a.code === b.code) {
      agreed++; note(what, 'vacuous');
      // A write over many documents that failed has applied some of them, and
      // which ones is not specified. See contentsVoid.
      if (!contentsVoid && MULTI_WRITE.test(what)) {
        contentsVoid = what + ' failed on both sides with ' + a.code;
        print('  VOID\tcontents are no longer comparable: ' + contentsVoid +
              ', which applies to some documents and stops');
      }
      return false;
    }
    // Both failed differently. Only interesting when the tiered side is not
    // simply saying no.
    if (/^thermocline:/.test(b.msg)) { refused++; note(what, 'real'); refusals.push([what, op, b.msg]); return false; }
    note(what, 'real');
    bugs.push([what, op, 'ref failed ' + a.code + ': ' + a.msg, 'tier failed ' + b.code + ': ' + b.msg]);
    return false;
  }
  if (a.ok && !b.ok) {
    if (/^thermocline:/.test(b.msg)) { refused++; note(what, 'real'); refusals.push([what, op, b.msg]); return false; }
    note(what, 'real');
    bugs.push([what, op, 'ref answered', 'tier failed ' + b.code + ': ' + b.msg]);
    return false;
  }
  if (!a.ok && b.ok) {
    note(what, 'real');
    bugs.push([what, op, 'ref failed ' + a.code + ': ' + a.msg, 'tier answered']);
    return false;
  }

  let ra = norm(a.v), rb = norm(b.v);
  if (ra === rb) { agreed++; note(what, 'real'); return true; }

  // The archive is written by the feeder after the fact, so a difference can be
  // a stale answer that is about to settle. Ask again before calling it a bug,
  // and wait for mirroring rather than for a guess.
  waitForMirroring();
  const a2 = attempt(fn, REF), b2 = attempt(fn, TIER);
  note(what, 'real');
  if (a2.ok && b2.ok && norm(a2.v) === norm(b2.v)) {
    settled++; stale.push([what, op, ra.slice(0, 160), rb.slice(0, 160)]);
    return true;
  }
  const d = describeDiff(a.v, b.v);
  if (d) { bugs.push([what, op, d.ref, d.got]); return true; }
  bugs.push([what, op, ra.slice(0, 400), rb.slice(0, 400)]);
  return true;
}

// describeDiff narrows a disagreement between two arrays of documents to the
// first place they differ.
//
// Without it a contents comparison reports the first 400 characters of two
// dumps that are identical for the first several documents, so every one of them
// looks the same and none of them says anything. Measured: 98 disagreements in
// one run, every report opening with the same _id 2, and the difference
// somewhere past the truncation.
//
// Returns null when the values are not both arrays, which leaves the plain
// report in place.
function describeDiff(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b)) return null;
  const n = Math.max(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const x = i < a.length ? ser(a[i]) : '(missing)';
    const y = i < b.length ? ser(b[i]) : '(missing)';
    if (x === y) continue;
    const id = (i < b.length && b[i] && b[i]._id !== undefined) ? b[i]._id
             : (i < a.length && a[i] && a[i]._id !== undefined) ? a[i]._id : null;
    return {
      ref: a.length + ' documents, first difference at ' + i + ': ' + x.slice(0, 700),
      got: b.length + ' documents, first difference at ' + i + ': ' + y.slice(0, 700)
           + eachTier(id),
    };
  }
  return null;
}

// eachTier says which tier the tiered side's answer came from, by asking both of
// them for the same document.
//
// A disagreement about a document is one of three things and they need different
// fixes: the archive holds the old value, which is mirroring; the fast tier holds
// the old value, which is a write that did not land; or both hold the new value
// and the router answered with neither, which is the router. Reporting only the
// router's answer leaves all three looking identical, and a whole evening went
// into guessing between them.
//
// Only the archive is reachable from here, because TC_COLD is what the runner
// passes. That is still the one that decides: the tiered read fell through to
// it, so if the archive agrees with the reference the answer did not come from
// where it should have.
function eachTier(id) {
  if (id === null || id === undefined) return '';
  let out = '';
  for (const [name, conn] of [['fast tier', HOT], ['archive', COLD]]) {
    if (!conn) continue;
    try {
      const d = conn.getDB(DB).getCollection(TIER.getName()).findOne({ _id: id });
      // A tombstone is not a document the archive holds, it is the archive
      // remembering one the application deleted. Reporting it as held sent a
      // reader looking for a lost write where there was a correct delete.
      const mark = d && d._tcDeletedAt ? ' (TOMBSTONED, deleted at ' + d._tcDeletedAt + ')' : '';
      out += '\n\t  ' + name + ' holds: ' + (d ? mark + ser(d).slice(0, 700) : '(nothing)');
    } catch (e) { /* a tier that cannot be read says nothing rather than lying */ }
  }
  return out;
}

// The archive this run's tiered collection falls through to, when the runner
// gave us its address. Read only, and only for the feeder's own checkpoint.
const COLD = process.env.TC_COLD ? new Mongo(process.env.TC_COLD) : null;
const HOT = process.env.TC_HOT ? new Mongo(process.env.TC_HOT) : null;
const META = process.env.TC_META || '_thermocline';

// waitForMirroring holds until mirroring has reached the instant this round has
// written up to, so a second look at a disagreement is a second look and not a
// coin toss.
//
// It used to be sleep(1500), which is a guess, and the guess was wrong under
// load. Measured on one round of 150 operations: 53 disagreements reported, and
// every one of them gone when the same two collections were compared 15 seconds
// later with the daemons still up. 58 documents each, nought differing
// positions. The router had logged "answering from the archive before mirroring
// caught up" eleven times in that round, which is the bounded behaviour working
// and saying so.
//
// A harness that reports 53 wrong answers where there are none is worse than no
// harness: the one real disagreement would be somewhere in the list and nobody
// would look.
//
// Two records could be waited on and only one of them is sound. The checkpoint
// answers "where does the feeder resume", and its instant is the *seconds* half
// of a cluster time, so everything inside one second reads as simultaneous:
// waiting on it returned "caught up" after 500ms while the archive still did not
// hold the write, and the round still reported 53 disagreements. The position
// record answers "how far has mirroring got", carries the cluster time whole,
// and is written after the batch lands. That is the one the router's own barrier
// compares against, and it is the one to compare against here.
//
// The target is a cluster time and not a clock reading, for the reason the
// position record itself gives: it is the replica set's own logical clock, so
// both sides read it from the same place and neither machine's clock comes into
// it. ping through the router carries one back from the fast tier, which is
// where the writes just went.
//
// Bounded, and it falls back to the old sleep when the runner did not pass an
// archive address, because a comparison that hangs is its own kind of useless.
function waitForMirroring() {
  // TC_NO_FEEDER says the two tiers were seeded directly and no feeder is
  // running, so there is no mirroring to wait for. Without it this waits the
  // full thirty seconds for a position that will never move and then warns that
  // the disagreements below may be the feeder, which is the one caveat that
  // must not be printed when it cannot be true: a warning on every round is a
  // warning nobody reads, and this one would discount a real disagreement.
  if (NO_FEEDER) { if (TRACE) print('  wait\tno feeder in this section, nothing to wait for'); return; }
  if (!COLD) { if (TRACE) print('  wait\tno TC_COLD, sleeping 1500ms'); sleep(1500); return; }

  const target = clusterTimeNow();
  if (!target) { sleep(1500); return; }
  const pos = COLD.getDB(META).getCollection('positions');
  for (let i = 0; i < 60; i++) {
    sleep(500);
    let p = null;
    try { p = pos.findOne({ _id: 'test' }); } catch (e) {
      if (TRACE) print('  wait\tcould not read the position: ' + String(e.message).slice(0, 80));
      break;
    }
    if (p && p.ts && cmpTS(p.ts, target) >= 0) {
      if (TRACE) print('  wait\tmirroring reached this cluster time after ' + ((i + 1) * 500) + 'ms');
      return;
    }
  }
  print('  HARNESS  mirroring did not reach this round\'s cluster time within 30s, so the ' +
        'disagreements below may be the feeder rather than the router');
}

// clusterTimeNow is the fast tier's own logical clock, taken through the router
// so it is the clock the writes above were stamped with.
function clusterTimeNow() {
  try {
    const r = db.getSiblingDB('admin').runCommand({ ping: 1 });
    return r.operationTime || (r.$clusterTime && r.$clusterTime.clusterTime) || null;
  } catch (e) { return null; }
}

// showTS prints a cluster time the way the two halves actually compare.
function showTS(v) {
  const t = Number(v.t !== undefined ? v.t : v.getTime());
  const i = Number(v.i !== undefined ? v.i : v.getInc());
  return t + '.' + i;
}

// cmpTS orders two BSON timestamps, seconds first and increment second, which is
// how a cluster time orders. Comparing the seconds alone would treat everything
// inside one second as simultaneous, which is the mistake the checkpoint's own
// instant made here.
function cmpTS(a, b) {
  const at = Number(a.t !== undefined ? a.t : a.getTime()), bt = Number(b.t !== undefined ? b.t : b.getTime());
  if (at !== bt) return at < bt ? -1 : 1;
  const ai = Number(a.i !== undefined ? a.i : a.getInc()), bi = Number(b.i !== undefined ? b.i : b.getInc());
  if (ai !== bi) return ai < bi ? -1 : 1;
  return 0;
}

// After a write the two collections have to hold the same documents, which is a
// stronger statement than the write's own reported counts and catches a write
// that landed on one document too many.
//
// Only after a write that succeeded on both sides, which is why every caller
// guards it on what ask returned. An updateMany is not atomic across documents:
// it applies them one at a time and stops at the first error, so a write that
// failed part way leaves a partial result whose extent depends on the order the
// documents were visited, and two collections need not visit them in the same
// order. Measured: {$pull: {tags: "common"}} over a collection where one
// document's tags had been $set to a string failed on both sides with "Cannot
// apply $pull to a non-array value", was correctly counted as agreed, and then
// the contents comparison after it reported 45 disagreements across the rest of
// the round. None of them was the router.
// Whether the two collections can still be expected to hold the same documents.
//
// They cannot, after a multi-document write that failed on both sides. An
// updateMany applies document by document and stops at the first error, keeping
// what it has already done, and MongoDB does not specify the order it visits
// them in. So both sides fail the same way -- the answers agree, and ask counts
// that -- while a different subset of documents carries the change.
//
// Measured, and it is the reason this exists: seed 800758 operation 68,
// `updateMany {$or: [{meta.score: {$mod: [1,0]}}, ...]} {$set: {n: 53}}`, with a
// unique partial index on n. Both sides returned E11000 for `dup key: { n: 53 }`
// and the reference had stopped before document 44 while the tiered side had
// already written it. Every contents comparison for the rest of the round then
// disagreed, 40 of them in a 150-operation round, all reporting the same
// document and none of them a defect. It cost most of a session to chase.
//
// So the round ends there, loudly. Skipping only the contents comparison was
// the first attempt and it was half a fix: once the two collections hold
// different documents, every *read* of them can differ too, and the same seed
// went on reporting aggregate, find, countDocuments and distinct disagreements
// instead -- 30 in one round. Nothing compared after this point means anything.
function contentsMatch(after, op) {
  if (contentsVoid) { note(after, 'vacuous'); return; }
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

// Index DDL, generated the same way everything else here is.
//
// The shape the hand-written suites cannot reach, and the one every defect found
// on 2026-09-05 lived in: this router remembers things about a namespace that
// only stay true while its indexes and its collection do. It caches the fast
// tier's unique indexes for a minute, whether a hint is answerable on the
// archive for the same, the collation, and the type brackets of the field a
// collection ages by. A read taken while one of those is stale is a wrong
// answer that no read-only grammar can produce, because nothing in it ever
// changes an index.
//
// Both sides get the same command, so the comparison is unchanged: the
// reference is untiered and mongod answers it, and the tiered side goes through
// the router's DDL guards first. Where a guard refuses, that is a REFUSAL and
// the census is worth having; where it does not, the answers to every read after
// it have to agree as before.
//
// Deterministic names, because the two sides are two collections and a drop has
// to be able to name the index the create made on both of them.
const IDX_SPECS = [
  { spec: { n: 1 },                     name: 'fz_n' },
  { spec: { grp: 1, n: -1 },            name: 'fz_grp_n' },
  { spec: { 'meta.score': 1 },          name: 'fz_score' },
  { spec: { tags: 1 },                  name: 'fz_tags' },
  { spec: { 'items.sku': 1, n: 1 },     name: 'fz_sku_n' },
  { spec: { when: -1 },                 name: 'fz_when' },
  { spec: { name: 1 },                  name: 'fz_name' },
];

// What is put on top of a spec, and each option is here because something in
// the router reads it.
//
//   unique     the registry claims keys across the tiers for one of these
//   sparse     narrows which documents an index covers, which is what decides
//              whether a key is claimed for a document at all
//   partial    the same, by predicate, and the fields the predicate reads are
//              what makes a write move a document into the index
//   collation  two spellings are one key and their bytes are not
//   ttl        refused on a tiered collection, because it is the archive
//              deleting history nobody asked it to
// The longest retention a TTL index can express, which is int32 seconds, about
// 68 years. It has to be a retention that expires nothing.
//
// A day was the first value here and it cost an evening. The tiered side refuses
// a TTL index, because that is the archive deleting history nobody asked it to;
// the reference is untiered, so mongod accepts it and starts expiring. The
// documents are dated 2024, so within a minute the reference had emptied itself
// and every contents comparison after that disagreed: 112 disagreements in ten
// rounds, none of them about the router. Measured on a throwaway collection of
// twenty documents with the same index: 20 before, 0 after.
//
// This is the harness lying about the router, which is the one failure this file
// cannot afford, and the second time this grammar has done it in a day. A
// generated operation is only safe when it means the same thing on both sides.
const TTL_NEVER = 2147483647;

function indexOptions() {
  const o = {};
  if (maybe(0.25)) o.unique = true;
  switch (ri(6)) {
    case 0: o.sparse = true; break;
    case 1: o.partialFilterExpression = { n: { $gt: 10 } }; break;
    case 2: o.partialFilterExpression = { grp: 'alpha' }; break;
    case 3: o.collation = { locale: 'en', strength: 2 }; break;
    case 4: if (!o.unique && !NO_TTL) o.expireAfterSeconds = TTL_NEVER; break;
    default: break;
  }
  return o;
}

// optionSuffix names an option shape, so one key can carry several indexes.
function optionSuffix(o) {
  let out = '';
  if (o.unique) out += '_u';
  if (o.sparse) out += '_s';
  if (o.partialFilterExpression) out += '_p' + Object.keys(o.partialFilterExpression).join('');
  if (o.collation) out += '_c';
  if (o.expireAfterSeconds !== undefined) out += '_ttl';
  return out;
}

// legalOptions is indexOptions with the combinations mongod itself rejects taken
// out, so a refusal in a round is this router's and not the server's.
//
// A TTL index is only legal on a single date field and never together with
// unique, and this collection's one date field is `when`. Generating an illegal
// pair would have both sides fail identically for ever, which counts as agreed
// and proves nothing.
function legalOptions(entry) {
  const o = indexOptions();
  if (o.expireAfterSeconds !== undefined && entry.name !== 'fz_when') {
    delete o.expireAfterSeconds;
  }
  return o;
}

// Every index this run has created, by name, so a drop names one that exists.
// Shared between the two sides on purpose: they are given the same commands, so
// a name created on one was created on the other.
const madeIndexes = [];

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
  // DDL is its own draw rather than a member of WRITES, because it is rare in
  // an application and every one of these changes what the router has cached
  // about the namespace for the operations that follow. Too many and a round
  // spends itself building indexes instead of comparing answers.
  const DDL = [17, 17, 18, 19, 19];
  const choice = RO ? pick(READS)
                    : maybe(0.10) ? pick(DDL)
                    : maybe(0.60) ? pick(READS) : pick(WRITES);
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
      // null is dropped from both answers, and it is the one value this
      // comparison cannot make a statement about.
      //
      // MongoDB reports a missing field as null only when it can answer from a
      // non-sparse index, because an index stores a missing field as a null
      // key: measured, ["NULL",5,7] with a plain index and [5,7] with none or
      // with a sparse one. The untiered reference is one collection with its
      // own indexes; the tiered side falls through to the archive, and every
      // archive read carries a predicate excluding tombstones, which turns the
      // DISTINCT_SCAN into an IXSCAN and loses the null. So the two disagree
      // about null whatever the router does, and the difference is written down
      // as a limitation rather than being reported here once per round.
      //
      // Every real value is still compared, which is what this shape is for.
      ask('distinct', 'distinct ' + f + ' ' + ser(F) + (Object.keys(M).length ? ' with ' + ser(M) : ''),
          c => c.distinct(f, F, M.collation ? { collation: M.collation } : {})
                .filter(v => v !== null).map(ser).sort(), 'seq');
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
      // multiSafe, because this one writes many documents and a half-applied
      // failure ends the round. See multiSafe.
      const UM = multiSafe(U);
      const op = 'updateMany ' + ser(F) + ' ' + ser(UM);
      if (ask('updateMany', op, c => { const r = c.updateMany(F, UM); return [r.matchedCount, r.modifiedCount]; }, 'seq')) contentsMatch('contents after updateMany', op);
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
      if (ask('deleteMany', op, c => c.deleteMany(f).deletedCount, 'seq')) contentsMatch('contents after deleteMany', op);
      break;
    }
    case 8: {
      // One-document writes are anchored on an _id. deleteOne on a filter that
      // matches several picks whichever document it likes, on both sides
      // independently, and comparing that reports MongoDB disagreeing with
      // itself.
      const f = { _id: anId };
      const op = 'updateOne ' + ser(f) + ' ' + ser(U);
      if (ask('updateOne', op, c => { const r = c.updateOne(f, U); return [r.matchedCount, r.modifiedCount]; }, 'seq')) contentsMatch('contents after updateOne', op);
      break;
    }
    case 9: {
      const f = { _id: anId };
      const op = 'deleteOne ' + ser(f);
      if (ask('deleteOne', op, c => c.deleteOne(f).deletedCount, 'seq')) contentsMatch('contents after deleteOne', op);
      break;
    }
    case 10: {
      const doc = { _id: anId, n: ri(70), grp: pick(['alpha', 'beta']), tags: ['r' + ri(3)],
                    meta: { score: ri(13), active: maybe(0.5) }, when: new Date(Date.UTC(2024, ri(12), 1 + ri(27))) };
      const op = 'replaceOne ' + ser(doc);
      if (ask('replaceOne', op, c => { const r = c.replaceOne({ _id: anId }, doc); return [r.matchedCount, r.modifiedCount]; }, 'seq')) contentsMatch('contents after replaceOne', op);
      break;
    }
    case 11: {
      const doc = { _id: 1000 + i, n: ri(70), grp: pick(['alpha', 'beta', 'gamma']),
                    name: 'new' + i, tags: ['t' + ri(3), 'common'],
                    items: [{ sku: 's' + ri(4), qty: ri(70) }],
                    meta: { score: ri(13), active: maybe(0.5) },
                    when: new Date(Date.UTC(2024, ri(12), 1 + ri(27))) };
      const op = 'insertOne ' + ser(doc);
      if (ask('insertOne', op, c => c.insertOne(doc).insertedId, 'seq')) contentsMatch('contents after insertOne', op);
      break;
    }
    case 12: {
      // An upsert whose filter carries the _id, so the document it creates when
      // nothing matches is the same document on both sides. Without that each
      // side invents its own ObjectId.
      const f = { _id: 2000 + ri(8), grp: pick(['alpha', 'beta']) };
      const op = 'upsert ' + ser(f) + ' ' + ser(U);
      if (ask('upsert', op, c => { const r = c.updateOne(f, U, { upsert: true }); return [r.matchedCount, r.modifiedCount, ser(r.upsertedId)]; }, 'seq')) contentsMatch('contents after upsert', op);
      break;
    }
    case 13: {
      const newDoc = maybe(0.5);
      const op = 'findOneAndUpdate ' + ser(F) + ' ' + ser(U) + ' by ' + ser(S) + (newDoc ? ' returning new' : '');
      if (ask('findOneAndUpdate', op,
              c => c.findOneAndUpdate(F, U, { sort: S, returnNewDocument: newDoc }), 'seq')) {
        contentsMatch('contents after findOneAndUpdate', op);
      }
      break;
    }
    case 14: {
      const op = 'findOneAndDelete ' + ser(F) + ' by ' + ser(S);
      if (ask('findOneAndDelete', op, c => c.findOneAndDelete(F, { sort: S }), 'seq')) contentsMatch('contents after findOneAndDelete', op);
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
      // attempts is counted and reported, not compared. A transaction the router
      // asked to be replayed runs its body more than once, by design, and the
      // reference never does; comparing the number would report the design as a
      // defect. It is in the op line because a transaction that gave up is a
      // transaction that ran its body many times first, and that is invisible
      // otherwise.
      const tries = { ref: 0, tier: 0 };
      const txnOK = ask('transaction', op, c => {
        const s = db.getMongo().startSession();
        let out = null;
        const side = c.getName() === REF.getName() ? 'ref' : 'tier';
        try {
          s.withTransaction(() => {
            tries[side]++;
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
      if (TRACE || tries.tier > 1) {
        print('  txn\tbody ran ' + tries.ref + ' times on the reference and ' +
              tries.tier + ' on the tiered collection');
      }
      if (txnOK) contentsMatch('contents after a transaction', op);
      break;
    }
    case 17: {
      const entry = pick(IDX_SPECS);
      const opts = legalOptions(entry);
      // The name carries the option shape, so two indexes on one key never
      // collide under one name.
      //
      // They did, and it cost the last disagreement in a clean round. A TTL
      // index is refused on a tiered collection and accepted on the untiered
      // reference, which is the refusal working; it leaves fz_when existing on
      // one side and not the other. The next createIndex on the same key with
      // different options was then an IndexOptionsConflict on the reference,
      // code 86, and a real build on the tiered side that failed 11000 on the
      // fast tier's own duplicates. Two failures with different codes is a
      // disagreement, and it was about the generator reusing a name.
      const name = entry.name + optionSuffix(opts);
      const full = Object.assign({ name: name }, opts);
      const op = 'createIndex ' + ser(entry.spec) + ' ' + ser(opts) + ' as ' + name;
      // No try/catch of its own: ask compares how the two sides failed as
      // carefully as it compares what they answered, and swallowing the error
      // here would throw away the half of the comparison that matters. A build
      // both sides refuse the same way is vacuous, one refused only on the
      // tiered side is a refusal, and one that fails on the tiered side alone
      // for another reason is a finding.
      const madeOK = ask('createIndex', op, c => c.createIndex(entry.spec, full), 'seq');
      // A refusal leaves the two catalogs different, and that difference outlives
      // the operation it came from.
      //
      // A TTL index on a tiered collection is refused by design -- it would
      // delete the newest documents and keep the oldest -- and the untiered
      // reference accepts it. Encoding the options in the name, above, stops two
      // indexes on one key colliding under one name, and does not help here:
      // mongod refuses a later *equivalent* index under a different name with
      // code 85, "An equivalent index already exists with a different name and
      // options". So the reference failed 85 where the tiered side built the
      // index happily, and that was reported as a disagreement about the router.
      // Measured on seed 840353, the last failure in an otherwise clean run.
      //
      // Only the reference is corrected, never the tiered side: the oracle is
      // ours to keep in step, and dropping an index through the router to make a
      // comparison agree would be hiding whatever it was about to find.
      if (!madeOK) {
        const onRef  = REF.getIndexes().some(i => i.name === name);
        const onTier = TIER.getIndexes().some(i => i.name === name);
        if (onRef && !onTier) {
          try {
            REF.dropIndex(name);
            if (TRACE) print('  op\tcatalogs realigned: dropped ' + name + ' from the reference, ' +
                             'which the tiered side refused');
          } catch (e) { /* nothing to put back */ }
        }
      }
      // Only a name both sides now carry. A TTL index is refused on the tiered
      // collection and accepted on the untiered reference, so recording it
      // regardless left a later dropIndex succeeding on one side and answering
      // "index not found" on the other, which is a disagreement about the
      // generator rather than about the router.
      if (madeOK && madeIndexes.indexOf(name) < 0) madeIndexes.push(name);
      // The documents cannot have changed, and this is where a stale cache
      // shows: the read below runs against a collection whose indexes have just
      // moved under a router that had listed them.
      if (madeOK) contentsMatch('contents after createIndex', op);
      break;
    }
    case 18: {
      if (!madeIndexes.length) { break; }
      const name = pick(madeIndexes);
      const op = 'dropIndex ' + name;
      // Whether the index is gone, which is what a drop is for.
      //
      // Not the whole reply, which carries $clusterTime and operationTime and
      // those differ between any two commands by construction: comparing them
      // reported a disagreement on every single drop. And not nIndexesWas
      // either, which was the first answer to that and is an index *count*
      // across two collections whose index sets legitimately differ. The router
      // builds the archive's indexes on the fast tier as documents arrive
      // there, so with an evictor running the counts move on one side and not
      // the other: measured, one disagreement in twelve churn rounds, and it
      // was this.
      const dropped = ask('dropIndex', op, c => {
        c.dropIndex(name);
        return c.getIndexes().some(i => i.name === name) ? 'still there' : 'gone';
      }, 'seq');
      if (dropped) contentsMatch('contents after dropIndex', op);
      break;
    }
    case 19: {
      // The index listing this router keeps is per namespace and a minute long,
      // so what it enforces after a create is what it read before one. A write
      // straight after the DDL is where that shows, and an insert is the write
      // that has to claim a key for every unique index there is.
      //
      // name is drawn from a small set on purpose, so a unique index on it has
      // duplicates to refuse rather than never firing.
      const doc = { _id: 4000 + i, n: ri(70), grp: pick(['alpha', 'beta']),
                    name: 'ddl' + ri(6), tags: ['t' + ri(3)],
                    items: [{ sku: 's' + ri(4), qty: ri(70) }],
                    meta: { score: ri(13), active: maybe(0.5) },
                    when: new Date(Date.UTC(2024, ri(12), 1 + ri(27))) };
      const op = 'insert straight after DDL ' + ser(doc);
      if (ask('insertAfterDDL', op, c => String(c.insertOne(doc).insertedId), 'seq')) contentsMatch('contents after an insert following DDL', op);
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
      const bulkOK = ask('bulkWrite', op, c => {
        const r = c.bulkWrite(ops, { ordered: false });
        return [r.matchedCount, r.modifiedCount, r.deletedCount, r.insertedCount];
      }, 'seq');
      if (bulkOK) contentsMatch('contents after bulkWrite', op);
    }
  }
}

// Views, when asked for. See VIEWS.
//
// A view's own pipeline is stored on the server and not sent with the query, so
// every judgement the router makes about a read of one is made without it. Four
// wrong answers on 2026-09-08 were exactly that, and not one of them was
// reachable from anything else this file generates: every operation above names
// a collection.
//
// Two views on each side. A plain one, where the two answers have to match
// exactly, which is what tests that a read through a view is resolved to the
// collection underneath it and judged there. And one that joins the second pair,
// where the tiered side's join is short on the fast tier: there the tiered side
// has to either match or refuse, and answering a short join is the defect.
//
// Each side's view joins its own second collection, so the reference's view
// joins the untiered one and is whole by construction. The reference is the
// oracle here as everywhere else in this file.
function viewsSection() {
  const plainOf  = c => d.getCollection(c.getName() === REF.getName() ? 'tcrefv'  : 'tcfuzzv');
  const joinedOf = c => d.getCollection(c.getName() === REF.getName() ? 'tcrefvj' : 'tcfuzzvj');
  const defs = [
    ['tcrefv',   REF.getName(),  null],
    ['tcfuzzv',  TIER.getName(), null],
    ['tcrefvj',  REF.getName(),  REF2.getName()],
    ['tcfuzzvj', TIER.getName(), TIER2.getName()],
  ];
  const dropAll = () => {
    for (const [name] of defs) { try { d.getCollection(name).drop(); } catch (e) { /* gone */ } }
  };
  for (const [name, on, from] of defs) {
    try { d.getCollection(name).drop(); } catch (e) { /* not there */ }
    const pipe = from ? [{ $lookup: { from, localField: 'grp', foreignField: 'grp', as: 'j' } }] : [];
    try { d.createView(name, on, pipe); }
    catch (e) {
      // Whatever was made before the one that failed goes too. A view left in a
      // worker's database is a collection the next suite's catalog checks find
      // and nobody put there.
      print('  HARNESS  could not create ' + name + ': ' + String(e.message).slice(0, 120));
      dropAll();
      return;
    }
  }
  // Created through the router, which mirrors a view down as it makes one. The
  // archive needs its copy or a read the fast tier cannot answer is refused for
  // the view being missing rather than for anything this section is about.
  sleep(1500);

  for (let i = 0; i < VIEW_OPS; i++) {
    const F = filter();
    const S = sortSpec();
    // The plain view: two answers that have to be the same.
    ask('view read', 'view find ' + ser(F) + ' sort ' + ser(S),
        c => plainOf(c).find(F).sort(S).limit(8).toArray(), 'seq');
    ask('view count', 'view count ' + ser(F), c => plainOf(c).countDocuments(F), 'seq');
    // The joining view. Only how many rows each document joined, because the
    // joined documents themselves are the second collection over again and the
    // number is what the defect changes.
    ask('joining view', 'joining view find ' + ser(F),
        c => joinedOf(c).find(F).sort({ _id: 1 }).limit(8).toArray()
              .map(x => x._id + ':' + (x.j ? x.j.length : 'none')), 'seq');
    ask('joining view aggregate', 'joining view aggregate ' + ser(F),
        c => joinedOf(c).aggregate([{ $match: F }, { $sort: { _id: 1 } }, { $limit: 8 },
                                    { $project: { _id: 1, jn: { $size: '$j' } } }]).toArray(), 'seq');
    // distinct over the joined half, which is the one read shape a $lookup can
    // shorten without changing how many rows come back.
    ask('joining view distinct', 'joining view distinct j.name ' + ser(F),
        c => joinedOf(c).distinct('j.name', F).filter(v => v !== null).map(ser).sort(), 'seq');
    // And a point read by _id, which skips the completeness guard on purpose and
    // has a join all the same.
    const one = 1 + ri(60);
    ask('joining view point read', 'joining view findOne _id ' + one,
        c => { const x = joinedOf(c).findOne({ _id: one }); return x ? [x._id, x.j.length] : []; }, 'seq');
  }

  dropAll();
}
if (VIEWS) viewsSection();

// Change streams, when asked for. See STREAM.
//
// Opened before the operations and drained after them, on both collections at
// once, so the two sequences describe the same work. The reference is untiered
// and its stream is MongoDB's own; the tiered one is served by the router from
// the fast tier, and it has to hide this system's own writes from it: a copy-up
// is an insert on the fast tier that the application did not make, and an
// eviction is a delete of a document that still exists. Both would otherwise
// arrive as ordinary events, which is a change stream telling an application
// its data changed when it did not.
//
// That is where the three defects of 2026-09-05 were, and all of them were
// about *when* an event is judged rather than whether. This asks the question
// the other way round: run the same writes against both and compare what the
// two streams delivered.
let csRef = null, csTier = null, csDb = null;
if (STREAM) {
  csRef  = REF.watch([]);
  csTier = TIER.watch([]);
  // The same collection watched a second way. A change stream opened on the
  // database, or on the whole deployment, carries the tiered collection's
  // events too, and the router has to filter those exactly as it filters the
  // collection's own stream. It did not filter them at all until 2026-09-05,
  // and nothing compared the two ways of asking.
  csDb = d.watch([]);
  // Primed, because watch() does not necessarily open the cursor: the driver can
  // wait until the first read, and then the stream starts at that instant rather
  // than at this one. Measured before this, with the operations running between
  // the two: the reference delivered 14 events and the tiered collection 9, and
  // the five missing ones were the *earliest*, which is a stream that started
  // late rather than a router that suppressed anything. A comparison of two
  // streams that began at different moments is not a comparison.
  csRef.tryNext(); csTier.tryNext(); csDb.tryNext();
}

let ran = 0;
// TC_WATCH=<id> prints one document from both sides after every operation.
//
// Kept because it is the only thing that has ever found which operation
// diverged two collections. The report names the operation that *noticed*, and
// that is rarely the one that did it: an operation whose own answer agreed gets
// no contents check, so the difference surfaces at the next write that does.
// Twice now the answer has come from watching one _id through a round.
//
// Printed as an op line because oneRound filters the round's output and a prefix
// of its own would be dropped before anybody saw it.
const WATCH = Number(process.env.TC_WATCH || 0);
function watch(i) {
  if (!WATCH) return;
  const f = d => d ? ('n=' + d.n) : 'MISSING';
  // The archive keeps a deleted document as a tombstone rather than removing
  // it, so "the archive holds it" and "the archive says it is gone" look the
  // same through findOne. They are the two halves of every question about a
  // delete, and reading `n=` for both is how an afternoon goes missing.
  const g = d => !d ? 'MISSING' : (d._tcDeletedAt ? 'TOMBSTONE' : 'n=' + d.n);
  const rs = f(REF.findOne({ _id: WATCH }));
  const ts = f(TIER.findOne({ _id: WATCH }));
  // Each tier as well as the router, for the reason the disagreement report
  // gives: the router answering differently from what both tiers hold is a
  // different fault from the tiers disagreeing with each other, and one line
  // showing only the router leaves them looking identical.
  const hs = HOT ? f(HOT.getDB(DB).getCollection(TIER.getName()).findOne({ _id: WATCH })) : '-';
  const cs = COLD ? g(COLD.getDB(DB).getCollection(TIER.getName()).findOne({ _id: WATCH })) : '-';
  print('  op\tWATCH ' + i + ': ref ' + rs + ' | router ' + ts + ' | fast ' + hs + ' | archive ' + cs +
        ' | ' + mirroringSays() +
        (rs === ts ? '' : '   <<<< ROUTER DIFFERS FROM REF'));
}

// mirroringSays reports how far the feeder claims to have mirrored, as a cluster
// time, beside the fast tier's own clock at this instant.
//
// The freshness barrier compares exactly these two numbers, so a read that was
// let through without waiting is a claim that the first had passed the second.
// Printing them beside the document turns "the barrier did not wait" from an
// inference about a counter into something that can be read off the line.
function mirroringSays() {
  if (!COLD) return 'position -';
  try {
    const pos = COLD.getDB(META).getCollection('positions').findOne({ _id: 'test' });
    const hot = HOT ? HOT.getDB('admin').runCommand({ hello: 1 }) : null;
    // Through showTS, because a BSON timestamp is spelled differently by every
    // driver and mongosh's own is `.t`/`.i`, not the getters the shell's help
    // suggests. See cmpTS, which learned the same thing.
    const at = pos && pos.ts ? showTS(pos.ts) : 'none';
    const now = hot && hot.lastWrite && hot.lastWrite.opTime
      ? showTS(hot.lastWrite.opTime.ts) : 'none';
    return 'mirrored ' + at + ' fast ' + now;
  } catch (e) { return 'position ' + String(e.message).slice(0, 40); }
}

for (let i = 0; i < OPS; i++) {
  try { oneOp(i); watch(i); }
  catch (e) {
    // A generator that produced something mongosh itself cannot send is a
    // harness fault, not a finding, and it must not be silent.
    print('  HARNESS  op ' + i + ': ' + String(e.message || e).slice(0, 160));
  }
  ran++;
  // The two collections no longer hold the same documents, so nothing after
  // this compares anything. See contentsVoid.
  if (contentsVoid) {
    print('  VOID\tround stopped after ' + ran + ' of ' + OPS + ' operations');
    break;
  }
}

if (STREAM) {
  // Held past the promotion window before anything is read.
  //
  // The router decides whether an insert on the fast tier is one of its own
  // copy-ups by how close the event is to the promotion it recorded, and that
  // window is thirty seconds. Judging it at hand-over time rather than at event
  // time is what made a promotion read thirty-five seconds later arrive as an
  // ordinary insert, which is the correct thing for an application to see never.
  // An application that stores its resume token and comes back to it gets a
  // backlog, and every promotion in that backlog is older than the window.
  //
  // So this waits. A stream drained the instant the writes finish cannot see
  // that class at all: measured, this section passed against the binary from
  // before the fix until the wait was put in.
  sleep(Number(process.env.TC_STREAM_DELAY || 35000));

  // Drain them, giving each a while to catch up. The router's stream is behind
  // the reference's by whatever the fast tier's own change stream costs, so a
  // fixed sleep before draining would compare a full sequence against a partial
  // one and call it a disagreement.
  // onlyColl keeps a database-level stream to one collection, so its sequence is
  // comparable with that collection's own.
  const drain = (cs, onlyColl) => {
    const out = [];
    for (let round = 0; round < 40; round++) {
      let got = false;
      for (;;) {
        const ev = cs.tryNext();
        if (!ev) break;
        got = true;
        if (onlyColl && !(ev.ns && ev.ns.coll === onlyColl)) continue;
        // The operation and which document, and nothing else. A resume token is
        // a position in one cluster's oplog and a cluster time is that cluster's
        // clock, so neither can be compared across two of them.
        out.push(ev.operationType + ':' + ser(ev.documentKey ? ev.documentKey._id : null));
      }
      if (!got && round > 3) break;
      sleep(250);
    }
    return out;
  };
  const evRef = drain(csRef), evTier = drain(csTier), evDb = drain(csDb, TIER.getName());
  try { csRef.close(); csTier.close(); csDb.close(); } catch (e) { /* nothing to do */ }

  const compare = (what, a, b) => {
    note(what, a.length || b.length ? 'real' : 'vacuous');
    if (ser(a) === ser(b)) { agreed++; return; }
    const diff = describeDiff(a, b);
    if (diff) bugs.push([what, 'the events the two streams delivered', diff.ref, diff.got]);
    else bugs.push([what, 'the events the two streams delivered',
                    a.length + ' events: ' + ser(a).slice(0, 400),
                    b.length + ' events: ' + ser(b).slice(0, 400)]);
  };
  // The events each document got, in the order it got them. The key is the
  // document, so the operation alone is what is compared.
  const byDoc = (evs) => {
    const m = new Map();
    evs.forEach((e) => {
      const cut = e.indexOf(':');
      const doc = e.slice(cut + 1);
      if (!m.has(doc)) m.set(doc, []);
      m.get(doc).push(e.slice(0, cut));
    });
    return m;
  };
  const compareUnordered = (what, a, b) => {
    note(what, a.length || b.length ? 'real' : 'vacuous');
    const sa = a.slice().sort(), sb = b.slice().sort();
    if (ser(sa) !== ser(sb)) {
      const diff = describeDiff(sa, sb);
      if (diff) bugs.push([what, 'the events the two streams delivered, as a set', diff.ref, diff.got]);
      else bugs.push([what, 'the events the two streams delivered, as a set',
                      a.length + ' events: ' + ser(sa).slice(0, 400),
                      b.length + ' events: ' + ser(sb).slice(0, 400)]);
      return;
    }
    const ma = byDoc(a), mb = byDoc(b);
    for (const doc of ma.keys()) {
      const seqA = ma.get(doc), seqB = mb.get(doc) || [];
      if (ser(seqA) !== ser(seqB)) {
        bugs.push([what, 'the events one document got, in order',
                   doc + ' ' + ser(seqA), doc + ' ' + ser(seqB)]);
        return;
      }
    }
    agreed++;
  };
  print('  stream\tthe reference delivered ' + evRef.length + ' events, the tiered collection ' +
        evTier.length + ', the database-level stream ' + evDb.length + ' for that collection');
  if (TRACE || ser(evRef) !== ser(evTier)) {
    print('  stream\tref  ' + evRef.join(' '));
    print('  stream\ttier ' + evTier.join(' '));
  }
  // Two different collections, so their global interleaving is not comparable
  // and comparing it was reporting a disagreement MongoDB never promised.
  //
  // A multi-document write makes one oplog entry per document, in the order the
  // write reached them, which is collection scan order. The tiered collection's
  // documents arrived by copy-up, in the order reads asked for them, so it is
  // laid out differently from the reference and one updateMany walks the two in
  // different orders. Measured at seed 90801 once the copy-up leak was fixed:
  // the same 105 events on both sides, with update:4001 two places earlier on
  // the tiered side and update:32 and update:45 swapped. Five positions, and no
  // event added, missing or duplicated.
  //
  // The router cannot be the cause either: the sieve only ever drops events
  // from a batch, in place, and nothing reorders one.
  //
  // So what is compared is what MongoDB does promise, and it is not weaker
  // where it matters: the same events with the same counts, which is a *set*
  // difference and is exactly what a copy-up delivered as an insert shows up
  // as -- four of them, which is what this check caught -- and then each
  // document's own events in order, which is the guarantee an application
  // reading one document's history actually depends on.
  compareUnordered('change stream', evRef, evTier);
  // Not against the reference, against the same collection's own stream: these
  // are two ways of asking one question and the router answers both. One
  // collection, one oplog, so here the order is promised and is compared
  // strictly. This is the half that catches a filter applied at the wrong
  // moment rather than not at all.
  compare('change stream on the database', evTier, evDb);
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
