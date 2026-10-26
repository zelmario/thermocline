// Dates YCSB's loaded records by key, newest key newest, so Thermocline's
// age rules have a field to judge and the history spans years.
//
//   mongosh "$ARCHIVE_URI" --file test/ycsb/datehistory.js \
//     --eval 'var YEARS=3, DB="ycsb", COLL="usertable"'
//
// YCSB itself writes no date. Its keys are "user" and a zero padded number
// (insertorder=ordered, zeropadding=12), so the number orders the records and
// the date is a linear function of it: the highest key loaded is now, key 0 is
// YEARS ago. Records the run inserts later carry no date at all, which the
// evictor reads as never old enough and the warmer as nothing to warm; they
// are the newest data and stay on the fast tier where they were written.
//
// One pipeline update over the whole collection, in key ranges so a failure
// part way says how far it got.
const dbName = typeof DB === "undefined" ? "ycsb" : DB;
const collName = typeof COLL === "undefined" ? "usertable" : COLL;
const years = typeof YEARS === "undefined" ? 3 : YEARS;
// FROM resumes at a key number after a run that stopped part way: every range
// below it printed "dated", and the date is a function of the key alone, so
// starting again is the same as never having stopped.
const from = typeof FROM === "undefined" ? 0 : FROM;
const c = db.getSiblingDB(dbName).getCollection(collName);

const num = { $toLong: { $substrBytes: ["$_id", 4, { $subtract: [{ $strLenBytes: "$_id" }, 4] }] } };
const last = c.find({}, { _id: 1 }).sort({ _id: -1 }).limit(1).toArray()[0];
if (!last) { print("no records"); quit(1); }
// LAST is the last loaded key's number. Without it the highest key present is
// taken as the end of the load, which after a run is one of the run's own
// inserts: a resumed dating then dated those as history, and the next run
// refused an archive that no longer looked like a load.
const maxN = typeof LAST === "undefined" ? parseInt(last._id.substring(4), 10) : LAST;
// NOW too, from the first run's own line, or the resumed ranges are dated
// against a later instant than the ones before them.
const now = typeof NOW === "undefined" ? new Date() : new Date(NOW);
const spanMS = years * 365 * 24 * 3600 * 1000;
const step = spanMS / Math.max(maxN, 1);
print(`dating ${maxN + 1} records over ${years} years, ${step.toFixed(1)}ms apart, NOW=${now.toISOString()}`);

const chunk = 1000000;
const pad = (n) => "user" + String(n).padStart(last._id.length - 4, "0");
for (let lo = from - (from % chunk); lo <= maxN; lo += chunk) {
  const hi = Math.min(lo + chunk, maxN + 1);
  const r = c.updateMany(
    { _id: { $gte: pad(lo), $lt: pad(hi) } },
    [{ $set: { at: { $subtract: [now, { $toLong: { $multiply: [{ $subtract: [maxN, num] }, step] } }] } } }]);
  print(`  ${pad(lo)} to ${pad(hi)}: ${r.modifiedCount} dated`);
}
c.createIndex({ at: 1 });
print("done; index at_1 built");
