// Aggregations an analyst would write against these datasets, asked of the
// router and of the archive.
//
// The reads already compared are per collection. These reach across them:
// $lookup joins a tiered collection to an untiered one and back the other way,
// $unwind opens nested arrays, and the grouped reports read every document in a
// collection that is mostly somewhere else. A join is the case the router
// cannot answer partially, because a joined collection is read by key rather
// than by time, so no watermark covers it.
// The archive keeps what the application deleted, carrying a tombstone, and the
// router filters those out. Read raw it is not a control: it answers with rows
// the router is right to withhold. A leading $match costs nothing against the
// router, where LIVE is empty.
const LIVE = process.env.TC_TARGET === "archive" ? {_tcDeletedAt: {$exists: false}} : {};
const out = [];
function ask(label, dbName, f) {
  let v;
  try { v = JSON.stringify(f(db.getSiblingDB(dbName))); }
  catch (e) { v = "ERR " + (e.codeName || "") + " " + String(e.message).slice(0, 150); }
  out.push({ q: label, v: v });
}
const A = (c, p) => c.aggregate([{$match: LIVE}].concat(p), {allowDiskUse: true}).toArray();

// Tiered collection, grouped over all of it. Most of these comments are in the
// archive: the hot window is ten years and they run back to 1970.
ask("comments per year, top 5", "sample_mflix", d =>
  A(d.comments, [{$group: {_id: {$year: "$date"}, n: {$sum: 1}}}, {$sort: {n: -1, _id: 1}}, {$limit: 5}]));

// The join that matters: tiered comments into untiered movies.
ask("comments joined to movies", "sample_mflix", d =>
  A(d.comments, [{$sort: {_id: 1}}, {$limit: 20},
                 {$lookup: {from: "movies", localField: "movie_id", foreignField: "_id", as: "m"}},
                 {$project: {_id: 0, name: 1, title: {$first: "$m.title"}}}]));

// And the other direction: untiered movies reaching into tiered comments.
ask("movies joined to comments", "sample_mflix", d =>
  A(d.movies, [{$sort: {_id: 1}}, {$limit: 10},
               {$lookup: {from: "comments", localField: "_id", foreignField: "movie_id", as: "c"}},
               {$project: {_id: 0, title: 1, comments: {$size: "$c"}}}]));

// Untiered to untiered, the control: this one has no tiering question in it.
ask("movies by year, top 5", "sample_mflix", d =>
  A(d.movies, [{$group: {_id: "$year", n: {$sum: 1}}}, {$sort: {n: -1}}, {$limit: 5}]));

// A tiered collection that lives entirely in the archive.
ask("trips by user type", "sample_training", d =>
  A(d.trips, [{$group: {_id: "$usertype", n: {$sum: 1}}}, {$sort: {_id: 1}}]));
ask("trips by start station, top 5", "sample_training", d =>
  A(d.trips, [{$group: {_id: "$start station name", n: {$sum: 1}}}, {$sort: {n: -1, _id: 1}}, {$limit: 5}]));

// Nested arrays opened up, on an untiered collection.
ask("transactions per customer, top 5", "sample_analytics", d =>
  A(d.transactions, [{$project: {n: {$size: "$transactions"}}}, {$sort: {n: -1, _id: 1}}, {$limit: 5}]));

// A tiered collection with money in it, grouped by month.
ask("sales by month", "sample_supplies", d =>
  A(d.sales, [{$group: {_id: {$dateToString: {format: "%Y-%m", date: "$saleDate"}}, n: {$sum: 1}}},
              {$sort: {_id: 1}}, {$limit: 6}]));
ask("sales by store, with revenue", "sample_supplies", d =>
  A(d.sales, [{$unwind: "$items"},
              {$group: {_id: "$storeLocation", revenue: {$sum: {$multiply: ["$items.price", "$items.quantity"]}}}},
              {$sort: {_id: 1}}]));

// Entirely archive-resident, and a $match that no watermark covers.
ask("weather by day", "sample_weatherdata", d =>
  A(d.data, [{$group: {_id: {$dayOfMonth: "$ts"}, n: {$sum: 1}}}, {$sort: {_id: 1}}, {$limit: 5}]));

// Index-dependent reads on untiered collections, which have to keep working.
ask("text search on movies", "sample_mflix", d =>
  d.movies.find(Object.assign({$text: {$search: "space station"}}, LIVE), {score: {$meta: "textScore"}, title: 1, _id: 0})
    .sort({score: {$meta: "textScore"}, title: 1}).limit(5).toArray());
ask("geo near a point", "sample_restaurants", d =>
  d.neighborhoods.find(Object.assign({geometry: {$geoIntersects: {$geometry: {type: "Point", coordinates: [-73.93414657, 40.82302903]}}}}, LIVE),
                       {name: 1, _id: 0}).toArray());

// A bounded read of a tiered collection, inside the hot window, which is the
// one case the fast tier can answer on its own.
ask("comments in 2017", "sample_mflix", d =>
  A(d.comments, [{$match: {date: {$gte: new Date("2017-01-01")}}},
                 {$group: {_id: null, n: {$sum: 1}}}]));

print(JSON.stringify(out));
