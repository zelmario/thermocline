#!/bin/bash
# Continuous mixed load through the router, for watching the dashboard move.
#
# traffic.sh sets a scene and stops. This keeps going, so the per-second chart
# has something in it and the numbers change while you look at them. It assumes
# traffic.sh has already built shop.tdash and left the daemons running.
NS=tdash
N=${N:-6000}
SECONDS_TO_RUN=${SECONDS_TO_RUN:-300}

mongosh --port 27100 -u dba -p secret --authenticationDatabase admin --quiet --eval "
const c = db.getSiblingDB('shop').$NS;
const u = db.getSiblingDB('shop').${NS}u;
const N = $N, HOT_FROM = Math.floor(N/4);
const until = Date.now() + $SECONDS_TO_RUN * 1000;
let round = 0;

while (Date.now() < until) {
  // Hits, which should be the overwhelming majority.
  for (let i = 0; i < 20; i++) {
    c.findOne({_id: HOT_FROM + ((round*37 + i*11) % (N - HOT_FROM))});
  }
  // A read the fast tier cannot prove it holds.
  if (round % 4 === 0) c.countDocuments({grp: 'g' + (round % 20)});
  // A document that lives only in the archive: served, then copied up.
  c.findOne({_id: (round * 3) % HOT_FROM});
  // A transaction that has to be replayed.
  if (round % 9 === 0) {
    const s = db.getMongo().startSession();
    try {
      s.withTransaction(() => {
        s.getDatabase('shop').$NS.updateOne({_id: (round * 5) % HOT_FROM}, {\$set: {seen: round}});
      });
    } catch (e) {} finally { s.endSession(); }
  }
  // A cross-tier duplicate the key registry refuses.
  if (round % 6 === 0) {
    try { u.insertOne({_id: 'load' + round, email: 'taken@x.com'}); } catch (e) {}
  }
  c.updateOne({_id: HOT_FROM + (round % 100)}, {\$inc: {hits: 1}});
  round++;
}
print('load finished after ' + round + ' rounds');
"
