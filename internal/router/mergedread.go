package router

import (
	"context"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/zelmario/thermocline/internal/checkpoint"
	"github.com/zelmario/thermocline/internal/livecache"
	"github.com/zelmario/thermocline/internal/wire"
)

// A read the fast tier cannot prove complete went to the archive whole, after
// waiting for mirroring to pass every write this router had seen. On a cold
// start that is nearly every customer's "my orders": the fast tier holds only
// what was written since adoption, so it can seldom prove it has a customer's
// ten newest. Measured on the 200 million order adoption of 2026-10-22: 632,067
// archive reads, a mean wait of 37.8 ms, and my orders at 27.9 ms against
// 10.3 ms on the old cluster alone.
//
// The wait protects one thing: an archived copy that is not current yet. And
// the oplog says which those are. Every write lands on the fast tier, an update
// of an archived document copying it up first, so a document no oplog entry has
// touched since the feeder's published position has an archived copy that is
// current. The answer is then the fast tier's answer and the archive's merged:
// the fast tier is authoritative for what it holds, the archive for the rest.
//
// So the archive is asked at once, and the fast tier's oplog is asked whether
// any document the archive returned was inserted, updated or deleted since that
// position, or whether a command (a transaction's applyOps, DDL) ran there. Any
// of those, and the read waits as before. None, and the two answers are merged
// and sorted the way MongoDB sorts them.
//
// The merge needs the top N of each side to give the top N of both, which holds
// because a document in the overall top N is in the top N of whichever side
// holds it, and a document on both sides is the same untouched document. It is
// attempted only where that reasoning is complete: a sorted find with a limit,
// no skip, no collation or hint, a projection that keeps _id and the sort keys,
// and sort values of one type bracket. Anything else takes the ordinary path.

// mergeMaxLimit bounds the reads merged, so both answers fit one batch.
const mergeMaxLimit = 100

// mergeCmdKeys are the fields a mergeable find may carry. Anything else, hint,
// collation, min, max, let, tailable and every field a later server adds, takes
// the ordinary path.
var mergeCmdKeys = map[string]bool{
	"find": true, "filter": true, "sort": true, "projection": true, "limit": true, "skip": true,
	"batchSize": true, "singleBatch": true, "$db": true, "lsid": true, "$clusterTime": true,
	"comment": true, "maxTimeMS": true, "readConcern": true, "$readPreference": true,
}

type mergePlan struct {
	limit int
	sort  []mergeSortKey
}

type mergeSortKey struct {
	field string
	desc  bool
}

// mergePlanOf reads what the merge needs off a find, or says it cannot.
func mergePlanOf(cmd bson.Raw) (mergePlan, bool) {
	var p mergePlan
	elems, err := cmd.Elements()
	if err != nil {
		return p, false
	}
	var proj bson.Raw
	batchSize := -1
	for _, e := range elems {
		k, v := e.Key(), e.Value()
		if !mergeCmdKeys[k] {
			return p, false
		}
		switch k {
		case "limit":
			n, ok := positiveInt(v)
			if !ok {
				return p, false
			}
			p.limit = n
		case "skip":
			if n, ok := intOf(v); !ok || n != 0 {
				return p, false
			}
		case "batchSize":
			n, ok := intOf(v)
			if !ok {
				return p, false
			}
			batchSize = n
		case "readConcern":
			if !mergeReadConcern(v) {
				return p, false
			}
		case "$readPreference":
			if mode, ok := readPrefModeOf(v); !ok || (mode != "primary" && mode != "primaryPreferred") {
				return p, false
			}
		case "sort":
			d, ok := v.DocumentOK()
			if !ok {
				return p, false
			}
			se, err := d.Elements()
			if err != nil || len(se) == 0 {
				return p, false
			}
			for _, s := range se {
				n, ok := intOf(s.Value())
				if !ok || (n != 1 && n != -1) || !plainField(s.Key()) {
					return p, false
				}
				p.sort = append(p.sort, mergeSortKey{field: s.Key(), desc: n < 0})
			}
		case "projection":
			d, ok := v.DocumentOK()
			if !ok {
				return p, false
			}
			proj = d
		}
	}
	if p.limit == 0 || p.limit > mergeMaxLimit || len(p.sort) == 0 {
		return p, false
	}
	if batchSize >= 0 && batchSize < p.limit {
		return p, false
	}
	return p, keepsSortKeys(proj, p.sort)
}

// keepsSortKeys reports whether a projection leaves _id and every sort key in
// the documents, which the merge compares and deduplicates by. Only plain
// inclusions or exclusions of top-level fields are read; anything else, an
// operator, a nested path, an expression, says no.
func keepsSortKeys(proj bson.Raw, keys []mergeSortKey) bool {
	if len(proj) == 0 {
		return true
	}
	elems, err := proj.Elements()
	if err != nil {
		return false
	}
	include := map[string]bool{}
	exclude := map[string]bool{}
	for _, e := range elems {
		if !plainField(e.Key()) {
			return false
		}
		on, ok := truthOf(e.Value())
		if !ok {
			return false
		}
		if on {
			include[e.Key()] = true
		} else {
			exclude[e.Key()] = true
		}
	}
	if exclude["_id"] {
		return false
	}
	delete(include, "_id")
	if len(include) > 0 {
		if len(exclude) > 0 {
			return false
		}
		for _, k := range keys {
			if !include[k.field] {
				return false
			}
		}
		return true
	}
	for _, k := range keys {
		if exclude[k.field] {
			return false
		}
	}
	return true
}

func plainField(f string) bool {
	if f == "" || f[0] == '$' {
		return false
	}
	for i := 0; i < len(f); i++ {
		if f[i] == '.' {
			return false
		}
	}
	return true
}

func truthOf(v bson.RawValue) (bool, bool) {
	if b, ok := v.BooleanOK(); ok {
		return b, true
	}
	n, ok := intOf(v)
	if !ok || (n != 0 && n != 1) {
		return false, false
	}
	return n == 1, true
}

// mergeReadConcern accepts a read concern the merge can honour: a level of
// local or majority and nothing else. A cluster time or a snapshot names a
// point the two tiers do not share.
func mergeReadConcern(v bson.RawValue) bool {
	d, ok := v.DocumentOK()
	if !ok {
		return false
	}
	elems, err := d.Elements()
	if err != nil {
		return false
	}
	for _, e := range elems {
		if e.Key() != "level" {
			return false
		}
		s, ok := e.Value().StringValueOK()
		if !ok || (s != "local" && s != "majority") {
			return false
		}
	}
	return true
}

func readPrefModeOf(v bson.RawValue) (string, bool) {
	d, ok := v.DocumentOK()
	if !ok {
		return "", false
	}
	return d.Lookup("mode").StringValueOK()
}

// mergedFind answers a find the fast tier could not prove complete by merging
// the fast tier's answer with the archive's. ok false means take the ordinary
// path, which waits for mirroring and asks the archive alone.
func (c *conn) mergedFind(ctx context.Context, ns string, msg, reply *wire.Message) (*wire.Message, bool) {
	// Each way out says why at Debug, because a merge that is never taken
	// is otherwise indistinguishable from one that is, the answer being
	// right either way.
	no := func(why string) (*wire.Message, bool) {
		c.log.Debug("not merging this read", "ns", ns, "why", why)
		return nil, false
	}
	cmd, err := msg.CommandDoc()
	if err != nil || inTransaction(cmd) {
		return no("unreadable, or in a transaction")
	}
	plan, ok := mergePlanOf(cmd)
	if !ok {
		return no("not a sorted find with a limit this can merge")
	}
	// Strings compare by collation, and the merge compares bytes.
	if _, has := c.r.defaultCollation(ctx, ns); has {
		return no("the collection has a default collation")
	}
	rdoc, err := reply.ReplyDoc()
	if err != nil || !replyOK(rdoc) {
		return no("the fast tier's reply is an error")
	}
	cur, ok := rdoc.Lookup("cursor").DocumentOK()
	if !ok {
		return no("the fast tier's reply has no cursor")
	}
	fastID, _ := cur.Lookup("id").AsInt64OK()
	fast, ok := rawBatch(cur.Lookup("firstBatch"))
	if !ok || len(fast) > plan.limit || (fastID != 0 && len(fast) < plan.limit) {
		// Not the fast tier's whole top N, so not something to merge.
		return no("the fast tier's batch is not its whole top N")
	}

	// Where the archive is current to. Read before the archive is, so
	// everything mirrored up to it is in what the archive returns.
	got := c.r.recentPosition(ctx)
	if !got.known || got.ts.IsZero() {
		return no("no feeder position with a cluster time")
	}
	until := checkpoint.UnpackTS(c.r.seenOpTime.Load())
	if until.IsZero() {
		return no("no cluster time seen yet")
	}
	behind := checkpoint.PackTS(until) > checkpoint.PackTS(got.ts)
	if behind && !c.r.windowCheckable(ctx, got.ts, until) {
		return no("the feeder is further behind than the oplog is read for")
	}

	db, _ := splitNS(ns)
	rewritten, _, err := rewriteForArchive(cmd, nil, nil, false)
	if err != nil {
		return no("the read could not be rewritten for the archive")
	}
	acur, err := c.r.cold.Database(db).RunCommandCursor(ctx, rewritten)
	if err != nil {
		return no("the archive refused the read: " + err.Error())
	}
	var archived []bson.Raw
	for len(archived) < plan.limit && acur.Next(ctx) {
		archived = append(archived, append(bson.Raw(nil), acur.Current...))
	}
	aerr := acur.Err()
	_ = acur.Close(ctx)
	if aerr != nil {
		return no("the archive read failed: " + aerr.Error())
	}

	if behind && len(archived) > 0 {
		ids := make(bson.A, 0, len(archived))
		for _, d := range archived {
			id, err := d.LookupErr("_id")
			if err != nil {
				return no("an archived document has no _id")
			}
			ids = append(ids, id)
		}
		if touched, err := c.r.oplogTouched(ctx, ns, got.ts, until, ids); err != nil || touched {
			c.r.Stats.MergeFallbacks.Add(1)
			if err != nil {
				return no("the oplog could not be read: " + err.Error())
			}
			return no("something the archive returned changed since the feeder's position")
		}
	}

	merged, ok := mergeSorted(fast, archived, plan)
	if !ok {
		return no("the sort values are not of one bracket")
	}
	if fastID != 0 {
		// The fast tier holds a cursor for the rest of an answer nothing will
		// read. See superseded.go.
		c.dropSupersededCursor(ctx, rdoc, db)
	}
	out, err := cursorReply(reply.Header.RequestID, msg.Header.RequestID, 0, ns, "firstBatch", merged)
	if err != nil {
		return no("the reply could not be built")
	}
	c.r.Stats.MergedReads.Add(1)
	c.cls = ClassArchive
	return out, true
}

// oplogWindow is the filter for the fast tier's oplog entries after from that
// could make an archived copy in ns stale.
//
// direct are the conditions on an entry for ns itself. inner is the condition
// on one operation inside an applyOps, for ns, that matters. A command entry
// on the database is DDL and always matters, and so does one on admin.$cmd that
// carries no applyOps, a prepared transaction's commit above all.
//
// An applyOps is looked inside rather than treated as a command. MongoDB 8.0
// writes an insertMany as one applyOps of inserts, and taking every command as
// something that could hide a delete made any recent batch insert, of new
// documents that the archive could not hold, send the read back to the wait.
// Measured on testfresh: the merged read never merged, and the entry that
// stopped it was an insertMany of five new orders through the router.
func oplogWindow(ns string, from bson.Timestamp, direct bson.A, inner bson.D) bson.D {
	db, _ := splitNS(ns)
	or := append(bson.A{}, direct...)
	or = append(or,
		bson.D{{Key: "op", Value: "c"}, {Key: "ns", Value: db + ".$cmd"}},
		bson.D{{Key: "op", Value: "c"}, {Key: "ns", Value: "admin.$cmd"}, {Key: "o.applyOps", Value: bson.D{{Key: "$exists", Value: false}}}},
		bson.D{{Key: "op", Value: "c"}, {Key: "ns", Value: "admin.$cmd"}, {Key: "o.applyOps", Value: bson.D{{Key: "$elemMatch", Value: inner}}}},
	)
	return bson.D{
		{Key: "ts", Value: bson.D{{Key: "$gt", Value: from}}},
		{Key: "$or", Value: or},
	}
}

// touchesIDs is the oplogWindow pair for "anything that may have changed one
// of ids in ns": an update or delete of any of them, an insert of one too when
// inserts says so, or inside an applyOps an operation on ns that is not a plain
// insert.
//
// An insert cannot leave an archived copy stale: the archive's copy of a
// document inserted since is that document, a copy-up inserts exactly the
// archive's version, and a document deleted and inserted again brings its
// delete with it. Only an update or a delete makes the archive's copy old. The
// merged read had counted inserts too, and on a feeder fast enough to have
// mirrored five new orders before the read reached the archive, their inserts
// sent it back to the wait every time.
func touchesIDs(ns string, ids bson.A, inserts bool) (bson.A, bson.D) {
	in := bson.D{{Key: "$in", Value: ids}}
	ops := bson.A{"u", "d"}
	if inserts {
		ops = append(ops, "i")
	}
	op := bson.D{{Key: "$in", Value: ops}}
	return bson.A{
			bson.D{{Key: "ns", Value: ns}, {Key: "op", Value: op}, {Key: "o._id", Value: in}},
			bson.D{{Key: "ns", Value: ns}, {Key: "op", Value: op}, {Key: "o2._id", Value: in}},
		}, bson.D{{Key: "ns", Value: ns}, {Key: "$or", Value: bson.A{
			bson.D{{Key: "op", Value: bson.D{{Key: "$ne", Value: "i"}}}},
			bson.D{{Key: "op", Value: op}, {Key: "o._id", Value: in}},
			bson.D{{Key: "op", Value: op}, {Key: "o2._id", Value: in}},
		}}}
}

// oplogTouched reports whether the fast tier's oplog has, after from and up to
// at least until, an insert, update or delete of any of ids in ns, or a command
// that could hide one: a transaction's applyOps or DDL on the database.
func (r *Router) oplogTouched(ctx context.Context, ns string, from, until bson.Timestamp, ids bson.A) (bool, error) {
	cctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	direct, inner := touchesIDs(ns, ids, false)
	filter := oplogWindow(ns, from, direct, inner)
	var out struct {
		Cursor struct {
			FirstBatch []bson.Raw `bson:"firstBatch"`
		} `bson:"cursor"`
	}
	err := r.hot.Database("local").RunCommand(cctx, bson.D{
		{Key: "find", Value: "oplog.rs"},
		{Key: "filter", Value: filter},
		{Key: "projection", Value: bson.D{{Key: "ts", Value: 1}}},
		{Key: "limit", Value: int32(1)},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "local"}, {Key: "afterClusterTime", Value: until}}},
	}, options.RunCmd().SetReadPreference(readpref.Primary())).Decode(&out)
	if err != nil {
		return false, err
	}
	return len(out.Cursor.FirstBatch) > 0, nil
}

// rawBatch reads a firstBatch as documents.
func rawBatch(v bson.RawValue) ([]bson.Raw, bool) {
	arr, ok := v.ArrayOK()
	if !ok {
		return nil, false
	}
	vals, err := arr.Values()
	if err != nil {
		return nil, false
	}
	out := make([]bson.Raw, 0, len(vals))
	for _, x := range vals {
		d, ok := x.DocumentOK()
		if !ok {
			return nil, false
		}
		out = append(out, d)
	}
	return out, true
}

// mergeSorted merges the two answers the way MongoDB would have sorted their
// union: the fast tier's copy of a document wins, each sort key must hold a
// value of one type bracket across every document, and the first limit are
// kept. It says no to anything it cannot order exactly.
func mergeSorted(fast, archived []bson.Raw, plan mergePlan) (bson.A, bool) {
	seen := make(map[string]bool, len(fast)+len(archived))
	docs := make([]bson.Raw, 0, len(fast)+len(archived))
	add := func(d bson.Raw) bool {
		id, err := d.LookupErr("_id")
		if err != nil {
			return false
		}
		k := string(rune(id.Type)) + string(id.Value)
		if !seen[k] {
			seen[k] = true
			docs = append(docs, d)
		}
		return true
	}
	for _, d := range fast {
		if !add(d) {
			return nil, false
		}
	}
	for _, d := range archived {
		if !add(d) {
			return nil, false
		}
	}
	vals := make([][]bson.RawValue, len(docs))
	brackets := make([]string, len(plan.sort))
	for i, d := range docs {
		vals[i] = make([]bson.RawValue, len(plan.sort))
		for j, k := range plan.sort {
			v, err := d.LookupErr(k.field)
			if err != nil {
				return nil, false
			}
			b := livecache.SortBracket(v)
			if b == "" || (brackets[j] != "" && brackets[j] != b) {
				return nil, false
			}
			brackets[j] = b
			vals[i][j] = v
		}
	}
	idx := make([]int, len(docs))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		for j, k := range plan.sort {
			n := livecache.CompareSame(vals[idx[a]][j], vals[idx[b]][j])
			if n == 0 {
				continue
			}
			if k.desc {
				return n > 0
			}
			return n < 0
		}
		return false
	})
	out := make(bson.A, 0, plan.limit)
	for _, i := range idx {
		if len(out) == plan.limit {
			break
		}
		out = append(out, docs[i])
	}
	return out, true
}
