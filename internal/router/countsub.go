package router

import (
	"context"
	"math"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/meta"
)

// A count with a filter is answered on the archive as two counts, so the filter
// can be counted from an index.
//
// The archive keeps a deleted document as a copy marked _tcDeletedAt, and every
// read there adds {_tcDeletedAt: {$exists: false}}. No index holds a missing
// field, so a count that could have been answered from an index alone had to
// fetch every matching document to look for the mark. Measured on the Hetzner
// staging archive, 60 million orders on network volumes: countDocuments of the
// last 30 days by _id took 53 seconds through the proxy and 0.3 seconds on the
// archive directly, for the same 1.64 million.
//
// So the live count is the filter's count less the count of the filter's
// deleted copies:
//
//	count(F and live) = count(F) - count(F and marked)
//
// The first is the query mongod can answer from the filter's own index. The
// second has meta.TombstoneIndex to choose from, which holds only the marked
// copies, and mongod's planner picks between that and the filter's index by
// which produces results faster. Measured on 1,000,000 documents with 5,000
// marked: the last 365 days by _id went from 176ms to 52ms (47 + 5); an
// indexed, selective filter stayed at 1-2ms; an unindexed one went from 233ms
// to 250ms, the 4ms of the second count.
//
// Only without skip, limit, hint or let, which the subtraction cannot carry, and
// only once TombstoneIndex exists: without it the second count costs what the
// one it replaces did.

// tombIndexKnown caches, per namespace, whether TombstoneIndex was there, for
// tombIndexTTL. Asked once per count otherwise, and the answer only ever
// changes once, from absent to present.
var (
	tombIndexMu    sync.Mutex
	tombIndexKnown = map[string]tombIndexSeen{}
)

type tombIndexSeen struct {
	ready bool
	at    time.Time
}

const tombIndexTTL = 30 * time.Second

func (c *conn) tombstoneIndexReady(ctx context.Context, db, coll string) bool {
	ns := db + "." + coll
	tombIndexMu.Lock()
	seen, ok := tombIndexKnown[ns]
	tombIndexMu.Unlock()
	if ok && (seen.ready || time.Since(seen.at) < tombIndexTTL) {
		return seen.ready
	}
	ready, err := meta.HasTombstoneIndex(ctx, c.r.cold, db, coll)
	if err != nil {
		return false
	}
	tombIndexMu.Lock()
	tombIndexKnown[ns] = tombIndexSeen{ready: ready, at: time.Now()}
	tombIndexMu.Unlock()
	return ready
}

// countDocumentsShape reads the pipeline drivers send for countDocuments:
// an optional {$match: F} and then {$group: {_id: <constant>, <name>: {$sum: 1}}}.
// It returns F (empty when there is no $match), the group's _id and the name
// of the count field.
func countDocumentsShape(cmd bson.Raw) (filter bson.Raw, groupID bson.RawValue, field string, ok bool) {
	pv, err := cmd.LookupErr("pipeline")
	if err != nil {
		return nil, bson.RawValue{}, "", false
	}
	arr, isArr := pv.ArrayOK()
	if !isArr {
		return nil, bson.RawValue{}, "", false
	}
	stages, err := arr.Values()
	if err != nil || len(stages) == 0 || len(stages) > 2 {
		return nil, bson.RawValue{}, "", false
	}
	if len(stages) == 2 {
		m, isDoc := stages[0].DocumentOK()
		if !isDoc {
			return nil, bson.RawValue{}, "", false
		}
		elems, eerr := m.Elements()
		if eerr != nil || len(elems) != 1 || elems[0].Key() != "$match" {
			return nil, bson.RawValue{}, "", false
		}
		f, isDoc := elems[0].Value().DocumentOK()
		if !isDoc {
			return nil, bson.RawValue{}, "", false
		}
		filter = f
		stages = stages[1:]
	}
	g, isDoc := stages[0].DocumentOK()
	if !isDoc {
		return nil, bson.RawValue{}, "", false
	}
	gelems, err := g.Elements()
	if err != nil || len(gelems) != 1 || gelems[0].Key() != "$group" {
		return nil, bson.RawValue{}, "", false
	}
	spec, isDoc := gelems[0].Value().DocumentOK()
	if !isDoc {
		return nil, bson.RawValue{}, "", false
	}
	selems, err := spec.Elements()
	if err != nil || len(selems) != 2 {
		return nil, bson.RawValue{}, "", false
	}
	for _, e := range selems {
		switch {
		case e.Key() == "_id":
			// A constant, so every document lands in one group. A string
			// starting with $ is a field path and groups by value.
			if s, isStr := e.Value().StringValueOK(); isStr && len(s) > 0 && s[0] == '$' {
				return nil, bson.RawValue{}, "", false
			}
			if _, isDoc := e.Value().DocumentOK(); isDoc {
				return nil, bson.RawValue{}, "", false
			}
			if _, isArr := e.Value().ArrayOK(); isArr {
				return nil, bson.RawValue{}, "", false
			}
			groupID = e.Value()
		default:
			acc, isDoc := e.Value().DocumentOK()
			if !isDoc {
				return nil, bson.RawValue{}, "", false
			}
			ae, aerr := acc.Elements()
			if aerr != nil || len(ae) != 1 || ae[0].Key() != "$sum" {
				return nil, bson.RawValue{}, "", false
			}
			if n, isNum := ae[0].Value().AsInt64OK(); !isNum || n != 1 {
				return nil, bson.RawValue{}, "", false
			}
			field = e.Key()
		}
	}
	if field == "" || groupID.Type == 0 {
		return nil, bson.RawValue{}, "", false
	}
	return filter, groupID, field, true
}

// subtractable reports whether nothing in the command changes which documents
// a plain count of its filter would count.
func subtractable(cmd bson.Raw) bool {
	for _, k := range []string{"hint", "let", "skip", "limit"} {
		if _, err := cmd.LookupErr(k); err == nil {
			return false
		}
	}
	return true
}

// countBySubtraction counts filter's live documents on the archive as two
// counts. carry is the archive command already rewritten for the archive,
// whose collation, readConcern and maxTimeMS the two counts take on.
func (c *conn) countBySubtraction(ctx context.Context, db, coll string, filter bson.Raw, carry bson.D) (int64, error) {
	// An empty filter makes count a metadata read, which is an estimate;
	// countDocuments promises an exact count. Every document has an _id, so
	// this counts them all from the _id index instead.
	base := bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: bson.MinKey{}}}}}
	if len(filter) > 0 {
		if elems, err := filter.Elements(); err == nil && len(elems) > 0 {
			base = bson.D{{Key: "$and", Value: bson.A{filter, base}}}
		}
	}
	marked := bson.D{{Key: "$and", Value: bson.A{base,
		bson.D{{Key: meta.TombstoneField, Value: bson.D{{Key: "$exists", Value: true}}}}}}}

	build := func(q bson.D) bson.D {
		cmd := bson.D{{Key: "count", Value: coll}, {Key: "query", Value: q}}
		for _, e := range carry {
			switch e.Key {
			case "collation", "readConcern", "maxTimeMS", "comment":
				cmd = append(cmd, e)
			}
		}
		return cmd
	}
	var wg sync.WaitGroup
	var all, gone int64
	var errAll, errGone error
	run := func(q bson.D, n *int64, errp *error) {
		defer wg.Done()
		var r struct {
			N int64 `bson:"n"`
		}
		*errp = c.r.cold.Database(db).RunCommand(ctx, build(q)).Decode(&r)
		*n = r.N
	}
	wg.Add(2)
	go run(base, &all, &errAll)
	go run(marked, &gone, &errGone)
	wg.Wait()
	if errAll != nil {
		return 0, errAll
	}
	if errGone != nil {
		return 0, errGone
	}
	if gone > all {
		return 0, nil
	}
	return all - gone, nil
}

// countValue is n as mongod's $sum of ones types it: an int while it fits.
func countValue(n int64) any {
	if n <= math.MaxInt32 {
		return int32(n)
	}
	return n
}
