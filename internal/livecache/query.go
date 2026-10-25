// Package livecache keeps the answers to a router's commonest reads in memory
// and keeps them right as the data changes, rather than throwing them away.
//
// The query cache in package cache stores whole replies and drops them when a
// write could have changed them. On a shop that is the whole problem: every
// checkout changes a product's stock, so every product listing is dropped many
// times a second and almost nothing is ever served. Measured on a store
// workload over 40 million orders: 3.1% of reads answered, and throughput lower
// with it on than off.
//
// This follows the idea ReadySet is built on: a cached answer is a view, keyed
// by the values the query was asked with, and the database's own change stream
// is applied to it as it happens. A product whose stock changed is replaced in
// every listing that holds it, in its sorted place, instead of the listings
// being thrown away. Only what ReadySet calls partial views are kept: an entry
// exists once somebody asked for it, and is filled by asking MongoDB.
//
// It is deliberately narrow, and says no to anything whose answer it could get
// wrong: equality and range ($gt, $gte, $lt, $lte) filters on top-level
// fields, a sort, a skip and a limit, a projection of top-level fields, and
// counts over the same filters. Arrays, nulls, collations, mixed types in a
// sort and other operators are all answered by MongoDB, as before.
package livecache

import (
	"math"
	"math/big"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Kind is what a cached answer is.
type Kind int

const (
	// KindFind is a list of documents, from find.
	KindFind Kind = iota
	// KindCount is a number, from the count command.
	KindCount
	// KindCountAgg is a number asked for as a pipeline, which is how drivers
	// send countDocuments: [{$match}, {$group: {_id: K, n: {$sum: 1}}}].
	KindCountAgg
	// KindGroup is a grouping pipeline, a report. See group.go.
	KindGroup
)

// firstBatchDefault is the first batch mongod returns when none is asked for.
const firstBatchDefault = 101

// Query is a read this package can answer, taken apart.
type Query struct {
	Kind Kind
	NS   string
	// Eq are the filter's field = value pairs, sorted by field.
	Eq []EqPair
	// Ranges are the filter's range bounds, sorted by field and operator. A
	// field is in Eq or in Ranges, never both.
	Ranges []RangePred
	// Sort is the sort, in order. Empty for a count.
	Sort        []SortKey
	Skip, Limit int
	Proj        Projection
	// GroupID is the _id a counting pipeline groups under, echoed back.
	GroupID bson.RawValue
	// CountField is the name the counting pipeline gives its total.
	CountField string
	// Limit and skip of a count command.
	CountSkip, CountLimit int
	// Group is the grouping of a KindGroup query.
	Group *GroupSpec
}

// EqPair is one field = value in a filter.
type EqPair struct {
	Field string
	Value bson.RawValue
	key   string
}

// RangePred is one bound of a range in a filter: Field Op Value, Op one of
// $gt, $gte, $lt and $lte.
type RangePred struct {
	Field string
	Op    string
	Value bson.RawValue
	key   string
}

// inRange says whether v, a value valueKey accepts, satisfies the bound the
// way mongod's match does. A range compares only within the bound's type
// bracket: {price: {$gt: 5}} matches no string, however it sorts.
func (r RangePred) inRange(v bson.RawValue) bool {
	if bracket(v) != bracket(r.Value) {
		return false
	}
	n := compareSame(v, r.Value)
	switch r.Op {
	case "$gt":
		return n > 0
	case "$gte":
		return n >= 0
	case "$lt":
		return n < 0
	case "$lte":
		return n <= 0
	}
	return false
}

// SortKey is one field of a sort.
type SortKey struct {
	Field string
	Desc  bool
}

// Projection is an inclusion or exclusion of top-level fields.
type Projection struct {
	Set     bool
	Include bool
	Fields  map[string]bool
	NoID    bool
}

// maxWindow bounds how deep into a sorted list an entry reaches.
const maxWindow = 500

// ParseFind takes a find command apart, or reports that it cannot be answered
// here. ns is db.collection.
func ParseFind(ns string, cmd bson.Raw) (Query, bool) {
	q := Query{Kind: KindFind, NS: ns, Limit: 0}
	elems, err := cmd.Elements()
	if err != nil {
		return q, false
	}
	batchSize := -1
	singleBatch := false
	for _, e := range elems {
		k, v := e.Key(), e.Value()
		switch k {
		case "find", "$db", "lsid", "$clusterTime", "comment", "maxTimeMS", "$readPreference", "txnNumber":
			if k == "txnNumber" {
				return q, false
			}
		case "readConcern":
			if !plainReadConcern(v) {
				return q, false
			}
		case "filter":
			d, ok := v.DocumentOK()
			if !ok {
				return q, false
			}
			eq, rng, ok := parseFilter(d)
			if !ok {
				return q, false
			}
			q.Eq, q.Ranges = eq, rng
		case "sort":
			d, ok := v.DocumentOK()
			if !ok {
				return q, false
			}
			s, ok := parseSort(d)
			if !ok {
				return q, false
			}
			q.Sort = s
		case "projection":
			d, ok := v.DocumentOK()
			if !ok {
				return q, false
			}
			p, ok := parseProjection(d)
			if !ok {
				return q, false
			}
			q.Proj = p
		case "skip":
			n, ok := asInt(v)
			if !ok || n < 0 {
				return q, false
			}
			q.Skip = n
		case "limit":
			// Negative was a single batch in the old OP_QUERY. The find
			// command refuses it ("BSON field 'limit' value must be >= 0"),
			// and read as a single batch it was answered where mongod
			// answers an error. Found by the router's verify mode on
			// testconform's "a negative limit".
			n, ok := asInt(v)
			if !ok || n < 0 {
				return q, false
			}
			q.Limit = n
		case "batchSize":
			n, ok := asInt(v)
			if !ok || n < 0 {
				return q, false
			}
			batchSize = n
		case "singleBatch":
			b, ok := v.BooleanOK()
			if !ok {
				return q, false
			}
			singleBatch = b
		default:
			// hint, collation, min, max, returnKey, showRecordId, tailable,
			// allowPartialResults, let, and anything a later server adds.
			return q, false
		}
	}
	// A list with no limit is the whole match, and a list longer than the
	// window would be half an answer.
	if q.Limit == 0 || q.Skip+q.Limit > maxWindow {
		return q, false
	}
	// The reply has to be one batch with the cursor closed, which is what the
	// client then expects: a batch smaller than the answer leaves a cursor this
	// cache does not have.
	if batchSize >= 0 && batchSize < q.Limit && !singleBatch {
		return q, false
	}
	return q, true
}

// ParseCount takes a count command apart.
func ParseCount(ns string, cmd bson.Raw) (Query, bool) {
	q := Query{Kind: KindCount, NS: ns}
	elems, err := cmd.Elements()
	if err != nil {
		return q, false
	}
	for _, e := range elems {
		k, v := e.Key(), e.Value()
		switch k {
		case "count", "$db", "lsid", "$clusterTime", "comment", "maxTimeMS", "$readPreference":
		case "readConcern":
			if !plainReadConcern(v) {
				return q, false
			}
		case "query":
			d, ok := v.DocumentOK()
			if !ok {
				return q, false
			}
			eq, rng, ok := parseFilter(d)
			if !ok {
				return q, false
			}
			q.Eq, q.Ranges = eq, rng
		case "skip", "limit":
			n, ok := asInt(v)
			if !ok || n != 0 {
				return q, false
			}
		default:
			return q, false
		}
	}
	return q, true
}

// cursorFits reports whether an aggregate's cursor option is a document this
// cache can answer: at most a batchSize, and that at least min.
func cursorFits(v bson.RawValue, min int) bool {
	d, ok := v.DocumentOK()
	if !ok {
		return false
	}
	ce, err := d.Elements()
	if err != nil {
		return false
	}
	for _, c := range ce {
		if c.Key() != "batchSize" {
			return false
		}
		n, ok := asInt(c.Value())
		if !ok || n < min {
			return false
		}
	}
	return true
}

// ParseCountAgg recognises the counting pipeline drivers send for
// countDocuments, and nothing else.
func ParseCountAgg(ns string, cmd bson.Raw) (Query, bool) {
	q := Query{Kind: KindCountAgg, NS: ns}
	elems, err := cmd.Elements()
	if err != nil {
		return q, false
	}
	var pipe bson.Raw
	hasCursor := false
	for _, e := range elems {
		k, v := e.Key(), e.Value()
		switch k {
		case "aggregate", "$db", "lsid", "$clusterTime", "comment", "maxTimeMS", "$readPreference":
		case "cursor":
			// Required, as mongod requires it, and a batch of at least the one
			// document the answer is: {batchSize: 0} gets an empty first
			// batch and an open cursor from mongod, and was answered here
			// with the count.
			if !cursorFits(v, 1) {
				return q, false
			}
			hasCursor = true
		case "readConcern":
			if !plainReadConcern(v) {
				return q, false
			}
		case "pipeline":
			a, ok := v.ArrayOK()
			if !ok {
				return q, false
			}
			pipe = bson.Raw(a)
		default:
			return q, false
		}
	}
	if !hasCursor {
		return q, false
	}
	stages, err := pipe.Values()
	if err != nil || len(stages) < 1 || len(stages) > 2 {
		return q, false
	}
	at := 0
	if len(stages) == 2 {
		d, ok := stages[0].DocumentOK()
		if !ok {
			return q, false
		}
		m, ok := single(d, "$match")
		if !ok {
			return q, false
		}
		md, ok := m.DocumentOK()
		if !ok {
			return q, false
		}
		eq, rng, ok := parseFilter(md)
		if !ok {
			return q, false
		}
		q.Eq, q.Ranges = eq, rng
		at = 1
	}
	gd, ok := stages[at].DocumentOK()
	if !ok {
		return q, false
	}
	g, ok := single(gd, "$group")
	if !ok {
		return q, false
	}
	gdoc, ok := g.DocumentOK()
	if !ok {
		return q, false
	}
	ge, err := gdoc.Elements()
	if err != nil || len(ge) != 2 || ge[0].Key() != "_id" {
		return q, false
	}
	id := ge[0].Value()
	switch id.Type {
	case bson.TypeInt32, bson.TypeInt64, bson.TypeNull, bson.TypeString:
		if id.Type == bson.TypeString && strings.HasPrefix(id.StringValue(), "$") {
			return q, false
		}
	default:
		return q, false
	}
	sum, ok := ge[1].Value().DocumentOK()
	if !ok {
		return q, false
	}
	one, ok := single(sum, "$sum")
	if !ok {
		return q, false
	}
	if n, ok := asInt(one); !ok || n != 1 {
		return q, false
	}
	q.GroupID, q.CountField = id, ge[1].Key()
	return q, true
}

func single(d bson.Raw, key string) (bson.RawValue, bool) {
	e, err := d.Elements()
	if err != nil || len(e) != 1 || e[0].Key() != key {
		return bson.RawValue{}, false
	}
	return e[0].Value(), true
}

func plainReadConcern(v bson.RawValue) bool {
	d, ok := v.DocumentOK()
	if !ok {
		return false
	}
	e, err := d.Elements()
	if err != nil {
		return false
	}
	for _, x := range e {
		if x.Key() != "level" {
			return false
		}
		s, ok := x.Value().StringValueOK()
		if !ok || (s != "local" && s != "majority" && s != "available") {
			return false
		}
	}
	return true
}

// parseFilter accepts a filter of top-level fields, each either equal to a
// literal or bounded by $gt, $gte, $lt and $lte, and nothing else.
//
// A bound is a value valueKey accepts, so the same values an equality takes:
// no null, which $lte: null would match a missing field by, no array, no
// document, no NaN, no MinKey or MaxKey.
func parseFilter(filter bson.Raw) ([]EqPair, []RangePred, bool) {
	e, err := filter.Elements()
	if err != nil {
		return nil, nil, false
	}
	out := make([]EqPair, 0, len(e))
	var rng []RangePred
	seen := map[string]bool{}
	for _, x := range e {
		k := x.Key()
		if k == "" || strings.HasPrefix(k, "$") || strings.Contains(k, ".") || seen[k] {
			return nil, nil, false
		}
		seen[k] = true
		if d, ok := x.Value().DocumentOK(); ok {
			ops, err := d.Elements()
			if err != nil || len(ops) == 0 {
				return nil, nil, false
			}
			for _, o := range ops {
				switch o.Key() {
				case "$gt", "$gte", "$lt", "$lte":
				default:
					return nil, nil, false
				}
				key, ok := valueKey(o.Value())
				if !ok {
					return nil, nil, false
				}
				for _, r := range rng {
					if r.Field == k && r.Op == o.Key() {
						return nil, nil, false
					}
				}
				rng = append(rng, RangePred{Field: k, Op: o.Key(), Value: o.Value(), key: key})
			}
			continue
		}
		key, ok := valueKey(x.Value())
		if !ok {
			return nil, nil, false
		}
		out = append(out, EqPair{Field: k, Value: x.Value(), key: key})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Field < out[j].Field })
	sort.Slice(rng, func(i, j int) bool {
		if rng[i].Field != rng[j].Field {
			return rng[i].Field < rng[j].Field
		}
		return rng[i].Op < rng[j].Op
	})
	return out, rng, true
}

func parseSort(d bson.Raw) ([]SortKey, bool) {
	e, err := d.Elements()
	if err != nil || len(e) > 3 {
		return nil, false
	}
	out := make([]SortKey, 0, len(e))
	for _, x := range e {
		k := x.Key()
		if k == "" || strings.HasPrefix(k, "$") || strings.Contains(k, ".") {
			return nil, false
		}
		n, ok := asInt(x.Value())
		if !ok || (n != 1 && n != -1) {
			return nil, false
		}
		out = append(out, SortKey{Field: k, Desc: n == -1})
	}
	return out, true
}

func parseProjection(d bson.Raw) (Projection, bool) {
	p := Projection{Fields: map[string]bool{}}
	e, err := d.Elements()
	if err != nil {
		return p, false
	}
	if len(e) == 0 {
		return p, true
	}
	p.Set = true
	mode := 0 // 1 include, -1 exclude
	for _, x := range e {
		k := x.Key()
		if k == "" || strings.HasPrefix(k, "$") || strings.Contains(k, ".") {
			return p, false
		}
		on, ok := truth(x.Value())
		if !ok {
			return p, false
		}
		if k == "_id" {
			p.NoID = !on
			continue
		}
		m := -1
		if on {
			m = 1
		}
		if mode != 0 && m != mode {
			return p, false
		}
		mode = m
		p.Fields[k] = true
	}
	p.Include = mode == 1 || (mode == 0 && !p.NoID)
	if mode == 0 && p.NoID {
		// {_id: 0} alone excludes only _id.
		p.Include = false
	}
	return p, true
}

func truth(v bson.RawValue) (bool, bool) {
	switch v.Type {
	case bson.TypeBoolean:
		return v.Boolean(), true
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble:
		n, ok := asInt(v)
		if !ok || (n != 0 && n != 1) {
			return false, false
		}
		return n == 1, true
	}
	return false, false
}

func asInt(v bson.RawValue) (int, bool) {
	switch v.Type {
	case bson.TypeInt32:
		return int(v.Int32()), true
	case bson.TypeInt64:
		return int(v.Int64()), true
	case bson.TypeDouble:
		f := v.Double()
		if f != math.Trunc(f) || math.IsInf(f, 0) || math.Abs(f) > 1<<31 {
			return 0, false
		}
		return int(f), true
	}
	return 0, false
}

// valueKey is a value's identity for equality as MongoDB judges it, or false
// for a value whose equality this does not reproduce.
//
// Numbers compare by value whatever their type, so 1, 1.0 and NumberLong(1)
// are one key. Strings compare by bytes, which is the simple collation; a
// collection with a default collation is refused before anything gets here.
// Arrays are left out because {tags: "a"} matches a document whose tags holds
// "a" among others, and null because it also matches a missing field.
func valueKey(v bson.RawValue) (string, bool) {
	switch v.Type {
	case bson.TypeString:
		return "s" + v.StringValue(), true
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble:
		if v.Type == bson.TypeDouble && (math.IsNaN(v.Double()) || math.IsInf(v.Double(), 0)) {
			return "", false
		}
		x := exact(v)
		if x.Sign() == 0 {
			// -0.0 is 0 to mongod. big.Float keeps the sign and wrote it "-0",
			// so {x: 0} missed a document holding -0.0, and a grouping put
			// the two in separate groups where mongod returns one.
			return "n0", true
		}
		// The exact rational, not Text('g', -1): that writes the shortest
		// form at the value's own precision, 64 bits for an integer and 53 for
		// a double, so NumberLong(2^60) and the double 2^60 were two keys for
		// one value. mongod compares numbers by value, and a document holding
		// the double was missed by an entry keyed on the long.
		r, _ := x.Rat(nil)
		return "n" + r.RatString(), true
	case bson.TypeBoolean:
		if v.Boolean() {
			return "bt", true
		}
		return "bf", true
	case bson.TypeObjectID:
		id := v.ObjectID()
		return "o" + string(id[:]), true
	case bson.TypeDateTime:
		return "d" + big.NewInt(v.DateTime()).String(), true
	}
	return "", false
}

func exact(v bson.RawValue) *big.Float {
	f := new(big.Float).SetPrec(0)
	switch v.Type {
	case bson.TypeInt32:
		return f.SetInt64(int64(v.Int32()))
	case bson.TypeInt64:
		return f.SetInt64(v.Int64())
	}
	return f.SetFloat64(v.Double())
}

// bracket is the class a sortable value sorts within. Values of two different
// brackets in one sort are refused rather than ordered.
func bracket(v bson.RawValue) string {
	switch v.Type {
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble:
		if v.Type == bson.TypeDouble && math.IsNaN(v.Double()) {
			return ""
		}
		return "n"
	case bson.TypeString:
		return "s"
	case bson.TypeDateTime:
		return "d"
	case bson.TypeObjectID:
		return "o"
	case bson.TypeBoolean:
		return "b"
	}
	return ""
}

// SortBracket is bracket for other packages: the class a sortable value sorts
// within, "" for one this does not order.
func SortBracket(v bson.RawValue) string { return bracket(v) }

// compareNumbers orders two numbers by value, as exact as exact() is, without
// its allocations where float64 already decides: two integers, two doubles, or
// an integer within 2^53 of zero against a double. Applying a change compared
// every range entry of a category through big.Float, under the lock every
// cached read waits on: on the Docker test server inBounds was 82% of Apply,
// and the p99 of reads that hold no range went from 0.71ms to 1.88ms.
func compareNumbers(a, b bson.RawValue) int {
	ai, aInt := intValue(a)
	bi, bInt := intValue(b)
	switch {
	case aInt && bInt:
		return cmp3(ai < bi, ai > bi)
	case !aInt && !bInt:
		x, y := a.Double(), b.Double()
		return cmp3(x < y, x > y)
	case aInt && ai >= -1<<53 && ai <= 1<<53:
		x, y := float64(ai), b.Double()
		return cmp3(x < y, x > y)
	case bInt && bi >= -1<<53 && bi <= 1<<53:
		x, y := a.Double(), float64(bi)
		return cmp3(x < y, x > y)
	}
	return exact(a).Cmp(exact(b))
}

func intValue(v bson.RawValue) (int64, bool) {
	switch v.Type {
	case bson.TypeInt32:
		return int64(v.Int32()), true
	case bson.TypeInt64:
		return v.Int64(), true
	}
	return 0, false
}

func cmp3(less, more bool) int {
	switch {
	case less:
		return -1
	case more:
		return 1
	}
	return 0
}

// CompareSame is compareSame for other packages.
func CompareSame(a, b bson.RawValue) int { return compareSame(a, b) }

// compareSame orders two values of one bracket the way MongoDB does.
func compareSame(a, b bson.RawValue) int {
	switch bracket(a) {
	case "n":
		return compareNumbers(a, b)
	case "s":
		return strings.Compare(a.StringValue(), b.StringValue())
	case "d":
		x, y := a.DateTime(), b.DateTime()
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		}
		return 0
	case "o":
		x, y := a.ObjectID(), b.ObjectID()
		return strings.Compare(string(x[:]), string(y[:]))
	case "b":
		x, y := a.Boolean(), b.Boolean()
		switch {
		case x == y:
			return 0
		case !x:
			return -1
		}
		return 1
	}
	return 0
}

// Shape names everything about a query except the values it was asked with,
// for logs that count which shapes the cache answers.
func (q Query) Shape() string { return q.shapeID() }

// shapeID names everything about a query except the values it was asked with.
func (q Query) shapeID() string {
	var b strings.Builder
	switch q.Kind {
	case KindFind:
		b.WriteString("find|")
	case KindGroup:
		b.WriteString("group|")
	default:
		b.WriteString("count|")
	}
	b.WriteString(q.NS)
	b.WriteString("|eq:")
	for _, p := range q.Eq {
		b.WriteString(p.Field)
		b.WriteByte(',')
	}
	// The fields and operators of a range are the shape, its bounds the key.
	if len(q.Ranges) > 0 {
		b.WriteString("|rng:")
		for _, r := range q.Ranges {
			b.WriteString(r.Field)
			b.WriteString(r.Op)
			b.WriteByte(',')
		}
	}
	if q.Kind == KindFind {
		b.WriteString("|sort:")
		for _, s := range q.Sort {
			b.WriteString(s.Field)
			if s.Desc {
				b.WriteString("-")
			}
			b.WriteByte(',')
		}
		// The projection is part of the shape, because an entry keeps only
		// the fields its projection leaves. See trimDoc.
		if q.Proj.Set {
			b.WriteString("|proj:")
			if q.Proj.Include {
				b.WriteByte('+')
			} else {
				b.WriteByte('-')
			}
			if q.Proj.NoID {
				b.WriteString("!id")
			}
			fields := make([]string, 0, len(q.Proj.Fields))
			for f := range q.Proj.Fields {
				fields = append(fields, f)
			}
			sort.Strings(fields)
			for _, f := range fields {
				b.WriteByte(',')
				b.WriteString(f)
			}
		}
	}
	if q.Group != nil {
		b.WriteString(q.Group.canon)
	}
	return b.String()
}

// entryKey names one entry: the shape and the values.
func (q Query) entryKey() string {
	g := q.groupKey()
	if len(q.Ranges) == 0 {
		return g
	}
	var b strings.Builder
	b.WriteString(g)
	for _, r := range q.Ranges {
		b.WriteByte(1)
		b.WriteString(r.key)
	}
	return b.String()
}

// groupKey names the entries of a shape asked with the same equalities, which
// differ only in their bounds: every entry a document can belong to, found by
// its values (keyOf). For a shape with no range it is the entry's key.
func (q Query) groupKey() string {
	var b strings.Builder
	b.WriteString(q.shapeID())
	for _, p := range q.Eq {
		b.WriteByte(0)
		b.WriteString(p.key)
	}
	return b.String()
}

// Detached is detached for other packages.
func (q Query) Detached() Query { return q.detached() }

// detached copies everything a query points to, so it can outlive the message
// it was read from.
func (q Query) detached() Query {
	eq := make([]EqPair, len(q.Eq))
	for i, p := range q.Eq {
		p.Value = bson.RawValue{Type: p.Value.Type, Value: append([]byte(nil), p.Value.Value...)}
		eq[i] = p
	}
	q.Eq = eq
	rng := make([]RangePred, len(q.Ranges))
	for i, r := range q.Ranges {
		r.Value = bson.RawValue{Type: r.Value.Type, Value: append([]byte(nil), r.Value.Value...)}
		rng[i] = r
	}
	q.Ranges = rng
	if q.Group != nil {
		g := *q.Group
		g.ID.konst = bson.RawValue{Type: g.ID.konst.Type, Value: append([]byte(nil), g.ID.konst.Value...)}
		g.Accs = append([]accSpec(nil), g.Accs...)
		g.Sort = append([]SortKey(nil), g.Sort...)
		q.Group = &g
	}
	q.GroupID = bson.RawValue{Type: q.GroupID.Type, Value: append([]byte(nil), q.GroupID.Value...)}
	return q
}
