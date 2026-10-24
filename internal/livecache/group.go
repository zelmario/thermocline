package livecache

import (
	"math"
	"math/big"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

// A report is an aggregation that groups: orders per hour, units per product.
// A dashboard asks the same few again and again, and each one reads every
// document its $match takes: on the Docker rig the store's sales by hour took
// 19.8ms and its top sellers 58ms, a thousand times a point read.
//
// The answer is kept the way ReadySet keeps a grouped view: every document the
// $match takes is remembered with what it adds to which group, so a change
// takes its old part away and adds its new one, and the groups are always the
// sums over the documents as they are now. Nothing is computed from a
// difference the stream does not carry.
//
// It is deliberately narrow:
//
//	[$match] [$unwind "$field"] $group [$sort] [$limit]
//
// with the $match a filter ParseFind takes, the group _id a constant, a field
// or $hour, $dayOfMonth, $month or $year of a date field, and the accumulators
// {$sum: 1} and {$sum: "$field"}. Anything whose answer depends on the order
// mongod reads documents in is refused or not answered: a Decimal128, NaN or
// an integer large enough for mongod's running total to overflow, one group
// _id seen as two numeric types, a $sort tie straddling the $limit, and more
// groups than the first batch holds.

// GroupSpec is the grouping part of a KindGroup query.
type GroupSpec struct {
	// Unwind is the top-level field unwound before grouping, "" for none.
	Unwind string
	ID     groupExpr
	Accs   []accSpec
	// Sort is over the output's fields, _id or an accumulator.
	Sort  []SortKey
	Limit int
	// Batch is the first batch the client asked for.
	Batch int
	canon string
}

// groupExpr is what a document is grouped by.
type groupExpr struct {
	// kind is exprConst, exprField or exprDatePart.
	kind  int
	konst bson.RawValue
	path  string
	part  string
}

const (
	exprConst = iota
	exprField
	exprDatePart
)

// accSpec is one accumulator: a count, or a sum of a field.
type accSpec struct {
	Name  string
	count bool
	path  string
}

// maxGroupDocs bounds the documents a grouped entry remembers. A $match that
// takes more is answered by mongod.
const maxGroupDocs = 200_000

// ParseGroupAgg takes a grouping pipeline apart, or reports that it cannot be
// answered here.
func ParseGroupAgg(ns string, cmd bson.Raw) (Query, bool) {
	q := Query{Kind: KindGroup, NS: ns}
	g := &GroupSpec{Batch: firstBatchDefault}
	elems, err := cmd.Elements()
	if err != nil {
		return q, false
	}
	var pipe bson.Raw
	for _, e := range elems {
		k, v := e.Key(), e.Value()
		switch k {
		case "aggregate", "$db", "lsid", "$clusterTime", "comment", "maxTimeMS", "$readPreference":
		case "allowDiskUse":
			if _, ok := v.BooleanOK(); !ok {
				return q, false
			}
		case "readConcern":
			if !plainReadConcern(v) {
				return q, false
			}
		case "cursor":
			d, ok := v.DocumentOK()
			if !ok {
				return q, false
			}
			ce, err := d.Elements()
			if err != nil {
				return q, false
			}
			for _, c := range ce {
				if c.Key() != "batchSize" {
					return q, false
				}
				n, ok := asInt(c.Value())
				if !ok || n < 0 {
					return q, false
				}
				g.Batch = n
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
	stages, err := pipe.Values()
	if err != nil || len(stages) == 0 {
		return q, false
	}
	i := 0
	stage := func() (string, bson.RawValue, bool) {
		if i >= len(stages) {
			return "", bson.RawValue{}, false
		}
		d, ok := stages[i].DocumentOK()
		if !ok {
			return "", bson.RawValue{}, false
		}
		e, err := d.Elements()
		if err != nil || len(e) != 1 {
			return "", bson.RawValue{}, false
		}
		return e[0].Key(), e[0].Value(), true
	}
	if name, v, ok := stage(); ok && name == "$match" {
		d, ok := v.DocumentOK()
		if !ok {
			return q, false
		}
		eq, rng, ok := parseFilter(d)
		if !ok {
			return q, false
		}
		q.Eq, q.Ranges = eq, rng
		i++
	}
	if name, v, ok := stage(); ok && name == "$unwind" {
		s, ok := v.StringValueOK()
		if !ok || !plainPath(s) {
			return q, false
		}
		g.Unwind = s[1:]
		i++
	}
	name, v, ok := stage()
	if !ok || name != "$group" {
		return q, false
	}
	if !g.parseGroup(v) {
		return q, false
	}
	i++
	if name, v, ok := stage(); ok && name == "$sort" {
		d, ok := v.DocumentOK()
		if !ok {
			return q, false
		}
		s, ok := parseSort(d)
		if !ok || len(s) == 0 {
			return q, false
		}
		for _, k := range s {
			if k.Field != "_id" && g.acc(k.Field) < 0 {
				return q, false
			}
		}
		g.Sort = s
		i++
	}
	if name, v, ok := stage(); ok && name == "$limit" {
		n, ok := asInt(v)
		if !ok || n <= 0 {
			return q, false
		}
		g.Limit = n
		i++
	}
	if i != len(stages) {
		return q, false
	}
	g.canon = g.canonical()
	q.Group = g
	return q, true
}

// plainPath is "$field" for a top-level field.
func plainPath(s string) bool {
	return len(s) > 1 && s[0] == '$' && !strings.ContainsAny(s[1:], ".$")
}

func (g *GroupSpec) acc(name string) int {
	for i, a := range g.Accs {
		if a.Name == name {
			return i
		}
	}
	return -1
}

// fieldPath reads "$path" as a path this can resolve: a top-level field, or
// one level into the unwound field's elements.
func (g *GroupSpec) fieldPath(s string) (string, bool) {
	if len(s) < 2 || s[0] != '$' || strings.Contains(s[1:], "$") {
		return "", false
	}
	p := s[1:]
	if !strings.Contains(p, ".") {
		return p, true
	}
	if g.Unwind == "" || !strings.HasPrefix(p, g.Unwind+".") || strings.Contains(p[len(g.Unwind)+1:], ".") {
		return "", false
	}
	return p, true
}

var dateParts = map[string]bool{"$hour": true, "$dayOfMonth": true, "$month": true, "$year": true}

func (g *GroupSpec) parseGroup(v bson.RawValue) bool {
	d, ok := v.DocumentOK()
	if !ok {
		return false
	}
	e, err := d.Elements()
	if err != nil || len(e) < 1 || e[0].Key() != "_id" {
		return false
	}
	id := e[0].Value()
	switch id.Type {
	case bson.TypeNull:
		g.ID = groupExpr{kind: exprConst, konst: id}
	case bson.TypeString:
		if strings.HasPrefix(id.StringValue(), "$") {
			p, ok := g.fieldPath(id.StringValue())
			if !ok {
				return false
			}
			g.ID = groupExpr{kind: exprField, path: p}
		} else {
			g.ID = groupExpr{kind: exprConst, konst: id}
		}
	case bson.TypeInt32, bson.TypeInt64:
		g.ID = groupExpr{kind: exprConst, konst: id}
	case bson.TypeEmbeddedDocument:
		pe, err := id.Document().Elements()
		if err != nil || len(pe) != 1 || !dateParts[pe[0].Key()] {
			return false
		}
		s, ok := pe[0].Value().StringValueOK()
		if !ok {
			return false
		}
		p, ok := g.fieldPath(s)
		if !ok {
			return false
		}
		g.ID = groupExpr{kind: exprDatePart, path: p, part: pe[0].Key()}
	default:
		return false
	}
	for _, x := range e[1:] {
		name := x.Key()
		if name == "" || strings.ContainsAny(name, ".$") || g.acc(name) >= 0 {
			return false
		}
		ad, ok := x.Value().DocumentOK()
		if !ok {
			return false
		}
		sum, ok := single(ad, "$sum")
		if !ok {
			return false
		}
		if s, ok := sum.StringValueOK(); ok {
			p, ok := g.fieldPath(s)
			if !ok {
				return false
			}
			g.Accs = append(g.Accs, accSpec{Name: name, path: p})
			continue
		}
		// {$sum: 1} counts, as an int: mongod adds the constant once per
		// document. Any other constant is refused rather than multiplied.
		if sum.Type != bson.TypeInt32 || sum.Int32() != 1 {
			return false
		}
		g.Accs = append(g.Accs, accSpec{Name: name, count: true})
	}
	return true
}

// canonical names the grouping for the shape, everything but the values.
func (g *GroupSpec) canonical() string {
	var b strings.Builder
	b.WriteString("|group:")
	b.WriteString(g.Unwind)
	b.WriteByte('|')
	switch g.ID.kind {
	case exprConst:
		k, _ := groupKeyOf(g.ID.konst)
		b.WriteString("c=" + k)
	case exprField:
		b.WriteString("f=" + g.ID.path)
	case exprDatePart:
		b.WriteString(g.ID.part + "=" + g.ID.path)
	}
	for _, a := range g.Accs {
		b.WriteString("," + a.Name + ":")
		if a.count {
			b.WriteString("#")
		} else {
			b.WriteString(a.path)
		}
	}
	b.WriteString("|sort:")
	for _, s := range g.Sort {
		b.WriteString(s.Field)
		if s.Desc {
			b.WriteString("-")
		}
		b.WriteByte(',')
	}
	b.WriteString("|limit:")
	b.WriteString(big.NewInt(int64(g.Limit)).String())
	b.WriteString("|batch:")
	b.WriteString(big.NewInt(int64(g.Batch)).String())
	return b.String()
}

// fields are the top-level fields a document's part in a group reads.
func (g *GroupSpec) fields() []string {
	var out []string
	add := func(p string) {
		if i := strings.IndexByte(p, '.'); i >= 0 {
			p = p[:i]
		}
		for _, f := range out {
			if f == p {
				return
			}
		}
		out = append(out, p)
	}
	if g.Unwind != "" {
		add(g.Unwind)
	}
	if g.ID.kind != exprConst {
		add(g.ID.path)
	}
	for _, a := range g.Accs {
		if !a.count {
			add(a.path)
		}
	}
	return out
}

// groupKeyOf is a group _id's identity: valueKey, and null for null and
// missing, which mongod groups together.
func groupKeyOf(v bson.RawValue) (string, bool) {
	if v.Type == 0 || v.Type == bson.TypeNull || v.Type == bson.TypeUndefined {
		return "z", true
	}
	return valueKey(v)
}

// contrib is what one document, or one element of its unwound array, adds to
// one group.
type contrib struct {
	key  string
	id   bson.RawValue
	vals []bson.RawValue
}

// resolve reads path from a document, or from el, the unwound element, when
// the path is the unwound field or under it. Type 0 is missing.
func (g *GroupSpec) resolve(d bson.Raw, el bson.RawValue, path string) bson.RawValue {
	if g.Unwind != "" {
		if path == g.Unwind {
			return el
		}
		if strings.HasPrefix(path, g.Unwind+".") {
			sub, ok := el.DocumentOK()
			if !ok {
				return bson.RawValue{}
			}
			v, err := sub.LookupErr(path[len(g.Unwind)+1:])
			if err != nil {
				return bson.RawValue{}
			}
			return v
		}
	}
	v, err := d.LookupErr(path)
	if err != nil {
		return bson.RawValue{}
	}
	return v
}

// contribs is a document's part in the groups, or false for a value this does
// not reproduce mongod's handling of, which refuses the shape.
func (g *GroupSpec) contribs(d bson.Raw) ([]contrib, bool) {
	var els []bson.RawValue
	if g.Unwind == "" {
		els = []bson.RawValue{{}}
	} else {
		v, err := d.LookupErr(g.Unwind)
		switch {
		case err != nil || v.Type == bson.TypeNull || v.Type == bson.TypeUndefined:
			// Missing or null: $unwind drops the document.
		case v.Type == bson.TypeArray:
			vals, err := v.Array().Values()
			if err != nil {
				return nil, false
			}
			els = vals
		default:
			// A value that is not an array is unwound as one element.
			els = []bson.RawValue{v}
		}
	}
	out := make([]contrib, 0, len(els))
	for _, el := range els {
		var id bson.RawValue
		switch g.ID.kind {
		case exprConst:
			id = g.ID.konst
		case exprField:
			id = g.resolve(d, el, g.ID.path)
			if id.Type == 0 {
				id = bson.RawValue{Type: bson.TypeNull}
			}
		case exprDatePart:
			v := g.resolve(d, el, g.ID.path)
			switch v.Type {
			case 0, bson.TypeNull, bson.TypeUndefined:
				id = bson.RawValue{Type: bson.TypeNull}
			case bson.TypeDateTime:
				id = datePart(g.ID.part, v.DateTime())
			default:
				// mongod fails the whole aggregation.
				return nil, false
			}
		}
		key, ok := groupKeyOf(id)
		if !ok {
			return nil, false
		}
		c := contrib{key: key, id: id, vals: make([]bson.RawValue, len(g.Accs))}
		for i, a := range g.Accs {
			if a.count {
				c.vals[i] = bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 1)}
				continue
			}
			v := g.resolve(d, el, a.path)
			switch v.Type {
			case bson.TypeInt32, bson.TypeInt64:
				n, _ := intValue(v)
				if n > maxSummand || n < -maxSummand {
					return nil, false
				}
				c.vals[i] = v
			case bson.TypeDouble:
				if math.IsNaN(v.Double()) || math.IsInf(v.Double(), 0) {
					return nil, false
				}
				c.vals[i] = v
			case bson.TypeDecimal128:
				return nil, false
			default:
				// $sum leaves out what is not a number, missing included.
			}
		}
		out = append(out, c)
	}
	return out, true
}

// maxSummand bounds an integer added to a $sum. mongod's running total turns
// into a double when it overflows a long and stays one, so the type of the
// answer would depend on the order documents were read in; with every summand
// under 2^40 and at most maxGroupDocs of them per group, nothing gets near.
const maxSummand = 1 << 40

func datePart(part string, ms int64) bson.RawValue {
	t := time.UnixMilli(ms).UTC()
	var n int32
	switch part {
	case "$hour":
		n = int32(t.Hour())
	case "$dayOfMonth":
		n = int32(t.Day())
	case "$month":
		n = int32(t.Month())
	case "$year":
		n = int32(t.Year())
	}
	return bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, n)}
}

// groupAcc is one group's running totals.
type groupAcc struct {
	id   bson.RawValue
	n    int
	sums []exactSum
}

// exactSum is a $sum kept exactly, so taking a part away leaves no rounding
// behind, and rounded the way mongod rounds only when it is read.
type exactSum struct {
	ints     big.Int
	dbl      *big.Float
	nDouble  int
	nLong    int
	nNumeric int
}

const sumPrec = 2200 // enough for any sum of float64s to stay exact

func (s *exactSum) add(v bson.RawValue, sign int) {
	switch v.Type {
	case bson.TypeInt32, bson.TypeInt64:
		n, _ := intValue(v)
		x := big.NewInt(n)
		if sign < 0 {
			s.ints.Sub(&s.ints, x)
		} else {
			s.ints.Add(&s.ints, x)
		}
		if v.Type == bson.TypeInt64 {
			s.nLong += sign
		}
	case bson.TypeDouble:
		if s.dbl == nil {
			s.dbl = new(big.Float).SetPrec(sumPrec)
		}
		x := new(big.Float).SetPrec(sumPrec).SetFloat64(v.Double())
		if sign < 0 {
			s.dbl.Sub(s.dbl, x)
		} else {
			s.dbl.Add(s.dbl, x)
		}
		s.nDouble += sign
	default:
		return
	}
	s.nNumeric += sign
}

// value is the sum as mongod gives it: an int while every summand was an int
// and the total fits one, a long while none was a double, and a double
// otherwise.
func (s *exactSum) value() bson.RawValue {
	if s.nDouble > 0 {
		t := new(big.Float).SetPrec(sumPrec).SetInt(&s.ints)
		if s.dbl != nil {
			t.Add(t, s.dbl)
		}
		f, _ := t.Float64()
		return bson.RawValue{Type: bson.TypeDouble, Value: bsoncore.AppendDouble(nil, f)}
	}
	n := s.ints.Int64()
	if s.nLong == 0 && n >= math.MinInt32 && n <= math.MaxInt32 {
		return bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, int32(n))}
	}
	return bson.RawValue{Type: bson.TypeInt64, Value: bsoncore.AppendInt64(nil, n)}
}

// groupState is a grouped entry's memory: every document's part, and the
// totals they add up to.
type groupState struct {
	spec   *GroupSpec
	groups map[string]*groupAcc
	byDoc  map[string][]contrib
	// refused is set when a document showed a value this cannot reproduce.
	refused bool
}

func newGroupState(spec *GroupSpec) *groupState {
	return &groupState{spec: spec, groups: map[string]*groupAcc{}, byDoc: map[string][]contrib{}}
}

// remove takes a document's part away. It reports whether there was one.
func (s *groupState) remove(id string) bool {
	cs, ok := s.byDoc[id]
	if !ok {
		return false
	}
	delete(s.byDoc, id)
	for _, c := range cs {
		a := s.groups[c.key]
		if a == nil {
			continue
		}
		for i, v := range c.vals {
			a.sums[i].add(v, -1)
		}
		a.n--
		if a.n == 0 {
			delete(s.groups, c.key)
		}
	}
	return true
}

// add puts a document's part in. False refuses the shape.
func (s *groupState) add(id string, d bson.Raw) bool {
	s.remove(id)
	cs, ok := s.spec.contribs(d)
	if !ok {
		s.refused = true
		return false
	}
	if len(s.byDoc) >= maxGroupDocs {
		s.refused = true
		return false
	}
	for _, c := range cs {
		a := s.groups[c.key]
		if a == nil {
			a = &groupAcc{id: c.id, sums: make([]exactSum, len(s.spec.Accs))}
			s.groups[c.key] = a
		} else if a.id.Type != c.id.Type {
			// One group _id seen as two numeric types: mongod keeps whichever
			// it read first.
			s.refused = true
			return false
		}
		for i, v := range c.vals {
			a.sums[i].add(v, 1)
		}
		a.n++
	}
	if len(cs) > 0 {
		s.byDoc[id] = cs
	}
	return true
}

// output is the answer's documents, or false when mongod's own answer would
// depend on an order this does not know: a tie across the limit, sort values
// of two brackets, or more groups than the first batch.
func (s *groupState) output() ([]bson.Raw, bool) {
	g := s.spec
	type row struct {
		doc  bson.Raw
		keys []bson.RawValue
	}
	rows := make([]row, 0, len(s.groups))
	for _, a := range s.groups {
		b := bsoncore.NewDocumentBuilder()
		b.AppendValue("_id", bsoncore.Value{Type: bsoncore.Type(a.id.Type), Data: a.id.Value})
		vals := make([]bson.RawValue, len(g.Accs))
		for i, acc := range g.Accs {
			v := a.sums[i].value()
			vals[i] = v
			b.AppendValue(acc.Name, bsoncore.Value{Type: bsoncore.Type(v.Type), Data: v.Value})
		}
		r := row{doc: bson.Raw(b.Build())}
		for _, k := range g.Sort {
			if k.Field == "_id" {
				r.keys = append(r.keys, a.id)
			} else {
				r.keys = append(r.keys, vals[g.acc(k.Field)])
			}
		}
		rows = append(rows, r)
	}
	if len(g.Sort) > 0 {
		for _, r := range rows {
			for i, k := range r.keys {
				if bracket(k) == "" || bracket(k) != bracket(rows[0].keys[i]) {
					return nil, false
				}
			}
		}
		cmp := func(a, b row) int {
			for i, k := range g.Sort {
				n := compareSame(a.keys[i], b.keys[i])
				if k.Desc {
					n = -n
				}
				if n != 0 {
					return n
				}
			}
			return 0
		}
		sort.SliceStable(rows, func(i, j int) bool { return cmp(rows[i], rows[j]) < 0 })
		if g.Limit > 0 && len(rows) > g.Limit {
			if cmp(rows[g.Limit-1], rows[g.Limit]) == 0 {
				return nil, false
			}
			rows = rows[:g.Limit]
		}
	} else if g.Limit > 0 && len(rows) > g.Limit {
		// Which groups a $limit keeps without a $sort is mongod's choice.
		return nil, false
	}
	if len(rows) > g.Batch {
		return nil, false
	}
	out := make([]bson.Raw, len(rows))
	for i, r := range rows {
		out[i] = r.doc
	}
	return out, true
}

// bytes is roughly what the state holds in memory.
func (s *groupState) bytes() int {
	return len(s.byDoc)*96 + len(s.groups)*(64+48*len(s.spec.Accs))
}

// SameGroupAnswer says whether two replies to a grouping are the same answer:
// the same documents, in the same order of the $sort's values where there is
// a $sort, and in any order where there is none, since mongod promises none.
// "" means the same.
func SameGroupAnswer(q Query, cached, real bson.Raw) string {
	if q.Group == nil {
		return "not a grouping"
	}
	cb, ok1 := batchOfRaw(cached)
	rb, ok2 := batchOfRaw(real)
	if !ok1 || !ok2 {
		return "a reply has no first batch"
	}
	if id, ok := real.Lookup("cursor", "id").AsInt64OK(); ok && id != 0 {
		return "the real answer is longer than one batch"
	}
	if len(cb) != len(rb) {
		return "length"
	}
	for i := range rb {
		for _, k := range q.Group.Sort {
			x, y := cb[i].Lookup(k.Field), rb[i].Lookup(k.Field)
			if x.Type != y.Type || !bytesEqual(x.Value, y.Value) {
				return "sort order"
			}
		}
	}
	ks := func(b []bson.Raw) []string {
		out := make([]string, len(b))
		for i, d := range b {
			out[i] = string(d)
		}
		sort.Strings(out)
		return out
	}
	c, r := ks(cb), ks(rb)
	for i := range c {
		if c[i] != r[i] {
			return "documents"
		}
	}
	return ""
}

func batchOfRaw(reply bson.Raw) ([]bson.Raw, bool) {
	arr, ok := reply.Lookup("cursor", "firstBatch").ArrayOK()
	if !ok {
		return nil, false
	}
	vals, err := bson.Raw(arr).Values()
	if err != nil {
		return nil, false
	}
	out := make([]bson.Raw, 0, len(vals))
	for _, v := range vals {
		d, ok := v.DocumentOK()
		if !ok {
			return nil, false
		}
		out = append(out, d)
	}
	return out, true
}

func bytesEqual(a, b []byte) bool { return string(a) == string(b) }
