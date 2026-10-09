package cache

import (
	"math"
	"math/big"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A write to a collection used to drop every answer that read it, and on a
// collection anything writes to that is the whole reason the cache does
// nothing. Measured on the shop: 8.6 orders a second against a monthly report
// asked 1.7 times a second, so a held report lived about 0.12s, and 1.3% of
// them were served from the cache. The report counts a closed month. No order
// placed today is in it, and no status change touches the one field it reads.
//
// So a write is now asked whether it can change an answer before the answer is
// dropped, and the question has two halves:
//
//   - An insert can change an answer only through a document that passes the
//     answer's filter. Every inserted document is checked against it.
//   - An update can change an answer only through a field the answer read: its
//     filter, its sort, its projection, the expressions of its pipeline. One
//     that changes none of those leaves the same documents matching, in the
//     same order, contributing the same values.
//
// Everything else drops the answer as before: deletes, replacements, upserts,
// and every query or write this cannot read completely. Each half answers
// "cannot change it" only for what it fully understands, and "may change it"
// for anything else, because keeping an answer that changed is a wrong answer
// and dropping one that did not is a lookup.

// Reads is what a cached answer depended on in its own collection.
type Reads struct {
	// Known is false when the command could not be read completely. Any write
	// may then change the answer.
	Known bool
	// filter is what a document had to pass to take part in the answer,
	// compiled. Nil means every document of the collection takes part.
	//
	// Compiled rather than kept as the command's bytes, which were a slice of
	// the client's whole message: an answer held for an hour kept that message
	// alive for an hour, and it was parsed again for every write.
	filter *matcher
	// Fields are the paths the answer read, and AllFields says it read whole
	// documents.
	Fields    []string
	AllFields bool
}

// WriteKind is what a write did to its collection.
type WriteKind int

const (
	// WriteOther is any write not described more closely, and drops every
	// answer that read the collection.
	WriteOther WriteKind = iota
	// WriteInsert added the documents in Inserted.
	WriteInsert
	// WriteUpdate changed the fields in Fields of documents already there, and
	// added none.
	WriteUpdate
)

// Write describes one write to one collection.
type Write struct {
	Kind      WriteKind
	Inserted  []bson.Raw
	Fields    []string
	AllFields bool
	// cut says Fields are already cut at their first array part, which
	// Prepared does once so a write judged against many answers does not
	// repeat it for each.
	cut bool
}

// Prepared returns the write with its fields cut once, for judging it against
// many answers.
func (w Write) Prepared() Write {
	if w.Kind != WriteUpdate || w.cut {
		return w
	}
	fs := make([]string, len(w.Fields))
	for i, p := range w.Fields {
		fs[i] = fieldPrefix(p)
	}
	w.Fields, w.cut = fs, true
	return w
}

// AnyWrite is a write this cache knows nothing about.
var AnyWrite = Write{Kind: WriteOther}

// MayChange reports whether w can change an answer that read r.
func (r Reads) MayChange(w Write) bool {
	if !r.Known {
		return true
	}
	switch w.Kind {
	case WriteInsert:
		if len(w.Inserted) == 0 || r.filter == nil {
			return true
		}
		for _, d := range w.Inserted {
			if !r.filter.cannot(d) {
				return true
			}
		}
		return false
	case WriteUpdate:
		if w.AllFields || r.AllFields {
			return true
		}
		for _, p := range w.Fields {
			if !w.cut {
				// Cut here as well as where the write was described, because a
				// change event names paths as the server writes them: items.0
				// replaced is items.qty changed, and the two share no prefix.
				p = fieldPrefix(p)
			}
			for _, q := range r.Fields {
				if pathsOverlap(p, q) {
					return true
				}
			}
		}
		return false
	}
	return true
}

// ReadsOf works out what a read command depended on. A command it cannot read
// completely comes back with Known false.
func ReadsOf(name string, cmd bson.Raw) Reads {
	elems, err := cmd.Elements()
	if err != nil {
		return Reads{}
	}
	var f fields
	var filter bson.Raw
	var projection, sortSpec bson.RawValue
	var pipeline bson.Raw
	key := ""
	for _, e := range elems {
		k := e.Key()
		v := e.Value()
		if k == name || SkipInShape(k) {
			continue
		}
		switch name + "." + k {
		case "find.filter", "count.query", "distinct.query":
			d, ok := v.DocumentOK()
			if !ok {
				return Reads{}
			}
			filter = bson.Raw(d)
		case "find.projection":
			projection = v
		case "find.sort":
			sortSpec = v
		case "distinct.key":
			s, ok := v.StringValueOK()
			if !ok {
				return Reads{}
			}
			key = s
		case "aggregate.pipeline":
			a, ok := v.ArrayOK()
			if !ok {
				return Reads{}
			}
			pipeline = bson.Raw(a)
		case "find.hint", "find.skip", "find.limit", "find.batchSize", "find.singleBatch",
			"find.collation", "find.let", "find.returnKey", "find.noCursorTimeout",
			"count.hint", "count.skip", "count.limit", "count.collation",
			"distinct.hint", "distinct.collation",
			"aggregate.cursor", "aggregate.allowDiskUse", "aggregate.collation",
			"aggregate.hint", "aggregate.let":
			// Which documents an index hint reads is decided by the filter it
			// is given here; let only names values for the expressions, whose
			// fields are collected where they are used.
		default:
			// min and max bound an index scan on fields this does not know,
			// and anything not listed is something nobody has thought about.
			return Reads{}
		}
	}

	switch name {
	case "find":
		if !f.addFilter(filter) || !f.addSort(sortSpec) {
			return Reads{}
		}
		if !f.addProjection(projection) {
			return Reads{}
		}
	case "count":
		if !f.addFilter(filter) {
			return Reads{}
		}
	case "distinct":
		if key == "" || !f.addFilter(filter) {
			return Reads{}
		}
		f.add(key)
	case "aggregate":
		if pipeline == nil {
			return Reads{}
		}
		var ok bool
		filter, ok = f.addPipeline(pipeline)
		if !ok {
			return Reads{}
		}
	default:
		return Reads{}
	}
	return Reads{Known: true, filter: compile(filter), Fields: f.paths, AllFields: f.all}
}

// fields collects the paths a command reads.
type fields struct {
	paths []string
	all   bool
}

func (f *fields) add(p string) {
	if p == "" {
		f.all = true
		return
	}
	f.paths = append(f.paths, fieldPrefix(p))
}

// addFilter adds the fields a query filter reads. False means the filter used
// something whose fields cannot be named.
func (f *fields) addFilter(filter bson.Raw) bool {
	if filter == nil {
		return true
	}
	elems, err := filter.Elements()
	if err != nil {
		return false
	}
	for _, e := range elems {
		k := e.Key()
		switch {
		case k == "$and" || k == "$or" || k == "$nor":
			a, ok := e.Value().ArrayOK()
			if !ok {
				return false
			}
			vals, err := bson.Raw(a).Values()
			if err != nil {
				return false
			}
			for _, v := range vals {
				d, ok := v.DocumentOK()
				if !ok || !f.addFilter(bson.Raw(d)) {
					return false
				}
			}
		case k == "$expr":
			if !f.addExpr(e.Value()) {
				return false
			}
		case k == "$comment":
		case strings.HasPrefix(k, "$"):
			// $where, $text, $jsonSchema and the like read the document in
			// ways that have no field list.
			f.all = true
		default:
			f.add(k)
		}
	}
	return true
}

// addExpr adds the fields an aggregation expression reads.
func (f *fields) addExpr(v bson.RawValue) bool {
	switch v.Type {
	case bson.TypeString:
		s := v.StringValue()
		switch {
		case strings.HasPrefix(s, "$$ROOT") || strings.HasPrefix(s, "$$CURRENT"):
			f.all = true
		case strings.HasPrefix(s, "$$"):
			// A variable: $$NOW, or one bound by $let, $map or $filter to
			// something whose own fields are collected where it is bound.
		case strings.HasPrefix(s, "$"):
			f.add(s[1:])
		}
	case bson.TypeEmbeddedDocument, bson.TypeArray:
		vals, err := bson.Raw(v.Value).Values()
		if err != nil {
			return false
		}
		for _, x := range vals {
			if !f.addExpr(x) {
				return false
			}
		}
	}
	return true
}

func (f *fields) addSort(v bson.RawValue) bool {
	if v.Type == 0 {
		return true
	}
	d, ok := v.DocumentOK()
	if !ok {
		return false
	}
	elems, err := bson.Raw(d).Elements()
	if err != nil {
		return false
	}
	for _, e := range elems {
		if strings.HasPrefix(e.Key(), "$") {
			return false
		}
		if _, isDoc := e.Value().DocumentOK(); isDoc {
			// {$meta: "textScore"} and the like.
			f.all = true
			continue
		}
		f.add(e.Key())
	}
	return true
}

// addProjection adds what a find returns. No projection, or one that excludes,
// returns whole documents.
func (f *fields) addProjection(v bson.RawValue) bool {
	if v.Type == 0 {
		f.all = true
		return true
	}
	d, ok := v.DocumentOK()
	if !ok {
		return false
	}
	elems, err := bson.Raw(d).Elements()
	if err != nil {
		return false
	}
	if len(elems) == 0 {
		f.all = true
		return true
	}
	if !inclusion(elems) {
		f.all = true
		return true
	}
	for _, e := range elems {
		k := e.Key()
		if strings.HasPrefix(k, "$") || strings.Contains(k, ".$") {
			return false
		}
		switch {
		case truthy(e.Value()):
			f.add(k)
		case isZeroOrFalse(e.Value()):
		default:
			// An operator or an expression. {comments: {$slice: 5}} on its own
			// returns every other field as well, so what it reads is not a
			// list this can make.
			f.all = true
		}
	}
	return true
}

// addPipeline adds the fields an aggregation reads, and returns the filter its
// leading $match stages apply. False means a stage this does not understand.
func (f *fields) addPipeline(p bson.Raw) (bson.Raw, bool) {
	stages, err := p.Values()
	if err != nil {
		return nil, false
	}
	var leading []bson.Raw
	inLead := true
	reshaped := false
	for _, s := range stages {
		d, ok := s.DocumentOK()
		if !ok {
			return nil, false
		}
		el, err := bson.Raw(d).Elements()
		if err != nil || len(el) != 1 {
			return nil, false
		}
		name, spec := el[0].Key(), el[0].Value()
		if name != "$match" {
			inLead = false
		}
		switch name {
		case "$match":
			m, ok := spec.DocumentOK()
			if !ok || !f.addFilter(bson.Raw(m)) {
				return nil, false
			}
			if inLead {
				leading = append(leading, bson.Raw(m))
			}
		case "$group", "$sortByCount":
			if !f.addExpr(spec) {
				return nil, false
			}
			reshaped = true
		case "$count":
			reshaped = true
		case "$sort":
			if !f.addSort(spec) {
				return nil, false
			}
		case "$limit", "$skip":
		case "$addFields", "$set":
			if !f.addExpr(spec) {
				return nil, false
			}
		case "$unset":
		case "$project":
			m, ok := spec.DocumentOK()
			if !ok {
				return nil, false
			}
			el, err := bson.Raw(m).Elements()
			if err != nil {
				return nil, false
			}
			if !f.addExpr(spec) {
				return nil, false
			}
			if inclusion(el) {
				for _, e := range el {
					if truthy(e.Value()) {
						f.add(e.Key())
					}
				}
				reshaped = true
			}
		default:
			// $lookup, $unionWith, $facet, $replaceRoot, $unwind and the rest.
			return nil, false
		}
		// Stages after a reshape read the reshaped documents, so the fields
		// they name add nothing, but they are still walked: a $lookup there
		// reads this collection again, whole.
	}
	if !reshaped {
		f.all = true
	}
	switch len(leading) {
	case 0:
		return nil, true
	case 1:
		return leading[0], true
	}
	and := bson.A{}
	for _, m := range leading {
		and = append(and, m)
	}
	out, err := bson.Marshal(bson.D{{Key: "$and", Value: and}})
	if err != nil {
		return nil, false
	}
	return out, true
}

// inclusion reports whether a projection keeps only what it names. _id may be
// excluded in one that otherwise includes.
func inclusion(elems []bson.RawElement) bool {
	for _, e := range elems {
		if e.Key() == "_id" {
			continue
		}
		if isZeroOrFalse(e.Value()) {
			return false
		}
	}
	for _, e := range elems {
		if e.Key() != "_id" {
			return true
		}
	}
	return false
}

func truthy(v bson.RawValue) bool {
	switch v.Type {
	case bson.TypeBoolean:
		return v.Boolean()
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble:
		n, _ := v.AsFloat64OK()
		return n != 0
	}
	return false
}

func isZeroOrFalse(v bson.RawValue) bool {
	switch v.Type {
	case bson.TypeBoolean:
		return !v.Boolean()
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble:
		n, _ := v.AsFloat64OK()
		return n == 0
	}
	return false
}

// WriteOfUpdate describes the update statements of an update command. Any
// upsert, replacement or pipeline makes it a write of unknown fields.
func WriteOfUpdate(statements []bson.Raw) Write {
	w := Write{Kind: WriteUpdate}
	for _, st := range statements {
		if up, ok := st.Lookup("upsert").BooleanOK(); ok && up {
			return AnyWrite
		}
		u, ok := st.Lookup("u").DocumentOK()
		if !ok {
			return AnyWrite
		}
		paths, ok := UpdatedPaths(bson.Raw(u))
		if !ok {
			return AnyWrite
		}
		w.Fields = append(w.Fields, paths...)
	}
	return w
}

// updateOperators are the update operators whose keys are the fields they
// change. $rename changes its values too, and is handled on its own.
var updateOperators = map[string]bool{
	"$set": true, "$unset": true, "$inc": true, "$mul": true, "$min": true,
	"$max": true, "$push": true, "$addToSet": true, "$pull": true,
	"$pullAll": true, "$pop": true, "$currentDate": true, "$bit": true,
	"$setOnInsert": true,
}

// UpdatedPaths lists the fields an update document changes. False for a
// replacement or an operator not in the list.
func UpdatedPaths(u bson.Raw) ([]string, bool) {
	elems, err := u.Elements()
	if err != nil || len(elems) == 0 {
		return nil, false
	}
	var out []string
	for _, e := range elems {
		op := e.Key()
		if op != "$rename" && !updateOperators[op] {
			return nil, false
		}
		d, ok := e.Value().DocumentOK()
		if !ok {
			return nil, false
		}
		fs, err := bson.Raw(d).Elements()
		if err != nil {
			return nil, false
		}
		for _, f := range fs {
			out = append(out, fieldPrefix(f.Key()))
			if op == "$rename" {
				to, ok := f.Value().StringValueOK()
				if !ok {
					return nil, false
				}
				out = append(out, fieldPrefix(to))
			}
		}
	}
	return out, true
}

// fieldPrefix cuts a path at its first positional or numeric part, which names
// an element of an array rather than a field: items.$.qty and items.0.qty both
// change something under items.
func fieldPrefix(p string) string {
	start := 0
	for start <= len(p) {
		end := strings.IndexByte(p[start:], '.')
		if end < 0 {
			end = len(p)
		} else {
			end += start
		}
		part := p[start:end]
		if strings.HasPrefix(part, "$") || isIndex(part) {
			if start == 0 {
				return ""
			}
			return p[:start-1]
		}
		start = end + 1
	}
	return p
}

func isIndex(s string) bool {
	if s == "" {
		return false
	}
	_, err := strconv.Atoi(s)
	return err == nil
}

// pathsOverlap reports whether a change to one path can change the other: the
// same field, or one inside the other.
func pathsOverlap(a, b string) bool {
	if a == "" || b == "" {
		return true
	}
	return a == b || strings.HasPrefix(a, b+".") || strings.HasPrefix(b, a+".")
}

// matcher is a query filter compiled for cannot: the parts it understands,
// with their values copied out of the command. Parts it does not understand are
// left out, which only ever makes it say "may match" more often.
type matcher struct {
	fields []fieldCond
	ands   []*matcher
	ors    [][]*matcher
}

type fieldCond struct {
	name string
	// eq is an implicit equality, and ops are {$op: value} conditions.
	eq  *bson.RawValue
	ops []opCond
}

type opCond struct {
	op string
	v  bson.RawValue
}

// compile builds a matcher from a filter. Nil for no filter at all.
func compile(filter bson.Raw) *matcher {
	if filter == nil {
		return nil
	}
	return compileOwned(bson.Raw(append([]byte(nil), filter...)))
}

// compileOwned compiles a filter whose bytes nothing else will change.
func compileOwned(filter bson.Raw) *matcher {
	m := &matcher{}
	elems, err := filter.Elements()
	if err != nil {
		return m
	}
	for _, e := range elems {
		k := e.Key()
		switch {
		case k == "$and":
			if subs, ok := subFilters(e.Value()); ok {
				for _, sub := range subs {
					m.ands = append(m.ands, compileOwned(sub))
				}
			}
		case k == "$or":
			if subs, ok := subFilters(e.Value()); ok && len(subs) > 0 {
				branch := make([]*matcher, 0, len(subs))
				for _, sub := range subs {
					branch = append(branch, compileOwned(sub))
				}
				m.ors = append(m.ors, branch)
			}
		case strings.HasPrefix(k, "$") || strings.Contains(k, "."):
		default:
			fc := fieldCond{name: k}
			v := e.Value()
			if d, isDoc := v.DocumentOK(); isDoc {
				ops, err := bson.Raw(d).Elements()
				if err != nil || len(ops) == 0 || !strings.HasPrefix(ops[0].Key(), "$") {
					continue
				}
				for _, op := range ops {
					switch op.Key() {
					case "$eq", "$gt", "$gte", "$lt", "$lte":
						if comparable(op.Value()) != "" {
							fc.ops = append(fc.ops, opCond{op: op.Key(), v: op.Value()})
						}
					}
				}
				if len(fc.ops) == 0 {
					continue
				}
			} else {
				if comparable(v) == "" {
					continue
				}
				fc.eq = &v
			}
			m.fields = append(m.fields, fc)
		}
	}
	return m
}

// cannot reports whether doc certainly fails the filter. It answers true only
// for what it fully understands: a top-level field holding a number, a date or
// an ObjectId, against $eq, $gt, $gte, $lt or $lte with a value of the same
// kind, combined with $and, $or and implicit conjunction. Strings are left out
// because a collation decides how they compare, arrays because any element can
// match, and a missing field because null has rules of its own.
func (m *matcher) cannot(doc bson.Raw) bool {
	for _, fc := range m.fields {
		if fc.cannot(doc.Lookup(fc.name)) {
			return true
		}
	}
	for _, a := range m.ands {
		if a.cannot(doc) {
			return true
		}
	}
	for _, branch := range m.ors {
		all := true
		for _, b := range branch {
			if !b.cannot(doc) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

// cannotMatch is cannot for a filter not compiled in advance.
func cannotMatch(filter, doc bson.Raw) bool {
	return compile(filter).cannot(doc)
}

func subFilters(v bson.RawValue) ([]bson.Raw, bool) {
	a, ok := v.ArrayOK()
	if !ok {
		return nil, false
	}
	vals, err := bson.Raw(a).Values()
	if err != nil {
		return nil, false
	}
	out := make([]bson.Raw, 0, len(vals))
	for _, v := range vals {
		d, ok := v.DocumentOK()
		if !ok {
			return nil, false
		}
		out = append(out, bson.Raw(d))
	}
	return out, true
}

// cannot judges one field's condition against the value the document holds.
func (fc fieldCond) cannot(have bson.RawValue) bool {
	if comparable(have) == "" {
		return false
	}
	if fc.eq != nil {
		c, ok := compare(have, *fc.eq)
		return ok && c != 0
	}
	for _, op := range fc.ops {
		c, ok := compare(have, op.v)
		if !ok {
			continue
		}
		switch op.op {
		case "$eq":
			if c != 0 {
				return true
			}
		case "$gt":
			if c <= 0 {
				return true
			}
		case "$gte":
			if c < 0 {
				return true
			}
		case "$lt":
			if c >= 0 {
				return true
			}
		case "$lte":
			if c > 0 {
				return true
			}
		}
	}
	return false
}

// comparable names the kinds this compares, and "" for everything else.
func comparable(v bson.RawValue) string {
	switch v.Type {
	case bson.TypeInt32, bson.TypeInt64:
		return "number"
	case bson.TypeDouble:
		if math.IsNaN(v.Double()) {
			return ""
		}
		return "number"
	case bson.TypeDateTime:
		return "date"
	case bson.TypeObjectID:
		return "oid"
	}
	return ""
}

// compare orders two values of the same kind, numbers by exact value.
func compare(a, b bson.RawValue) (int, bool) {
	ka, kb := comparable(a), comparable(b)
	if ka == "" || ka != kb {
		return 0, false
	}
	switch ka {
	case "date":
		x, y := a.DateTime(), b.DateTime()
		return cmp(x < y, x > y), true
	case "oid":
		x, y := a.ObjectID(), b.ObjectID()
		return strings.Compare(string(x[:]), string(y[:])), true
	}
	return exact(a).Cmp(exact(b)), true
}

func cmp(less, more bool) int {
	switch {
	case less:
		return -1
	case more:
		return 1
	}
	return 0
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
