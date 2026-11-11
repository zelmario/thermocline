package router

import (
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// liveShapeSignature describes a read the live cache would not take, without
// any of its values: the operators of its filter, the stages of its pipeline,
// the options it set. Verify mode logs it beside each skip, so a sweep says
// which shapes are worth teaching the cache rather than only how many reads it
// missed. See tc-nmg.
func liveShapeSignature(name string, cmd bson.Raw) string {
	var b strings.Builder
	b.WriteString(name)
	elems, err := cmd.Elements()
	if err != nil {
		return name + " unreadable"
	}
	var opts []string
	for _, e := range elems {
		k := e.Key()
		switch k {
		case name, "$db", "lsid", "$clusterTime", "$readPreference", "cursor", "maxTimeMS", "comment",
			"startTransaction", "autocommit", "apiStrict", "apiDeprecationErrors":
			continue
		// Shown because the cache decides by them: a batch shorter than the
		// answer leaves a cursor it does not have, a transaction reads its
		// own snapshot, and a find with no limit is answered only whole.
		// Hidden, 50 of 66 skips of "find filter{}" in one sweep were a
		// batchSize of 7, and read as a shape the cache should learn.
		case "batchSize", "singleBatch", "apiVersion":
			opts = append(opts, k)
		case "txnNumber":
			opts = append(opts, "txn")
		case "limit", "skip":
			if n, ok := e.Value().AsInt64OK(); ok && n != 0 {
				opts = append(opts, k)
			}
		case "readConcern":
			if d, ok := e.Value().DocumentOK(); ok {
				if lv, ok := d.Lookup("level").StringValueOK(); ok {
					opts = append(opts, "readConcern:"+lv)
				}
			}
		case "filter", "query":
			if d, ok := e.Value().DocumentOK(); ok {
				b.WriteString(" filter" + filterSignature(d))
			}
		case "sort":
			if d, ok := e.Value().DocumentOK(); ok {
				b.WriteString(" sort" + keysSignature(d))
			}
		case "projection":
			if d, ok := e.Value().DocumentOK(); ok {
				b.WriteString(" proj" + projSignature(d))
			}
		case "pipeline":
			if a, ok := e.Value().ArrayOK(); ok {
				b.WriteString(" " + pipelineSignature(bson.Raw(a)))
			}
		default:
			opts = append(opts, k)
		}
	}
	if len(opts) > 0 {
		sort.Strings(opts)
		b.WriteString(" opts{" + strings.Join(opts, ",") + "}")
	}
	return b.String()
}

// filterSignature is a filter with each value replaced by its operator, or by
// its type for an equality.
func filterSignature(d bson.Raw) string {
	elems, err := d.Elements()
	if err != nil {
		return "{?}"
	}
	parts := make([]string, 0, len(elems))
	for _, e := range elems {
		k := e.Key()
		v := e.Value()
		switch {
		case k == "$and" || k == "$or" || k == "$nor":
			var subs []string
			if a, ok := v.ArrayOK(); ok {
				vals, _ := bson.Raw(a).Values()
				for _, s := range vals {
					if sd, ok := s.DocumentOK(); ok {
						subs = append(subs, filterSignature(sd))
					}
				}
			}
			parts = append(parts, k+"["+strings.Join(subs, ",")+"]")
		case strings.HasPrefix(k, "$"):
			parts = append(parts, k)
		default:
			parts = append(parts, k+":"+valueSignature(v))
		}
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// valueSignature is an equality's type, or the operators of an operator
// document.
func valueSignature(v bson.RawValue) string {
	if d, ok := v.DocumentOK(); ok {
		if elems, err := d.Elements(); err == nil && len(elems) > 0 && strings.HasPrefix(elems[0].Key(), "$") {
			ops := make([]string, 0, len(elems))
			for _, e := range elems {
				op := e.Key()
				if op == "$not" || op == "$elemMatch" {
					if sd, ok := e.Value().DocumentOK(); ok {
						op += filterSignature(sd)
					}
				}
				ops = append(ops, op)
			}
			return strings.Join(ops, "+")
		}
		return "eq(object)"
	}
	return "eq(" + v.Type.String() + ")"
}

func keysSignature(d bson.Raw) string {
	elems, err := d.Elements()
	if err != nil {
		return "{?}"
	}
	keys := make([]string, 0, len(elems))
	for _, e := range elems {
		keys = append(keys, e.Key())
	}
	return "{" + strings.Join(keys, ",") + "}"
}

// projSignature names a projection's keys, marking those that are not a plain
// include or exclude.
func projSignature(d bson.Raw) string {
	elems, err := d.Elements()
	if err != nil {
		return "{?}"
	}
	keys := make([]string, 0, len(elems))
	for _, e := range elems {
		k := e.Key()
		switch e.Value().Type {
		case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble, bson.TypeBoolean:
		default:
			k += ":expr"
		}
		keys = append(keys, k)
	}
	return "{" + strings.Join(keys, ",") + "}"
}

// pipelineSignature names each stage, with a $match's filter and a $group's
// accumulators.
func pipelineSignature(a bson.Raw) string {
	vals, err := a.Values()
	if err != nil {
		return "[?]"
	}
	stages := make([]string, 0, len(vals))
	for _, s := range vals {
		sd, ok := s.DocumentOK()
		if !ok {
			stages = append(stages, "?")
			continue
		}
		elems, err := sd.Elements()
		if err != nil || len(elems) == 0 {
			stages = append(stages, "?")
			continue
		}
		name := elems[0].Key()
		body := elems[0].Value()
		switch name {
		case "$match":
			if d, ok := body.DocumentOK(); ok {
				name += filterSignature(d)
			}
		case "$group":
			if d, ok := body.DocumentOK(); ok {
				name += groupSignature(d)
			}
		case "$sort":
			if d, ok := body.DocumentOK(); ok {
				name += keysSignature(d)
			}
		}
		stages = append(stages, name)
	}
	return "[" + strings.Join(stages, " ") + "]"
}

// groupSignature is a $group's _id kind and its accumulators' operators.
func groupSignature(d bson.Raw) string {
	elems, err := d.Elements()
	if err != nil {
		return "{?}"
	}
	parts := make([]string, 0, len(elems))
	for _, e := range elems {
		v := e.Value()
		if e.Key() == "_id" {
			switch {
			case v.Type == bson.TypeString:
				parts = append(parts, "_id:field")
			case v.Type == bson.TypeEmbeddedDocument:
				sd, _ := v.DocumentOK()
				if es, err := sd.Elements(); err == nil && len(es) > 0 && strings.HasPrefix(es[0].Key(), "$") {
					parts = append(parts, "_id:"+es[0].Key())
				} else {
					parts = append(parts, "_id:object")
				}
			default:
				parts = append(parts, "_id:const")
			}
			continue
		}
		acc := "?"
		if sd, ok := v.DocumentOK(); ok {
			if es, err := sd.Elements(); err == nil && len(es) > 0 {
				acc = es[0].Key()
				switch es[0].Value().Type {
				case bson.TypeString:
					acc += "(field)"
				case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble:
					acc += "(const)"
				default:
					acc += "(expr)"
				}
			}
		}
		parts = append(parts, acc)
	}
	return "{" + strings.Join(parts, ",") + "}"
}
