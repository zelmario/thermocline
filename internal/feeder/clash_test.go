package feeder

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestClearFirst(t *testing.T) {
	op := func(id int32, image string, del bool) pendingOp {
		typ, data, err := bson.MarshalValue(id)
		if err != nil {
			t.Fatal(err)
		}
		key, err := bson.Marshal(bson.D{{Key: "_id", Value: id}})
		if err != nil {
			t.Fatal(err)
		}
		k := actionUpsert
		if del {
			k = actionTombstone
		}
		return pendingOp{ns: "a", key: key, id: bson.RawValue{Type: typ, Value: data}, doc: bson.Raw(image), kind: k}
	}
	// One word per operation: -N a delete put first, an image, or xN a mark.
	show := func(ops []pendingOp) string {
		var w []string
		for _, o := range ops {
			n := o.id.Int32()
			switch {
			case o.clearFirst:
				w = append(w, "-"+string(rune('0'+n)))
			case o.kind == actionTombstone:
				w = append(w, "x"+string(rune('0'+n)))
			default:
				w = append(w, string(o.doc))
			}
		}
		return strings.Join(w, " ")
	}
	kinds := func(ms []mongo.WriteModel) string {
		var w []string
		for _, m := range ms {
			switch m.(type) {
			case *mongo.DeleteOneModel:
				w = append(w, "del")
			default:
				w = append(w, "w")
			}
		}
		return strings.Join(w, " ")
	}
	// 1 is upserted twice, 2 once, 3 only marked deleted: one delete each for
	// 1 and 2, none for 3, and everything else as it was.
	ops := []pendingOp{op(1, "a0", false), op(2, "b", false), op(3, "", true), op(1, "a1", false)}
	ms := make([]mongo.WriteModel, len(ops))
	for i := range ms {
		ms[i] = mongo.NewReplaceOneModel()
	}
	ms, ops = clearFirst(ms, ops)
	if got, want := show(ops), "-1 -2 a0 b x3 a1"; got != want {
		t.Errorf("cleared: %q, want %q", got, want)
	}
	if got, want := kinds(ms), "del del w w w w"; got != want {
		t.Errorf("models: %q, want %q", got, want)
	}
	// Refusing 1 takes its delete and its images out, so the archive keeps
	// the copy it had; 2 is untouched.
	ms, ops = dropDoc(ms, ops, docKey(ops[2]))
	if got, want := show(ops), "-2 b x3"; got != want {
		t.Errorf("after refusing 1: %q, want %q", got, want)
	}
	if got, want := kinds(ms), "del w w"; got != want {
		t.Errorf("models after refusing 1: %q, want %q", got, want)
	}
	// A tombstone of a refused document stays: it marks the copy kept.
	ops = []pendingOp{op(1, "a0", false), op(1, "", true)}
	ms = []mongo.WriteModel{mongo.NewReplaceOneModel(), mongo.NewUpdateOneModel()}
	ms, ops = clearFirst(ms, ops)
	_, ops = dropDoc(ms, ops, docKey(ops[0]))
	if got, want := show(ops), "x1"; got != want {
		t.Errorf("refused with a mark: %q, want %q", got, want)
	}
}
