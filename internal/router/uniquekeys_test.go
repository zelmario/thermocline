package router

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A key sometimes has to be claimed for a document whose _id is not known yet:
// an upsert creates a document mongod names itself, and an insert can arrive
// without one for the same reason.
//
// The registry row was written by marshalling keyClaim, and bson cannot marshal
// a zero RawValue at all. That did not fail the write, it failed the claim, and
// the guard's answer to a registry it cannot write is to let the write through.
// So the one case with no owner was the one case nothing was enforced.
func TestClaimRowWithoutAnOwner(t *testing.T) {
	now := time.Now().UTC()

	// The shape that used to be written, kept here as the reason claimRow
	// exists rather than as something the code still does.
	if _, err := bson.Marshal(keyClaim{ID: "k", NS: "db.coll", Index: "email_1", Doc: bson.RawValue{}, At: now}); err == nil {
		t.Fatal("marshalling a claim with no owner used to fail, and this test is why claimRow exists")
	}

	raw, err := bson.Marshal(claimRow("k", "db.coll", "email_1", bson.RawValue{}, now))
	if err != nil {
		t.Fatalf("a claim with no owner does not marshal: %v", err)
	}
	if _, lerr := bson.Raw(raw).LookupErr("doc"); lerr == nil {
		t.Error("a claim with no owner carries a doc field, which claimIsLive would read as an owner to re-read")
	}
	for _, f := range []string{"_id", "ns", "idx", "at"} {
		if _, lerr := bson.Raw(raw).LookupErr(f); lerr != nil {
			t.Errorf("the row is missing %s", f)
		}
	}

	// With an owner it is recorded, because that is what makes a claim
	// resolvable by re-reading the document that made it.
	owner := bson.RawValue{Type: bson.TypeInt32, Value: []byte{7, 0, 0, 0}}
	raw, err = bson.Marshal(claimRow("k", "db.coll", "email_1", owner, now))
	if err != nil {
		t.Fatalf("a claim with an owner does not marshal: %v", err)
	}
	got, lerr := bson.Raw(raw).LookupErr("doc")
	if lerr != nil {
		t.Fatal("a claim with an owner did not record it")
	}
	if !got.Equal(owner) {
		t.Errorf("recorded owner %v, want %v", got, owner)
	}
}
