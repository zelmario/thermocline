package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// A partly refused insert's reply is built here, and it has to carry the fast
// tier's clock as mongod's would: without operationTime a causally consistent
// session can read from before the documents it has just written.
func TestInsertErrorReplyCarriesTheClock(t *testing.T) {
	op := bson.Timestamp{T: 1792800000, I: 7}
	upstream, err := bson.Marshal(bson.D{
		{Key: "n", Value: int32(2)},
		{Key: "ok", Value: 1.0},
		{Key: "$clusterTime", Value: bson.D{{Key: "clusterTime", Value: op}}},
		{Key: "operationTime", Value: op},
	})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := wire.NewMsg(5, 0, bson.D{{Key: "insert", Value: "c"}})
	if err != nil {
		t.Fatal(err)
	}
	r := &Router{}
	out, err := r.insertErrorReply(msg, "db.c", 2, []keyClash{{doc: 1, index: "_id_"}}, upstream, []int{0, 2})
	if err != nil {
		t.Fatal(err)
	}
	doc, err := out.ReplyDoc()
	if err != nil {
		t.Fatal(err)
	}
	got, i, ok := doc.Lookup("operationTime").TimestampOK()
	if !ok || got != op.T || i != op.I {
		t.Fatalf("operationTime %v/%v (%v), want %v", got, i, ok, op)
	}
	if _, err := doc.LookupErr("$clusterTime"); err != nil {
		t.Fatalf("no $clusterTime in %v", doc)
	}
	// And a batch refused whole, which wrote nothing, has no clock to carry.
	none, err := r.insertErrorReply(msg, "db.c", 0, []keyClash{{doc: 0, index: "_id_"}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ndoc, _ := none.ReplyDoc()
	if _, err := ndoc.LookupErr("operationTime"); err == nil {
		t.Fatalf("a refused batch carries an operationTime: %v", ndoc)
	}
}
