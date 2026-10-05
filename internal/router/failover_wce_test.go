package router

import (
	"testing"

	"github.com/zelmario/thermocline/internal/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// A write the old primary applied and then stopped waiting on comes back ok:1
// with a write concern error. It is a failover reply all the same: read as
// success, the connection stayed on the old member and the driver's retry was
// refused there for a write that had landed. See isFailoverReply.
func TestAWriteConcernErrorFromAStepDownIsAFailover(t *testing.T) {
	reply := func(wce bson.D) bson.Raw {
		d := bson.D{{Key: "n", Value: int32(1)}, {Key: "ok", Value: 1.0}}
		if wce != nil {
			d = append(d, bson.E{Key: "writeConcernError", Value: wce})
		}
		raw, err := bson.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	cases := []struct {
		name string
		doc  bson.Raw
		want bool
	}{
		{"interrupted by the step-down", reply(bson.D{{Key: "code", Value: int32(11602)},
			{Key: "codeName", Value: "InterruptedDueToReplStateChange"}}), true},
		{"primary stepped down", reply(bson.D{{Key: "code", Value: int32(189)}}), true},
		{"shutting down", reply(bson.D{{Key: "code", Value: int64(91)}}), true},
		// A timeout says nothing about who is primary.
		{"write concern timeout", reply(bson.D{{Key: "code", Value: int32(64)},
			{Key: "codeName", Value: "WriteConcernFailed"}}), false},
		{"plain success", reply(nil), false},
	}
	for _, c := range cases {
		if got := isFailoverReply(c.doc); got != c.want {
			t.Errorf("%s: isFailoverReply = %v, want %v", c.name, got, c.want)
		}
	}
}

// An aggregate that writes is not sent again after a failover. It may have run,
// and a $merge run twice is applied twice; now that a write concern error from
// a step-down counts as a failover reply, one that certainly ran reaches here.
func TestAnAggregateThatWritesIsNotReplayed(t *testing.T) {
	msg := func(pipeline bson.A) *wire.Message {
		m, err := wire.NewMsg(1, 0, bson.D{{Key: "aggregate", Value: "orders"},
			{Key: "pipeline", Value: pipeline}, {Key: "cursor", Value: bson.D{}},
			{Key: "$db", Value: "shop"}})
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	c := &conn{}
	if !c.replayable(msg(bson.A{bson.D{{Key: "$match", Value: bson.D{}}}}), "aggregate") {
		t.Error("a read-only aggregate should be replayable")
	}
	for _, stage := range []string{"$merge", "$out"} {
		if c.replayable(msg(bson.A{bson.D{{Key: "$match", Value: bson.D{}}},
			bson.D{{Key: stage, Value: "summary"}}}), "aggregate") {
			t.Errorf("an aggregate ending in %s was replayable", stage)
		}
	}
}
