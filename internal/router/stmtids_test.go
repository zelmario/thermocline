package router

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A trimmed retryable write keeps its statements' places in what the client
// sent, so a retry of the whole command after a lost reply runs the dropped
// statement once and the kept ones not again.
func TestTrimmedRetryableWriteKeepsStatementNumbers(t *testing.T) {
	raw := func(d bson.D) bson.Raw {
		b, err := bson.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	ids := func(d bson.D) []int32 {
		for _, e := range d {
			if e.Key == "stmtIds" {
				var out []int32
				for _, v := range e.Value.(bson.A) {
					out = append(out, v.(int32))
				}
				return out
			}
		}
		return nil
	}
	retryable := raw(bson.D{{Key: "update", Value: "c"}, {Key: "lsid", Value: bson.D{}}, {Key: "txnNumber", Value: int64(3)}})
	if got := ids(withStmtIDs(bson.D{{Key: "update", Value: "c"}}, retryable, []int{0, 2})); len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Errorf("stmtIds = %v, want [0 2]", got)
	}
	given := raw(bson.D{{Key: "update", Value: "c"}, {Key: "txnNumber", Value: int64(3)}, {Key: "stmtIds", Value: bson.A{int32(7), int32(8), int32(9)}}})
	if got := ids(withStmtIDs(bson.D{{Key: "update", Value: "c"}, {Key: "stmtIds", Value: bson.A{int32(7), int32(8), int32(9)}}}, given, []int{1})); len(got) != 1 || got[0] != 8 {
		t.Errorf("with the client's own stmtIds, got %v, want [8]", got)
	}
	inTxn := raw(bson.D{{Key: "update", Value: "c"}, {Key: "txnNumber", Value: int64(3)}, {Key: "autocommit", Value: false}})
	if got := ids(withStmtIDs(bson.D{{Key: "update", Value: "c"}}, inTxn, []int{0, 2})); got != nil {
		t.Errorf("inside a transaction stmtIds were added: %v", got)
	}
	plain := raw(bson.D{{Key: "update", Value: "c"}})
	if got := ids(withStmtIDs(bson.D{{Key: "update", Value: "c"}}, plain, []int{0, 2})); got != nil {
		t.Errorf("a write that is not retryable got stmtIds: %v", got)
	}
}
