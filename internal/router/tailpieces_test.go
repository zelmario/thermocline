package router

import (
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// 32 events filled from the archive went to the fast tier as one command, and
// 32 documents of 600KB is about 19MB, past mongod's limit on a command: the
// stream died on BSONObjectTooLarge. They go in runs of at most the bound, and
// one event larger than it goes alone.
func TestTailPiecesStayUnderTheBound(t *testing.T) {
	ev := func(n int) any {
		return bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: make([]byte, n)}
	}
	var batch bson.A
	for i := 0; i < 32; i++ {
		batch = append(batch, ev(600<<10))
	}
	pieces := tailPieces(batch, 8<<20)
	total := 0
	for _, p := range pieces {
		size := 0
		for _, e := range batch[p[0]:p[1]] {
			size += eventSize(e)
		}
		if size > 8<<20 {
			t.Errorf("piece %v is %d bytes, past the bound", p, size)
		}
		total += p[1] - p[0]
	}
	if total != 32 || len(pieces) < 3 {
		t.Errorf("32 events of 600KB came out as %v, want every event once in at least three pieces", pieces)
	}
	big := tailPieces(bson.A{ev(10), ev(9 << 20), ev(10)}, 8<<20)
	if fmt.Sprint(big) != "[[0 1] [1 2] [2 3]]" {
		t.Errorf("an event past the bound came out as %v, want it alone", big)
	}
}
