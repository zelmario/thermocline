package watermark

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// What one row of the watermarks collection claims.
//
// The claim is the completeFrom field, not the row. It used to be the row:
// Claimed came from the document existing and completeFrom was decoded into a
// plain time.Time, so a row carrying nothing but its _id read as complete from
// the zero time, which is the most permissive claim this system can hold. Every
// range query on that namespace would have been answered from the fast tier
// alone, however far back it reached. Measured through `cline watermark` before
// the fix, on a row inserted with no other field: "complete for all of history".
func TestMarkOf(t *testing.T) {
	at := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	cases := []struct {
		name        string
		doc         bson.D
		wantClaimed bool
		wantAll     bool
		wantErr     bool
	}{
		{
			name:        "a claim back to an instant",
			doc:         bson.D{{Key: "_id", Value: "shop.orders"}, {Key: "completeFrom", Value: at}},
			wantClaimed: true,
		},
		{
			// A fast tier that genuinely holds everything says so with the zero
			// time, which is a claim somebody made rather than the absence of one.
			name:        "a claim on all of history",
			doc:         bson.D{{Key: "_id", Value: "shop.orders"}, {Key: "completeFrom", Value: time.Time{}}},
			wantClaimed: true,
			wantAll:     true,
		},
		{
			name: "a row with nothing but its id claims nothing",
			doc:  bson.D{{Key: "_id", Value: "shop.orders"}},
		},
		{
			name: "and neither does one carrying only other fields",
			doc:  bson.D{{Key: "_id", Value: "shop.orders"}, {Key: "updatedAt", Value: at}},
		},
		{
			// Refused rather than guessed at: the caller reports the claim as
			// unreadable, which is a different message from "nothing has
			// claimed this" and a different remedy.
			name:    "completeFrom that is not a date",
			doc:     bson.D{{Key: "_id", Value: "shop.orders"}, {Key: "completeFrom", Value: "yesterday"}},
			wantErr: true,
		},
		{
			name:    "completeFrom as a number, which a hand-written row invites",
			doc:     bson.D{{Key: "_id", Value: "shop.orders"}, {Key: "completeFrom", Value: int64(1772000000)}},
			wantErr: true,
		},
	}

	// The index generation rides on this document because it is already read on
	// the hot path. It has to survive both shapes of row: a claim, and a row
	// that is only there to carry the number because the indexes changed before
	// anything warmed. Dropping it in the second case would leave every other
	// router on its old listing for the full minute.
	t.Run("the index generation is carried with a claim", func(t *testing.T) {
		raw, _ := bson.Marshal(bson.D{
			{Key: "_id", Value: "shop.orders"},
			{Key: "completeFrom", Value: at},
			{Key: "indexGen", Value: int64(7)},
		})
		m, err := markOf(bson.Raw(raw), "shop.orders")
		if err != nil || !m.Claimed || m.IndexGen != 7 {
			t.Fatalf("got %+v, err %v; want a claim carrying generation 7", m, err)
		}
	})
	t.Run("and without one", func(t *testing.T) {
		raw, _ := bson.Marshal(bson.D{
			{Key: "_id", Value: "shop.orders"},
			{Key: "indexGen", Value: int64(3)},
		})
		m, err := markOf(bson.Raw(raw), "shop.orders")
		if err != nil {
			t.Fatalf("refused: %v", err)
		}
		if m.Claimed {
			t.Error("a row carrying only a generation reads as a claim")
		}
		if m.IndexGen != 3 {
			t.Errorf("IndexGen = %d, want 3", m.IndexGen)
		}
	})
	t.Run("an absent generation is zero, not an error", func(t *testing.T) {
		raw, _ := bson.Marshal(bson.D{{Key: "_id", Value: "shop.orders"}, {Key: "completeFrom", Value: at}})
		m, err := markOf(bson.Raw(raw), "shop.orders")
		if err != nil || m.IndexGen != 0 || !m.Claimed {
			t.Fatalf("got %+v, err %v; want a claim with generation 0", m, err)
		}
	})

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw, err := bson.Marshal(c.doc)
			if err != nil {
				t.Fatal(err)
			}
			mark, merr := markOf(bson.Raw(raw), "shop.orders")
			if c.wantErr {
				if merr == nil {
					t.Fatalf("accepted it, and read the claim as %+v", mark)
				}
				return
			}
			if merr != nil {
				t.Fatalf("refused a row it should read: %v", merr)
			}
			if mark.Claimed != c.wantClaimed {
				t.Errorf("Claimed = %v, want %v", mark.Claimed, c.wantClaimed)
			}
			if got := mark.CoversAll(); got != c.wantAll {
				t.Errorf("CoversAll = %v, want %v", got, c.wantAll)
			}
			if c.wantClaimed && !c.wantAll && !mark.From.Equal(at) {
				t.Errorf("From = %v, want %v", mark.From, at)
			}
		})
	}
}
