package feeder

import (
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// An archive with write blocking on refuses every write with code 371, and the
// feeder names that rather than reporting it as a change stream ending. The
// classification is what decides which of those an operator reads, so it has to
// answer no for every other failure: a duplicate key is the one that is never
// retried, and a network error is the one that is.
func TestWritesBlocked(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nothing", nil, false},
		{"an ordinary error", errors.New("connection reset"), false},
		{"a bulk write with the blocked code", mongo.BulkWriteException{
			WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 371, Message: "User writes blocked"}}},
		}, true},
		{"a bulk write with a duplicate key", mongo.BulkWriteException{
			WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 11000, Message: "E11000"}}},
		}, false},
		// One blocked write among others still means the archive is refusing
		// everything: the batch is the unit the feeder retries.
		{"a mixed batch", mongo.BulkWriteException{
			WriteErrors: []mongo.BulkWriteError{
				{WriteError: mongo.WriteError{Code: 11000, Message: "E11000"}},
				{WriteError: mongo.WriteError{Code: 371, Message: "User writes blocked"}},
			},
		}, true},
		{"a command error with the blocked code", mongo.CommandError{Code: 371, Message: "User writes blocked"}, true},
		{"a command error with another code", mongo.CommandError{Code: 26, Message: "NamespaceNotFound"}, false},
		{"a wrapped bulk write", errors.Join(errors.New("mirror shop.orders"), mongo.BulkWriteException{
			WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 371}}},
		}), true},
	}
	for _, c := range cases {
		if got := writesBlocked(c.err); got != c.want {
			t.Errorf("%s: writesBlocked = %v, want %v", c.name, got, c.want)
		}
	}
}
