package feeder

import (
	"encoding/hex"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A resume token carries the cluster time of the position it marks, and that is
// the only honest way to say how far mirroring has got.
//
// The alternative, and what this replaces, is the feeder stamping its own clock
// when a poll comes back empty. That is wrong twice over. It claims a moment it
// has not proved: an empty poll says the server had nothing more to send when it
// answered, not that a write committed a millisecond earlier had been delivered.
// And it publishes a reading of the *feeder's* clock for the *router* to compare
// against its own, so on separate hosts the barrier is only as sound as the skew
// between two machines nothing keeps in step.
//
// A cluster time has neither problem. It is the replica set's own logical clock,
// so both daemons read it from the same place, and MongoDB advances the
// post-batch resume token past an empty batch precisely to say "nothing exists
// up to here". Measured against a real 8.0 primary, the token from an empty poll
// sat exactly one increment past the operationTime the same server reported,
// which is that statement written down.
//
// The token's `_data` is a hex-encoded KeyString, and its first field is the
// cluster time: one type byte, then four bytes of seconds and four of increment,
// both big-endian. That layout is not in the manual, so this reads it
// defensively and says so when it cannot: a feeder that cannot decode a token
// keeps the old behaviour rather than publishing a time it guessed.
const (
	// tokenTimestampPrefix is the KeyString type byte for the timestamp that
	// opens every v1 resume token. Anything else is a format this does not
	// know, and guessing at it would put a wrong instant in front of a read
	// that trusts it.
	tokenTimestampPrefix = 0x82
	// tokenTimestampLen is the type byte plus the eight bytes of timestamp.
	tokenTimestampLen = 9
)

// clusterTimeFromToken reads the cluster time a resume token marks.
//
// The second return is false for a token this cannot read, which includes an
// empty one, a `_data` that is not hex, one too short to hold a timestamp, and
// any future format that does not open with a timestamp. Every one of those is
// a reason to fall back, never a reason to publish a guess.
func clusterTimeFromToken(tok bson.Raw) (bson.Timestamp, bool) {
	if len(tok) == 0 {
		return bson.Timestamp{}, false
	}
	data, ok := tok.Lookup("_data").StringValueOK()
	if !ok || len(data) < tokenTimestampLen*2 {
		return bson.Timestamp{}, false
	}
	raw, err := hex.DecodeString(data[:tokenTimestampLen*2])
	if err != nil {
		return bson.Timestamp{}, false
	}
	if raw[0] != tokenTimestampPrefix {
		return bson.Timestamp{}, false
	}
	return bson.Timestamp{
		T: uint32(raw[1])<<24 | uint32(raw[2])<<16 | uint32(raw[3])<<8 | uint32(raw[4]),
		I: uint32(raw[5])<<24 | uint32(raw[6])<<16 | uint32(raw[7])<<8 | uint32(raw[8]),
	}, true
}
