package checkpoint

import (
	"errors"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Lives here rather than beside either of its callers because both of them ask
// the same question of the same kind of token, and a predicate about a resume
// token belongs with the package that stores one. It was the feeder's alone
// until the router started following the fast tier for DDL it did not perform,
// and a copy in each would be two places to get a new server wording wrong.

// ResumePointGone reports whether MongoDB refused the resume token, so
// mirroring cannot carry on from where it stopped.
//
// Two codes and several wordings. ChangeStreamHistoryLost is the specific one.
// ChangeStreamFatalError is what a server says when the token is simply not
// there any more: measured against PSMDB 8.0, "cannot resume stream; the resume
// token was not found". So the code narrows it to a fatal change stream error
// and the word resume in the message decides, rather than matching one phrasing
// that a release can change.
//
// A token from another cluster lands here too, and belongs here: it names a
// position this oplog has never had, mirroring cannot resume from it, and the
// operator has the same hole to think about either way.
func ResumePointGone(err error) bool {
	var ce mongo.CommandError
	if !errors.As(err, &ce) {
		return false
	}
	if ce.Code == 286 || ce.Name == "ChangeStreamHistoryLost" {
		return true
	}
	if ce.Code == 280 || ce.Name == "ChangeStreamFatalError" {
		return strings.Contains(strings.ToLower(ce.Message), "resume")
	}
	return false
}
