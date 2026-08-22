package feeder

import (
	"errors"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Which refusals mean mirroring cannot carry on from where it stopped.
//
// This is a unit test rather than a suite section because the condition cannot
// be arranged reliably from outside. Rewriting a checkpoint's resume token to
// name an instant years before the oplog begins was refused once and accepted
// minutes later by the same server, so a suite built on it would report a
// working feeder as a broken one, or the reverse, depending on the day.
//
// The messages here were taken from a PSMDB 8.0 primary that did refuse.
func TestResumePointGone(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			"history lost by code",
			mongo.CommandError{Code: 286, Name: "ChangeStreamHistoryLost",
				Message: "PlanExecutor error during aggregation :: caused by :: Resume of change stream was not possible, as the resume point may no longer be in the oplog."},
			true,
		},
		{
			"history lost by name, on a server that did not set the code this way",
			mongo.CommandError{Name: "ChangeStreamHistoryLost", Message: "resume point may no longer be in the oplog"},
			true,
		},
		{
			// Measured. This is what the server actually said.
			"the token is simply not there",
			mongo.CommandError{Code: 280, Name: "ChangeStreamFatalError",
				Message: "PlanExecutor error during aggregation :: caused by :: cannot resume stream; the resume token was not found."},
			true,
		},
		{
			// A fatal change stream error that is about something else must not
			// stop the feeder: it would turn a reopen into an outage.
			"a fatal change stream error with another cause",
			mongo.CommandError{Code: 280, Name: "ChangeStreamFatalError",
				Message: "$changeStream may not be opened on the internal config database"},
			false,
		},
		{"an ordinary command error", mongo.CommandError{Code: 26, Name: "NamespaceNotFound", Message: "ns not found"}, false},
		{"not a command error at all", errors.New("connection refused"), false},
		{"nothing", nil, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := resumePointGone(c.err); got != c.want {
				t.Errorf("resumePointGone(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// The sentinel has to survive being wrapped, because that is how it reaches the
// loop that decides whether to stop or reopen.
func TestResumePointLostSurvivesWrapping(t *testing.T) {
	err := fmt.Errorf("%w: %v", ErrResumePointLost,
		mongo.CommandError{Code: 286, Name: "ChangeStreamHistoryLost"})
	if !errors.Is(err, ErrResumePointLost) {
		t.Fatal("the wrapped error is not recognised, so the feeder would reopen for ever instead of stopping")
	}
	if errors.Is(err, ErrUnannouncedDelete) {
		t.Error("it matches the other sentinel, so the two halt paths cannot be told apart")
	}
}
