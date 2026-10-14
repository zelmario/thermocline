package feeder

import (
	"errors"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/zelmario/thermocline/internal/checkpoint"
)

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

// "Not found" is not "history lost": the first can come with the oplog minutes
// old, when the checkpoint's event is on a namespace since removed from the
// config. Both still stop a feeder whose checkpoint cannot say which.
func TestTokenEventMissingIsItsOwnAnswer(t *testing.T) {
	notFound := mongo.CommandError{Code: 280, Name: "ChangeStreamFatalError",
		Message: "PlanExecutor error during aggregation :: caused by :: cannot resume stream; the resume token was not found. {_data: \"826ACFD3FC\"}"}
	lost := mongo.CommandError{Code: 286, Name: "ChangeStreamHistoryLost",
		Message: "Resume of change stream was not possible, as the resume point may no longer be in the oplog."}
	if !checkpoint.TokenEventMissing(notFound) || checkpoint.TokenEventMissing(lost) {
		t.Error("the two answers are not told apart")
	}
	if !checkpoint.ResumePointGone(notFound) || !checkpoint.ResumePointGone(lost) {
		t.Error("both have to stop a feeder that cannot tell which case it is in")
	}
}
