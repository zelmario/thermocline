package feeder

import (
	"errors"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
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
