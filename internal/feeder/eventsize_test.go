package feeder

import (
	"errors"
	"testing"
)

// The two errors the 7MB _id probe logged on 2026-09-26, against a write error
// that is the feeder's own business.
func TestAnEventTooLargeIsToldFromAWriteTooLarge(t *testing.T) {
	for _, m := range []string{
		"(BSONObjectTooLarge) Executor error during getMore :: caused by :: BSONObj size: 29360471 (0x1C00157) is invalid.",
		"open change stream: (BSONObjectTooLarge) PlanExecutor error during aggregation :: caused by :: BSONObj size: 29360471 (0x1C00157) is invalid.",
	} {
		if !eventTooLarge(errors.New(m)) {
			t.Errorf("not recognised: %s", m)
		}
	}
	if eventTooLarge(errors.New("write exception: write errors: [BSONObjectTooLarge: Resulting document after update is larger than 16777216]")) {
		t.Error("a write error was read as the change stream's")
	}
}
