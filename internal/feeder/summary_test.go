package feeder

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// The summary says what the last minute mirrored, and nothing when nothing
// was.
func TestLogSummary(t *testing.T) {
	var buf bytes.Buffer
	f := &Feeder{log: slog.New(slog.NewTextHandler(&buf, nil))}
	f.logSummary() // the first call only starts the clock
	f.counters.mirrored, f.counters.tombstoned, f.counters.unmirrorable = 600, 60, 1
	f.logSummary()
	if buf.Len() != 0 {
		t.Fatalf("summarised before a minute had passed: %s", buf.String())
	}
	f.summarisedAt = time.Now().Add(-summaryEvery)
	f.logSummary()
	line := buf.String()
	for _, want := range []string{"msg=feeder", "mirrored=600", "tombstoned=60", "refused_by_archive=1", "per_second=11"} {
		if !strings.Contains(line, want) {
			t.Errorf("summary %q lacks %s", line, want)
		}
	}
	buf.Reset()
	f.summarisedAt = time.Now().Add(-summaryEvery)
	f.logSummary()
	if buf.Len() != 0 {
		t.Errorf("an idle minute was summarised: %s", buf.String())
	}
}
