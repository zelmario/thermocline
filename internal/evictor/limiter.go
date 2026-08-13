package evictor

import (
	"context"
	"time"
)

// limiter paces removal to a documents-per-second budget.
//
// Deliberately simple: it tracks how long the work so far should have taken and
// sleeps off the difference. Eviction runs for hours, so smoothing over the
// whole run matters more than smoothing each batch, and a batch that lands
// during a slow moment on the cluster gets its pause back rather than being
// rushed to catch up.
type limiter struct {
	rate    int
	started time.Time
	spent   int
}

func newLimiter(ratePerSecond int) *limiter {
	if ratePerSecond < 1 {
		ratePerSecond = 1
	}
	return &limiter{rate: ratePerSecond}
}

// spend records n documents removed and sleeps if that put the run ahead of
// budget. It returns early if ctx ends.
func (l *limiter) spend(ctx context.Context, n int) {
	if l.started.IsZero() {
		l.started = time.Now()
	}
	l.spent += n

	owed := time.Duration(float64(l.spent) / float64(l.rate) * float64(time.Second))
	elapsed := time.Since(l.started)
	if owed <= elapsed {
		return
	}

	select {
	case <-ctx.Done():
	case <-time.After(owed - elapsed):
	}
}
