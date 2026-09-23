package router

import "testing"

// Pure so it can be checked without setting the parameter: setting it changes
// every read on that cluster for as long as it is set, which is the same reason
// concernDrift is tested this way rather than end to end.
func TestReadDeadlineBypassed(t *testing.T) {
	for _, c := range []struct {
		name      string
		cold, hot int64
		warn      bool
	}{
		{"unset everywhere, which is every ordinary pair", 0, 0, false},
		{"set on both, the configuration an operator intends", 5000, 5000, true},
		{"set on the fast tier alone", 0, 5000, true},
		{"set on the archive alone, which an adoption carries over", 5000, 0, true},
	} {
		if got := readDeadlineBypassed(c.cold, c.hot); got != c.warn {
			t.Errorf("%s: archive=%d fast=%d, warn %v want %v", c.name, c.cold, c.hot, got, c.warn)
		}
	}
}
