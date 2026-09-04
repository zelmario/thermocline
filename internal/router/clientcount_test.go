package router

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/zelmario/thermocline/internal/config"
)

// One upstream connection per client is correct for SCRAM and does not scale,
// and the wall is mongod's rather than this router's, so a deployment meets it
// as connection errors on the fast tier. This says so first.
func TestWarnAboutClientCount(t *testing.T) {
	router := func(terminate bool) (*Router, *bytes.Buffer) {
		var buf bytes.Buffer
		r := &Router{
			cfg: config.RouterConfig{TerminateAuth: terminate},
			log: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})),
		}
		return r, &buf
	}
	said := func(b *bytes.Buffer) bool {
		return strings.Contains(b.String(), "costs this many connections")
	}

	t.Run("quiet below the threshold", func(t *testing.T) {
		clientCountWarned.Store(false)
		r, buf := router(false)
		r.warnAboutClientCount(proxiedClientWarnAt - 1)
		if said(buf) {
			t.Error("warned about an ordinary number of clients")
		}
	})

	t.Run("says so at the threshold", func(t *testing.T) {
		clientCountWarned.Store(false)
		r, buf := router(false)
		r.warnAboutClientCount(proxiedClientWarnAt)
		if !said(buf) {
			t.Error("said nothing at the threshold")
		}
	})

	// Every accept above the threshold would otherwise log, which is what turns
	// a useful line into a reason to stop reading the log.
	t.Run("and only once", func(t *testing.T) {
		clientCountWarned.Store(false)
		r, buf := router(false)
		for i := 0; i < 50; i++ {
			r.warnAboutClientCount(proxiedClientWarnAt + int64(i))
		}
		if n := strings.Count(buf.String(), "costs this many connections"); n != 1 {
			t.Errorf("warned %d times, want 1", n)
		}
	})

	// With terminate_auth on, connections are pooled per command and an idle
	// client holds nothing upstream, so the number means nothing.
	t.Run("nothing to say with the connections pooled", func(t *testing.T) {
		clientCountWarned.Store(false)
		r, buf := router(true)
		r.warnAboutClientCount(proxiedClientWarnAt * 100)
		if said(buf) {
			t.Error("warned about pooled connections, where an idle client holds none")
		}
	})
}
