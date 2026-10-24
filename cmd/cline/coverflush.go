package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime/coverage"
	"strings"
	"time"
)

// A coverage binary (go build -cover) writes its counters when it exits
// normally, and the live suites stop most daemons with kill -9, which writes
// nothing: the router, the code that most needs a map of which suite reaches
// it, would read as run by almost nobody. So while GOCOVERDIR is set this
// rewrites one counters file per process every two seconds, and a killed daemon
// loses two seconds of counts rather than all of them.
//
// The binary has to be built with -covermode=atomic. WriteCounters refuses a
// program in the default set mode, and the failure is silent here: the first
// coverage sweep, on 2026-10-23, had counters only from the commands that exit
// on their own and none from a single daemon. In an ordinary build it fails the
// same way and the goroutine returns, so this costs nothing in the binary that
// ships.
func init() {
	dir := os.Getenv("GOCOVERDIR")
	if dir == "" {
		return
	}
	go flushCoverage(dir)
}

func flushCoverage(dir string) {
	var buf bytes.Buffer
	if err := coverage.WriteCounters(&buf); err != nil {
		return
	}
	if err := coverage.WriteMetaDir(dir); err != nil {
		return
	}
	metas, _ := filepath.Glob(filepath.Join(dir, "covmeta.*"))
	if len(metas) == 0 {
		return
	}
	hash := strings.TrimPrefix(filepath.Base(metas[0]), "covmeta.")
	// The name covdata expects is covcounters.<meta hash>.<pid>.<nanotime>. A
	// fixed last field keeps it one file per process; the file the runtime
	// writes at a normal exit has its own, and counting a block twice does not
	// change whether it ran.
	name := filepath.Join(dir, fmt.Sprintf("covcounters.%s.%d.1", hash, os.Getpid()))
	tmp := filepath.Join(dir, fmt.Sprintf(".flush.%d", os.Getpid()))
	for {
		buf.Reset()
		if coverage.WriteCounters(&buf) == nil &&
			os.WriteFile(tmp, buf.Bytes(), 0o644) == nil {
			_ = os.Rename(tmp, name)
		}
		time.Sleep(2 * time.Second)
	}
}
