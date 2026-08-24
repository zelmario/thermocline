package router

import (
	"runtime/debug"
)

// serve recovers a panic so one client pays for a defect instead of every
// client. That reasoning does not stop at the connection: a panic in any
// goroutine kills the process, and this one starts several that a client's data
// reaches. A copy-up reads a document out of the archive, the indexer reads
// index specifications, the invalidation watcher reads change events, and the
// inventory reads a catalog. Any of them panicking would stop the proxy for
// everybody, and none of them is answering a client at the time, so nobody
// would even see which request did it.
//
// safely is the same bargain in one place: the work is done, a panic is
// reported with its stack and counted, and the router carries on.
//
// The background work itself is not retried. Every caller here is something
// that runs again on its own schedule, so the next pass is the retry, and one
// that panics twice says so twice rather than spinning.

// safely runs fn, turning a panic into a reported defect rather than the end of
// the process. what names the work, so the log says which one it was.
func (r *Router) safely(what string, fn func()) {
	defer func() {
		p := recover()
		if p == nil {
			return
		}
		r.Stats.Panics.Add(1)
		r.log.Error("recovered a panic in background work",
			"work", what,
			"panic", p,
			"stack", string(debug.Stack()),
			"effect", "this piece of work was abandoned and the router carried on; it runs again "+
				"on its own schedule",
			"action", "this is a defect: report the stack above")
	}()
	fn()
}

// goSafely starts fn in its own goroutine under the same protection.
func (r *Router) goSafely(what string, fn func()) {
	go r.safely(what, fn)
}
