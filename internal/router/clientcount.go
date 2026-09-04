package router

import "sync/atomic"

// proxiedClientWarnAt is the number of open clients that earns a warning while
// authentication is proxied.
//
// One upstream connection per client is simple and correct for SCRAM: the
// connection carries the client's identity and cannot be anybody else's. It
// also does not scale, and the wall is mongod's rather than this router's, so
// the deployment meets it as connection errors on the fast tier rather than as
// anything here saying no.
//
// A thousand is well short of that wall and well past any ordinary application's
// pool, which is what makes it a useful place to speak: early enough to act on,
// late enough not to be noise. With terminate_auth on, connections are pooled
// per command and an idle client holds nothing upstream, so the number means
// nothing there and nothing is said.
const proxiedClientWarnAt = 1000

// clientCountWarned makes the warning once per router rather than once per
// connection. Above the threshold every accept would otherwise log, which is
// the shape that turns a useful line into a reason to stop reading the log.
var clientCountWarned atomic.Bool

// warnAboutClientCount says so when the number of clients starts to matter.
func (r *Router) warnAboutClientCount(open int64) {
	if r.cfg.TerminateAuth || open < proxiedClientWarnAt {
		return
	}
	if !clientCountWarned.CompareAndSwap(false, true) {
		return
	}
	r.log.Warn("this many clients costs this many connections to the fast tier",
		"clients", open,
		"reason", "authentication is proxied, so each client gets its own upstream connection: "+
			"the connection carries that client's SCRAM identity and cannot be shared",
		"effect", "the fast tier sees one connection per client, and the limit that stops this "+
			"growing is mongod's rather than the router's",
		"action", "set router.terminate_auth to pool them per command instead, where an idle "+
			"client holds nothing upstream, and read docs/dashboard.md first because it moves "+
			"the decision about who may read what into this proxy")
}
