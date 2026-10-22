package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"
)

// servePprof runs Go's profiler on addr until ctx is done. See
// router.pprof.
//
// Measured before optimising: a sorted slice that replaced a map, on reasoning
// alone, allocated 26% more and ran slower. The profiler is how the router's
// per-command cost is read instead of guessed.
func servePprof(ctx context.Context, addr string, log *slog.Logger) {
	// Lock and blocking profiles are off unless asked for, and they are the
	// ones a contended mutex shows up in. Sampled, so their cost is small.
	runtime.SetMutexProfileFraction(100)
	runtime.SetBlockProfileRate(int(time.Millisecond))
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	if host, _, err := net.SplitHostPort(addr); err == nil {
		if ip := net.ParseIP(host); host == "" || (ip != nil && !ip.IsLoopback()) {
			log.Warn("the profiler is bound beyond localhost", "addr", addr,
				"why", "a profile exposes command shapes and internals, and collecting one costs CPU")
		}
	}
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	log.Info("profiler listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("profiler stopped", "err", err)
	}
}
