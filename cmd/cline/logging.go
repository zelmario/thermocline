package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/zelmario/thermocline/internal/config"
)

// setupLogging picks where a daemon writes its logs.
//
// Default is stderr, which is what you want under systemd: journald captures
// the stream and rotation is not your problem. A file is for running cline
// outside a service manager, and then it behaves like mongod's systemLog.path,
// meaning the file receives everything and the terminal receives nothing.
//
// The --log-file flag wins over log.file in the config, so a one-off run can
// send its output somewhere else without editing cline.yaml.
func setupLogging(flagPath string, cfg config.Config, verbose bool) (*slog.Logger, func(), error) {
	level := map[string]slog.Level{"debug": slog.LevelDebug, "warn": slog.LevelWarn,
		"error": slog.LevelError}[cfg.Log.Level] // info, the zero Level, otherwise
	if verbose {
		level = slog.LevelDebug
	}
	handler := func(w io.Writer) slog.Handler {
		opts := &slog.HandlerOptions{Level: level}
		if cfg.Log.Format == "json" {
			return slog.NewJSONHandler(w, opts)
		}
		return slog.NewTextHandler(w, opts)
	}

	path := cfg.Log.File
	if flagPath != "" {
		path = flagPath
	}
	if path == "" {
		log := slog.New(handler(os.Stderr))
		// A daemon must not die of the signal that means "reopen your log".
		//
		// Go terminates the process for an unhandled SIGHUP, and the handler
		// below is only installed when there is a file to reopen, so a daemon
		// logging to stderr -- which is what the systemd units do, because
		// journald takes the stream -- was killed by it. Measured: a router
		// started without --log-file was gone three seconds after kill -HUP.
		//
		// It is reachable from the packaging this repository ships. The
		// logrotate file signals every cline process by name, so a deployment
		// that gives one daemon a file and leaves the others on the journal
		// kills the others at the first rotation. systemd restarts them, so the
		// symptom is daemons restarting nightly for no visible reason, and a
		// feeder that dies this way does not release its lease, which costs its
		// replacement the full thirty seconds. An operator's reflex reload,
		// kill -HUP, does the same thing by hand.
		//
		// Drained rather than acted on: there is no file to reopen, and saying
		// so once is worth more than a line per signal.
		hup := make(chan os.Signal, 1)
		signal.Notify(hup, syscall.SIGHUP)
		go func() {
			for range hup {
				log.Info("SIGHUP with no log file configured, so there is nothing to reopen",
					"meaning", "logs are going to stderr, which journald captures and rotates",
					"action", "nothing; set log.file if you want files and logrotate")
			}
		}()
		return log, func() { signal.Stop(hup); close(hup) }, nil
	}

	w, err := openLogFile(path)
	if err != nil {
		return nil, nil, err
	}
	log := slog.New(handler(w))
	w.watchSIGHUP(log)
	log.Info("logging to file", "path", path)
	return log, func() { _ = w.Close() }, nil
}

// logFile is an io.Writer over an append-only file that can be reopened in
// place.
//
// Rotation is logrotate's job, not ours, but it only works if the daemon
// cooperates: logrotate renames the file and signals the process, and a process
// that never reopens keeps writing to the renamed inode forever. Disk fills up
// and the file you are tailing stays empty. Hence SIGHUP.
type logFile struct {
	mu   sync.Mutex
	path string
	f    *os.File
}

func openLogFile(path string) (*logFile, error) {
	w := &logFile{path: path}
	if err := w.reopen(); err != nil {
		return nil, fmt.Errorf("open log file %s: %w", path, err)
	}
	return w, nil
}

func (w *logFile) reopen() error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	w.mu.Lock()
	old := w.f
	w.f = f
	w.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}

func (w *logFile) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Write(p)
}

func (w *logFile) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}

// watchSIGHUP reopens the file whenever logrotate asks. A failed reopen leaves
// the old handle in place and complains on stderr, because losing the log is
// better than losing the daemon.
func (w *logFile) watchSIGHUP(log *slog.Logger) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	go func() {
		for range ch {
			if err := w.reopen(); err != nil {
				fmt.Fprintln(os.Stderr, "cline: reopening log file:", err)
				continue
			}
			log.Info("log file reopened")
		}
	}()
}
