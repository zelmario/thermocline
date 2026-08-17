package main

import (
	"fmt"
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
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}

	path := cfg.Log.File
	if flagPath != "" {
		path = flagPath
	}
	if path == "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})), func() {}, nil
	}

	w, err := openLogFile(path)
	if err != nil {
		return nil, nil, err
	}
	log := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level}))
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
