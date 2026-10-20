package router

import (
	"testing"
	"time"

	"github.com/zelmario/thermocline/internal/config"
)

// repeated copies a document up on its second miss inside the window, and a
// first miss, or one after the window, answers from the archive alone.
func TestCopyUpReadsRepeated(t *testing.T) {
	r := &Router{cfg: config.RouterConfig{CopyUpReads: config.CopyUpRepeated, CopyUpWindow: config.Duration(time.Minute)}}
	if r.copyUpRead("ns|a") {
		t.Fatal("the first miss of a document copied it up")
	}
	if !r.copyUpRead("ns|a") {
		t.Fatal("the second miss inside the window did not copy it up")
	}
	if r.copyUpRead("ns|a") {
		t.Fatal("a copy-up was not forgotten, so the next miss after it copied again at once")
	}

	var m missMemory
	now := time.Now()
	m.again("ns|b", now, time.Minute)
	if m.again("ns|b", now.Add(2*time.Minute), time.Minute) {
		t.Fatal("a miss outside the window counted as the second")
	}
}

func TestCopyUpReadsAlwaysAndNever(t *testing.T) {
	always := &Router{cfg: config.RouterConfig{CopyUpReads: config.CopyUpAlways}}
	never := &Router{cfg: config.RouterConfig{CopyUpReads: config.CopyUpNever}}
	unset := &Router{}
	if !always.copyUpRead("ns|a") || never.copyUpRead("ns|a") || !unset.copyUpRead("ns|a") {
		t.Fatal("always must copy, never must not, and an unset value is the old behaviour, always")
	}
}
