package router

import (
	"strings"
	"testing"
)

// The note only exists for a deployment running with allow_version_skew, and it
// has to be silent everywhere else: an application's developer reading an error
// message should never be pointed at a version gap that is not there.
func TestArchiveDialectNote(t *testing.T) {
	t.Run("silent when the releases match", func(t *testing.T) {
		var r Router
		if note := r.archiveDialectNote(168, "Unrecognized expression '$foo'"); note != "" {
			t.Errorf("note on a matched pair: %q", note)
		}
	})

	t.Run("silent when the archive is the newer one", func(t *testing.T) {
		// rememberReleases only stores a gap when the archive is behind, so an
		// unset pointer is what the safe direction looks like here.
		var r Router
		if note := r.archiveDialectNote(40324, "Unrecognized pipeline stage name: '$bar'"); note != "" {
			t.Errorf("note when the archive is newer: %q", note)
		}
	})

	t.Run("names both releases on a refusal to recognise", func(t *testing.T) {
		var r Router
		r.olderArchive.Store(&releaseGap{hot: "8.0", cold: "7.0"})
		note := r.archiveDialectNote(0, "Invalid $addFields :: caused by :: $convert found an unknown argument: format")
		if note == "" {
			t.Fatal("no note on the error a release gap actually produces")
		}
		for _, want := range []string{"7.0", "8.0", "allow_version_skew"} {
			if !strings.Contains(note, want) {
				t.Errorf("note does not mention %q: %q", want, note)
			}
		}
	})

	t.Run("silent on an error any release would give", func(t *testing.T) {
		var r Router
		r.olderArchive.Store(&releaseGap{hot: "8.0", cold: "7.0"})
		for _, e := range []struct {
			code int32
			msg  string
		}{
			{11000, "E11000 duplicate key error collection: shop.orders index: email_1"},
			{50, "operation exceeded time limit"},
			{9, "BSON field 'find.filter' is the wrong type 'string', expected type 'object'"},
			{13, "not authorized on shop to execute command"},
		} {
			if note := r.archiveDialectNote(e.code, e.msg); note != "" {
				t.Errorf("note on %d %q: %q", e.code, e.msg, note)
			}
		}
	})
}
