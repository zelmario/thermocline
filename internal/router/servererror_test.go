package router

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// An error the archive returned is MongoDB's answer, and an application branches
// on the code rather than on the prose. Replacing it with a generic
// CommandFailed and a sentence about completeness told the application its query
// was refused by this proxy when in fact its query was invalid.
func TestArchiveErrorsKeepTheirCode(t *testing.T) {
	r := &Router{}
	ce := mongo.CommandError{
		Code:    2,
		Name:    "BadValue",
		Message: "unknown top level operator: $nosuchop",
	}

	msg, passed := r.serverErrorReply(7, 7, fmt.Errorf("archive read: %w", ce))
	if !passed {
		t.Fatal("a server error was not passed through")
	}
	doc, err := msg.ReplyDoc()
	if err != nil {
		t.Fatalf("reply doc: %v", err)
	}
	for _, want := range []struct {
		key string
		val any
	}{
		{"ok", 0.0},
		{"code", int32(2)},
		{"codeName", "BadValue"},
		{"errmsg", "unknown top level operator: $nosuchop"},
	} {
		got, lerr := doc.LookupErr(want.key)
		if lerr != nil {
			t.Fatalf("%s missing from the reply", want.key)
		}
		var v any
		if uerr := got.Unmarshal(&v); uerr != nil {
			t.Fatalf("unmarshal %s: %v", want.key, uerr)
		}
		if fmt.Sprint(v) != fmt.Sprint(want.val) {
			t.Errorf("%s = %v, want %v", want.key, v, want.val)
		}
	}
}

// The labels are the half a driver acts on without being asked. A transient
// transaction error that arrives stripped of its label is one the driver will
// not retry, and the retry is the whole contract.
func TestArchiveErrorLabelsSurvive(t *testing.T) {
	r := &Router{}
	ce := mongo.CommandError{Code: 251, Name: "NoSuchTransaction", Labels: []string{"TransientTransactionError"}}
	msg, passed := r.serverErrorReply(1, 1, ce)
	if !passed {
		t.Fatal("a server error was not passed through")
	}
	doc, err := msg.ReplyDoc()
	if err != nil {
		t.Fatalf("reply doc: %v", err)
	}
	val, lerr := doc.LookupErr("errorLabels")
	if lerr != nil {
		t.Fatal("errorLabels missing from the reply")
	}
	var labels []string
	if uerr := val.Unmarshal(&labels); uerr != nil {
		t.Fatalf("unmarshal errorLabels: %v", uerr)
	}
	if len(labels) != 1 || labels[0] != "TransientTransactionError" {
		t.Errorf("errorLabels = %v", labels)
	}
}

// Failing to reach the archive is this system failing to answer, not MongoDB
// refusing to. Those keep the refusal, which says what the operator can do
// about it; dressing one up as a server error would send them looking at their
// query instead of at the archive.
func TestReachabilityFailuresAreNotPassedOff(t *testing.T) {
	r := &Router{}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"a plain error", errors.New("connection refused")},
		{"a context deadline", context.DeadlineExceeded},
		{"a coded network error", mongo.CommandError{Code: 89, Name: "NetworkTimeout", Labels: []string{"NetworkError"}}},
		{"an error with no code", mongo.CommandError{Message: "something went wrong"}},
		// The archive is reached as the router, so this is about the router.
		{"the router's own credentials", mongo.CommandError{Code: 13, Name: "Unauthorized",
			Message: "not authorized on shop to execute command find"}},
		{"a failure to authenticate to the archive", mongo.CommandError{Code: 18, Name: "AuthenticationFailed"}},
	} {
		if _, passed := r.serverErrorReply(1, 1, tc.err); passed {
			t.Errorf("%s was passed off as a server error", tc.name)
		}
	}
}
