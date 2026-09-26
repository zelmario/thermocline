package router

import (
	"testing"
	"time"
)

// A lookup that names the member which just refused a write is a view that has
// not heard of the step-down, and pinning it sent a reconnecting client back to
// the member that refused it. Named again within primaryWait it is no answer;
// after that the member may legitimately have been elected again.
func TestTheMemberThatRefusedIsNotAnAnswer(t *testing.T) {
	now := time.Now()
	r := &Router{refusedBy: "a:27017", refusedAt: now}

	if !r.namesTheRefuser("a:27017", now.Add(time.Second)) {
		t.Error("the member that refused a second ago was accepted as the primary")
	}
	if r.namesTheRefuser("b:27017", now.Add(time.Second)) {
		t.Error("a different member was refused as if it had refused")
	}
	if r.namesTheRefuser("a:27017", now.Add(primaryWait+time.Second)) {
		t.Error("a member was still refused after the step-down window; it could never be primary again")
	}
	if (&Router{}).namesTheRefuser("a:27017", now) {
		t.Error("with no refusal on record every answer was refused")
	}
}
