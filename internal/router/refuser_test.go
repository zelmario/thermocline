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
	r := &Router{refusedBy: map[string]time.Time{"a:27017": now}}

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

// A connection keeps the member it was opened to, so the old primary's refusal
// can arrive after the router has moved its pin to the new one. Charged to the
// pin, it turned the real primary away for primaryWait: a host sweep on
// 2026-09-29 saw every lookup answer "no primary yet" for five seconds while
// the new primary was writable.
func TestALateRefusalIsChargedToTheMemberThatSentIt(t *testing.T) {
	r := &Router{primary: "new:27017", primaryProbing: make(chan struct{})}

	r.primaryMoved("old:27017")
	if r.namesTheRefuser("new:27017", time.Now()) {
		t.Error("the new primary was refused for a refusal the old one sent")
	}
	if !r.namesTheRefuser("old:27017", time.Now()) {
		t.Error("the member that refused was accepted as primary again straight away")
	}
	if !r.primaryKnown() {
		t.Error("a refusal from a member the router had moved off made the pin doubtful")
	}

	r.primaryMoved("new:27017")
	if !r.namesTheRefuser("new:27017", time.Now()) || r.primaryKnown() {
		t.Error("a refusal from the pinned member itself was not taken as evidence against the pin")
	}
}

// The pinned member refuses, and then a member the router left earlier refuses
// too, late. The second must not wipe out the first: a lagging view naming the
// pinned member would then be taken as an answer and the client sent straight
// back to it.
func TestALateRefusalKeepsThePinnedMembersOwn(t *testing.T) {
	r := &Router{primary: "pinned:27017", primaryProbing: make(chan struct{})}

	r.primaryMoved("pinned:27017")
	r.primaryMoved("older:27017")
	if !r.namesTheRefuser("pinned:27017", time.Now()) {
		t.Error("the pinned member's refusal was forgotten when another member refused")
	}
	if !r.namesTheRefuser("older:27017", time.Now()) {
		t.Error("the later refusal was not recorded")
	}
}

// A lagging view names the member that refused, and is turned away. The same
// member answering for itself that it is the writable primary is not a view:
// it was elected again, and turning it away left a client waiting for a
// primary that was already there.
func TestARefuserElectedAgainIsAccepted(t *testing.T) {
	now := time.Now()
	r := &Router{refusedBy: map[string]time.Time{"a:27017": now}}

	if r.acceptsPrimary("a:27017", "b:27017", false, now.Add(time.Second)) {
		t.Error("another member naming the refuser was taken as an answer")
	}
	if r.acceptsPrimary("a:27017", "a:27017", false, now.Add(time.Second)) {
		t.Error("the refuser was accepted without saying it is writable")
	}
	if !r.acceptsPrimary("a:27017", "a:27017", true, now.Add(time.Second)) {
		t.Error("the refuser, elected again and saying so itself, was turned away")
	}
}
