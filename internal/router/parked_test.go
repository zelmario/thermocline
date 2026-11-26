package router

import (
	"net"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

func TestParkedSetFindsByUserAndMember(t *testing.T) {
	var s parkedSet
	now := time.Now()
	up := func() net.Conn { a, _ := net.Pipe(); return a }
	s.park(up(), "m1:27017", "app@admin", now)
	s.park(up(), "m2:27017", "app@admin", now)
	s.park(up(), "m1:27017", "other@admin", now)
	got := s.forUser("app@admin", "m2:27017", now)
	if len(got) != 1 || got[0].member != "m1:27017" {
		t.Fatalf("app@admin away from m2: got %d, want the one on m1", len(got))
	}
	if n := len(s.forUser("nobody@admin", "", now)); n != 0 {
		t.Fatalf("a user that parked nothing found %d", n)
	}
	if n := len(s.forUser("app@admin", "", now.Add(parkKeep+time.Second))); n != 0 {
		t.Fatalf("kept past parkKeep: %d", n)
	}
	if s.park(up(), "m1:27017", "", now) {
		t.Fatal("parked a connection with no known user")
	}
}

func TestParkedSetIsBounded(t *testing.T) {
	var s parkedSet
	now := time.Now()
	for i := 0; i < parkMax+5; i++ {
		a, _ := net.Pipe()
		s.park(a, "m1:27017", "app@admin", now)
	}
	if n := len(s.forUser("app@admin", "", now)); n != parkMax {
		t.Fatalf("kept %d, want %d", n, parkMax)
	}
}

func TestParkedExchangeRefusesAnotherRequestsReply(t *testing.T) {
	a, b := net.Pipe()
	p := &parkedUp{conn: a, member: "m1:27017", user: "app@admin", used: time.Now()}
	msg, err := wire.NewMsg(77, 0, bson.D{{Key: "getMore", Value: int64(5)}, {Key: "collection", Value: "c"}, {Key: "$db", Value: "d"}})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		if _, err := wire.Read(b); err != nil {
			return
		}
		// A reply left over from a command the parked connection never read.
		stale, _ := wire.NewMsg(1, 12, bson.D{{Key: "ok", Value: 1}})
		_ = wire.Write(b, stale)
	}()
	if _, ok := p.exchange(msg, time.Now()); ok {
		t.Fatal("took a reply to request 12 as the answer to request 77")
	}
	if !p.dead {
		t.Fatal("a parked connection out of step was kept")
	}
}
