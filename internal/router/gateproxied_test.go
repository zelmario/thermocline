package router

import (
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/wire"
)

// fastTier answers the gate's probes the way mongod would for one client, and
// counts them, on the other end of a pipe.
type fastTier struct {
	users      bson.A
	privileges bson.A
	authOff    bool
	probes     []string
}

func (f *fastTier) serve(t *testing.T, conn net.Conn) {
	for {
		msg, err := wire.Read(conn)
		if err != nil {
			return
		}
		name, _, _ := msg.CommandName()
		f.probes = append(f.probes, name)
		var out bson.D
		switch name {
		case "connectionStatus":
			out = bson.D{
				{Key: "authInfo", Value: bson.D{
					{Key: "authenticatedUsers", Value: f.users},
					{Key: "authenticatedUserRoles", Value: bson.A{}},
					{Key: "authenticatedUserPrivileges", Value: f.privileges},
				}},
				{Key: "ok", Value: 1.0},
			}
		case "listCollections":
			if f.authOff {
				out = bson.D{{Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)},
					{Key: "ns", Value: "admin.$cmd.listCollections"}, {Key: "firstBatch", Value: bson.A{}}}},
					{Key: "ok", Value: 1.0}}
			} else {
				out = bson.D{{Key: "ok", Value: 0.0}, {Key: "errmsg", Value: "command listCollections requires authentication"},
					{Key: "code", Value: int32(13)}, {Key: "codeName", Value: "Unauthorized"}}
			}
		default:
			t.Errorf("the gate sent %q to the fast tier", name)
			return
		}
		reply, err := wire.NewMsg(0, msg.Header.RequestID, out)
		if err != nil {
			t.Errorf("reply: %v", err)
			return
		}
		if err := wire.Write(conn, reply); err != nil {
			return
		}
	}
}

func gatedConn(t *testing.T, f *fastTier) *conn {
	t.Helper()
	mine, theirs := net.Pipe()
	t.Cleanup(func() { _ = mine.Close(); _ = theirs.Close() })
	go f.serve(t, theirs)
	r := &Router{cfg: config.RouterConfig{}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return &conn{r: r, upstream: mine, log: r.log}
}

func command(t *testing.T, doc bson.D) *wire.Message {
	t.Helper()
	m, err := wire.NewMsg(1, 0, doc)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func refusalText(t *testing.T, m *wire.Message) string {
	t.Helper()
	doc, err := m.ReplyDoc()
	if err != nil {
		t.Fatal(err)
	}
	s, _ := doc.Lookup("errmsg").StringValueOK()
	return s
}

var insertOrders = bson.D{{Key: "insert", Value: "orders"},
	{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: 1}}}}, {Key: "$db", Value: "shop"}}

func privilege(db, coll string, actions ...string) bson.D {
	acts := bson.A{}
	for _, a := range actions {
		acts = append(acts, a)
	}
	return bson.D{{Key: "resource", Value: bson.D{{Key: "db", Value: db}, {Key: "collection", Value: coll}}},
		{Key: "actions", Value: acts}}
}

var alice = bson.A{bson.D{{Key: "user", Value: "alice"}, {Key: "db", Value: "admin"}}}

// The shape of the findings: a client the fast tier would refuse reaches a
// guard that acts with the router's credentials. Here it is refused first.
func TestProxiedGateRefusesAWriteTheClientMayNotDo(t *testing.T) {
	f := &fastTier{users: alice, privileges: bson.A{privilege("shop", "", "find")}}
	c := gatedConn(t, f)
	out, denied := c.proxiedGate(command(t, insertOrders), "insert", "shop")
	if !denied {
		t.Fatal("an insert by a read-only user reached the guards")
	}
	if msg := refusalText(t, out); !strings.Contains(msg, "not authorized on shop") {
		t.Fatalf("refusal %q is not mongod's shape", msg)
	}
}

func TestProxiedGateLetsAnAllowedWriteThrough(t *testing.T) {
	f := &fastTier{users: alice, privileges: bson.A{privilege("shop", "orders", "insert")}}
	c := gatedConn(t, f)
	if _, denied := c.proxiedGate(command(t, insertOrders), "insert", "shop"); denied {
		t.Fatal("an insert the user is granted was refused")
	}
}

// An upsert needs insert as well as update, so update alone is not enough.
func TestProxiedGateJudgesAnUpsertAsAnInsertToo(t *testing.T) {
	upsert := bson.D{{Key: "update", Value: "orders"}, {Key: "updates", Value: bson.A{bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: 1}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "a", Value: 1}}}}},
		{Key: "upsert", Value: true}}}}, {Key: "$db", Value: "shop"}}
	f := &fastTier{users: alice, privileges: bson.A{privilege("shop", "orders", "update")}}
	if _, denied := gatedConn(t, f).proxiedGate(command(t, upsert), "update", "shop"); !denied {
		t.Fatal("an upsert by a user without insert reached the guards")
	}
}

func TestProxiedGateRefusesAnUnauthenticatedClient(t *testing.T) {
	f := &fastTier{}
	c := gatedConn(t, f)
	out, denied := c.proxiedGate(command(t, insertOrders), "insert", "shop")
	if !denied {
		t.Fatal("an unauthenticated insert reached the guards")
	}
	if msg := refusalText(t, out); !strings.Contains(msg, "requires authentication") {
		t.Fatalf("refusal %q is not mongod's shape", msg)
	}
	if c.r.authDisabled.Load() {
		t.Fatal("access control was recorded as off")
	}
}

// With access control off every connection has no users and no privileges,
// and refusing them would refuse every write a deployment without auth makes.
func TestProxiedGateWithAccessControlOff(t *testing.T) {
	f := &fastTier{authOff: true}
	c := gatedConn(t, f)
	if _, denied := c.proxiedGate(command(t, insertOrders), "insert", "shop"); denied {
		t.Fatal("an insert was refused on a fast tier with access control off")
	}
	if !c.r.authDisabled.Load() {
		t.Fatal("access control off was not remembered")
	}
	f.probes = nil
	if _, denied := c.proxiedGate(command(t, insertOrders), "insert", "shop"); denied {
		t.Fatal("the second insert was refused")
	}
	for _, p := range f.probes {
		if p == "listCollections" {
			t.Fatal("asked again whether access control is off, after it was seen off")
		}
	}
}

// A find reaches the fast tier before anything acts on it, so it pays nothing.
func TestProxiedGateDoesNotAskForReads(t *testing.T) {
	f := &fastTier{}
	c := gatedConn(t, f)
	find := bson.D{{Key: "find", Value: "orders"}, {Key: "$db", Value: "shop"}}
	if _, denied := c.proxiedGate(command(t, find), "find", "shop"); denied {
		t.Fatal("a find was refused by the write gate")
	}
	if len(f.probes) != 0 {
		t.Fatalf("a find cost probes %v", f.probes)
	}
}

func TestProxiedGateFailsClosedWithoutAnAnswer(t *testing.T) {
	mine, theirs := net.Pipe()
	_ = theirs.Close()
	r := &Router{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	c := &conn{r: r, upstream: mine, log: r.log}
	out, denied := c.proxiedGate(command(t, insertOrders), "insert", "shop")
	if !denied {
		t.Fatal("an insert reached the guards although the fast tier could not be asked")
	}
	if msg := refusalText(t, out); !strings.Contains(msg, "could not ask the fast tier") {
		t.Fatalf("refusal %q does not say why", msg)
	}
}

// filemd5 counts chunks on both tiers, so it is judged on the chunks
// collection, which is where mongod judges it.
func TestProxiedGateJudgesFilemd5OnTheChunks(t *testing.T) {
	cmd := bson.D{{Key: "filemd5", Value: 1}, {Key: "root", Value: "photos"}, {Key: "$db", Value: "shop"}}
	f := &fastTier{users: alice, privileges: bson.A{privilege("shop", "photos.files", "find")}}
	if _, denied := gatedConn(t, f).proxiedGate(command(t, cmd), "filemd5", "shop"); !denied {
		t.Fatal("filemd5 by a user who may not read the chunks reached the guard")
	}
	f = &fastTier{users: alice, privileges: bson.A{privilege("shop", "photos.chunks", "find")}}
	if _, denied := gatedConn(t, f).proxiedGate(command(t, cmd), "filemd5", "shop"); denied {
		t.Fatal("filemd5 by a user who may read the chunks was refused")
	}
}

// mayRun clears and refills a $out or $merge target with the router's own
// credentials. An explain that failed for a reason other than authorization
// was read as permission, so only an explain that succeeds may let it run.
func TestMayRunAllowsOnlyAnExplainThatSucceeds(t *testing.T) {
	cmd := bson.D{{Key: "aggregate", Value: "orders"},
		{Key: "pipeline", Value: bson.A{bson.D{{Key: "$out", Value: "copy"}}}},
		{Key: "cursor", Value: bson.D{}}, {Key: "$db", Value: "shop"}}
	for _, c := range []struct {
		name    string
		reply   bson.D
		allowed bool
	}{
		{"explain succeeded", bson.D{{Key: "ok", Value: 1.0}}, true},
		{"explain refused as unauthorized", bson.D{{Key: "ok", Value: 0.0}, {Key: "errmsg", Value: "not authorized"},
			{Key: "code", Value: int32(13)}, {Key: "codeName", Value: "Unauthorized"}}, false},
		{"explain failed for another reason", bson.D{{Key: "ok", Value: 0.0}, {Key: "errmsg", Value: "bad field"},
			{Key: "code", Value: int32(40415)}, {Key: "codeName", Value: "Location40415"}}, false},
	} {
		mine, theirs := net.Pipe()
		go func(reply bson.D) {
			msg, err := wire.Read(theirs)
			if err != nil {
				return
			}
			out, _ := wire.NewMsg(0, msg.Header.RequestID, reply)
			_ = wire.Write(theirs, out)
		}(c.reply)
		r := &Router{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
		conn := &conn{r: r, upstream: mine, log: r.log}
		m := command(t, cmd)
		raw, _ := m.CommandDoc()
		_, ok := conn.mayRun(m, raw, "shop")
		if ok != c.allowed {
			t.Errorf("%s: allowed = %v, want %v", c.name, ok, c.allowed)
		}
		_ = mine.Close()
		_ = theirs.Close()
	}

	mine, theirs := net.Pipe()
	_ = theirs.Close()
	r := &Router{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	conn := &conn{r: r, upstream: mine, log: r.log}
	m := command(t, cmd)
	raw, _ := m.CommandDoc()
	if _, ok := conn.mayRun(m, raw, "shop"); ok {
		t.Error("an aggregate ran although the fast tier could not be asked")
	}
}

// MONGODB-X509 takes the identity from the connection's certificate, and the
// connection to the fast tier carries the router's. Through the proxy it can
// only ever authenticate a client as the router.
func TestX509IsRecognisedWhereverItIsAsked(t *testing.T) {
	x509 := bson.D{{Key: "mechanism", Value: "MONGODB-X509"}}
	for _, c := range []struct {
		name string
		cmd  bson.D
		want bool
	}{
		{"authenticate", append(bson.D{{Key: "authenticate", Value: 1}}, x509...), true},
		{"saslStart", append(bson.D{{Key: "saslStart", Value: 1}}, x509...), true},
		{"hello", bson.D{{Key: "hello", Value: 1}, {Key: "speculativeAuthenticate",
			Value: append(bson.D{{Key: "authenticate", Value: 1}}, x509...)}}, true},
		{"isMaster", bson.D{{Key: "isMaster", Value: 1}, {Key: "speculativeAuthenticate",
			Value: bson.D{{Key: "authenticate", Value: 1}, {Key: "mechanism", Value: "mongodb-x509"}}}}, true},
		{"saslStart", bson.D{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: "SCRAM-SHA-256"}}, false},
		{"hello", bson.D{{Key: "hello", Value: 1}, {Key: "speculativeAuthenticate",
			Value: bson.D{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: "SCRAM-SHA-256"}}}}, false},
		{"hello", bson.D{{Key: "hello", Value: 1}}, false},
	} {
		if got := takesIdentityFromTransport(c.name, rawDoc(t, c.cmd)); got != c.want {
			t.Errorf("%s %v: %v, want %v", c.name, c.cmd, got, c.want)
		}
	}
}

// A driver sends an update's statements as a payload sequence, and an upsert
// among them needs insert as well as update, however it arrived.
func TestUpsertInASequenceNeedsInsert(t *testing.T) {
	msg, err := wire.NewMsgSequenced(1, 0, bson.D{{Key: "update", Value: "orders"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: 1}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "a", Value: 1}}}}},
			{Key: "upsert", Value: true}}}},
		{Key: "$db", Value: "shop"}}, "updates")
	if err != nil {
		t.Fatal(err)
	}
	f := &fastTier{users: alice, privileges: bson.A{privilege("shop", "orders", "update")}}
	if _, denied := gatedConn(t, f).proxiedGate(msg, "update", "shop"); !denied {
		t.Fatal("an upsert sent as a sequence passed with update alone")
	}
}
