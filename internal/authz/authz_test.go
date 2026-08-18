package authz

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// decode builds a Privilege the way one arrives from usersInfo, through BSON,
// because the whole question is what the keys were rather than what a struct
// literal says they are.
func decode(t *testing.T, resource bson.D, actions ...string) Privilege {
	t.Helper()
	raw, err := bson.Marshal(bson.D{
		{Key: "resource", Value: resource},
		{Key: "actions", Value: actions},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var p Privilege
	if err := bson.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return p
}

// The escalation this was written for. A real cluster returns a system_buckets
// resource among a normal user's inherited privileges, and decoded into a plain
// struct it is indistinguishable from {db:"", collection:""}, which means every
// collection in every database.
func TestSystemBucketsIsNotAWildcard(t *testing.T) {
	g := Grants{Privileges: []Privilege{
		decode(t, bson.D{{Key: "db", Value: "shop"}, {Key: "system_buckets", Value: "metrics"}}, "find", "insert"),
	}}
	if g.Allowed("payroll", "salaries", "find") {
		t.Fatal("a system_buckets privilege granted find on an unrelated collection")
	}
	if g.Allowed("shop", "orders", "find") {
		t.Fatal("a system_buckets privilege granted find on an ordinary collection")
	}
}

// The same protection for anything a later release invents.
func TestUnknownResourceKindCoversNothing(t *testing.T) {
	g := Grants{Privileges: []Privilege{
		decode(t, bson.D{{Key: "somethingNew", Value: "x"}}, "find"),
	}}
	if g.Allowed("shop", "orders", "find") {
		t.Fatal("an unrecognised resource kind was treated as a wildcard")
	}
}

func TestNamespaceMatching(t *testing.T) {
	exact := decode(t, bson.D{{Key: "db", Value: "shop"}, {Key: "collection", Value: "orders"}}, "find")
	wholeDB := decode(t, bson.D{{Key: "db", Value: "shop"}, {Key: "collection", Value: ""}}, "find")
	namedAnyDB := decode(t, bson.D{{Key: "db", Value: ""}, {Key: "collection", Value: "orders"}}, "find")
	everything := decode(t, bson.D{{Key: "db", Value: ""}, {Key: "collection", Value: ""}}, "find")

	cases := []struct {
		name string
		priv Privilege
		db   string
		coll string
		want bool
	}{
		{"exact matches itself", exact, "shop", "orders", true},
		{"exact does not match another collection", exact, "shop", "invoices", false},
		{"exact does not match another database", exact, "payroll", "orders", false},
		{"whole database matches any collection in it", wholeDB, "shop", "invoices", true},
		{"whole database does not reach another database", wholeDB, "payroll", "invoices", false},
		{"named in any database matches", namedAnyDB, "payroll", "orders", true},
		{"named in any database does not match another name", namedAnyDB, "payroll", "salaries", false},
		{"everything matches everything", everything, "payroll", "salaries", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := Grants{Privileges: []Privilege{c.priv}}
			if got := g.Allowed(c.db, c.coll, "find"); got != c.want {
				t.Fatalf("Allowed(%q,%q) = %v, want %v", c.db, c.coll, got, c.want)
			}
		})
	}
}

// A privilege on one collection must not satisfy a question about the database
// as a whole, which is what listCollections asks.
func TestCollectionPrivilegeDoesNotCoverTheDatabase(t *testing.T) {
	g := Grants{Privileges: []Privilege{
		decode(t, bson.D{{Key: "db", Value: "shop"}, {Key: "collection", Value: "orders"}}, "listCollections"),
	}}
	if g.Allowed("shop", "", "listCollections") {
		t.Fatal("a single collection privilege answered a database-wide question")
	}
}

// A cluster privilege is not a namespace privilege, and the other way round.
func TestClusterAndNamespaceDoNotSubstitute(t *testing.T) {
	cluster := Grants{Privileges: []Privilege{
		decode(t, bson.D{{Key: "cluster", Value: true}}, "listDatabases", "find"),
	}}
	if cluster.Allowed("shop", "orders", "find") {
		t.Fatal("a cluster privilege granted find on a collection")
	}
	if !cluster.AllowedCluster("listDatabases") {
		t.Fatal("a cluster privilege did not grant a cluster action")
	}

	ns := Grants{Privileges: []Privilege{
		decode(t, bson.D{{Key: "db", Value: ""}, {Key: "collection", Value: ""}}, "listDatabases"),
	}}
	if ns.AllowedCluster("listDatabases") {
		t.Fatal("a namespace privilege granted a cluster action")
	}
}

// anyResource is the one that really does mean everything.
func TestAnyResource(t *testing.T) {
	g := Grants{Privileges: []Privilege{
		decode(t, bson.D{{Key: "anyResource", Value: true}}, "find", "listDatabases"),
	}}
	if !g.Allowed("anything", "at-all", "find") {
		t.Fatal("anyResource did not cover a namespace")
	}
	if !g.AllowedCluster("listDatabases") {
		t.Fatal("anyResource did not cover a cluster action")
	}
}

// An action the privilege does not list is refused even on a resource it covers.
func TestActionMustMatch(t *testing.T) {
	g := Grants{Privileges: []Privilege{
		decode(t, bson.D{{Key: "db", Value: "shop"}, {Key: "collection", Value: ""}}, "find"),
	}}
	if g.Allowed("shop", "orders", "insert") {
		t.Fatal("a read privilege allowed a write")
	}
}

// Nothing granted means nothing allowed, which is the posture the whole design
// rests on.
func TestEmptyGrantsAllowNothing(t *testing.T) {
	var g Grants
	if g.Allowed("shop", "orders", "find") || g.AllowedCluster("listDatabases") {
		t.Fatal("an empty grant allowed something")
	}
}

// An explain carries the privileges of the command it wraps. Judged by name it
// would be refused, which turns every driver's explain into an error; judged as
// harmless it would hand a user the shape of a collection they may not read.
func TestExplainNeedsWhatItExplains(t *testing.T) {
	raw := func(d bson.D) bson.Raw {
		b, err := bson.Marshal(d)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return b
	}

	cmd := raw(bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: "salaries"},
			{Key: "filter", Value: bson.D{}},
		}},
		{Key: "verbosity", Value: "queryPlanner"},
	})
	needs, err := Needs("explain", "payroll", cmd)
	if err != nil {
		t.Fatalf("explain of a find: %v", err)
	}
	if len(needs) != 1 || needs[0].DB != "payroll" || needs[0].Coll != "salaries" ||
		len(needs[0].Actions) != 1 || needs[0].Actions[0] != "find" {
		t.Fatalf("needs = %+v", needs)
	}

	// A write explained is still a write as far as authorization goes.
	del := raw(bson.D{{Key: "explain", Value: bson.D{
		{Key: "delete", Value: "salaries"},
		{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{}}, {Key: "limit", Value: 1}}}},
	}}})
	needs, err = Needs("explain", "payroll", del)
	if err != nil {
		t.Fatalf("explain of a delete: %v", err)
	}
	if len(needs) != 1 || needs[0].Actions[0] != "remove" {
		t.Fatalf("needs = %+v", needs)
	}

	// The inner command may name its own database, and that is the one the
	// privilege is about.
	other := raw(bson.D{{Key: "explain", Value: bson.D{
		{Key: "find", Value: "salaries"},
		{Key: "$db", Value: "payroll"},
	}}})
	needs, err = Needs("explain", "shop", other)
	if err != nil {
		t.Fatalf("explain naming its own database: %v", err)
	}
	if needs[0].DB != "payroll" {
		t.Fatalf("db = %q, want payroll", needs[0].DB)
	}

	// And an inner command this package cannot judge is still refused.
	unknown := raw(bson.D{{Key: "explain", Value: bson.D{{Key: "thisIsNotACommand", Value: 1}}}})
	if _, err := Needs("explain", "shop", unknown); err == nil {
		t.Fatal("an explain of an unknown command was allowed")
	}
}

// An aggregation whose first stage produces its own input never reads the
// collection it was sent to, and mongod does not ask for find on it. Asking
// anyway refuses an ordinary db.coll.stats() from a user who holds collStats
// and nothing else.
func TestInitialSourcesDoNotNeedFind(t *testing.T) {
	raw := func(d bson.D) bson.Raw {
		b, err := bson.Marshal(d)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return b
	}
	agg := func(stages bson.A) bson.Raw {
		return raw(bson.D{{Key: "aggregate", Value: "orders"}, {Key: "pipeline", Value: stages}})
	}

	for _, stage := range []string{"$collStats", "$indexStats", "$planCacheStats", "$documents", "$changeStream"} {
		needs, err := Needs("aggregate", "shop", agg(bson.A{bson.D{{Key: stage, Value: bson.D{}}}}))
		if err != nil {
			t.Fatalf("%s: %v", stage, err)
		}
		if len(needs) != 0 {
			t.Errorf("%s asked for %+v, want nothing from the base check", stage, needs)
		}
	}

	// Everything else reads the collection, including an empty pipeline.
	for _, stages := range []bson.A{{}, {bson.D{{Key: "$match", Value: bson.D{}}}}} {
		needs, err := Needs("aggregate", "shop", agg(stages))
		if err != nil {
			t.Fatalf("reading pipeline: %v", err)
		}
		if len(needs) != 1 || needs[0].Actions[0] != "find" {
			t.Errorf("needs = %+v, want find", needs)
		}
	}
}
