package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zelmario/thermocline/internal/config"
)

var adoptNow = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

// docs builds n documents whose _id time and field f are both spread evenly
// over the last span; skew moves f away from the _id by that much.
func spreadDocs(n int, span time.Duration, oid bool, f string, skew time.Duration) []sampledDoc {
	out := make([]sampledDoc, n)
	for i := range out {
		t := adoptNow.Add(-(span / time.Duration(n)) * time.Duration(i))
		d := sampledDoc{Dates: map[string]time.Time{}, IDType: "objectID"}
		if oid {
			d.IDTime = t
		} else {
			d.IDType = "int64"
		}
		if f != "" {
			d.Dates[f] = t.Add(-skew)
			d.HasDate = true
		}
		out[i] = d
	}
	return out
}

func TestAdoptAgesByIDWhenItAgreesWithTheDate(t *testing.T) {
	p := planCollection(collSample{NS: "app.orders", Count: 1000, Docs: spreadDocs(100, 365*24*time.Hour, true, "createdAt", time.Hour)})
	if !p.Tier || p.DateField != "" {
		t.Fatalf("want tiered by _id, got tier=%v field=%q skip=%q", p.Tier, p.DateField, p.Skip)
	}
	if !strings.Contains(p.Why, "agrees with createdAt") {
		t.Fatalf("evidence missing: %q", p.Why)
	}
}

// A migration re-inserted everything: the ObjectIds were all minted within an
// hour, while the documents' own dates span a year. Ageing by _id evicts
// nothing (docs/adoption.md step 5).
func TestAdoptSeesAMigrationAndAgesByTheField(t *testing.T) {
	docs := spreadDocs(100, 365*24*time.Hour, true, "at", 0)
	for i := range docs {
		docs[i].IDTime = adoptNow.Add(-time.Hour + time.Duration(i)*time.Second)
	}
	p := planCollection(collSample{NS: "app.orders", Count: 1000, Docs: docs,
		Indexes: [][]indexKey{{{"at", -1}, {"_id", -1}}}})
	if !p.Tier || p.DateField != "at" {
		t.Fatalf("want tiered by at, got tier=%v field=%q why=%q", p.Tier, p.DateField, p.Why)
	}
	if p.NeedIndex {
		t.Fatalf("{at: -1, _id: -1} is there, but an index was asked for")
	}
	if got := keptFraction(p.Ages, adoptNow, 90*24*time.Hour); got < 0.2 || got > 0.3 {
		t.Fatalf("90 days of a year should keep about a quarter, got %.2f", got)
	}
}

func TestAdoptNonObjectIDNeedsADateField(t *testing.T) {
	p := planCollection(collSample{NS: "app.events", Count: 1000, Docs: spreadDocs(100, 30*24*time.Hour, false, "updatedAt", 0)})
	if !p.Tier || p.DateField != "updatedAt" || !p.NeedIndex {
		t.Fatalf("want tiered by updatedAt needing an index, got %+v", p)
	}

	p = planCollection(collSample{NS: "app.lookup", Count: 1000, Docs: spreadDocs(100, 30*24*time.Hour, false, "", 0)})
	if p.Tier || !p.CopyWhole {
		t.Fatalf("an integer _id with no date cannot be tiered, got %+v", p)
	}
}

func TestAdoptPrefersTheCreationField(t *testing.T) {
	docs := spreadDocs(100, 30*24*time.Hour, false, "updatedAt", 0)
	for i := range docs {
		docs[i].Dates["createdAt"] = docs[i].Dates["updatedAt"].Add(-time.Hour)
	}
	p := planCollection(collSample{NS: "app.events", Count: 1000, Docs: docs})
	if p.DateField != "createdAt" {
		t.Fatalf("want createdAt over updatedAt, got %q", p.DateField)
	}
}

// A field present in only some documents would leave the rest unaged.
func TestAdoptIgnoresASparseDateField(t *testing.T) {
	docs := spreadDocs(100, 30*24*time.Hour, false, "", 0)
	for i := 0; i < 50; i++ {
		docs[i].Dates["shippedAt"] = adoptNow
	}
	p := planCollection(collSample{NS: "app.orders", Count: 1000, Docs: docs})
	if p.Tier {
		t.Fatalf("a date in half the documents is not an age, got %+v", p)
	}
}

func TestAdoptSkipsWhatCannotBeMirrored(t *testing.T) {
	if p := planCollection(collSample{NS: "app.log", Unmirrored: "a capped collection", Count: 5}); p.Tier || !p.CopyWhole {
		t.Fatalf("capped: %+v", p)
	}
	if p := planCollection(collSample{NS: "app.v", View: true}); p.Tier || p.CopyWhole || !p.View {
		t.Fatalf("view: %+v", p)
	}
	if p := planCollection(collSample{NS: "app.empty"}); p.Tier || !p.CopyWhole {
		t.Fatalf("empty: %+v", p)
	}
}

func TestAdoptChoosesTheLongestWindowThatFits(t *testing.T) {
	// 100 GB spread evenly over 365 days: 90 days is about 25 GB.
	p := planCollection(collSample{NS: "app.orders", Count: 1000, Storage: 100 << 30,
		Docs: spreadDocs(365, 365*24*time.Hour, true, "", 0)})
	w, ok := chooseWindow([]collPlan{p}, adoptNow, 60<<30) // budget 30 GB
	if !ok || w != 90*24*time.Hour {
		t.Fatalf("want 90d, got %s ok=%v", config.Duration(w), ok)
	}
	w, ok = chooseWindow([]collPlan{p}, adoptNow, 1<<30)
	if ok || w != 7*24*time.Hour {
		t.Fatalf("want the shortest, not fitting; got %s ok=%v", config.Duration(w), ok)
	}
}

func TestAdoptProxyURI(t *testing.T) {
	cases := []struct{ in, want string }{
		{"mongodb://u:secret@a:27017,b:27017/?replicaSet=rs0&authSource=admin", "mongodb://u:<password>@proxy:27017/?authSource=admin"},
		{"mongodb://a,b,c/app?replicaSet=rs0", "mongodb://proxy:27017/app"},
		{"mongodb://a", "mongodb://proxy:27017/"},
		{"mongodb://u@a/?readPreference=secondary&w=majority", "mongodb://u@proxy:27017/?w=majority"},
	}
	for _, c := range cases {
		if got, _ := proxyURI(c.in, "proxy:27017"); got != c.want {
			t.Errorf("proxyURI(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	if _, notes := proxyURI("mongodb+srv://u:p@cluster.example.com/", "proxy:27017"); len(notes) == 0 {
		t.Errorf("an SRV URI should say what it implied")
	}
}

func TestAdoptParseSize(t *testing.T) {
	for in, want := range map[string]int64{"100GB": 100 << 30, "2T": 2 << 40, "512MiB": 512 << 20, "1.5g": 3 << 29} {
		if got, err := parseSize(in); err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "GB", "-1G", "lots"} {
		if _, err := parseSize(in); err == nil {
			t.Errorf("parseSize(%q) should fail", in)
		}
	}
}

// The file adopt writes has to load, with each collection where the long form
// puts it, and names that need quoting quoted.
func TestAdoptYAMLLoads(t *testing.T) {
	plans := []collPlan{
		planCollection(collSample{NS: "app.orders", Count: 10, Docs: spreadDocs(100, 365*24*time.Hour, true, "", 0)}),
		planCollection(collSample{NS: "app.ev: #1", Count: 10, Docs: spreadDocs(100, 365*24*time.Hour, false, "ts", 0)}),
		planCollection(collSample{NS: "app.lookup"}),
	}
	text := adoptYAML(plans, "mongodb://h1/?replicaSet=rsHot", "mongodb://c1/?replicaSet=rsCold",
		"0.0.0.0:27017", 30*24*time.Hour, adoptNow, 100, "--keep")
	path := filepath.Join(t.TempDir(), "cline.yaml")
	if err := writeChecked(path, text); err != nil {
		t.Fatalf("%v\n%s", err, text)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Feeder.Namespaces) != 2 {
		t.Fatalf("want two tiered namespaces, got %v\n%s", cfg.Feeder.Namespaces, text)
	}
	for _, r := range cfg.Evictor.Rules {
		if r.NS == "app.ev: #1" && r.DateField != "ts" {
			t.Fatalf("date_field lost: %+v", r)
		}
		if r.OlderThan.D() != 30*24*time.Hour {
			t.Fatalf("window lost: %+v", r)
		}
	}
	if st, _ := os.Stat(path); st.Mode().Perm() != 0o600 {
		t.Fatalf("the file holds credentials and is %v", st.Mode().Perm())
	}
}

// A collection too small to be worth tiering leaves the list, whatever ages it.
func TestAdoptKeepsSmallCollectionsWhole(t *testing.T) {
	plans := []collPlan{
		planCollection(collSample{NS: "app.orders", Count: 10, Storage: 50 << 30, Docs: spreadDocs(100, 365*24*time.Hour, true, "", 0)}),
		planCollection(collSample{NS: "app.customers", Count: 10, Storage: 300 << 20, Docs: spreadDocs(100, 365*24*time.Hour, false, "since", 0)}),
	}
	keepWhole(plans, 1<<30)
	if !plans[0].Tier {
		t.Fatalf("50 GB should stay tiered: %+v", plans[0])
	}
	if plans[1].Tier || !plans[1].CopyWhole || !strings.Contains(plans[1].Skip, "by since") {
		t.Fatalf("300 MB should be kept whole, naming its clock: %+v", plans[1])
	}
	keepWhole(plans, 0)
	if !plans[0].Tier {
		t.Fatalf("0 keeps everything tiered")
	}
}

// A counted window wins over the sample, and caps at the whole collection.
func TestAdoptCountedWindowWinsOverTheSample(t *testing.T) {
	p := planCollection(collSample{NS: "app.orders", Count: 1000, Storage: 1 << 30, Docs: spreadDocs(100, 365*24*time.Hour, true, "", 0)})
	w := 30 * 24 * time.Hour
	if f, exact := keptOf(p, adoptNow, w); exact || f < 0.05 || f > 0.12 {
		t.Fatalf("sampled: %.2f exact=%v", f, exact)
	}
	p.Counted = map[time.Duration]int64{w: 250, 2 * w: 5000}
	if f, exact := keptOf(p, adoptNow, w); !exact || f != 0.25 {
		t.Fatalf("counted: %.2f exact=%v", f, exact)
	}
	if f, _ := keptOf(p, adoptNow, 2*w); f != 1 {
		t.Fatalf("a count past the collection's own is the whole of it, got %.2f", f)
	}
}
