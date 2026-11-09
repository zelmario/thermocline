package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// The decisions cline adopt makes, kept apart from the reading so they can be
// tested on samples written by hand.
//
// docs/adoption.md asks three questions of every collection, and an operator
// meeting this system for the first time cannot answer them from the
// application's code alone:
//
//   - Can it be tiered at all? A view, a capped collection, a time series and a
//     clustered collection with a TTL cannot (the feeder's own list).
//   - What clock ages it? A rule with no date_field reads the timestamp inside
//     an ObjectId _id, which is right for a collection that accumulated over
//     time, wrong for one a migration re-inserted, and nothing at all for an
//     integer, string or UUID _id (step 5).
//   - How far back does the fast tier keep? One window for the whole
//     deployment, chosen against the size of the fast tier.
//
// The data answers the first two better than a person reading code does, so a
// sample of each collection is read and the answer is written into the file
// with the evidence beside it.

// sampledDoc is what one sampled document contributes: the timestamp inside
// its _id if that is an ObjectId, and every top level field holding a date.
type sampledDoc struct {
	IDTime  time.Time // zero when the _id is not an ObjectId
	IDType  string    // the BSON type of the _id, for the report
	Dates   map[string]time.Time
	HasDate bool
}

// collSample is everything the planner is told about one collection.
type collSample struct {
	NS         string
	Unmirrored string // non-empty: the kind that cannot be tiered, and why
	View       bool
	Count      int64
	DataSize   int64 // uncompressed
	Storage    int64 // on disk, data only
	Docs       []sampledDoc
	// Indexes on the archive, each as its ordered key fields with directions,
	// for telling whether a date_field rule has the index it needs.
	Indexes [][]indexKey
}

type indexKey struct {
	Field string
	Dir   int
}

// agreeWithin is how close an ObjectId's timestamp has to be to a date field
// for the two to be the same clock. A day absorbs an application that sets the
// field from its own clock or a little after the insert.
const agreeWithin = 24 * time.Hour

// collPlan is the decision for one collection.
type collPlan struct {
	NS        string
	Tier      bool
	DateField string // "" ages by _id
	// Why is the evidence for the age source, written into the file.
	Why string
	// Warn is something the operator should check before trusting the rule.
	Warn string
	// Skip is why a collection is not tiered, and what to do about it instead.
	Skip string
	// CopyWhole: not tiered, but it holds documents the application reads,
	// so it has to reach the fast tier some other way at the cutover.
	CopyWhole bool
	View      bool
	// Ages are the sampled documents' ages by the chosen clock, sorted
	// oldest first, for estimating what a window keeps.
	Ages      []time.Time
	Count     int64
	DataSize  int64
	Storage   int64
	NeedIndex bool // a date_field rule whose archive lacks {field: -1, _id: -1}
	// Indexed: an index on the archive leads with the age (the _id, or the
	// date field), so what a window keeps can be counted rather than sampled.
	Indexed bool
	// Counted is the number of documents each window keeps, counted on the
	// archive. A window missing here is estimated from the sample.
	Counted map[time.Duration]int64
}

// planCollection decides one collection.
func planCollection(s collSample) collPlan {
	p := collPlan{NS: s.NS, Count: s.Count, DataSize: s.DataSize, Storage: s.Storage, View: s.View}
	switch {
	case s.View:
		p.Skip = "a view holds no documents; recreate it on the fast tier and tier what it reads"
		return p
	case s.Unmirrored != "":
		p.Skip = s.Unmirrored + ", which the feeder cannot mirror; copy it whole to the fast tier"
		p.CopyWhole = true
		return p
	case s.Count == 0 || len(s.Docs) == 0:
		p.Skip = "empty, so there is no age to read; copy it (its indexes) to the fast tier"
		p.CopyWhole = true
		return p
	}

	n := len(s.Docs)
	oids := 0
	types := map[string]int{}
	for _, d := range s.Docs {
		if !d.IDTime.IsZero() {
			oids++
		}
		types[d.IDType]++
	}
	allOID := oids == n

	// Candidate date fields: present as a date in at least 90% of the sample.
	// A field that is often missing ages the rest of the collection by
	// nothing, and the rule would never address them.
	coverage := map[string]int{}
	for _, d := range s.Docs {
		for f := range d.Dates {
			coverage[f]++
		}
	}
	var cands []string
	for f, c := range coverage {
		if c*10 >= n*9 {
			cands = append(cands, f)
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		ri, rj := dateFieldRank(cands[i]), dateFieldRank(cands[j])
		if ri != rj {
			return ri < rj
		}
		return cands[i] < cands[j]
	})

	if !allOID {
		if len(cands) == 0 {
			p.Skip = fmt.Sprintf("its _id is %s, which carries no time, and no top level field holds a "+
				"date in 90%% of %d sampled documents; copy it whole to the fast tier, or add a date "+
				"field and tier it by that", describeTypes(types, n), n)
			p.CopyWhole = true
			return p
		}
		p.Tier, p.DateField = true, cands[0]
		p.Indexed = leadsWith(s.Indexes, cands[0])
		p.Why = fmt.Sprintf("_id is %s, so it ages by %s (a date in %d%% of %d sampled documents)",
			describeTypes(types, n), cands[0], coverage[cands[0]]*100/n, n)
		p.Ages = agesBy(s.Docs, cands[0])
		p.NeedIndex = !hasAgeIndex(s.Indexes, cands[0])
		return p
	}

	idAges := agesBy(s.Docs, "")
	idSpread := spread(idAges)
	p.Indexed = true // every collection has its _id index

	// Every _id is an ObjectId. The question left is whether its timestamp is
	// the clock the application means.
	for _, f := range cands {
		if agreement(s.Docs, f) >= 0.8 {
			p.Tier = true
			p.Why = fmt.Sprintf("_id is an ObjectId whose time agrees with %s within a day in %d%% of "+
				"%d sampled documents, so it ages by _id and needs no index",
				f, int(agreement(s.Docs, f)*100), n)
			p.Ages = idAges
			return p
		}
	}
	if len(cands) > 0 {
		f := cands[0]
		fieldAges := agesBy(s.Docs, f)
		// The _id timestamps are bunched while the field spreads out: the
		// documents were re-inserted (a migration, a reload), so every _id
		// carries the load date. Measured in docs/adoption.md step 5: a rule
		// ageing those by _id evicted nothing.
		if fs := spread(fieldAges); fs > 0 && idSpread*10 < fs {
			p.Tier, p.DateField = true, f
			p.Indexed = leadsWith(s.Indexes, f)
			p.Why = fmt.Sprintf("_id is an ObjectId, but its times span %s where %s spans %s: the "+
				"documents look re-inserted (a migration or a reload), so it ages by %s",
				roundSpan(idSpread), f, roundSpan(fs), f)
			p.Ages = fieldAges
			p.NeedIndex = !hasAgeIndex(s.Indexes, f)
			return p
		}
		p.Tier = true
		p.Why = "_id is an ObjectId, aged by its creation time"
		p.Warn = fmt.Sprintf("%s disagrees with the _id's time in %d%% of %d sampled documents; if "+
			"%s is when a document stops being read, set date_field: %s", f,
			100-int(agreement(s.Docs, f)*100), n, f, f)
		p.Ages = idAges
		return p
	}
	p.Tier = true
	p.Why = "_id is an ObjectId and no field holds a date, so it ages by the _id's creation time"
	if idSpread < agreeWithin && s.Count >= 10000 {
		p.Warn = fmt.Sprintf("every sampled _id was created within %s; if this collection was loaded "+
			"in one go, its _ids carry the load date and nothing will age out until that date does",
			roundSpan(idSpread))
	}
	p.Ages = idAges
	return p
}

// leadsWith reports whether some index starts with field f, which is what a
// count of {f: {$gte: cutoff}} needs to avoid reading the collection.
func leadsWith(indexes [][]indexKey, f string) bool {
	for _, ix := range indexes {
		if len(ix) > 0 && ix[0].Field == f {
			return true
		}
	}
	return false
}

// keepWhole takes the tiered collections smaller than minTier off the list.
//
// Tiering a collection is worth what it moves off the fast tier, and it costs
// an archive read for every document past the window. Measured on a staging
// archive of 60 million orders: customers, 325 MB on disk and aged by their
// sign-up date, would have freed 240 MB of a 16 GB fast tier and sent every
// long-standing customer's lookup to the archive; a one-document bookkeeping
// collection was tiered on a sample of one. Kept whole, they cost a copy at the
// cutover and nothing after it.
func keepWhole(plans []collPlan, minTier int64) {
	for i := range plans {
		p := &plans[i]
		if !p.Tier || p.Storage >= minTier {
			continue
		}
		by := "_id"
		if p.DateField != "" {
			by = p.DateField
		}
		p.Tier, p.CopyWhole, p.Warn = false, true, ""
		p.Skip = fmt.Sprintf("%s on disk, under --min-tier %s: tiering it (by %s) would free at most "+
			"that much of the fast tier and make every older document an archive read, so keep "+
			"it whole on the fast tier", humanBytes(p.Storage), humanBytes(minTier), by)
	}
}

// keptOf is the share of a collection the fast tier keeps with window: counted
// on the archive when it could be, otherwise read from the sample.
//
// The sample alone was wrong where it matters. $sample on a large collection
// walks a WiredTiger random cursor, and measured on 60 million orders it
// under-represents the newest documents: three samples of 1000 held 18, 7 and
// 11 orders from the last 30 days where 27 were due, so the fast tier's size
// came out at 0.83 GB for 30 days against 1.43 GB counted.
func keptOf(p collPlan, now time.Time, window time.Duration) (float64, bool) {
	if c, ok := p.Counted[window]; ok && p.Count > 0 {
		f := float64(c) / float64(p.Count)
		if f > 1 {
			f = 1
		}
		return f, true
	}
	return keptFraction(p.Ages, now, window), false
}

// dateFieldRank orders candidate fields by how likely each is to be when a
// document was created. Lower is better.
func dateFieldRank(f string) int {
	l := strings.ToLower(f)
	for _, bad := range []string{"updat", "modif", "expir", "due", "delet", "last", "next", "end", "until"} {
		if strings.Contains(l, bad) {
			return 3
		}
	}
	if strings.Contains(l, "creat") || strings.Contains(l, "insert") {
		return 0
	}
	switch l {
	case "date", "ts", "timestamp", "time", "at", "when", "placed", "placedat", "orderdate":
		return 1
	}
	return 2
}

// agreement is the fraction of documents with field f whose ObjectId time is
// within agreeWithin of it.
func agreement(docs []sampledDoc, f string) float64 {
	both, agree := 0, 0
	for _, d := range docs {
		t, ok := d.Dates[f]
		if !ok || d.IDTime.IsZero() {
			continue
		}
		both++
		diff := t.Sub(d.IDTime)
		if diff < 0 {
			diff = -diff
		}
		if diff <= agreeWithin {
			agree++
		}
	}
	if both == 0 {
		return 0
	}
	return float64(agree) / float64(both)
}

// agesBy returns the documents' times by field f, or by the _id when f is
// empty, sorted oldest first. A document without one is left out.
func agesBy(docs []sampledDoc, f string) []time.Time {
	var out []time.Time
	for _, d := range docs {
		var t time.Time
		if f == "" {
			t = d.IDTime
		} else {
			t = d.Dates[f]
		}
		if !t.IsZero() {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

// spread is the 10th to 90th percentile range, so a few stray documents do
// not make a migrated collection look accumulated.
func spread(ages []time.Time) time.Duration {
	if len(ages) < 2 {
		return 0
	}
	lo := ages[len(ages)/10]
	hi := ages[len(ages)*9/10]
	if len(ages)*9/10 >= len(ages) {
		hi = ages[len(ages)-1]
	}
	return hi.Sub(lo)
}

// keptFraction is the share of a collection the fast tier keeps with window,
// read from the sample.
func keptFraction(ages []time.Time, now time.Time, window time.Duration) float64 {
	if len(ages) == 0 {
		return 1
	}
	cut := now.Add(-window)
	i := sort.Search(len(ages), func(i int) bool { return !ages[i].Before(cut) })
	return float64(len(ages)-i) / float64(len(ages))
}

// hasAgeIndex reports whether an index serves a date_field rule: it leads
// with the field and follows with _id, in matching directions, which is what
// lets the warmer read newest first without a sort (docs/adoption.md step 16).
func hasAgeIndex(indexes [][]indexKey, f string) bool {
	for _, ix := range indexes {
		if len(ix) >= 2 && ix[0].Field == f && ix[1].Field == "_id" && ix[0].Dir == ix[1].Dir && ix[0].Dir != 0 {
			return true
		}
	}
	return false
}

func describeTypes(types map[string]int, n int) string {
	if len(types) == 1 {
		for t := range types {
			return article(t)
		}
	}
	var parts []string
	for t, c := range types {
		parts = append(parts, fmt.Sprintf("%s (%d%%)", t, c*100/n))
	}
	sort.Strings(parts)
	return "mixed: " + strings.Join(parts, ", ")
}

func article(t string) string {
	if t == "" {
		return "unknown"
	}
	switch t[0] {
	case 'a', 'e', 'i', 'o', 'u':
		return "an " + t
	}
	return "a " + t
}

func roundSpan(d time.Duration) string {
	switch {
	case d >= 48*time.Hour:
		return fmt.Sprintf("%d days", int(d.Hours()/24))
	case d >= 2*time.Hour:
		return fmt.Sprintf("%d hours", int(d.Hours()))
	case d >= 2*time.Minute:
		return fmt.Sprintf("%d minutes", int(d.Minutes()))
	case d < time.Minute:
		return "under a minute"
	}
	return d.Round(time.Second).String()
}

// candidateWindows are the windows the report prices, and the ones --fast-size
// chooses among.
var candidateWindows = []time.Duration{
	7 * 24 * time.Hour, 30 * 24 * time.Hour, 90 * 24 * time.Hour,
	180 * 24 * time.Hour, 365 * 24 * time.Hour,
}

// fastTierBytes is the on-disk size the tiered collections keep with window.
func fastTierBytes(plans []collPlan, now time.Time, window time.Duration) int64 {
	var total float64
	for _, p := range plans {
		if !p.Tier {
			continue
		}
		f, _ := keptOf(p, now, window)
		total += float64(p.Storage) * f
	}
	return int64(total)
}

// copiedBytes is what the collections copied whole put on the fast tier.
func copiedBytes(plans []collPlan) int64 {
	var total int64
	for _, p := range plans {
		if p.CopyWhole {
			total += p.Storage
		}
	}
	return total
}

// fastSizeShare is the part of the fast tier's disk the documents may take.
// The rest is the oplog a new set has to grow, the key registry (one claim per
// unique value, docs/adoption.md step 11), the indexes and the warmer's
// min_free_disk.
const fastSizeShare = 0.5

// chooseWindow is the longest candidate window whose documents fit in half the
// fast tier's disk, or the shortest one when none does.
func chooseWindow(plans []collPlan, now time.Time, fastSize int64) (time.Duration, bool) {
	budget := int64(float64(fastSize) * fastSizeShare)
	best := time.Duration(0)
	for _, w := range candidateWindows {
		if fastTierBytes(plans, now, w)+copiedBytes(plans) <= budget {
			best = w
		}
	}
	if best == 0 {
		return candidateWindows[0], false
	}
	return best, true
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// parseSize reads "100GB", "512MiB", "2T" and plain bytes, in powers of 1024:
// disks and the sizes MongoDB reports are both read that way here.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	s = strings.TrimSuffix(strings.TrimSuffix(s, "IB"), "B")
	mult := int64(1)
	if s != "" {
		switch s[len(s)-1] {
		case 'K':
			mult = 1 << 10
		case 'M':
			mult = 1 << 20
		case 'G':
			mult = 1 << 30
		case 'T':
			mult = 1 << 40
		}
		if mult > 1 {
			s = s[:len(s)-1]
		}
	}
	var v float64
	if _, err := fmt.Sscanf(s, "%g", &v); err != nil || v <= 0 {
		return 0, fmt.Errorf("a size such as 100GB or 2TB, not %q", s)
	}
	return int64(v * float64(mult)), nil
}

// proxyURI is the connection string the application should use after the
// cutover: the archive's, with the proxy's address for its hosts, no
// replicaSet (the proxy answers hello as a mongos, docs/adoption.md step 2),
// and the password replaced, since this is printed.
func proxyURI(coldURI, proxyAddr string) (string, []string) {
	var notes []string
	scheme, rest, ok := strings.Cut(coldURI, "://")
	if !ok {
		return "mongodb://" + proxyAddr + "/", nil
	}
	if scheme == "mongodb+srv" {
		notes = append(notes, "the archive's URI uses an SRV record, which implies tls=true and may "+
			"carry authSource and replicaSet in DNS; the proxy is dialled by plain host, so add "+
			"tls=true and authSource here if the application relied on them")
	}
	authority, tail := rest, ""
	if i := strings.IndexAny(rest, "/?"); i >= 0 {
		authority, tail = rest[:i], rest[i:]
	}
	user := ""
	if at := strings.LastIndex(authority, "@"); at >= 0 {
		user = authority[:at]
		if u, _, hasPw := strings.Cut(user, ":"); hasPw {
			user = u + ":<password>"
		}
		user += "@"
	}
	path, query := tail, ""
	if i := strings.IndexByte(tail, '?'); i >= 0 {
		path, query = tail[:i], tail[i+1:]
	}
	if path == "" {
		path = "/"
	}
	var kept []string
	for _, kv := range strings.FieldsFunc(query, func(r rune) bool { return r == '&' || r == ';' }) {
		k, _, _ := strings.Cut(kv, "=")
		if strings.EqualFold(k, "replicaSet") {
			continue
		}
		if strings.EqualFold(k, "directConnection") || strings.EqualFold(k, "readPreference") {
			notes = append(notes, "dropped "+k+": the proxy is one address that routes reads itself")
			continue
		}
		kept = append(kept, kv)
	}
	out := "mongodb://" + user + proxyAddr + path
	if len(kept) > 0 {
		out += "?" + strings.Join(kept, "&")
	}
	return out, notes
}
