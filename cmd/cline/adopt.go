package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/feeder"
	"github.com/zelmario/thermocline/internal/tierver"
)

// cline adopt turns docs/adoption.md from a checklist into a command.
//
// The mechanics of a deployment were already one binary, a short file and
// cline run. What a stranger could not do was write the file: which
// collections to tier, which clock ages each one, how long the fast tier keeps
// them, and the half dozen things the checklist says go wrong quietly. Those
// are questions about the data, and the archive holds the data.
//
// So this reads the cluster being adopted, never writes to it, decides each
// collection from a sample with the evidence written beside the decision,
// writes the short form, and runs preflight against what it wrote. What it
// cannot decide, the accounts' passwords and the moment of the cutover, it
// prints as the next steps, with the commands.
const adoptUsage = `Usage: cline adopt --hot URI --cold URI [--fast-size SIZE | --keep AGE] [--out FILE]

Read the cluster you are adopting and write the cline.yaml that tiers it.

The cold URI is your existing replica set, which becomes the archive as it is.
The hot URI is the new, empty replica set that becomes the fast tier. Neither
is written to: every collection is sampled, and for each one this decides
whether it can be tiered, what ages its documents (the ObjectId in _id, or a
date field when _id is not an ObjectId or the documents were re-inserted), and
what the fast tier would hold. The decision and its evidence go into the file
as comments, so the file can be reviewed rather than trusted.

The window is the same for every collection: --keep sets it, --fast-size
chooses the longest one whose documents fit in half of a fast tier that size,
and with neither it is 90d.

Then it runs cline preflight against the new file and prints what is left for
you to do before the cutover. Exits non-zero when preflight would.
`

func runAdopt(args []string) error {
	fs := flag.NewFlagSet("adopt", flag.ExitOnError)
	hotURI := fs.String("hot", "", "the new replica set that becomes the fast tier")
	coldURI := fs.String("cold", "", "the replica set you have, which becomes the archive")
	listen := fs.String("listen", "0.0.0.0:27017", "the address the proxy will serve on")
	proxyHost := fs.String("proxy-host", "", "the host name the application will dial, for the connection string printed at the end (default: this host)")
	keepS := fs.String("keep", "", "how long documents stay on the fast tier, such as 90d")
	fastSizeS := fs.String("fast-size", "", "the fast tier's disk, such as 500GB, to choose --keep from the data")
	out := fs.String("out", "cline.yaml", "where to write the configuration")
	force := fs.Bool("force", false, "overwrite --out if it exists")
	sampleN := fs.Int("sample", 1000, "documents sampled per collection")
	dbsS := fs.String("db", "", "only these databases, comma separated (default: every application database)")
	noPreflight := fs.Bool("no-preflight", false, "write the file and stop, without running preflight on it")
	fs.Usage = func() { fmt.Print(adoptUsage); fs.PrintDefaults() }
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *hotURI == "" || *coldURI == "" {
		fs.Usage()
		return errors.New("adopt needs --hot and --cold")
	}
	if *keepS != "" && *fastSizeS != "" {
		return errors.New("--keep and --fast-size both choose the window: give one")
	}
	var keep time.Duration
	if *keepS != "" {
		d, err := config.ParseDuration(*keepS)
		if err != nil || d <= 0 {
			return fmt.Errorf("--keep: an age such as 90d, not %q", *keepS)
		}
		keep = d.D()
	}
	var fastSize int64
	if *fastSizeS != "" {
		n, err := parseSize(*fastSizeS)
		if err != nil {
			return fmt.Errorf("--fast-size: %w", err)
		}
		fastSize = n
	}
	if *sampleN < 10 {
		return errors.New("--sample: at least 10 documents, or nothing can be told from it")
	}
	if !*force {
		if _, err := os.Stat(*out); err == nil {
			return fmt.Errorf("%s exists: move it, choose another --out, or pass --force", *out)
		}
	}
	var onlyDBs map[string]bool
	if *dbsS != "" {
		onlyDBs = map[string]bool{}
		for _, d := range strings.Split(*dbsS, ",") {
			if d = strings.TrimSpace(d); d != "" {
				onlyDBs[d] = true
			}
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	cold, err := connect(ctx, *coldURI, "cold")
	if err != nil {
		return fmt.Errorf("the cluster being adopted could not be reached: %w", err)
	}
	defer disconnect(cold, log)
	hot, err := connect(ctx, *hotURI, "hot")
	if err != nil {
		return fmt.Errorf("the fast tier could not be reached: %w", err)
	}
	defer disconnect(hot, log)

	fmt.Printf("Reading the cluster being adopted. Nothing is written to either tier.\n\n")

	// The one refusal no file can fix, said first: every daemon would refuse
	// the pair, and the cure is an upgrade, not a setting.
	var blockers []string
	if verr := tierver.Check(ctx, hot, cold, false, log); verr != nil {
		blockers = append(blockers, verr.Error())
		fmt.Printf("The two tiers do not run the same MongoDB release, and every daemon refuses\n"+
			"that: %v\nUpgrade the archive to the fast tier's release first (docs/adoption.md).\n\n", verr)
	}
	if extra, herr := hotNamespaces(ctx, hot); herr == nil && len(extra) > 0 {
		fmt.Printf("The fast tier already holds %d collection(s) (%s). It is meant to be a new,\n"+
			"empty replica set; the proxy will serve those as they are.\n\n",
			len(extra), strings.Join(firstN(extra, 5), ", "))
	}

	samples, err := sampleArchive(ctx, cold, onlyDBs, *sampleN)
	if err != nil {
		return err
	}
	if len(samples) == 0 {
		return errors.New("the archive holds no application collections to tier")
	}

	now := time.Now()
	plans := make([]collPlan, 0, len(samples))
	for _, s := range samples {
		plans = append(plans, planCollection(s))
	}

	chosenBy := "the default"
	switch {
	case keep > 0:
		chosenBy = "--keep"
	case fastSize > 0:
		w, fits := chooseWindow(plans, now, fastSize)
		keep = w
		chosenBy = fmt.Sprintf("--fast-size %s: the longest window whose documents fit in half of it", humanBytes(fastSize))
		if !fits {
			chosenBy = fmt.Sprintf("--fast-size %s, which not even %s fits in half of", humanBytes(fastSize), config.Duration(w))
			blockers = append(blockers, "the fast tier is too small for the shortest window")
		}
	default:
		keep = 90 * 24 * time.Hour
	}

	printPlans(plans, now, keep)
	printWindows(plans, now, keep, chosenBy)

	tiered := 0
	for _, p := range plans {
		if p.Tier {
			tiered++
		}
	}
	if tiered == 0 {
		// An empty list is not "nothing": feeder.namespaces empty tiers every
		// collection, so a file with no collections means the opposite of what
		// this found.
		return errors.New("no collection can be tiered, so no file was written: an empty " +
			"collections list would tier everything")
	}

	addr := *proxyHost
	if addr == "" {
		addr = proxyAddress(*listen)
	} else if _, port, perr := net.SplitHostPort(*listen); perr == nil && !strings.Contains(addr, ":") {
		addr = net.JoinHostPort(addr, port)
	}

	text := adoptYAML(plans, *hotURI, *coldURI, *listen, keep, now, *sampleN, chosenBy)
	if err := writeChecked(*out, text); err != nil {
		return err
	}
	fmt.Printf("\nWrote %s: %d collection(s) tiered, kept %s on the fast tier.\n", *out, tiered, config.Duration(keep))

	var pfErr error
	if !*noPreflight {
		fmt.Printf("\nRunning cline preflight --config %s\n", *out)
		fmt.Println(strings.Repeat("-", 72))
		pfErr = runPreflight([]string{"--config", *out})
		fmt.Println(strings.Repeat("-", 72))
		if pfErr != nil {
			fmt.Printf("preflight: %v\n", pfErr)
		}
	}

	printNextSteps(plans, *out, *coldURI, addr, blockers, pfErr != nil, *noPreflight)
	if pfErr != nil {
		return fmt.Errorf("preflight refused the pair: fix what it names and run cline preflight --config %s again", *out)
	}
	if len(blockers) > 0 {
		return errors.New(blockers[0])
	}
	return nil
}

// sampleArchive reads every application collection on the archive: its kind,
// its size, its indexes and a sample of its documents.
func sampleArchive(ctx context.Context, cold *mongo.Client, only map[string]bool, n int) ([]collSample, error) {
	dbs, err := cold.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list the archive's databases: %w", err)
	}
	var out []collSample
	for _, db := range dbs {
		switch db {
		case "admin", "local", "config":
			continue
		}
		if isBookkeepingDB(db) || (only != nil && !only[db]) {
			continue
		}
		cur, err := cold.Database(db).ListCollections(ctx, bson.D{})
		if err != nil {
			return nil, fmt.Errorf("list collections in %s: %w", db, err)
		}
		var infos []bson.Raw
		for cur.Next(ctx) {
			infos = append(infos, append(bson.Raw(nil), cur.Current...))
		}
		cerr := cur.Err()
		_ = cur.Close(ctx)
		if cerr != nil {
			return nil, fmt.Errorf("list collections in %s: %w", db, cerr)
		}
		for _, info := range infos {
			name, _ := info.Lookup("name").StringValueOK()
			if name == "" || strings.HasPrefix(name, "system.") {
				continue
			}
			progress("  sampling %s.%s", printable(db), printable(name))
			s := collSample{NS: db + "." + name}
			if t, _ := info.Lookup("type").StringValueOK(); t == "view" {
				s.View = true
				out = append(out, s)
				continue
			}
			if kind, why := feeder.UnmirrorableKind(info); kind != "" {
				s.Unmirrored = kind + " (" + why + ")"
			}
			coll := cold.Database(db).Collection(name)
			readStats(ctx, coll, &s)
			if s.Unmirrored == "" {
				readIndexes(ctx, coll, &s)
				if err := readSample(ctx, coll, n, &s); err != nil {
					return nil, fmt.Errorf("sample %s: %w", s.NS, err)
				}
			}
			out = append(out, s)
		}
	}
	progress("")
	sort.Slice(out, func(i, j int) bool { return out[i].NS < out[j].NS })
	return out, nil
}

func readStats(ctx context.Context, coll *mongo.Collection, s *collSample) {
	cur, err := coll.Aggregate(ctx, mongo.Pipeline{
		{{Key: "$collStats", Value: bson.D{{Key: "storageStats", Value: bson.D{}}}}},
	})
	if err != nil {
		return
	}
	defer func() { _ = cur.Close(ctx) }()
	for cur.Next(ctx) {
		st, ok := cur.Current.Lookup("storageStats").DocumentOK()
		if !ok {
			continue
		}
		s.Count += asInt64(st.Lookup("count"))
		s.DataSize += asInt64(st.Lookup("size"))
		s.Storage += asInt64(st.Lookup("storageSize"))
	}
}

func asInt64(v bson.RawValue) int64 {
	if n, ok := v.AsInt64OK(); ok {
		return n
	}
	if f, ok := v.DoubleOK(); ok {
		return int64(f)
	}
	return 0
}

func readIndexes(ctx context.Context, coll *mongo.Collection, s *collSample) {
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		return
	}
	defer func() { _ = cur.Close(ctx) }()
	for cur.Next(ctx) {
		key, ok := cur.Current.Lookup("key").DocumentOK()
		if !ok {
			continue
		}
		elems, _ := key.Elements()
		var ix []indexKey
		for _, e := range elems {
			dir := 0
			if f, ok := e.Value().AsFloat64OK(); ok {
				switch {
				case f > 0:
					dir = 1
				case f < 0:
					dir = -1
				}
			}
			ix = append(ix, indexKey{Field: e.Key(), Dir: dir})
		}
		s.Indexes = append(s.Indexes, ix)
	}
}

// readSample takes n documents with $sample, which reads a random cursor on a
// large collection rather than scanning it, and keeps only the _id's time and
// the top level dates.
func readSample(ctx context.Context, coll *mongo.Collection, n int, s *collSample) error {
	sctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cur, err := coll.Aggregate(sctx, mongo.Pipeline{
		{{Key: "$sample", Value: bson.D{{Key: "size", Value: n}}}},
	}, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return err
	}
	defer func() { _ = cur.Close(ctx) }()
	for cur.Next(sctx) {
		d := sampledDoc{Dates: map[string]time.Time{}}
		elems, err := cur.Current.Elements()
		if err != nil {
			continue
		}
		for _, e := range elems {
			v := e.Value()
			if e.Key() == "_id" {
				d.IDType = v.Type.String()
				if oid, ok := v.ObjectIDOK(); ok {
					d.IDTime = oid.Timestamp()
				}
				continue
			}
			if dt, ok := v.DateTimeOK(); ok {
				d.Dates[e.Key()] = time.UnixMilli(dt)
				d.HasDate = true
			}
		}
		s.Docs = append(s.Docs, d)
	}
	return cur.Err()
}

func printPlans(plans []collPlan, now time.Time, keep time.Duration) {
	w := 0
	for _, p := range plans {
		if l := len(printable(p.NS)); l > w {
			w = l
		}
	}
	fmt.Printf("%d collection(s) on the archive:\n\n", len(plans))
	for _, p := range plans {
		ns := printable(p.NS)
		switch {
		case p.View:
			fmt.Printf("  %-*s  view\n", w, ns)
		case p.Tier:
			by := "by _id"
			if p.DateField != "" {
				by = "by " + printable(p.DateField)
			}
			fmt.Printf("  %-*s  %13s docs  %9s  tier %-14s  %3.0f%% within %s\n", w, ns,
				commas(p.Count), humanBytes(p.Storage), by,
				100*keptFraction(p.Ages, now, keep), config.Duration(keep))
		default:
			fmt.Printf("  %-*s  %13s docs  %9s  not tiered\n", w, ns, commas(p.Count), humanBytes(p.Storage))
		}
		for _, line := range wrap(firstNonEmpty(p.Why, p.Skip), 72) {
			fmt.Printf("  %-*s    %s\n", w, "", line)
		}
		if p.Warn != "" {
			for i, line := range wrap(p.Warn, 66) {
				lead := "check:"
				if i > 0 {
					lead = "      "
				}
				fmt.Printf("  %-*s    %s %s\n", w, "", lead, line)
			}
		}
	}
}

func printWindows(plans []collPlan, now time.Time, keep time.Duration, chosenBy string) {
	copied := copiedBytes(plans)
	fmt.Printf("\nWhat the fast tier would hold on disk, by window (tiered collections")
	if copied > 0 {
		fmt.Printf(", plus %s copied whole", humanBytes(copied))
	}
	fmt.Printf("):\n\n")
	listed := false
	for _, w := range candidateWindows {
		mark := ""
		if w == keep {
			mark, listed = "   <- "+chosenBy, true
		}
		fmt.Printf("  %5s  %9s%s\n", config.Duration(w), humanBytes(fastTierBytes(plans, now, w)+copied), mark)
	}
	if !listed {
		fmt.Printf("  %5s  %9s   <- %s\n", config.Duration(keep), humanBytes(fastTierBytes(plans, now, keep)+copied), chosenBy)
	}
	fmt.Printf("\nData only, from the sample. Leave room for indexes, the oplog, the unique key\n" +
		"registry (about 56 bytes per unique value on disk) and growth: half the disk is\n" +
		"the budget --fast-size uses.\n")
}

// adoptYAML is the short form, with the evidence for each decision beside it.
func adoptYAML(plans []collPlan, hot, cold, listen string, keep time.Duration, now time.Time, n int, chosenBy string) string {
	var b strings.Builder
	k := config.Duration(keep).String()
	fmt.Fprintf(&b, "# Written by cline adopt on %s from a sample of up to %d documents per\n", now.UTC().Format("2006-01-02"), n)
	b.WriteString("# collection. Every decision has its evidence beside it: review them, and see\n")
	b.WriteString("# docs/adoption.md for what each one means. cline.example.yaml has every other\n")
	b.WriteString("# setting; any of them can sit beside the short form below.\n\n")
	fmt.Fprintf(&b, "hot: %s\n", yamlString(hot))
	fmt.Fprintf(&b, "cold: %s\n", yamlString(cold))
	fmt.Fprintf(&b, "listen: %s\n\n", yamlString(listen))
	for _, line := range wrap(fmt.Sprintf("The fast tier keeps %s (chosen by %s): about %s of the tiered "+
		"collections on disk at today's data.", k, chosenBy, humanBytes(fastTierBytes(plans, now, keep))), 76) {
		b.WriteString("# " + line + "\n")
	}
	b.WriteString("collections:\n")
	for _, p := range plans {
		if !p.Tier {
			continue
		}
		fmt.Fprintf(&b, "  # %s documents, %s on disk, %.0f%% of them within %s.\n",
			commas(p.Count), humanBytes(p.Storage), 100*keptFraction(p.Ages, now, keep), k)
		for _, line := range wrap(p.Why+".", 74) {
			b.WriteString("  # " + line + "\n")
		}
		if p.Warn != "" {
			for _, line := range wrap("CHECK: "+p.Warn+".", 74) {
				b.WriteString("  # " + line + "\n")
			}
		}
		if p.DateField == "" {
			fmt.Fprintf(&b, "  %s: %s\n", yamlString(p.NS), k)
		} else {
			fmt.Fprintf(&b, "  %s:\n    older_than: %s\n    date_field: %s\n", yamlString(p.NS), k, yamlString(p.DateField))
		}
	}
	var skipped []collPlan
	for _, p := range plans {
		if !p.Tier {
			skipped = append(skipped, p)
		}
	}
	if len(skipped) > 0 {
		b.WriteString("\n# Not tiered. The proxy sends these to the fast tier, so each one has to be\n")
		b.WriteString("# there at the cutover (cline adopt printed how):\n")
		for _, p := range skipped {
			lines := wrap(p.Skip+".", 70)
			fmt.Fprintf(&b, "#   %s\n", printable(p.NS))
			for _, line := range lines {
				b.WriteString("#     " + line + "\n")
			}
		}
	}
	return b.String()
}

// writeChecked writes the file only after config.Load accepts it, so the file
// on disk is one every daemon will read.
func writeChecked(path, text string) error {
	tmp := path + ".adopt-tmp"
	if err := os.WriteFile(tmp, []byte(text), 0o600); err != nil {
		return err
	}
	if _, err := config.Load(tmp); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("the configuration written was refused by its own loader, which is a defect "+
			"in cline adopt: %w", err)
	}
	return os.Rename(tmp, path)
}

func printNextSteps(plans []collPlan, out, coldURI, addr string, blockers []string, pfFailed, pfSkipped bool) {
	fmt.Printf("\nWhat is left before the cutover\n\n")
	step := 0
	next := func(title string) { step++; fmt.Printf("%d. %s\n", step, title) }

	if len(blockers) > 0 || pfFailed {
		next("Fix what refuses the pair, then run preflight again:")
		for _, bl := range blockers {
			fmt.Printf("     %s\n", firstLine(bl))
		}
		fmt.Printf("     cline preflight --config %s\n\n", out)
	} else if pfSkipped {
		next("Run preflight, which writes nothing:")
		fmt.Printf("     cline preflight --config %s\n\n", out)
	}

	next("Create the application's roles and accounts on the fast tier.")
	fmt.Printf("   Roles first, then users, with the passwords the application already uses:\n" +
		"   preflight names the ones the archive has and the fast tier does not. Give the\n" +
		"   proxy's own account killAnyCursor on the fast tier too.\n\n")

	var idx []collPlan
	for _, p := range plans {
		if p.Tier && p.NeedIndex {
			idx = append(idx, p)
		}
	}
	if len(idx) > 0 {
		next("Build the index each date_field rule reads by, on the archive:")
		for _, p := range idx {
			db, c, _ := strings.Cut(p.NS, ".")
			fmt.Printf("     %s.createIndex({%s: -1, _id: -1})\n", collRef(db, c), jsString(p.DateField))
		}
		fmt.Printf("   Without it every warm batch sorts the whole collection. The proxy builds\n" +
			"   it on the fast tier at its first start.\n\n")
	}

	var copies, views []collPlan
	for _, p := range plans {
		switch {
		case p.View:
			views = append(views, p)
		case p.CopyWhole:
			copies = append(copies, p)
		}
	}
	if len(copies) > 0 {
		next("At the cutover, with the application stopped, copy what is not tiered:")
		fmt.Printf("     export COLD='<archive URI>' HOT='<fast tier URI>'\n")
		for _, p := range copies {
			fmt.Printf("     mongodump --uri \"$COLD\" --archive --nsInclude=%s | mongorestore --uri \"$HOT\" --archive\n",
				shellQuote(p.NS))
		}
		fmt.Println()
	}
	if len(views) > 0 {
		next("Recreate the views on the fast tier, with createView:")
		for _, p := range views {
			fmt.Printf("     %s\n", printable(p.NS))
		}
		fmt.Println()
	}

	uri, notes := proxyURI(coldURI, addr)
	next("Point the application at the proxy, with a new connection string:")
	fmt.Printf("     %s\n", uri)
	fmt.Printf("   No replicaSet: the proxy answers as a mongos, and a driver looking for a\n" +
		"   replica set times out without saying why. Several proxies are named in the\n" +
		"   string, not put behind one load balanced address.\n")
	for _, n := range notes {
		for i, line := range wrap(n, 70) {
			lead := "   note: "
			if i > 0 {
				lead = "         "
			}
			fmt.Printf("%s%s\n", lead, line)
		}
	}
	fmt.Println()

	next("Start change stream consumers fresh.")
	fmt.Printf("   A resume token from the old cluster is accepted through the proxy and\n" +
		"   delivers nothing.\n\n")

	next("Start it:")
	fmt.Printf("     cline run --config %s\n", out)
	fmt.Printf("   It starts the feeder, then the proxy, claims the unique keys, then the\n" +
		"   evictor and the warmer, in that order. Eviction holds off until the fast\n" +
		"   tier's oplog window passes 24h, which on a new set is its first day.\n" +
		"   Under systemd: packaging/install.sh, then systemctl enable --now cline.\n\n")

	next("Know the way back before you need it:")
	fmt.Printf("     cline rollback --config %s\n", out)
	fmt.Printf("   counts what repointing the application at the archive would need.\n")
}

// proxyAddress is the address the application dials, from the listen address:
// a wildcard listen is this host's name.
func proxyAddress(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		if h, herr := os.Hostname(); herr == nil {
			host = h
		}
	}
	return net.JoinHostPort(host, port)
}

// progress overwrites one line on a terminal and says nothing otherwise, so a
// captured run is not full of carriage returns.
var progressWidth int

func progress(format string, a ...any) {
	if st, err := os.Stderr.Stat(); err != nil || st.Mode()&os.ModeCharDevice == 0 {
		return
	}
	line := fmt.Sprintf(format, a...)
	pad := ""
	if n := progressWidth - len(line); n > 0 {
		pad = strings.Repeat(" ", n)
	}
	fmt.Fprintf(os.Stderr, "\r%s%s\r", line, pad)
	progressWidth = len(line)
}

// yamlString is s as a YAML double-quoted scalar. JSON is one, and without
// HTML escaping a connection string keeps its & instead of \u0026.
func yamlString(s string) string {
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		return `""`
	}
	return strings.TrimSuffix(b.String(), "\n")
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func commas(n int64) string {
	s := fmt.Sprintf("%d", n)
	if n < 0 {
		return "-" + commas(-n)
	}
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func wrap(s string, width int) []string {
	var lines []string
	line := ""
	for _, w := range strings.Fields(s) {
		if line != "" && len(line)+1+len(w) > width {
			lines = append(lines, line)
			line = ""
		}
		if line != "" {
			line += " "
		}
		line += w
	}
	if line != "" {
		lines = append(lines, line)
	}
	return lines
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

func firstN(ss []string, n int) []string {
	if len(ss) <= n {
		return ss
	}
	return append(append([]string(nil), ss[:n]...), "...")
}
