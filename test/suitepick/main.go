// Command suitepick names the live suites a change reaches, so a change can be
// checked in minutes instead of by a full sweep.
//
// Which suites a change reaches is not read from the code, it is measured. A
// sweep run with a coverage binary (COVER=1 psweep.sh, then mkcovmap.sh) writes
// test/suitemap/funcs.tsv: every function each suite executed. suitepick diffs
// the working tree against a base revision, finds the function around every
// changed line, and looks those functions up.
//
// By default it prints the cheapest set of suites that runs every changed
// function at least once, weighted by how long each suite takes. -all prints
// every suite that runs any of them. Either way the reasoning goes to stderr:
// which function chose which suite, and which changed functions no suite runs
// at all, because a change there is checked only by its unit tests.
//
//	go run ./test/suitepick              # against HEAD, uncommitted changes
//	go run ./test/suitepick -base master~3
package main

import (
	"bufio"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func main() {
	base := flag.String("base", "HEAD", "revision to diff the working tree against")
	dir := flag.String("map", "test/suitemap", "directory holding funcs.tsv and times.tsv")
	all := flag.Bool("all", false, "print every suite that reaches the change, not the cheapest cover")
	k := flag.Int("k", 2, "suites that must run each changed function, where that many do")
	flag.Parse()

	funcs, err := readFuncs(filepath.Join(*dir, "funcs.tsv"))
	if err != nil {
		fatal("%v (build it with COVER=1 psweep.sh and test/mkcovmap.sh)", err)
	}
	times, err := readTimes(filepath.Join(*dir, "times.tsv"))
	if err != nil {
		fatal("%v", err)
	}

	changes, err := diff(*base)
	if err != nil {
		fatal("%v", err)
	}
	if len(changes) == 0 {
		fmt.Fprintln(os.Stderr, "suitepick: nothing changed against", *base)
		return
	}

	var targets []target
	direct := map[string]bool{}
	harness := false
	for file, lines := range changes {
		switch {
		case strings.HasPrefix(file, "test/") && strings.HasSuffix(file, ".sh"):
			s := strings.TrimSuffix(filepath.Base(file), ".sh")
			// A suite run in parts is timed as suite@1, suite@2...; a change
			// to its file is a change to every part.
			for t := range times {
				if t == s || strings.HasPrefix(t, s+"@") {
					direct[t] = true
					fmt.Fprintf(os.Stderr, "  %-40s the suite itself\n", file)
				}
			}
			if s == "psweep" || s == "mkworker" {
				harness = true
			}
		case strings.HasSuffix(file, "_test.go"):
			// Unit tests only. The suites run the binary, which does not hold them.
		case strings.HasSuffix(file, ".go") && !strings.HasPrefix(file, "test/"):
			targets = append(targets, enclosing(file, lines)...)
		}
	}
	if harness {
		fmt.Fprintln(os.Stderr, "suitepick: the sweep harness itself changed, run the full sweep")
	}

	// Each target is reached by the suites that ran it. A function no suite ran
	// falls back to its file, since it is most often new and called from code
	// beside it; a file no suite ran is reported and reaches nothing.
	reach := map[string]map[string]bool{}
	var unreached []string
	for _, t := range targets {
		key := t.String()
		got := funcs.byFunc[t.file+"\t"+t.fn]
		if t.fn == "" || len(got) == 0 {
			if t.fn != "" {
				fmt.Fprintf(os.Stderr, "  %-40s no suite runs it, using its file\n", key)
			}
			got = funcs.byFile[t.file]
		}
		if len(got) == 0 {
			unreached = append(unreached, key)
			continue
		}
		reach[key] = got
	}
	for _, u := range unreached {
		fmt.Fprintf(os.Stderr, "  %-40s NO SUITE RUNS THIS, only its unit tests check it\n", u)
	}

	chosen := map[string]bool{}
	for s := range direct {
		chosen[s] = true
	}
	if *all {
		for _, ss := range reach {
			for s := range ss {
				chosen[s] = true
			}
		}
	} else {
		cover(reach, times, chosen, *k)
	}

	var out []string
	total := 0.0
	for s := range chosen {
		out = append(out, s)
		total += times[s]
	}
	// Longest first, which is the order a sweep should start them in.
	sort.Slice(out, func(i, j int) bool { return times[out[i]] > times[out[j]] })
	fmt.Fprintf(os.Stderr, "suitepick: %d suites, %.0fs of suite time\n", len(out), total)
	for _, s := range out {
		fmt.Println(s)
	}
}

// cover adds to chosen the suites a greedy weighted set cover picks, so that
// every target is run by k chosen suites, or by all that reach it when fewer
// do. Each round takes the suite that meets the most outstanding need per
// second of its run time.
//
// k is 2 by default because running a function is not checking it. The
// cheapest single cover of the commits of 2026-10-23 was testretry, fourteen
// seconds, for nearly every function: it passes through the merged read and
// asserts nothing about it. A second suite makes it likelier that one of them
// looks at the answer, and a mutant is still what proves one does.
func cover(reach map[string]map[string]bool, times map[string]float64, chosen map[string]bool, k int) {
	need := map[string]int{}
	for t, ss := range reach {
		n := k
		if len(ss) < n {
			n = len(ss)
		}
		for s := range ss {
			if chosen[s] {
				n--
			}
		}
		if n > 0 {
			need[t] = n
		}
	}
	for len(need) > 0 {
		best, bestScore := "", -1.0
		for s, secs := range times {
			if chosen[s] {
				continue
			}
			n := 0
			for t := range need {
				if reach[t][s] {
					n++
				}
			}
			if n == 0 {
				continue
			}
			score := float64(n) / (secs + 1)
			if score > bestScore || (score == bestScore && s < best) {
				best, bestScore = s, score
			}
		}
		if best == "" {
			return
		}
		chosen[best] = true
		for t := range need {
			if reach[t][best] {
				fmt.Fprintf(os.Stderr, "  %-40s %s\n", t, best)
				if need[t]--; need[t] == 0 {
					delete(need, t)
				}
			}
		}
	}
}

type target struct{ file, fn string }

func (t target) String() string {
	if t.fn == "" {
		return t.file
	}
	return t.file + " " + t.fn
}

// enclosing names the function around each changed line, in the form
// `go tool covdata func` prints: Name, T.Name or *T.Name. A line outside every
// function (a type, a var, an import) stands for the whole file.
func enclosing(file string, lines []int) []target {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, parser.SkipObjectResolution)
	if err != nil {
		// Deleted, or not parseable: the whole file.
		return []target{{file: file}}
	}
	seen := map[string]bool{}
	var out []target
	for _, ln := range lines {
		name := ""
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if fset.Position(fd.Pos()).Line <= ln && ln <= fset.Position(fd.End()).Line {
				name = funcName(fd)
				break
			}
		}
		if !seen[name] {
			seen[name] = true
			out = append(out, target{file: file, fn: name})
		}
	}
	return out
}

func funcName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	return recvName(fd.Recv.List[0].Type) + "." + fd.Name.Name
}

func recvName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return "*" + recvName(t.X)
	case *ast.IndexExpr:
		return recvName(t.X)
	case *ast.IndexListExpr:
		return recvName(t.X)
	case *ast.Ident:
		return t.Name
	}
	return "?"
}

var hunk = regexp.MustCompile(`^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@`)

// diff returns, for every changed file, the lines of the working tree copy that
// changed. A pure deletion counts as a change at the line it left behind.
// Untracked files are included whole.
func diff(base string) (map[string][]int, error) {
	out, err := exec.Command("git", "diff", "-U0", "--no-color", base).Output()
	if err != nil {
		return nil, fmt.Errorf("git diff %s: %v", base, err)
	}
	changes := map[string][]int{}
	file := ""
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		l := sc.Text()
		switch {
		case strings.HasPrefix(l, "+++ "):
			file = strings.TrimPrefix(strings.TrimPrefix(l, "+++ "), "b/")
			if file == "/dev/null" {
				file = ""
			}
		case strings.HasPrefix(l, "--- a/"):
			// A deleted file has +++ /dev/null; keep its old name.
			file = strings.TrimPrefix(l, "--- a/")
		case strings.HasPrefix(l, "@@"):
			m := hunk.FindStringSubmatch(l)
			if m == nil || file == "" {
				continue
			}
			start, _ := strconv.Atoi(m[1])
			n := 1
			if m[2] != "" {
				n, _ = strconv.Atoi(m[2])
			}
			if n == 0 {
				changes[file] = append(changes[file], start)
			}
			for i := 0; i < n; i++ {
				changes[file] = append(changes[file], start+i)
			}
		}
	}
	untracked, err := exec.Command("git", "ls-files", "--others", "--exclude-standard").Output()
	if err == nil {
		for _, f := range strings.Fields(string(untracked)) {
			if strings.HasSuffix(f, ".go") || strings.HasSuffix(f, ".sh") {
				changes[f] = append(changes[f], 0)
			}
		}
	}
	return changes, nil
}

type funcMap struct {
	byFunc map[string]map[string]bool // "file\tfunc" -> suites
	byFile map[string]map[string]bool // file -> suites
}

func readFuncs(path string) (funcMap, error) {
	m := funcMap{byFunc: map[string]map[string]bool{}, byFile: map[string]map[string]bool{}}
	f, err := os.Open(path)
	if err != nil {
		return m, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		p := strings.Split(sc.Text(), "\t")
		if len(p) != 3 || strings.HasPrefix(p[0], "#") {
			continue
		}
		suite, file, fn := p[0], p[1], p[2]
		add(m.byFunc, file+"\t"+fn, suite)
		add(m.byFile, file, suite)
	}
	return m, sc.Err()
}

func add(m map[string]map[string]bool, k, v string) {
	if m[k] == nil {
		m[k] = map[string]bool{}
	}
	m[k][v] = true
}

func readTimes(path string) (map[string]float64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	t := map[string]float64{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		p := strings.Fields(sc.Text())
		if len(p) != 2 || strings.HasPrefix(p[0], "#") {
			continue
		}
		secs, err := strconv.ParseFloat(p[1], 64)
		if err != nil {
			return nil, fmt.Errorf("%s: %q: %v", path, sc.Text(), err)
		}
		t[p[0]] = secs
	}
	return t, sc.Err()
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "suitepick: "+format+"\n", a...)
	os.Exit(2)
}
