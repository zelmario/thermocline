#!/usr/bin/env python3
# Break a guard in the feeder or the evictor and see whether anything notices.
#
#   python3 test/mutate_daemons.py
#
# The sibling of test/mutate_router.py, and the same rules apply: a SURVIVED
# line is the finding, a SKIP proves nothing and means the anchor went stale,
# and a mutant that does not build is INVALID rather than caught.
#
# What it found on 2026-09-18, and what the survivors turned out to mean:
#
#   partition, cutoff, unmirrorableKind   all caught by unit tests
#   warmingActive x2, promotingActive     survived `go test` entirely
#
# A survivor here is not automatically an uncovered guard, because these
# daemons are mostly covered by suites rather than by unit tests. Breaking
# warmingActive and running the suites against that binary gave:
#
#   go test, every package    passed
#   testwarm                  57 passed, 0 failed
#   testchange                49 passed, 0 failed
#   testecho                  18 passed, 1 failed
#
# So it was covered, by one check in one suite, and only indirectly. testecho
# now has a direct check for it. The lesson is that a survivor here buys you a
# question, not an answer: run the suites against the mutant before concluding
# anything, and remember to put the real binary back afterwards, verified
# against a clean build rather than against bin/cline, which is itself built
# from whatever source is on disk.

import subprocess, shutil, os

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EV = "internal/evictor/eligible.go"
EVC = "internal/evictor/evictor.go"
FD = "internal/feeder/echo.go"
CK = "internal/feeder/collkind.go"

MUTANTS = [
 ("partition: evict documents residency holds", EV,
  "\t\tcase held[c.docID]:\n\t\t\tout.held++", "\t\tcase false:\n\t\t\tout.held++"),
 ("partition: evict what the archive does not confirm", EV,
  "\t\tcase !archived[c.docID]:", "\t\tcase archived[c.docID]:"),
 ("cutoff: age forward instead of back", EV,
  "return now.Add(-olderThan).UTC()", "return now.Add(olderThan).UTC()"),
 ("warmingActive: never suppress warm echoes", FD,
  "suppressed. See meta.HeartbeatUpdate.\n\tactive := f.archiveNow(ctx).Sub(marker.Seen) < markerFresh",
  "suppressed. See meta.HeartbeatUpdate.\n\tactive := false"),
 ("warmingActive: always suppress warm echoes", FD,
  "suppressed. See meta.HeartbeatUpdate.\n\tactive := f.archiveNow(ctx).Sub(marker.Seen) < markerFresh",
  "suppressed. See meta.HeartbeatUpdate.\n\tactive := true"),
 ("promotingActive: never suppress promotion echoes", FD,
  "// be on the router's machine. See warmingActive above.\n\tactive := f.archiveNow(ctx).Sub(marker.Seen) < markerFresh",
  "// be on the router's machine. See warmingActive above.\n\tactive := false"),
 ("unmirrorableKind: stop refusing capped collections", CK,
  'if capped, ok := odoc.Lookup("capped").BooleanOK(); ok && capped {',
  'if capped, ok := odoc.Lookup("capped").BooleanOK(); ok && !capped {'),
 ("unmirrorableKind: stop refusing time series", CK,
  'if t, ok := info.Lookup("type").StringValueOK(); ok && t == "timeseries" {',
  'if t, ok := info.Lookup("type").StringValueOK(); ok && t == "timeseriesXX" {'),
]

PKG = {EV: "./internal/evictor/", EVC: "./internal/evictor/", FD: "./internal/feeder/", CK: "./internal/feeder/"}
survivors = []
for label, rel, old, new in MUTANTS:
    path = os.path.join(REPO, rel)
    src = open(path).read()
    if old not in src:
        print(f"SKIP  (anchor missing)   {label}"); continue
    if src.count(old) != 1:
        print(f"SKIP  (anchor x{src.count(old)})    {label}"); continue
    shutil.copy(path, path + ".bak")
    open(path, "w").write(src.replace(old, new, 1))
    r = subprocess.run(["go", "test", PKG[rel]], cwd=REPO, capture_output=True, text=True)
    shutil.move(path + ".bak", path)
    out = r.stdout + r.stderr
    if r.returncode == 0:
        print(f"SURVIVED                {label}"); survivors.append(label)
    elif "build failed" in out or "declared and not used" in out or "undefined" in out:
        print(f"INVALID (did not build) {label}")
    else:
        hit = [l for l in out.splitlines() if l.startswith("--- FAIL") or l.startswith("    --- FAIL")]
        print(f"caught                  {label}   <- {hit[0].strip() if hit else 'test failure'}")
print()
print(f"{len(survivors)} survivor(s) of {len(MUTANTS)}")
for s in survivors: print("   ", s)
