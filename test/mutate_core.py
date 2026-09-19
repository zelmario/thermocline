#!/usr/bin/env python3
# Break a guard in the packages a wrong answer is worst in, and see whether
# anything notices.
#
#   python3 test/mutate_core.py
#
# The third of these, after mutate_router.py and mutate_daemons.py, and the one
# covering the decisions the rest of the system rests on: whether a completeness
# claim covers a query, whether a document counts as deleted, whether an oplog
# position falls inside this system's own DDL, and whether a user may read a
# namespace.
#
# Twelve mutants, zero survivors on 2026-09-19. That is the result to keep: the
# core predicates are held down by unit tests, so a regression in them shows up
# in seconds rather than in a thirty minute sweep. Unlike the daemons, where
# three guards survive `go test` and are covered by suites alone.
#
# The authorization four are the ones to re-run after any change to authz:
# granting without checking the resource, granting without checking the action,
# letting a cluster privilege satisfy a namespace action, and letting any action
# satisfy any request. Each is one client reading another's data.
#
# Runs the whole test tree per mutant rather than one package, because these
# types are used far from where they are defined: NotArchivedDeleted is caught
# by a router test about joined collections, not by a meta test.

import subprocess, shutil, os

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WM = "internal/watermark/watermark.go"
MT = "internal/meta/meta.go"
CP = "internal/checkpoint/position.go"
AZ = "internal/authz/authz.go"

MUTANTS = [
 ("Mark.Complete: ignore whether a claim was made", WM,
  "return m.Claimed && !bound.Before(m.From)", "return !bound.Before(m.From)"),
 ("Mark.Complete: off by one at the boundary", WM,
  "return m.Claimed && !bound.Before(m.From)", "return m.Claimed && !bound.After(m.From)"),
 ("Mark.CoversAll: a partial claim covers everything", WM,
  "return m.Claimed && m.From.IsZero()", "return m.Claimed"),
 ("NotArchivedDeleted: stop excluding tombstones", MT,
  'return bson.E{Key: TombstoneField, Value: bson.D{{Key: "$exists", Value: false}}}',
  'return bson.E{Key: TombstoneField, Value: bson.D{{Key: "$exists", Value: true}}}'),
 ("OwnDDL.Covers: a zero position is covered", MT,
  "\tif pos.T == 0 && pos.I == 0 {\n\t\treturn false\n\t}",
  "\tif pos.T == 0 && pos.I == 0 {\n\t\treturn true\n\t}"),
 ("OwnDDL.Covers: an open span never ends", MT,
  "if pos.T <= s.From.T+OpenSpanSeconds {", "if true {"),
 ("before: ties count as before", MT,
  "func before(a, b bson.Timestamp) bool {\n\tif a.T != b.T {",
  "func before(a, b bson.Timestamp) bool {\n\tif a == b {\n\t\treturn true\n\t}\n\tif a.T != b.T {"),
 ("Allowed: grant without checking the resource", AZ,
  "\t\tif !p.Resource.covers(db, coll) {\n\t\t\tcontinue\n\t\t}",
  "\t\tif false {\n\t\t\tcontinue\n\t\t}"),
 ("Allowed: grant without checking the action", AZ,
  "\t\tif hasAction(p.Actions, action) {\n\t\t\treturn true\n\t\t}\n\t}\n\treturn false\n}\n\n// AllowedCluster",
  "\t\treturn true\n\t}\n\treturn false\n}\n\n// AllowedCluster"),
 ("covers: a cluster privilege satisfies a namespace action", AZ,
  "\tdefault:\n\t\t// A cluster privilege is not about any particular collection, so it",
  "\tcase kindCluster:\n\t\treturn true\n\tdefault:\n\t\t// A cluster privilege is not about any particular collection, so it"),
 ("hasAction: any action satisfies any request", AZ,
  "\tfor _, a := range actions {\n\t\tif a == want {\n\t\t\treturn true\n\t\t}\n\t}\n\treturn false",
  "\treturn len(actions) > 0"),
 ("PackTS: drop the increment", CP,
  "func PackTS(ts bson.Timestamp) uint64 { return uint64(ts.T)<<32 | uint64(ts.I) }",
  "func PackTS(ts bson.Timestamp) uint64 { return uint64(ts.T) << 32 }"),
]
PKG = {WM: "./internal/watermark/", MT: "./internal/meta/", CP: "./internal/checkpoint/", AZ: "./internal/authz/"}
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
    # the whole tree, because these types are used far from where they live
    r = subprocess.run(["go", "test", "./..."], cwd=REPO, capture_output=True, text=True)
    shutil.move(path + ".bak", path)
    out = r.stdout + r.stderr
    if r.returncode == 0:
        print(f"SURVIVED                 {label}"); survivors.append(label)
    elif "build failed" in out or "declared and not used" in out:
        print(f"INVALID (did not build)  {label}")
    else:
        pkgs = sorted({l.split()[1] for l in out.splitlines() if l.startswith("FAIL\t") or l.startswith("--- FAIL")} ) if False else []
        hit = [l for l in out.splitlines() if l.startswith("--- FAIL")]
        print(f"caught                   {label}   <- {hit[0].strip() if hit else 'test failure'}")
print()
print(f"{len(survivors)} survivor(s) of {len(MUTANTS)}")
for s in survivors: print("   ", s)
