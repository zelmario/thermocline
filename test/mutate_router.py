#!/usr/bin/env python3
# Break a guard and see whether anything notices.
#
#   python3 test/mutate_router.py
#
# Each entry below is one edit that should make some test fail. A SURVIVED line
# is a guard nothing covers, which is the finding; a caught line names the test
# that caught it. The file is restored after every mutant, including on a test
# that does not build.
#
# It found two things on 2026-09-18, both in code written that day: streamScope
# answered for a database with nothing tiered and no test said otherwise, and
# reservedInNewRoot's $literal arm could be deleted with the suite still green.
#
# Anchors are exact source lines, so they go stale as the code moves. A SKIP
# line says which one, and a skipped mutant is a mutant that proved nothing:
# fix the anchor or drop the entry, do not leave it skipping.
#
# Every mutant must still compile. One that does not is reported INVALID rather
# than caught, because a build failure proves nothing about the tests.

import subprocess, shutil, os

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
R = "internal/router/reserved.go"
CS = "internal/router/changestream.go"
AG = "internal/router/aggregate.go"
PE = "internal/router/promexport.go"

OPS_OLD = """// Paths, not names: an operator's keys are paths, so
				// $set of \"_tcDeletedAt.x\" writes the mark's name at the
				// top level just as $set of \"_tcDeletedAt\" does.
				if f, bad := reservedInPaths(d); bad {"""
OPS_NEW = "if f, bad := reservedTopLevel(d); bad {"

MUTANTS = [
 ("reservedPath: drop the dotted-path arm", R,
  'if key == f || strings.HasPrefix(key, f+".") {', 'if key == f {'),
 ("reservedPath: prefix without the dot (a spelling rule)", R,
  'if key == f || strings.HasPrefix(key, f+".") {', 'if strings.HasPrefix(key, f) {'),
 ("reservedInNewRoot: drop $mergeObjects", R,
  'case "$mergeObjects":', 'case "$mergeObjectsXX":'),
 ("reservedInNewRoot: drop $setField", R,
  'case "$setField":', 'case "$setFieldXX":'),
 ("reservedInNewRoot: drop $literal", R,
  'case "$literal":', 'case "$literalXX":'),
 ("reservedInStage: stop unwrapping newRoot", R,
  'if nr, nerr := d.LookupErr("newRoot"); nerr == nil {',
  'if nr, nerr := d.LookupErr("newRootXX"); nerr == nil {'),
 ("reservedInStage: stop reading $replaceWith", R,
  'if v, err := sd.LookupErr("$replaceWith"); err == nil {',
  'if v, err := sd.LookupErr("$replaceWithXX"); err == nil {'),
 ("reservedInStage: stop reading $project", R,
  'for _, op := range []string{"$set", "$addFields", "$project"} {',
  'for _, op := range []string{"$set", "$addFields"} {'),
 ("reservedInPaths: pre-filter inverted", R,
  'if !bytes.Contains(doc, reservedCommonPrefix) {',
  'if bytes.Contains(doc, reservedCommonPrefix) {'),
 ("update operators read as names again, not paths", R, OPS_OLD, OPS_NEW),
 ("reservedInUpdate: stop skipping $unset", R,
  'if key == "$unset" || key == "$rename" {', 'if key == "$renameXX" {'),
 ("streamScope: the whole deployment stops mattering", CS,
  'return "the whole deployment", true', 'return "the whole deployment", false'),
 ("streamScope: a database with nothing tiered still matters", CS,
  'return db + ", the whole database", r.managesAnythingIn(db)',
  'return db + ", the whole database", true'),
 ("streamScope: a collection nothing tiers still matters", CS,
  'return ns, r.cfg.Manages(ns)', 'return ns, true'),
 ("guardAggregate: back to Manages on the raw collection", AG,
  'if watching, matters := c.r.streamScope(coll, db); matters {',
  'if watching, matters := coll, c.r.cfg.Manages(coll); matters {'),
 ("events_hidden exported under a different name", PE,
  'counter("events_hidden_total"', 'counter("events_hiddenX_total"'),
 ("commands_total falls out of the export", PE,
  'counter("commands_total"', 'counter("commandsX_total"'),
]

survivors = []
for label, rel, old, new in MUTANTS:
    path = os.path.join(REPO, rel)
    src = open(path).read()
    if old not in src:
        print(f"SKIP  (anchor missing)  {label}"); continue
    if src.count(old) != 1:
        print(f"SKIP  (anchor x{src.count(old)})   {label}"); continue
    shutil.copy(path, path + ".bak")
    open(path, "w").write(src.replace(old, new, 1))
    r = subprocess.run(["go", "test", "./internal/router/"], cwd=REPO,
                       capture_output=True, text=True)
    shutil.move(path + ".bak", path)
    out = r.stdout + r.stderr
    if r.returncode == 0:
        print(f"SURVIVED               {label}"); survivors.append(label)
    elif "build failed" in out or "cannot use" in out or "declared and not used" in out or "undefined" in out:
        print(f"INVALID (did not build) {label}")
    else:
        hit = [l for l in out.splitlines() if l.startswith("--- FAIL") or l.startswith("    --- FAIL")]
        print(f"caught                 {label}   <- {hit[0].strip() if hit else 'test failure'}")
print()
print(f"{len(survivors)} survivor(s) of {len(MUTANTS)}")
for s in survivors: print("   ", s)
