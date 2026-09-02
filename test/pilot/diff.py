"""Print every question the router and the archive answered differently.

Both harnesses emit a JSON list of records whose "v" is the answer. agree.js
keys them by namespace and question, aggs.js by question alone, so the key here
is whatever identifies a record apart from its answer.

Exits non-zero when anything disagreed, so setup.sh can be run from something
that checks, and exits 2 when an answer set could not be read at all. A harness
that failed to run prints its error instead of JSON, and reporting "no
disagreements" for that would be the worst answer available.

A refusal is counted apart from a disagreement, and does not fail the run.
That is this project's standing rule rather than a convenience: a refusal is a
limitation and a wrong answer is a bug. The router refuses a pipeline it cannot
answer whole, saying so and naming what is missing, and the archive answers the
same question because nothing refuses anything there. Reporting that as a wrong
answer buries the real ones, which is what it did: two of the nineteen
aggregations are a join onto an untiered collection, refused by design since
2026-09-01, and they sat in the disagreement list every run.

So that it can never go quiet, every refusal is printed in full with the
router's own words, and the count is in the headline beside the questions. A
refusal that was not there before shows up in one line.
"""

import json
import sys

name = sys.argv[1]
try:
    router = json.load(open(f"/tmp/{name}_router.json"))
    archive = json.load(open(f"/tmp/{name}_archive.json"))
except Exception as exc:
    print(f"  {name}: could not read an answer set, so nothing was compared: {exc}")
    sys.exit(2)


def key(rec):
    return tuple(v for k, v in sorted(rec.items()) if k != "v")


def refusal(v):
    # What the harnesses record for a command that came back as an error, and
    # the only errors this system raises on purpose say so in these words.
    return isinstance(v, str) and v.startswith("ERR ") and "thermocline: refusing" in v


ra = {key(x): x["v"] for x in router}
aa = {key(x): x["v"] for x in archive}
keys = sorted(set(ra) | set(aa))
differ = [k for k in keys if ra.get(k) != aa.get(k)]
refused = [k for k in differ if refusal(ra.get(k))]
diff = [k for k in differ if k not in refused]

line = f"  {name}: {len(keys)} questions asked, {len(diff)} disagreements"
if refused:
    line += f", {len(refused)} refused"
print(line)
for k in diff:
    print("    " + " ".join(str(p) for p in k))
    print(f"      router  {str(ra.get(k))[:160]}")
    print(f"      archive {str(aa.get(k))[:160]}")
for k in refused:
    print("    REFUSED  " + " ".join(str(p) for p in k))
    print(f"      {ra.get(k)}")

sys.exit(1 if diff else 0)
