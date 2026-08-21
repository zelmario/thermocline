"""Print every question the router and the archive answered differently.

Both harnesses emit a JSON list of records whose "v" is the answer. agree.js
keys them by namespace and question, aggs.js by question alone, so the key here
is whatever identifies a record apart from its answer.

Exits non-zero when anything disagreed, so setup.sh can be run from something
that checks, and exits 2 when an answer set could not be read at all. A harness
that failed to run prints its error instead of JSON, and reporting "no
disagreements" for that would be the worst answer available.
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


ra = {key(x): x["v"] for x in router}
aa = {key(x): x["v"] for x in archive}
keys = sorted(set(ra) | set(aa))
diff = [k for k in keys if ra.get(k) != aa.get(k)]

print(f"  {name}: {len(keys)} questions asked, {len(diff)} disagreements")
for k in diff:
    print("    " + " ".join(str(p) for p in k))
    print(f"      router  {str(ra.get(k))[:160]}")
    print(f"      archive {str(aa.get(k))[:160]}")

sys.exit(1 if diff else 0)
