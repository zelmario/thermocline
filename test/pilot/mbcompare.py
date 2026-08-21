"""Compare every number Metabase produces against the archive.

Metabase writes the queries. For each collection it can see, this asks it for a
count and for the distributions an X-ray generates, then asks the archive the
same questions directly and diffs. Nothing here asserts a value chosen by hand.

The archive side excludes tombstones, because the archive keeps a record for
every document the application deleted and the router hides them. Without that
the control answers with rows the router is right to withhold.

Exits non-zero if anything disagreed.
"""

import json
import os
import subprocess
import sys

MB = os.environ.get("MB_URL", "http://127.0.0.1:3000")
COOK = os.environ.get("MB_COOKIES", "/tmp/pilot-metabase/cookies")
NODE = os.environ.get("NODE", "thermocline-zelmar-node0")
COLD = os.environ.get(
    "TC_COLD",
    "mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017"
    "/?replicaSet=rsCold&authSource=admin",
)


def mb(path, method="GET", body=None):
    cmd = ["curl", "-s", "-b", COOK, "-X", method, MB + path]
    if body is not None:
        cmd += ["-H", "Content-Type: application/json", "-d", json.dumps(body)]
    out = subprocess.run(cmd, capture_output=True, text=True).stdout
    try:
        return json.loads(out)
    except Exception:
        return {"__unparseable": out[:200]}


def dataset(dbid, query):
    d = mb("/api/dataset", "POST", {"database": dbid, "type": "query", "query": query})
    if d.get("status") != "completed":
        return "ERROR " + str(d.get("error") or d.get("__unparseable"))[:120]
    return d["data"]["rows"]


def archive(js):
    out = subprocess.run(
        ["docker", "exec", NODE, "mongosh", COLD, "--quiet", "--eval", js],
        capture_output=True, text=True).stdout.strip()
    return out.splitlines()[-1] if out else ""


agree = disagree = 0


def check(label, got, want):
    """Compare what the two answered, not how they printed it.

    Both sides emit JSON and they punctuate it differently: json.dumps writes
    ", " where mongosh's JSON.stringify writes ",". Comparing the strings
    reported five disagreements where every number was equal, which is a check
    lying in the direction that wastes an afternoon rather than the direction
    that hides a bug, but lying either way.
    """
    global agree, disagree

    def parsed(v):
        try:
            return json.loads(v) if isinstance(v, str) else v
        except Exception:
            return v

    ok = parsed(got) == parsed(want) or str(got) == str(want)
    if ok:
        agree += 1
    else:
        disagree += 1
    print(f"[{' ok ' if ok else 'DIFF'}] {label:44} metabase={str(got)[:60]}  archive={str(want)[:60]}")


dbs = mb("/api/database")
dbs = dbs["data"] if isinstance(dbs, dict) else dbs
for d in dbs:
    if d.get("engine") != "mongo":
        continue
    dbid, dbname = d["id"], d["details"]["dbname"]
    meta = mb(f"/api/database/{dbid}/metadata")
    for t in sorted(meta.get("tables", []), key=lambda x: x["name"]):
        ns = f"{dbname}.{t['name']}"
        rows = dataset(dbid, {"source-table": t["id"], "aggregation": [["count"]]})
        got = rows[0][0] if isinstance(rows, list) and rows and isinstance(rows[0], list) else rows
        check(f"{ns} count", got,
              archive(f'print(db.getSiblingDB("{dbname}")["{t["name"]}"]'
                      f'.countDocuments({{_tcDeletedAt:{{$exists:false}}}}))'))

        # A date field, if the collection has one, gives the distributions an
        # X-ray builds. Those read every document and are the shape that goes
        # wrong quietly when most of them are on the other tier.
        date_field = next((f for f in t.get("fields", [])
                           if f.get("base_type") in ("type/Instant", "type/DateTime")), None)
        if not date_field:
            continue
        rows = dataset(dbid, {"source-table": t["id"], "aggregation": [["count"]],
                              "breakout": [["field", date_field["id"], {"temporal-unit": "year"}]],
                              "order-by": [["asc", ["field", date_field["id"], {"temporal-unit": "year"}]]],
                              "limit": 3})
        # A document with no date groups under null, which is a real answer and
        # not a reason to stop.
        mbyears = ([[(r[0][:4] if isinstance(r[0], str) else r[0]), r[1]] for r in rows]
                   if isinstance(rows, list) else rows)
        want = archive(
            f'print(JSON.stringify(db.getSiblingDB("{dbname}")["{t["name"]}"].aggregate(['
            f'{{$match:{{_tcDeletedAt:{{$exists:false}}}}}},'
            f'{{$group:{{_id:{{$year:"${date_field["name"]}"}},n:{{$sum:1}}}}}},'
            f'{{$sort:{{_id:1}}}},{{$limit:3}}]).toArray()'
            f'.map(r=>[r._id===null?null:String(r._id),r.n])))')
        check(f"{ns} by year", json.dumps(mbyears), want)

print(f"\n{agree} agree, {disagree} disagree")
sys.exit(1 if disagree else 0)
