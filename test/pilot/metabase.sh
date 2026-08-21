#!/bin/bash
# Point Metabase at the router and check every number it produces.
#
#   bash test/pilot/metabase.sh          start it, add the databases, compare
#   bash test/pilot/metabase.sh compare  just the comparison
#   bash test/pilot/metabase.sh stop     take it down
#
# Metabase is the first client here that nobody wrote for this. It syncs its own
# schema by sampling documents, decides its own field types, and generates its
# own aggregation pipelines with date extraction operators nothing in test/ ever
# sends. What it must not do is produce a number that differs from the archive.
#
# It reaches the router from inside the docker network, on the address the tiers
# use. Note what the connection string does not say: no replicaSet=, which is
# step 2 of the runbook and the first thing an application gets wrong.
set -u
NODE=${NODE:-thermocline-zelmar-node0}
NET=${NET:-thermocline-zelmar-anydbver}
ROUTER_HOST=${ROUTER_HOST:-172.24.0.2}
ROUTER_PORT=${ROUTER_PORT:-27500}
MB=${MB:-http://127.0.0.1:3000}
WORK=${WORK:-/tmp/pilot-metabase}
DBS=${DBS:-"sample_mflix sample_training sample_weatherdata sample_supplies"}
mkdir -p "$WORK"

start() {
  docker rm -f pilot-metabase >/dev/null 2>&1
  docker run -d --name pilot-metabase --network "$NET" -p 3000:3000 metabase/metabase:latest >/dev/null
  echo "waiting for metabase"
  until curl -sf "$MB/api/health" >/dev/null 2>&1; do sleep 5; done

  local token
  token=$(curl -s "$MB/api/session/properties" | python3 -c "import sys,json;print(json.load(sys.stdin)['setup-token'])")
  cat > "$WORK/setup.json" <<EOF
{"token":"$token",
 "user":{"first_name":"Pilot","last_name":"User","email":"pilot@example.com","password":"Thermocline123!","site_name":"Thermocline pilot"},
 "prefs":{"site_name":"Thermocline pilot","allow_tracking":false}}
EOF
  curl -s -c "$WORK/cookies" -X POST "$MB/api/setup" -H 'Content-Type: application/json' \
       -d @"$WORK/setup.json" >/dev/null
  echo "metabase set up"
}

addDatabases() {
  # One Metabase database per Mongo database, which is how its mongo driver
  # works: a connection names one.
  #
  # Each of these logs "Failed to connect to Database: Timed out after 10.0 s"
  # and then works. That is Metabase, not the router: the same test against a
  # mongod directly takes the same 10.048 seconds and also reports valid. It is
  # written down here so nobody spends an afternoon on it.
  for db in $DBS; do
    python3 - "$db" "$ROUTER_HOST" "$ROUTER_PORT" > "$WORK/db.json" <<'PY'
import json, sys
name, host, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
print(json.dumps({
    "engine": "mongo", "name": "router: " + name,
    "details": {"host": host, "port": port, "dbname": name, "user": "dba",
                "pass": "secret", "authdb": "admin", "ssl": False,
                "tunnel-enabled": False, "advanced-options": False},
    "is_full_sync": True, "is_on_demand": False}))
PY
    curl -s -b "$WORK/cookies" -X POST "$MB/api/database" -H 'Content-Type: application/json' \
         -d @"$WORK/db.json" \
      | python3 -c "import sys,json; d=json.load(sys.stdin); print('  added', d.get('name'), 'as id', d.get('id'))"
  done
  echo "waiting for the schema sync"
  sleep 45
}

compare() {
  cp "$(dirname "$0")/mbcompare.py" "$WORK/mbcompare.py"
  MB_URL="$MB" MB_COOKIES="$WORK/cookies" NODE="$NODE" python3 "$WORK/mbcompare.py"
}

case "${1:-all}" in
  compare) compare ;;
  stop)    docker rm -f pilot-metabase >/dev/null 2>&1; echo "stopped" ;;
  all)     start; addDatabases; compare ;;
  *)       echo "usage: $0 [all|compare|stop]"; exit 2 ;;
esac
