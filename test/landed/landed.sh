#!/bin/bash
# Is a write the client was told failed ever there afterwards? Through a router
# and straight to the replica set, step-downs every 20s. See main.go.
#
#   bash test/landed/landed.sh
#   FOR=5m bash test/landed/landed.sh
#   BIN=/root/cline.old bash test/landed/landed.sh
#   TERMINATE_AUTH=1 SKIP_DIRECT=1 bash test/landed/landed.sh
NODE=${NODE:-thermocline-zelmar-node0}
FOR=${FOR:-2m}; BIN=${BIN:-cline}
CFG=/root/landed.yaml
HOT='mongodb://dba:secret@172.24.0.5:27017,172.24.0.6:27017,172.24.0.7:27017/?replicaSet=rsHot&authSource=admin'
COLD='mongodb://dba:secret@172.24.0.2:27017,172.24.0.3:27017,172.24.0.4:27017/?replicaSet=rsCold&authSource=admin'
R='mongodb://dba:secret@127.0.0.1:27900/?authSource=admin'
N() { docker exec "$NODE" bash -c "$1"; }
ND() { docker exec -d "$NODE" bash -c "$1"; }
Q() { docker exec "$NODE" mongosh "$1" --quiet --eval "$2" 2>/dev/null | tail -1; }
stop() { N "for p in \$(pgrep -f '[c]onfig /root/landed'); do kill -9 \$p; done" >/dev/null 2>&1; }

CGO_ENABLED=0 go build -buildvcs=false -o bin/landed ./test/landed || exit 2
docker cp bin/landed "$NODE":/usr/local/bin/landed >/dev/null
sed -e 's/_thermocline_shared/_thermocline_landed/' -e 's/id: "shared"/id: "landed"/' \
    -e 's/feeder_id: "shared"/feeder_id: "landed"/' -e 's/shopC\.orders/landed.docs/' \
    -e '/- ns: "shopC\.order_events"/,+1d' -e '/shopC\.order_events/d' -e 's/27700/27900/' -e 's/27710/27910/' \
    test/store/shared.yaml > /tmp/landed.$$.yaml
# TERMINATE_AUTH=1 is the other shape: the router owns the upstream
# connections and replays a retryable write itself.
[ -n "${TERMINATE_AUTH:-}" ] && sed -i 's/^  listen:/  terminate_auth: true\n  listen:/' /tmp/landed.$$.yaml
docker cp /tmp/landed.$$.yaml "$NODE":$CFG >/dev/null; rm -f /tmp/landed.$$.yaml
N "grep -q 27900 $CFG && ! grep -q shopC $CFG" || { echo "FATAL could not derive the config"; exit 2; }

stop; sleep 1
for u in "$HOT" "$COLD"; do Q "$u" 'db.getSiblingDB("landed").dropDatabase(); db.getSiblingDB("_thermocline_landed").dropDatabase()' >/dev/null; done
Q "$HOT" 'db.getSiblingDB("landed").createCollection("docs")' >/dev/null
Q "$COLD" 'db.getSiblingDB("landed").createCollection("docs")' >/dev/null
N "mkdir -p /root/landedlog; rm -f /root/landedlog/*"
ND "$BIN feeder --config $CFG --log-file /root/landedlog/feeder.log"
sleep 4
ND "$BIN router ${ROUTER_FLAGS:-} --config $CFG --log-file /root/landedlog/router.log"
for _ in $(seq 1 30); do [ "$(Q "$R" 'print(db.adminCommand({ping:1}).ok)')" = 1 ] && break; sleep 1; done
[ "$(Q "$R" 'print(db.adminCommand({ping:1}).ok)')" = 1 ] || { echo "FATAL the router never answered"; N "tail -3 /root/landedlog/*.log"; stop; exit 2; }
N "readlink -f /proc/\$(pgrep -f '[r]outer .*--config /root/landed')/exe"

echo "== through the router =="
N "landed -uri '$R' -hot '$HOT' -for $FOR -ns landed.docs ${ARGS:-}"
[ -n "${SKIP_DIRECT:-}" ] && { stop; exit 0; }
echo "== straight to the replica set =="
N "landed -uri '$HOT' -hot '$HOT' -for $FOR -ns landed.direct ${ARGS:-}"
stop
