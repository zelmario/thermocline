#!/bin/bash
# TLS, which the address this proxy takes over may already require.
#
# Runs on the HOST and builds its own pair, because the anydbver nodes speak
# plaintext and a TLS fast tier is the point:
#
#   bash test/testtls.sh
#
# An application whose connection string says tls=true cannot be moved onto a
# plaintext proxy at all: the driver insists and nothing connects. Terminating
# TLS in front works and puts a second process in the path of every query, which
# is a strange thing to require of something whose whole purpose is to be
# transparent.
#
# Two halves. Clients arrive over TLS, so the listener presents a certificate.
# And the fast tier may require TLS of its own: the driver handles that for the
# connections it owns, but the router also proxies raw bytes over a connection it
# opens itself, and that one has to be wrapped the same way or it speaks
# plaintext at a server that will not answer.
BIN=${BIN:-./bin/cline}
DIR=$(mktemp -d)
HOTPORT=${HOTPORT:-28050}
COLDPORT=${COLDPORT:-28051}
PORT=${PORT:-27820}
PASS=0; FAIL=0; declare -a FAILED
chk() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf '  PASS  %-46s %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  FAIL  %-46s got=%.90s want=%s\n' "$1" "$3" "$2"; fi
}
cleanup() {
  pkill -9 -f "cline.*$DIR" 2>/dev/null
  mongosh --tls --tlsCAFile "$DIR/ca.pem" --tlsCertificateKeyFile "$DIR/srv-combined.pem" --quiet --port $HOTPORT --eval 'db.getSiblingDB("admin").shutdownServer()' >/dev/null 2>&1
  mongosh --quiet --port $COLDPORT --eval 'db.getSiblingDB("admin").shutdownServer()' >/dev/null 2>&1
  sleep 1
  rm -rf "$DIR"
}
trap cleanup EXIT

echo "== setup =="
# A certificate authority and a server certificate signed by it. mongod refuses
# TLS without a chain of trust: "The use of TLS without specifying a chain of
# trust is no longer supported", SERVER-72839.
openssl req -x509 -newkey rsa:2048 -sha256 -days 30 -nodes -keyout "$DIR/ca-key.pem" -out "$DIR/ca.pem" \
  -subj "/CN=thermocline-test-ca" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$DIR/srv-key.pem" -out "$DIR/srv.csr" \
  -subj "/CN=127.0.0.1" >/dev/null 2>&1
printf 'subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth\n' > "$DIR/srv.ext"
openssl x509 -req -in "$DIR/srv.csr" -CA "$DIR/ca.pem" -CAkey "$DIR/ca-key.pem" -CAcreateserial \
  -out "$DIR/srv.pem" -days 30 -sha256 -extfile "$DIR/srv.ext" >/dev/null 2>&1
cat "$DIR/srv.pem" "$DIR/srv-key.pem" > "$DIR/srv-combined.pem"
chk "a certificate was issued" "true" "$([ -s "$DIR/srv.pem" ] && echo true || echo false)"

mkdir -p "$DIR/hot" "$DIR/cold"
mongod --replSet tlshot --port $HOTPORT --dbpath "$DIR/hot" --bind_ip 127.0.0.1 --fork \
  --logpath "$DIR/hot.log" --wiredTigerCacheSizeGB 0.25 --tlsMode requireTLS \
  --tlsCertificateKeyFile "$DIR/srv-combined.pem" --tlsCAFile "$DIR/ca.pem" \
  --tlsAllowConnectionsWithoutCertificates >/dev/null 2>&1
mongod --replSet tlscold --port $COLDPORT --dbpath "$DIR/cold" --bind_ip 127.0.0.1 --fork \
  --logpath "$DIR/cold.log" --wiredTigerCacheSizeGB 0.25 >/dev/null 2>&1
sleep 3
mongosh --tls --tlsCAFile "$DIR/ca.pem" --quiet --port $HOTPORT --eval \
  "rs.initiate({_id:'tlshot',members:[{_id:0,host:'127.0.0.1:$HOTPORT'}]})" >/dev/null 2>&1
mongosh --quiet --port $COLDPORT --eval \
  "rs.initiate({_id:'tlscold',members:[{_id:0,host:'127.0.0.1:$COLDPORT'}]})" >/dev/null 2>&1
sleep 6
chk "the fast tier requires TLS" "true" \
    "$(mongosh --tls --tlsCAFile "$DIR/ca.pem" --quiet --port $HOTPORT --eval 'print(db.hello().isWritablePrimary)' 2>&1 | tail -1)"
chk "and refuses a plaintext client" "true" \
    "$(timeout 20 mongosh --quiet --port $HOTPORT --eval 'print("connected")' >/dev/null 2>&1 && echo false || echo true)"

# router.freshness: permissive. The documents are planted in the archive with
# no feeder running, and nothing mirroring can never be shown fresh, so the
# default since 2026-10-12, strict, refuses the reads this suite checks.
# testfresh tests strict itself.
cat > "$DIR/cline.yaml" <<YAML
hot:
  uri: "mongodb://127.0.0.1:$HOTPORT/?replicaSet=tlshot&tls=true&tlsCAFile=$DIR/ca.pem"
cold:
  uri: "mongodb://127.0.0.1:$COLDPORT/?replicaSet=tlscold"
log:
  file: ""
feeder:
  id: "tlscheck"
  namespaces:
    - "probe.tiered"
  checkpoint_every: 1s
router:
  listen: "127.0.0.1:$PORT"
  dashboard: ""
  completeness: archive
  freshness: permissive
  tls:
    cert: "$DIR/srv.pem"
    key: "$DIR/srv-key.pem"
    ca: "$DIR/ca.pem"
YAML

# Two documents on the fast tier, two more only in the archive, so a read has to
# cross both TLS connections to come back whole.
mongosh --tls --tlsCAFile "$DIR/ca.pem" --quiet --port $HOTPORT --eval \
  'db.getSiblingDB("probe").tiered.insertMany([{_id:1,v:"hot"},{_id:2,v:"hot"}])' >/dev/null 2>&1
mongosh --quiet --port $COLDPORT --eval \
  'db.getSiblingDB("probe").tiered.insertMany([{_id:1,v:"hot"},{_id:2,v:"hot"},{_id:3,v:"archived"},{_id:4,v:"archived"}])' >/dev/null 2>&1

$BIN router --config "$DIR/cline.yaml" --log-file "$DIR/router.log" >/dev/null 2>&1 &
for _ in $(seq 1 30); do
  grep -q 'router listening' "$DIR/router.log" 2>/dev/null && break
  sleep 1
done
if ! grep -q 'router listening' "$DIR/router.log" 2>/dev/null; then
  echo "  FATAL  the router never came up"; tail -3 "$DIR/router.log"; exit 2
fi

echo "== what the router says it is =="
chk "the startup line names both sides"  "true" \
    "$(grep -q 'clients: TLS.*fast tier: TLS' "$DIR/router.log" && echo true || echo false)"

echo "== a client over TLS =="
TLSURI="mongodb://127.0.0.1:$PORT/?tls=true&tlsCAFile=$DIR/ca.pem&directConnection=true"
chk "a read crosses both TLS connections" "4" \
    "$(mongosh "$TLSURI" --quiet --eval 'print(db.getSiblingDB("probe").tiered.countDocuments({}))' 2>&1 | tail -1)"
chk "a write goes through and reads back" "written" \
    "$(mongosh "$TLSURI" --quiet --eval 'const c=db.getSiblingDB("probe").tiered; c.insertOne({_id:77,v:"written"}); const d=c.findOne({_id:77}); print(d?d.v:"MISSING")' 2>&1 | tail -1)"
chk "and it landed on the fast tier"      "written" \
    "$(mongosh --tls --tlsCAFile "$DIR/ca.pem" --quiet --port $HOTPORT --eval 'const d=db.getSiblingDB("probe").tiered.findOne({_id:77}); print(d?d.v:"MISSING")' 2>&1 | tail -1)"

echo "== and a plaintext client is not answered =="
chk "plaintext gets no answer"            "true" \
    "$(timeout 20 mongosh "mongodb://127.0.0.1:$PORT/?directConnection=true" --quiet --eval 'print("connected")' >/dev/null 2>&1 && echo false || echo true)"

echo "== a fast tier that requires a client certificate =="
# The driver presents tlsCertificateKeyFile from hot.uri on its own
# connections; the router's raw one presented nothing, so against a fast tier
# that requires a certificate every client through the router failed.
pkill -f "router --config $DIR/cline.yaml" 2>/dev/null
mongosh --tls --tlsCAFile "$DIR/ca.pem" --quiet --port $HOTPORT --eval 'db.getSiblingDB("admin").shutdownServer({force:true})' >/dev/null 2>&1
sleep 3
mongod --replSet tlshot --port $HOTPORT --dbpath "$DIR/hot" --bind_ip 127.0.0.1 --fork \
  --logpath "$DIR/hot2.log" --wiredTigerCacheSizeGB 0.25 --tlsMode requireTLS \
  --tlsCertificateKeyFile "$DIR/srv-combined.pem" --tlsCAFile "$DIR/ca.pem" >/dev/null 2>&1
sleep 6
chk "it refuses a client with no certificate" "true" \
    "$(timeout 20 mongosh --tls --tlsCAFile "$DIR/ca.pem" --quiet --port $HOTPORT --eval 'print(db.hello().ok)' >/dev/null 2>&1 && echo false || echo true)"
sed "s#tls=true&tlsCAFile=$DIR/ca.pem#tls=true\&tlsCAFile=$DIR/ca.pem\&tlsCertificateKeyFile=$DIR/srv-combined.pem#" \
  "$DIR/cline.yaml" > "$DIR/cline-cert.yaml"
rm -f "$DIR/router2.log"
$BIN router --config "$DIR/cline-cert.yaml" --log-file "$DIR/router2.log" >/dev/null 2>&1 &
for _ in $(seq 1 30); do
  grep -q 'router listening' "$DIR/router2.log" 2>/dev/null && break
  sleep 1
done
chk "a read through the router presents the certificate" "4" \
    "$(mongosh "$TLSURI" --quiet --eval 'print(db.getSiblingDB("probe").tiered.countDocuments({_id:{$lte:4}}))' 2>&1 | tail -1)"
pkill -f "router --config $DIR/cline-cert.yaml" 2>/dev/null

echo
echo "================================"
echo "PASS $PASS   FAIL $FAIL"
[ $FAIL -gt 0 ] && printf 'failed: %s\n' "${FAILED[*]}"
exit $FAIL
