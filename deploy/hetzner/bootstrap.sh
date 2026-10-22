#!/bin/bash
# After `bash tf.sh apply`: waits for every node to finish installing, checks
# they all run the same release, initiates both replica sets, creates the dba
# account, and puts cline and a cline.yaml on the app node.
set -euo pipefail
cd "$(dirname "$0")"
TF=${TERRAFORM:-terraform}
REPO=$(cd ../.. && pwd)
SSH="ssh -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$PWD/.known_hosts -o ConnectTimeout=10 -o BatchMode=yes"

ips=$($TF output -json public_ips)
ip() { echo "$ips" | jq -r --arg k "$1" '.[$k]'; }
PASS=$($TF output -raw dba_password)
HOT_URI=$($TF output -raw hot_uri)
COLD_URI=$($TF output -raw cold_uri)
BIG_URI=$($TF output -raw big_uri)
# The database nodes this state has, so a third set is waited for and
# initiated only when it exists. See big_server_type.
DBS=$(echo "$ips" | jq -r 'keys[] | select(. != "app")' | sort)

echo "== waiting for cloud-init on every node"
for n in $DBS app; do
  for _ in $(seq 1 60); do $SSH root@"$(ip $n)" true 2>/dev/null && break; sleep 5; done
  st=$($SSH root@"$(ip $n)" 'cloud-init status --wait >/dev/null 2>&1; cloud-init status' | awk '/status:/{print $2}')
  echo "  $n $st"
  [ "$st" = "done" ] || { $SSH root@"$(ip $n)" 'cat /root/PSMDB_MISSING 2>/dev/null; tail -20 /var/log/cloud-init-output.log'; exit 1; }
done

echo "== one release on every database node"
vers=$(for n in $DBS; do $SSH root@"$(ip $n)" cat /root/PSMDB_INSTALLED; done | sort -u)
echo "  $vers"
[ "$(echo "$vers" | wc -l)" = "1" ] || { echo "the nodes run different releases" >&2; exit 1; }

initiate() { # node set m1 m2 m3
  local n=$1 set=$2
  $SSH root@"$(ip $n)" "mongosh --quiet --eval '
    if (db.hello().setName) { print(\"already initiated\") } else {
      rs.initiate({_id: \"$set\", members: [
        {_id: 0, host: \"$3:27017\", priority: 2},
        {_id: 1, host: \"$4:27017\"},
        {_id: 2, host: \"$5:27017\"}]}) }
    for (let i = 0; i < 120 && !db.hello().isWritablePrimary; i++) sleep(500);
    print(\"writable primary: \" + db.hello().isWritablePrimary)'"
  # hello needs no authentication, which is why it is the question above:
  # rs.status() does once dba exists, so a second run took the catch and tried
  # to initiate again.
  # The localhost exception: the first user may be created from the node itself,
  # once, before any user exists.
  $SSH root@"$(ip $n)" "mongosh --quiet --eval '
    try { db.getSiblingDB(\"admin\").createUser({user: \"dba\", pwd: \"$PASS\", roles: [\"root\"]}); print(\"dba created\") }
    catch (e) { print(\"dba: \" + e.codeName) }'"
}
HAS_HOT=$(echo "$DBS" | grep -qx hot1 && echo yes || true)
if [ -n "$HAS_HOT" ]; then echo "== rsHot";  initiate hot1  rsHot  10.20.1.11 10.20.1.12 10.20.1.13; fi
if echo "$DBS" | grep -qx cold1; then echo "== rsCold"; initiate cold1 rsCold 10.20.1.21 10.20.1.22 10.20.1.23; fi
if [ -n "$BIG_URI" ]; then echo "== rsBig"; initiate big1 rsBig 10.20.1.31 10.20.1.32 10.20.1.33; fi

echo "== cline on the app node"
(cd "$REPO" && CGO_ENABLED=0 go build -buildvcs=false -o bin/cline ./cmd/cline)
scp -q -o UserKnownHostsFile=$PWD/.known_hosts -o BatchMode=yes "$REPO/bin/cline" root@"$(ip app)":/usr/local/bin/cline
# Escaped for the replacement side of sed, where & means the matched text: the
# URIs carry &authSource, and unescaped each one came out with the example's
# own uri line pasted into the middle of it. The check below looked for the
# addresses, which were there, so it said nothing.
esc() { printf '%s' "$1" | sed 's/[&|\\]/\\&/g'; }
sed -e "s|^  uri: \"mongodb://rs-fast-1[^\"]*\"|  uri: \"$(esc "$HOT_URI")\"|" \
    -e "s|^  uri: \"mongodb://rs-cold-1[^\"]*\"|  uri: \"$(esc "$COLD_URI")\"|" \
    "$REPO/cline.example.yaml" > .cline.yaml
grep -Fqx "  uri: \"$HOT_URI\"" .cline.yaml && grep -Fqx "  uri: \"$COLD_URI\"" .cline.yaml ||
  { echo "could not write the URIs into cline.yaml" >&2; exit 1; }
scp -q -o UserKnownHostsFile=$PWD/.known_hosts -o BatchMode=yes .cline.yaml root@"$(ip app)":/etc/thermocline/cline.yaml
rm -f .cline.yaml
# The baseline's URI beside the config, not in it: nothing Thermocline runs
# may ever be pointed at it.
if [ -n "$BIG_URI" ]; then
  $SSH root@"$(ip app)" "umask 077; printf '%s\n' '$BIG_URI' > /etc/thermocline/big.uri"
fi
$SSH root@"$(ip app)" "chmod 0640 /etc/thermocline/cline.yaml && cline version 2>/dev/null || true
  for u in ${HAS_HOT:+'$HOT_URI'} '$COLD_URI' ${BIG_URI:+'$BIG_URI'}; do mongosh \"\$u\" --quiet --eval 'print(db.hello().setName + \" \" + db.version() + \" primary \" + db.hello().primary)'; done"
echo "== ready: ssh root@$(ip app), config in /etc/thermocline/cline.yaml"
