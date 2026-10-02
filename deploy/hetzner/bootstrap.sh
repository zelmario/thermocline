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

echo "== waiting for cloud-init on every node"
for n in hot1 hot2 hot3 cold1 cold2 cold3 app; do
  for _ in $(seq 1 60); do $SSH root@"$(ip $n)" true 2>/dev/null && break; sleep 5; done
  st=$($SSH root@"$(ip $n)" 'cloud-init status --wait >/dev/null 2>&1; cloud-init status' | awk '/status:/{print $2}')
  echo "  $n $st"
  [ "$st" = "done" ] || { $SSH root@"$(ip $n)" 'cat /root/PSMDB_MISSING 2>/dev/null; tail -20 /var/log/cloud-init-output.log'; exit 1; }
done

echo "== one release on all six"
vers=$(for n in hot1 hot2 hot3 cold1 cold2 cold3; do $SSH root@"$(ip $n)" cat /root/PSMDB_INSTALLED; done | sort -u)
echo "  $vers"
[ "$(echo "$vers" | wc -l)" = "1" ] || { echo "the nodes run different releases" >&2; exit 1; }

initiate() { # node set m1 m2 m3
  local n=$1 set=$2
  $SSH root@"$(ip $n)" "mongosh --quiet --eval '
    try { rs.status(); print(\"already initiated\") } catch (e) {
      rs.initiate({_id: \"$set\", members: [
        {_id: 0, host: \"$3:27017\", priority: 2},
        {_id: 1, host: \"$4:27017\"},
        {_id: 2, host: \"$5:27017\"}]}) }
    for (let i = 0; i < 120 && !db.hello().isWritablePrimary; i++) sleep(500);
    print(\"writable primary: \" + db.hello().isWritablePrimary)'"
  # The localhost exception: the first user may be created from the node itself,
  # once, before any user exists.
  $SSH root@"$(ip $n)" "mongosh --quiet --eval '
    try { db.getSiblingDB(\"admin\").createUser({user: \"dba\", pwd: \"$PASS\", roles: [\"root\"]}); print(\"dba created\") }
    catch (e) { print(\"dba: \" + e.codeName) }'"
}
echo "== rsHot";  initiate hot1  rsHot  10.20.1.11 10.20.1.12 10.20.1.13
echo "== rsCold"; initiate cold1 rsCold 10.20.1.21 10.20.1.22 10.20.1.23

echo "== cline on the app node"
(cd "$REPO" && CGO_ENABLED=0 go build -buildvcs=false -o bin/cline ./cmd/cline)
scp -q -o UserKnownHostsFile=$PWD/.known_hosts -o BatchMode=yes "$REPO/bin/cline" root@"$(ip app)":/usr/local/bin/cline
sed -e "s|^  uri: \"mongodb://rs-fast-1[^\"]*\"|  uri: \"$HOT_URI\"|" \
    -e "s|^  uri: \"mongodb://rs-cold-1[^\"]*\"|  uri: \"$COLD_URI\"|" \
    "$REPO/cline.example.yaml" > .cline.yaml
grep -q 10.20.1.11 .cline.yaml && grep -q 10.20.1.21 .cline.yaml || { echo "could not write the URIs into cline.yaml" >&2; exit 1; }
scp -q -o UserKnownHostsFile=$PWD/.known_hosts -o BatchMode=yes .cline.yaml root@"$(ip app)":/etc/thermocline/cline.yaml
rm -f .cline.yaml
$SSH root@"$(ip app)" "chmod 0640 /etc/thermocline/cline.yaml && cline version 2>/dev/null || true
  for u in '$HOT_URI' '$COLD_URI'; do mongosh \"\$u\" --quiet --eval 'print(db.hello().setName + \" \" + db.version() + \" primary \" + db.hello().primary)'; done"
echo "== ready: ssh root@$(ip app), config in /etc/thermocline/cline.yaml"
