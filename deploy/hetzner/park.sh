#!/bin/bash
# Parks staging between tests: stops mongod cleanly on the archive nodes,
# unmounts their data volumes, and removes every server. The volumes stay, so
# the archive's data does not have to be loaded again; only they are paid for.
# Bring it back with unpark.sh.
set -euo pipefail
cd "$(dirname "$0")"
SSH="ssh -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$PWD/.known_hosts -o ConnectTimeout=15 -o BatchMode=yes"
ips=$(bash tf.sh output -json public_ips)
for n in $(echo "$ips" | jq -r 'keys[] | select(startswith("cold"))'); do
  # Stopped and unmounted before the server goes, so the volume is clean when
  # a new server mounts it.
  $SSH root@"$(echo "$ips" | jq -r --arg k "$n" '.[$k]')" 'systemctl stop mongod; sync; umount /var/lib/mongodb' && echo "  $n stopped and unmounted"
done
bash tf.sh apply -auto-approve -var cold_server_type= -var hot_server_type= -var big_server_type= -var app_server_type= | tail -1
echo "  servers left: $(bash tf.sh state list | grep -c '^hcloud_server' || true), volumes kept: $(bash tf.sh state list | grep -c '^hcloud_volume\.' || true)"
