#!/bin/bash
# Brings parked staging back with the server types in terraform.tfvars: the
# archive's servers mount their volumes again and its replica set comes back
# with its data, the fast tier and the app node start fresh. Then bootstrap.
# Measured 2026-10-22 with 200 million orders: 63s of apply, 72s of bootstrap.
set -euo pipefail
cd "$(dirname "$0")"
bash tf.sh apply -auto-approve | tail -1
for ip in $(bash tf.sh output -json public_ips | jq -r '.[]'); do
  # Hetzner reuses addresses, so a key from an earlier server is stale.
  ssh-keygen -f .known_hosts -R "$ip" >/dev/null 2>&1 || true
done
bash bootstrap.sh
