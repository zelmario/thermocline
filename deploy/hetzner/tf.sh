#!/bin/bash
# Terraform with the Hetzner token taken from ~/.config/hcloud/token, so it is
# never typed, never in shell history and never in a file Terraform writes.
#
#   bash tf.sh init
#   bash tf.sh plan
#   bash tf.sh apply
#   bash tf.sh destroy
set -euo pipefail
cd "$(dirname "$0")"
TOKEN_FILE=${TOKEN_FILE:-$HOME/.config/hcloud/token}
[ -r "$TOKEN_FILE" ] || { echo "no token at $TOKEN_FILE" >&2; exit 2; }
HCLOUD_TOKEN=$(cat "$TOKEN_FILE") exec "${TERRAFORM:-terraform}" "$@"
