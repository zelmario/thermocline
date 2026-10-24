#!/bin/bash
# Turn a coverage sweep into the map test/suitepick reads.
#
#   COVER=1 psweep.sh, run in the node with a binary built by
#   `go build -cover -covermode=atomic -coverpkg=./...`, leaves one counter directory per suite
#   under $OUT/cov. Copy $OUT back here and run:
#
#   test/mkcovmap.sh DIR      DIR holds cov/<suite>/ and summary.txt
#
# It writes test/suitemap/funcs.tsv, every function each suite executed at
# least once, and test/suitemap/times.tsv, how long each suite took in that
# sweep. Function names, not line numbers, so the map stays usable while the
# code moves under it; rebuild it when suites are added or a batch lands.
set -euo pipefail
DIR=${1:?usage: mkcovmap.sh DIR}
OUTD=$(dirname "$0")/suitemap
MOD=$(awk '/^module /{print $2; exit}' "$(dirname "$0")/../go.mod")
mkdir -p "$OUTD"

{
  echo "# suite	file	function, from $(basename "$DIR") on $(date -u +%Y-%m-%d)"
  for d in "$DIR"/cov/*/; do
    s=$(basename "$d")
    # A suite whose daemons were all killed with -9 wrote nothing, and would
    # read as reaching no code at all. Said, rather than left as a gap.
    if ! ls "$d"covmeta.* >/dev/null 2>&1; then
      echo "mkcovmap: $s wrote no coverage" >&2; continue
    fi
    go tool covdata func -i "$d" |
      awk -v s="$s" -v mod="$MOD/" '$NF != "0.0%" && $1 != "total" {
        split($1, a, ":"); f = a[1]; sub("^" mod, "", f); print s "\t" f "\t" $2 }'
  done
} > "$OUTD/funcs.tsv"

{
  echo "# suite seconds, from $(basename "$DIR")"
  awk '$3 ~ /^rc=/ { sub("s$", "", $4); print $1, $4 }' "$DIR/summary.txt"
} > "$OUTD/times.tsv"

echo "mkcovmap: $(grep -vc '^#' "$OUTD/funcs.tsv") suite-function pairs, $(grep -vc '^#' "$OUTD/times.tsv") suites timed"
