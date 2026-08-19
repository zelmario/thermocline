#!/bin/bash
# Write one worker's config, derived from the shared cline.yaml.
#
# A worker is isolated by three things and this file sets all of them: its own
# databases, so no two feeders ever watch the same namespace; its own
# bookkeeping database, so one worker's setup cannot wipe another's watermarks,
# residency holds and singleton markers; and its own ports.
#
# The namespace substitution is why the databases have to be renamed rather than
# the collections prefixed. Two feeders watching one namespace is not merely
# duplicated work: the second feeder sees the first worker's eviction as an
# unannounced delete, because the announcement is in the other worker's
# bookkeeping database, and tombstones a document that was only being tiered.
#
#   DB DB2 META PORT PDASH   are read from the environment
#   $1  the shared config to derive from
#   $2  where to write this worker's config
set -e
SRC=${1:?usage: mkworker.sh SRC DST}
DST=${2:?usage: mkworker.sh SRC DST}

sed -e "s|\"shop\.|\"${DB}.|g" \
    -e "s|\"wh\.|\"${DB2}.|g" \
    -e "s|listen: \"0.0.0.0:27100\"|listen: \"0.0.0.0:${PORT}\"|" \
    -e "s|dashboard: \"0.0.0.0:27110\"|dashboard: \"0.0.0.0:${PDASH}\"|" \
    "$SRC" > "$DST"

# Appended rather than inserted: meta is a top-level key, so the end of the file
# is as good a place as any and there is no anchor to get wrong.
cat >> "$DST" <<YAML

# Set by mkworker.sh. Everything this worker records about itself lives here,
# which is what lets the suites keep wiping watermarks and residency wholesale
# without reaching another worker's state.
meta:
  db: "${META}"
YAML

# Assert what the config should say rather than what it should not. The first
# version of this check looked for the shared database and port instead, and
# fired on the one worker whose port legitimately equalled the default: a guard
# that reports a problem the run does not have costs as much as one that misses
# a problem it does.
for want in "\"${DB}." "listen: \"0.0.0.0:${PORT}\"" "dashboard: \"0.0.0.0:${PDASH}\"" "db: \"${META}\""; do
  grep -qF "$want" "$DST" || {
    echo "mkworker: $DST does not carry $want, so this worker is not isolated" >&2
    exit 1; }
done
# And nothing of the shared layout may survive into a worker's own config.
grep -qE '"(shop|wh)\.' "$DST" && {
  echo "mkworker: $DST still tiers the shared databases" >&2; exit 1; }
exit 0
