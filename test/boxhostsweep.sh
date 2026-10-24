#!/bin/bash
# The host-side suites on the Docker test box, the way quick.sh runs the
# in-node ones there.
#
#   test/boxhostsweep.sh                  ship this working tree, run hostsweep
#   SUITES="testtls testchaos" test/boxhostsweep.sh
#   test/boxhostsweep.sh -wait            wait for a run already started
#
# hostsweep.sh expects the repository root, a built bin/cline and bin/store,
# Go, a host mongod and mongos, and some images. On the box these live in the
# home directory (see BOXGO and BOXMONGO), so nothing is installed system wide.
# The suites name the cluster by the address range of the machine they were
# written on, 172.24, and the box's is 172.18, so the copy shipped there is
# rewritten on the way, every file under test/ and not only the scripts: a
# suite's own config names the cluster too, and testrollback failed 16 checks
# against a router that could not reach 172.24 from two.yaml.
#
# Never while an in-node sweep runs there: hostsweep refuses, and so does this.
set -uo pipefail
cd "$(dirname "$0")/.."

BOX=${BOX:-zelmario@192.168.1.50}
NODE=${NODE:-thermocline-zelmario-node0}
DIR=${DIR:-thermocline}
BOXGO=${BOXGO:-go-sdk/bin}
BOXMONGO=${BOXMONGO:-mongodb/bin}
OUT=${OUT:-/tmp/hostsweep}

rsh() { local i; for i in 1 2 3; do ssh -o ConnectTimeout=10 "$BOX" "$@" && return 0; sleep 3; done; return 1; }

if [ "${1:-}" != "-wait" ]; then
  make -s build store >/dev/null || exit 2
  # The tracked files as they are in this working tree, plus the two binaries.
  { git ls-files; echo bin/cline; echo bin/store; } | tar -cz -T - |
    rsh "rm -rf ~/$DIR && mkdir -p ~/$DIR && tar -xz -C ~/$DIR &&
         grep -rlI '172\\.24\\.' ~/$DIR/test | xargs -r sed -i 's/172\\.24\\./172.18./g'" || exit 2
  rsh "cd ~/$DIR && export PATH=\$HOME/$BOXGO:\$HOME/$BOXMONGO:\$PATH NODE=$NODE OUT=$OUT SUITES='${SUITES:-}' &&
       [ -n \"\$SUITES\" ] || unset SUITES;
       setsid nohup bash test/hostsweep.sh > ~/hostsweep.out 2>&1 < /dev/null &" || exit 2
  echo "boxhostsweep: started on $BOX, summary in $OUT/summary.txt there"
fi

# Waited on the last line hostsweep writes, polled, since the ssh it would
# otherwise hang on can drop.
until rsh "grep -qE '^HOSTSWEEP DONE|^the node is running|already running here' ~/hostsweep.out $OUT/summary.txt 2>/dev/null"; do
  sleep 30
done
rsh "cat ~/hostsweep.out"
rsh "grep -qE 'FAIL [1-9]|TIMED OUT|SETUP FAILED|MISSING|already' ~/hostsweep.out" && exit 1
exit 0
