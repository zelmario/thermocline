#!/bin/bash
# Every suite belongs to exactly one sweep, checked rather than remembered.
#
#   bash test/partition.sh
#
# psweep.sh runs the suites that live inside node0 and hostsweep.sh runs the
# ones that drive the host, and the split is two hard-coded lists. A suite added
# to neither is a file nothing ever runs, and nothing says so: the sweep passes,
# the total goes up by nothing, and the file looks like coverage it is not. A
# suite in both is worse, because the two runs fight over one cluster.
#
# The lists were last reconciled by hand, and by hand is how this drifts. Both
# sweeps run this first and refuse to start when it fails.
cd "$(dirname "$0")" || exit 2
python3 - <<'PY'
import re, sys, glob, os

def listed(path):
    src = open(path).read()
    m = re.search(r'SUITES=\$\{SUITES:-"(.*?)"\}', src, re.S)
    if not m:
        print(f"partition: no SUITES list in {path}")
        sys.exit(2)
    # suite@n is part n of one suite file.
    return {x.split('@')[0] for x in m.group(1).split()}

p, h = listed('psweep.sh'), listed('hostsweep.sh')
files = {os.path.basename(f)[:-3] for f in glob.glob('test*.sh')}
files |= {os.path.basename(f)[:-3] for f in glob.glob('verskew.sh')}
# Not suites: the partition check itself, and the sweep drivers.
files -= {'partition'}

rc = 0
for label, bad in (("run by both sweeps", p & h),
                   ("run by no sweep", files - (p | h)),
                   ("named by a sweep and not a file", (p | h) - files)):
    if bad:
        print(f"partition: {label}: {' '.join(sorted(bad))}")
        rc = 1
if rc == 0:
    print(f"partition: {len(p)} in psweep + {len(h)} in hostsweep, every suite in exactly one")
sys.exit(rc)
PY
