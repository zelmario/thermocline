#!/usr/bin/env python3
"""Derive a config that runs the shop in its own database.

The shop wants a tiered collection, and the obvious one is shop.orders, which
the suites already tier. Do not use it. Six suites read shop.orders without
dropping it first, so a few thousand seeded orders change what they measure, and
they change it quietly: testevict counts documents there and would still pass
while counting somebody else's.

So the shop gets shopstore.orders and shopstore.order_events, its own
bookkeeping database and its own ports, the same isolation mkworker.sh gives a
sweep worker.

    python3 test/store/mkconfig.py test/cline.yaml > /tmp/store.yaml
"""

import re
import sys

DB = "shopstore"
PORT = 27400
DASH = 27410
META = "_thermocline_store"


def one_list(block, key, body):
    """Replace the first YAML list under key, keeping the key itself."""
    return re.sub(r"(" + key + r":\n)(?:    (?:-|#| ).*\n)+", r"\1" + body, block, count=1)


def main() -> int:
    src = open(sys.argv[1]).read()

    src = one_list(src, "  namespaces",
                   f'    - "{DB}.orders"\n    - "{DB}.order_events"\n')

    # The evictor's rules and the warmer's are both called "rules", so each
    # block is edited on its own rather than by a pattern that would match the
    # first one twice.
    ev, wa, ro = src.index("evictor:"), src.index("warmer:"), src.index("router:")
    head, evictor, warmer, router = src[:ev], src[ev:wa], src[wa:ro], src[ro:]
    evictor = one_list(evictor, "  rules", f'    - ns: "{DB}.orders"\n      older_than: 40m\n')
    warmer = one_list(warmer, "  rules", f'    - ns: "{DB}.orders"\n      back: 40m\n')
    router = router.replace('listen: "0.0.0.0:27100"', f'listen: "0.0.0.0:{PORT}"')
    router = router.replace('dashboard: "0.0.0.0:27110"', f'dashboard: "0.0.0.0:{DASH}"')

    out = head + evictor + warmer + router + f'''
# Set by test/store/mkconfig.py, so a store run collides with no suite.
meta:
  db: "{META}"
'''
    # Assert what it should say rather than what it should not, the same way
    # mkworker.sh does: a derivation that silently did nothing is worse than one
    # that fails.
    for want in [f'"{DB}.orders"', f'listen: "0.0.0.0:{PORT}"', f'db: "{META}"']:
        if want not in out:
            print(f"mkconfig: the derived config does not carry {want}", file=sys.stderr)
            return 1
    if re.search(r'"shop\.', out):
        print("mkconfig: the derived config still tiers the suites' database", file=sys.stderr)
        return 1
    sys.stdout.write(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
