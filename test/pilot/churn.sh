#!/bin/bash
# Move documents between the tiers while a client is asking questions.
#
# A warm copying up and an evictor taking away, both running against the same
# collections the comparison is reading. Nothing about which tier answers may
# change what the answer is.
trap 'pkill -9 -f "cline warmer --config /root/pilot_churn" 2>/dev/null; pkill -9 -f "cline evictor --config /root/pilot_churn" 2>/dev/null' EXIT
(
  for i in 1 2 3 4 5 6; do
    cline warmer --config /root/pilot_churn.yaml >/dev/null 2>&1
    sleep 2
  done
) &
WARM=$!
(
  for i in 1 2 3 4 5 6; do
    cline evictor --config /root/pilot_churn.yaml --once >/dev/null 2>&1
    sleep 3
  done
) &
EV=$!
echo "warm and evict running as $WARM and $EV"
wait $WARM $EV
echo "churn finished"
