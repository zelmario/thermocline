# A shop, not a benchmark

`test/workload` measures throughput. This measures whether an application keeps
working. Those are different questions, and only one of them is what this system
promises: your application does not change.

A benchmark cannot check that promise, because it does not know what its own
answers should be. It reads a document, gets a document, and records how long it
took. If the router had returned the wrong document, or the same document twice,
or a document from before the last write, the benchmark would have reported the
same number.

So this is a shop instead. It has the shape a real one has:

| collection            | tiered | why                                            |
|-----------------------|--------|------------------------------------------------|
| `shop.products`       | no     | the catalogue is small and read constantly     |
| `shop.customers`      | no     | it does not grow with traffic                  |
| `shop.orders`         | yes    | it only grows, and old orders are read rarely  |
| `shop.order_events`   | yes    | an append-only log per order                   |

That mix is deliberate. A router that is only ever asked about tiered
collections is not being tested the way an application uses it. Here, most reads
in the run never touch the archive at all, and the ones that do are the ones a
shop would notice.

## What it checks while it runs

Every one of these is a way this system could be quietly broken while every
latency number stays healthy.

1. **An order just placed can be read back**, with the total it was placed with.
   This is read-your-writes across two tiers, and it is the promise most likely
   to break under a copy-up.
2. **Placing one order moves that customer's order count by exactly one.** Not
   two, which is a write applied to both tiers or counted from both. Not zero,
   which is a write reported as done and lost.
3. **An order's total equals the sum of its lines**, whichever tier answered.
   A partial document reassembled from the wrong tier fails here.
4. **A customer's order list never shows the same order twice.** A document that
   exists in the fast tier and the archive at once shows up here first.
5. **A report over a month that has already ended returns the same number every
   time.** Nothing writes to the past, so an answer that moves is an answer that
   was wrong once. This is the check that catches an archive read that returned
   a short batch, or a copy-up that was counted twice.

Violations are counted separately from errors, and the tool exits non-zero if
there were any. An error is the system saying no, which is at least honest. A
violation is it saying something untrue.

## Running it

Stock the shop once, straight at the fast tier, with the feeder already running
so the archive gets a copy:

    go run ./test/store -uri "mongodb://127.0.0.1:27017/" -orders 50000 -seed-only

Then shop, against whatever you want to test:

    go run ./test/store -uri "mongodb://127.0.0.1:27100/" -skip-seed \
      -rounds 300 -conc 8 -snapshot http://127.0.0.1:27110/api/snapshot

`-skip-seed` reads a marker document the stocking wrote, so a later run asks for
customers and products that exist and reads dates that have orders in them.
Without it every read would miss and the numbers would describe nothing.

**Run it against MongoDB directly first.** That is the control, and it matters
more than the router run: if a check fails with no proxy and no tiering in the
way, the check is wrong, and nothing it says afterwards means anything.

## Two ways to write a check that lies

Both of these were made here, and both produce failures that look exactly like a
product bug.

**A window that moves is not a fixed question.** Check 5 first computed its month
as `now` minus two months, on every call. `now` changes between calls, so the
window slid, and a sliding window over evenly spaced orders returns a count one
lower or higher depending on where its edge falls. The first run reported
twenty-one violations against the router and none against MongoDB, which looked
damning until the numbers gave it away: every 30-day month reported 4109 or 4110
and every 31-day month 4246 or 4247, on 50,000 orders spread evenly over a year.
The windows are now whole calendar months fixed at startup.

**The control passing does not clear a check.** That same broken check passed the
control run, because the control finished in nine seconds and the window barely
moved. A control is necessary and not sufficient: it catches a check that is
wrong always, not one that is wrong slowly.

Writes are partitioned by shopper, so shopper 3 only ever places orders for
customers 3, 11, 19 and so on. Check 2 compares a count taken before an insert
with a count taken after, and two shoppers writing to the same customer at the
same time would fail it with nothing wrong. A false alarm there is worse than no
alarm, because it trains you to ignore the real ones.
