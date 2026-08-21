# A pilot, on data nobody here wrote

Every other test in this repository uses fixtures written to test this system.
That is a real limit, and it is the one the suites cannot fix from the inside:
a fixture only asks the questions its author thought of, and the shapes it
carries are the shapes that occurred to them.

So this adopts MongoDB's published sample datasets instead. 425,367 documents in
twenty-three collections across nine databases, with text indexes, 2dsphere
indexes, unique indexes, nested arrays and dates spanning 1970 to 2017, none of
it designed with tiering in mind.

    bash test/pilot/setup.sh

It restores the datasets into the **archive only**, which is what an adoption
means, then follows the runbook in the README: read what the router says at
startup, copy the collections nothing tiers, warm, rebuild the key registry.
Then it runs the comparison.

## What the comparison is

`agree.js` asks the router and the archive the same questions and prints every
disagreement. It asserts no expected values at all. The archive is the control,
and the only claim being checked is the one this system makes: that a client
cannot tell the difference.

Nine questions per collection: two counts, a stats count, the first and last
five `_id`s by sort, the field set of a document, a group-by over the whole
collection, a distinct, and a sorted page with a skip. 199 questions.

The group-by matters most. It is the shape a report has, it reads every document
in the collection, and it is the shape that goes wrong quietly when half of them
are somewhere else.

## What it found

**Four whole databases invisible, with no warning.** The startup report is what
step 1 of the runbook tells an operator to read, and it promised to name
everything the fast tier is missing. It derived the databases to compare from
`feeder.namespaces`, so it could only see databases that already had something
tiered in them. Here four collections were tiered across four databases, and the
other five databases were never looked at: 44,000 documents, invisible to the
application, absent from `listDatabases`, and named nowhere. Fixed, and pinned
by testrouter.sh.

**A stats call counted the fast tier's share.** `$collStats` and `collStats`
were let through as metadata, on the stated grounds that they describe the
collection rather than read it, so there is nothing for the answer to be short
of. True of every field but the count. `sample_training.trips` reported 0
documents where the archive holds 10,000, because that collection is dated 2016
and the hot window is ten years, so none of it is on the fast tier. Meanwhile
`countDocuments` on it answered 10,000. Two ways of asking the same question,
disagreeing by the whole collection. Fixed, and pinned by testops.sh.

After both, the 199 questions produce no disagreements.

## Two traps this dataset has naturally

Both are written up in the runbook as things to watch for, and both occur here
without anybody arranging them, which is the argument for using real data.

**ObjectIds carrying the wrong date.** `sample_mflix.comments` has ObjectId
`_id`s minted when the dataset was published and comments dated 1970 to 2017. A
rule with no `date_field` takes a document's age from the timestamp inside its
`_id`, so it would address the publication date and tier nothing. Every rule in
`pilot.yaml` sets `date_field` for this reason.

**A date field with a space in its name.** `sample_training.trips` dates its
documents in `start time`. It works, and the warmer says so:
`selector="start time"`.

## The shape it sets up

| collection                 | tiered | where it ends up                          |
|----------------------------|--------|-------------------------------------------|
| `sample_mflix.comments`    | yes    | split: 2016 onward hot, 1970 to 2015 cold |
| `sample_supplies.sales`    | yes    | split                                     |
| `sample_training.trips`    | yes    | entirely cold, dated 2016                 |
| `sample_weatherdata.data`  | yes    | entirely cold, dated 1984                 |
| the other nineteen         | no     | copied whole, per runbook step 4          |

A collection that lives entirely in the archive is worth having deliberately.
It is what an application reading history looks like, and it is the case where
a number describing the fast tier is most obviously wrong.

## Running an application against it

The router listens on 27500 inside node0, which is reachable from another
container on the same docker network:

    docker run -d --name pilot-mongoexpress \
      --network thermocline-zelmar-anydbver -p 8081:8081 \
      -e ME_CONFIG_MONGODB_URL='mongodb://dba:secret@172.24.0.2:27500/?authSource=admin' \
      -e ME_CONFIG_BASICAUTH=false mongo-express:latest

Read the addresses rather than assuming them; see the test environment notes.
Nothing in the connection string says `replicaSet=`, which is runbook step 2 and
the first thing an application gets wrong.
