// A probe for item 9 of docs/production-readiness.md: two copy-ups of one
// document at once, and the loser withdrawing the winner's echo.
//
// Each round evicts one archived document through the real evictor, then
// fires many simultaneous point reads of it through two routers so that more
// than one copy-up is in flight. What it then asks is what an application
// would be told: a consumer reading its change stream later than the
// promotion window, from a position before the race, must not see the
// copy-up as an insert.
//
// It records the intermediate state as well, so a round that shows nothing is
// distinguishable from a round that never raced: how many copy-up inserts
// landed, whether the feeder wrote down that insert's position, and whether
// the echo was still pending.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	r1 := flag.String("r1", "", "first router")
	r2 := flag.String("r2", "", "second router")
	hotURI := flag.String("hot", "", "fast tier")
	coldURI := flag.String("cold", "", "archive")
	metaDB := flag.String("meta", "_thermocline_p9", "bookkeeping database")
	db := flag.String("db", "probe9", "database")
	coll := flag.String("coll", "docs", "collection")
	cfg := flag.String("config", "/root/p9.yaml", "config the evictor runs with")
	rounds := flag.Int("rounds", 10, "rounds")
	readers := flag.Int("readers", 32, "simultaneous reads per round, split across the routers")
	late := flag.Duration("late", 35*time.Second, "how far behind the consumer reads")
	calm := flag.Int("calm", 0, "rounds at the start with one reader, so an uncontested promotion writes a position down first")
	// update races write misses instead of reads. A read hands its copy-up the
	// document it already took from the archive; a write miss has nothing, so
	// its copy-up records the promotion while reading the archive and undoes
	// it if the archive turns out to lack the document. Every update must also
	// land: the matched count is checked against the number sent.
	op := flag.String("op", "read", "read or update")
	flag.Parse()

	ctx := context.Background()
	connect := func(uri string) *mongo.Client {
		c, err := mongo.Connect(options.Client().ApplyURI(uri).SetMaxPoolSize(uint64(*readers)))
		if err != nil {
			die("connect %s: %v", uri, err)
		}
		if err := c.Ping(ctx, nil); err != nil {
			die("ping %s: %v", uri, err)
		}
		return c
	}
	routers := []*mongo.Client{connect(*r1), connect(*r2)}
	hot, cold := connect(*hotURI), connect(*coldURI)
	ns := *db + "." + *coll

	var id bson.ObjectID
	{
		var d struct {
			ID bson.ObjectID `bson:"_id"`
		}
		if err := cold.Database(*db).Collection(*coll).FindOne(ctx, bson.D{}).Decode(&d); err != nil {
			die("the archive holds no document to race on: %v", err)
		}
		id = d.ID
	}
	fmt.Printf("racing on %s %s\n", ns, id.Hex())

	phantoms, raced, lostWrites := 0, 0, 0
	for round := 1; round <= *rounds; round++ {
		// Off the fast tier, the way it really leaves: the evictor, with the
		// hold made to have lapsed, so the eviction is announced.
		_, _ = cold.Database(*metaDB).Collection("residency").UpdateMany(ctx,
			bson.D{{Key: "doc", Value: id}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "hotUntil", Value: time.Now().Add(-time.Hour)}}}})
		for try := 0; try < 10; try++ {
			if n, _ := hot.Database(*db).Collection(*coll).CountDocuments(ctx, bson.D{{Key: "_id", Value: id}}); n == 0 {
				break
			}
			out, err := exec.Command("cline", "evictor", "--config", *cfg, "--once").CombinedOutput()
			if err != nil {
				fmt.Printf("  evictor: %v %s\n", err, strings.TrimSpace(string(out)))
			}
			time.Sleep(time.Second)
		}
		if n, _ := hot.Database(*db).Collection(*coll).CountDocuments(ctx, bson.D{{Key: "_id", Value: id}}); n != 0 {
			die("round %d: the evictor would not take the document off the fast tier", round)
		}
		// Let the feeder account for the eviction before the race, so it is
		// not in the same batch as the copy-ups.
		time.Sleep(2 * time.Second)

		t0 := lastWrite(ctx, hot)

		n := *readers
		if round <= *calm {
			n = 1
		}
		var wg sync.WaitGroup
		var matched atomic.Int64
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				c := routers[i%2].Database(*db).Collection(*coll)
				if *op == "update" {
					if res, err := c.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
						bson.D{{Key: "$inc", Value: bson.D{{Key: "raced", Value: 1}}}}); err == nil {
						matched.Add(res.MatchedCount)
					}
					return
				}
				_ = c.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Err()
			}(i)
		}
		close(start)
		wg.Wait()
		if *op == "update" && matched.Load() != int64(n) {
			lostWrites++
			fmt.Printf("  round %d: %d updates sent, %d matched\n", round, n, matched.Load())
		}
		time.Sleep(3 * time.Second)

		inserts := insertsSince(ctx, hot, ns, id, t0)
		var rec struct {
			Pending bool             `bson:"echoPending"`
			Echoed  []bson.Timestamp `bson:"echoed"`
		}
		_ = cold.Database(*metaDB).Collection("residency").
			FindOne(ctx, bson.D{{Key: "doc", Value: id}}).Decode(&rec)
		recorded := 0
		for _, ins := range inserts {
			for _, e := range rec.Echoed {
				if e == ins {
					recorded++
				}
			}
		}

		// The consumer that is behind: opened after the window has passed,
		// from a position before the race.
		time.Sleep(*late)
		shown := insertsShown(ctx, routers[round%2], *db, *coll, id, t0)

		lost := len(inserts) - recorded
		if lost > 0 {
			raced++
		}
		if shown > 0 {
			phantoms++
		}
		fmt.Printf("round %2d: copy-up inserts %d, positions recorded %d, echo pending %v, echoed list %d, late consumer shown %d insert(s)\n",
			round, len(inserts), recorded, rec.Pending, len(rec.Echoed), shown)
	}
	fmt.Printf("\n%d round(s) where a copy-up insert went unrecorded, %d where a late consumer was shown one\n", raced, phantoms)
	if *op == "update" {
		fmt.Printf("%d round(s) where an update matched nothing\n", lostWrites)
	}
	if phantoms > 0 || lostWrites > 0 {
		os.Exit(1)
	}
}

func lastWrite(ctx context.Context, hot *mongo.Client) bson.Timestamp {
	var h struct {
		LastWrite struct {
			OpTime struct {
				TS bson.Timestamp `bson:"ts"`
			} `bson:"opTime"`
		} `bson:"lastWrite"`
	}
	if err := hot.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&h); err != nil {
		die("hello: %v", err)
	}
	return h.LastWrite.OpTime.TS
}

// insertsSince is every insert of this document the fast tier's oplog holds
// after t0, which is every copy-up that landed: nothing else inserts it.
func insertsSince(ctx context.Context, hot *mongo.Client, ns string, id bson.ObjectID, t0 bson.Timestamp) []bson.Timestamp {
	cur, err := hot.Database("local").Collection("oplog.rs").Find(ctx, bson.D{
		{Key: "ts", Value: bson.D{{Key: "$gt", Value: t0}}},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: ns}, {Key: "o._id", Value: id}},
			bson.D{{Key: "op", Value: "c"}, {Key: "o.applyOps", Value: bson.D{{Key: "$elemMatch", Value: bson.D{
				{Key: "op", Value: "i"}, {Key: "ns", Value: ns}, {Key: "o._id", Value: id}}}}}},
		}},
	})
	if err != nil {
		die("oplog: %v", err)
	}
	var out []bson.Timestamp
	for cur.Next(ctx) {
		var e struct {
			TS bson.Timestamp `bson:"ts"`
		}
		if cur.Decode(&e) == nil {
			out = append(out, e.TS)
		}
	}
	return out
}

// insertsShown is what an application reading its change stream from t0 is
// told about this document.
func insertsShown(ctx context.Context, r *mongo.Client, db, coll string, id bson.ObjectID, t0 bson.Timestamp) int {
	cs, err := r.Database(db).Collection(coll).Watch(ctx, mongo.Pipeline{},
		options.ChangeStream().SetStartAtOperationTime(&t0))
	if err != nil {
		die("watch: %v", err)
	}
	defer func() { _ = cs.Close(context.WithoutCancel(ctx)) }()
	n := 0
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if !cs.TryNext(ctx) {
			if cs.Err() != nil {
				die("stream: %v", cs.Err())
			}
			continue
		}
		var ev struct {
			Op  string `bson:"operationType"`
			Key struct {
				ID bson.ObjectID `bson:"_id"`
			} `bson:"documentKey"`
		}
		if cs.Decode(&ev) == nil && ev.Op == "insert" && ev.Key.ID == id {
			n++
		}
	}
	return n
}

func die(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "echorace: "+f+"\n", a...)
	os.Exit(2)
}
