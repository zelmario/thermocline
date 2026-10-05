// Command landed asks one question of a failover: is a write the client was told
// failed ever there afterwards?
//
// Workers insert documents with fresh _ids, through whatever -uri names, while
// the fast tier's primary is stepped down every -stepdown-every. Every insert
// that returned an error is looked up on the fast tier at the end. An error for
// a write that landed is fine when the error says the outcome is unknown, as a
// network error does, and wrong when it says the write was refused, as
// NotWritablePrimary does: the application believes it and writes again or
// gives up on something that happened.
//
// Run with -uri at a router and again with -uri at the replica set; the second
// is what the first is judged against. See test/landed/landed.sh.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	uri := flag.String("uri", "", "where the application connects")
	hot := flag.String("hot", "", "the fast tier, for step-downs and the final lookup")
	ns := flag.String("ns", "landed.docs", "database.collection to insert into")
	workers := flag.Int("workers", 8, "concurrent inserters")
	dur := flag.Duration("for", 2*time.Minute, "how long to insert")
	every := flag.Duration("stepdown-every", 20*time.Second, "step the fast tier's primary down this often")
	// A pause between inserts leaves connections idle in the driver's pool,
	// which is what a real application's pool holds most of the time and what
	// a failover treats differently from one in use.
	// 0 is no deadline, which is what the shop does. With one, Go driver v2
	// retries a retryable error until the deadline, which hides the failure
	// this probe exists for: measured, 0 errors through a router in 6
	// step-downs where the shop, with no deadline, had them.
	timeout := flag.Duration("timeout", 0, "a deadline on each insert, 0 for none")
	pause := flag.Duration("pause", 0, "sleep up to this long, at random, between a worker's inserts")
	flag.Parse()
	if *uri == "" || *hot == "" {
		fmt.Fprintln(os.Stderr, "-uri and -hot are required")
		os.Exit(2)
	}
	dbName, collName, _ := strings.Cut(*ns, ".")

	ctx := context.Background()
	// Every attempt the driver makes, by the _id it inserts, so a failure can
	// be shown as the sequence of attempts that produced it.
	var amu sync.Mutex
	attempts := map[string][]string{}
	byReq := map[int64]string{}
	note := func(req int64, line string) {
		amu.Lock()
		defer amu.Unlock()
		if id, ok := byReq[req]; ok {
			attempts[id] = append(attempts[id], time.Now().Format("15:04:05.000")+" "+line)
		}
	}
	mon := &event.CommandMonitor{
		Started: func(_ context.Context, e *event.CommandStartedEvent) {
			if e.CommandName != "insert" {
				return
			}
			docs, err := e.Command.LookupErr("documents")
			if err != nil {
				return
			}
			vals, _ := docs.Array().Values()
			if len(vals) == 0 {
				return
			}
			id := vals[0].Document().Lookup("_id").ObjectID().Hex()
			amu.Lock()
			byReq[e.RequestID] = id
			txn, _ := e.Command.LookupErr("txnNumber")
			attempts[id] = append(attempts[id], fmt.Sprintf("%s sent on %s txnNumber=%v",
				time.Now().Format("15:04:05.000"), e.ConnectionID, txn))
			amu.Unlock()
		},
		Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
			note(e.RequestID, "ok "+string(e.Reply))
		},
		Failed: func(_ context.Context, e *event.CommandFailedEvent) {
			note(e.RequestID, "failed "+e.Failure.Error())
		},
	}
	app, err := mongo.Connect(options.Client().ApplyURI(*uri).SetMonitor(mon))
	must(err)
	hc, err := mongo.Connect(options.Client().ApplyURI(*hot))
	must(err)
	coll := app.Database(dbName).Collection(collName)

	type failure struct {
		id  bson.ObjectID
		err string
	}
	var (
		mu     sync.Mutex
		failed []failure
		ok     []bson.ObjectID
	)
	stop := time.Now().Add(*dur)

	steps := 0
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(*every)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				// The reply is a network error when the primary steps down
				// under its own connection, so it is not checked.
				c, cancel := context.WithTimeout(ctx, 30*time.Second)
				_ = hc.Database("admin").RunCommand(c, bson.D{
					{Key: "replSetStepDown", Value: 10}, {Key: "secondaryCatchUpPeriodSecs", Value: 5}}).Err()
				cancel()
				steps++
				fmt.Printf("[%s] stepped down\n", time.Now().Format("15:04:05.000"))
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), uint64(time.Now().UnixNano())))
			for time.Now().Before(stop) {
				if *pause > 0 {
					time.Sleep(time.Duration(rng.Int64N(int64(*pause))))
				}
				id := bson.NewObjectID()
				c, cancel := context.WithCancel(ctx)
				if *timeout > 0 {
					c, cancel = context.WithTimeout(ctx, *timeout)
				}
				_, err := coll.InsertOne(c, bson.D{{Key: "_id", Value: id}, {Key: "w", Value: w}})
				cancel()
				mu.Lock()
				if err != nil {
					fmt.Printf("[%s] insert %s failed: %s labels=%v\n", time.Now().Format("15:04:05.000"), id.Hex(), kind(err.Error()), labels(err))
					amu.Lock()
					for _, a := range attempts[id.Hex()] {
						fmt.Printf("    %s\n", a)
					}
					amu.Unlock()
					failed = append(failed, failure{id, err.Error()})
				} else {
					ok = append(ok, id)
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	close(done)

	// Long enough for any write still in flight on the server to finish.
	time.Sleep(5 * time.Second)
	lc, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	there := func(ids []bson.ObjectID) map[bson.ObjectID]bool {
		out := map[bson.ObjectID]bool{}
		for i := 0; i < len(ids); i += 1000 {
			j := min(i+1000, len(ids))
			cur, err := hc.Database(dbName).Collection(collName).Find(lc,
				bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids[i:j]}}}},
				options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
			must(err)
			for cur.Next(lc) {
				out[cur.Current.Lookup("_id").ObjectID()] = true
			}
			must(cur.Err())
		}
		return out
	}

	ids := make([]bson.ObjectID, len(failed))
	for i, f := range failed {
		ids[i] = f.id
	}
	landed := there(ids)
	okThere := there(ok)

	// Grouped by the error's kind, with addresses and ports taken out so the
	// same failure from two connections counts once.
	type tally struct{ failed, landed int }
	kinds := map[string]*tally{}
	for _, f := range failed {
		k := kind(f.err)
		if kinds[k] == nil {
			kinds[k] = &tally{}
		}
		kinds[k].failed++
		if landed[f.id] {
			kinds[k].landed++
		}
	}
	names := make([]string, 0, len(kinds))
	for k := range kinds {
		names = append(names, k)
	}
	sort.Strings(names)

	fmt.Printf("\nstep-downs %d, inserts acknowledged %d, of those on the fast tier %d\n", steps, len(ok), len(okThere))
	fmt.Printf("inserts that reported an error %d, of those on the fast tier anyway %d\n", len(failed), len(landed))
	for _, k := range names {
		fmt.Printf("  %4d failed, %4d landed  %s\n", kinds[k].failed, kinds[k].landed, k)
	}
	wrong := 0
	for _, k := range names {
		if kinds[k].landed > 0 && refusal(k) {
			wrong += kinds[k].landed
		}
	}
	fmt.Printf("RESULT refused-but-landed=%d lost-acknowledged=%d\n", wrong, len(ok)-len(okThere))
}

// labels are the error labels a driver attached, which decide what it retries.
func labels(err error) []string {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return ce.Labels
	}
	var we mongo.WriteException
	if errors.As(err, &we) {
		return we.Labels
	}
	return nil
}

// kind is an error's message with what varies between connections removed.
func kind(s string) string {
	for _, cut := range []string{"connection(", "read tcp", "write tcp", "dial tcp"} {
		if i := strings.Index(s, cut); i >= 0 {
			return strings.TrimSpace(s[:i]) + " " + cut + "...)"
		}
	}
	if len(s) > 120 {
		s = s[:120]
	}
	return s
}

// refusal says an error tells the application the write was not applied, as
// against one that leaves the outcome open.
func refusal(k string) bool {
	for _, s := range []string{"NotWritablePrimary", "NotPrimary", "not primary", "ShutdownInProgress", "InterruptedDueToReplStateChange", "PrimarySteppedDown"} {
		if strings.Contains(k, s) {
			return true
		}
	}
	return false
}

func must(err error) {
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
