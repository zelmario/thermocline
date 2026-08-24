package router

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// When the archive's answer replaces the fast tier's, the fast tier has already
// run the query and may be holding a cursor for the rest of it. Nothing will
// ever ask for that rest: the client is given the archive's cursor instead, and
// the id it would have to name is not one it has ever seen.
//
// So it sits there until mongod's own timeout, ten minutes by default, pinning
// the snapshot it was opened on. One is nothing. A dashboard refreshing every
// few seconds against a collection the fast tier cannot answer in full opens
// one per refresh, and they overlap.
//
// It only happens when the fast tier had more to give than one batch, which for
// a query carrying a limit means a batchSize smaller than that limit. That is
// narrow, and it is also exactly the shape a driver produces when an
// application sets batchSize and forgets.
func (c *conn) dropSupersededCursor(ctx context.Context, rdoc bson.Raw, db string) {
	cur, err := rdoc.LookupErr("cursor")
	if err != nil {
		return
	}
	cdoc, ok := cur.DocumentOK()
	if !ok {
		return
	}
	id, ok := intOf64(cdoc.Lookup("id"))
	if !ok || id == 0 {
		// Zero means the fast tier already gave everything it had.
		return
	}
	coll, ok := collectionOfCursorNS(cdoc)
	if !ok {
		return
	}
	err = c.r.hot.Database(db).RunCommand(ctx, bson.D{
		{Key: "killCursors", Value: coll},
		{Key: "cursors", Value: bson.A{id}},
	}).Err()
	if err != nil {
		// Not worth failing the client's read over. It expires on its own.
		c.log.Debug("could not close the fast tier cursor this answer replaced",
			"db", db, "cursor", id, "err", err)
		return
	}
	c.r.Stats.SupersededCursors.Add(1)
}

// collectionOfCursorNS reads the collection out of a cursor reply's ns, which
// carries the database as well and is the only place the reply names it.
func collectionOfCursorNS(cdoc bson.Raw) (string, bool) {
	ns, err := cdoc.LookupErr("ns")
	if err != nil {
		return "", false
	}
	full, ok := ns.StringValueOK()
	if !ok {
		return "", false
	}
	_, coll := splitNS(full)
	if coll == "" {
		return "", false
	}
	return coll, true
}
