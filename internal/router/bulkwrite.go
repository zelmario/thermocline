package router

import (
	"context"
	"sort"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/wire"
)

// MongoDB 8.0 added a second way to write, and it does not look like the first.
//
// Every other write command names its collection where the guards look for it:
// `insert: "orders"`, `update: "orders"`. bulkWrite goes to the admin database,
// lists its collections in nsInfo, and has its operations refer to them by
// position. A router reading a collection name out of the command finds nothing
// at all, decides the command touches nothing it manages, and forwards it
// untouched.
//
// What went with it was every protection this system has for writes: the unique
// key registry that stops an insert duplicating a document the fast tier can no
// longer see, and the copy-up that lets a multi-document write reach documents
// that have been evicted. Measured on a collection holding three of twelve
// documents, a bulkWrite delete reported success having removed three, and the
// other nine stayed readable in the archive. Both failures were fixed on the
// ordinary write path long ago; this command reopened them through a side door.
//
// So the ops are decomposed here and handed to exactly the same guards.

// bulkOp is one entry of a bulkWrite ops array, normalised into the shape the
// existing guards already understand.
type bulkOp struct {
	kind string // insert, update or delete
	ns   string
	doc  bson.Raw        // insert
	stmt updateStatement // update and delete
	raw  bson.Raw        // the operation as sent, for a trimmed command
}

// parseBulkWrite reads the command into namespaces and operations.
func parseBulkWrite(cmd bson.Raw, msg *wire.Message) ([]bulkOp, bool) {
	var namespaces []string
	nsVal, err := cmd.LookupErr("nsInfo")
	if err != nil {
		return nil, false
	}
	arr, ok := nsVal.ArrayOK()
	if !ok {
		return nil, false
	}
	vals, verr := arr.Values()
	if verr != nil {
		return nil, false
	}
	for _, v := range vals {
		d, dok := v.DocumentOK()
		if !dok {
			return nil, false
		}
		ns, nok := stringField(d, "ns")
		if !nok {
			return nil, false
		}
		namespaces = append(namespaces, ns)
	}

	// ops may arrive inline or as a payload sequence, the same as the documents
	// of an ordinary insert.
	ops := msg.AllOf("ops")
	if len(ops) == 0 {
		if v, oerr := cmd.LookupErr("ops"); oerr == nil {
			if a, aok := v.ArrayOK(); aok {
				if ovals, oerr2 := a.Values(); oerr2 == nil {
					for _, o := range ovals {
						if d, dok := o.DocumentOK(); dok {
							ops = append(ops, d)
						}
					}
				}
			}
		}
	}
	if len(ops) == 0 {
		return nil, false
	}

	out := make([]bulkOp, 0, len(ops))
	for _, op := range ops {
		b := bulkOp{raw: op}
		switch {
		case hasKey(op, "insert"):
			b.kind = "insert"
			idx, iok := intOf(op.Lookup("insert"))
			if !iok || idx < 0 || idx >= len(namespaces) {
				return nil, false
			}
			b.ns = namespaces[idx]
			d, dok := op.Lookup("document").DocumentOK()
			if !dok {
				return nil, false
			}
			b.doc = d
		case hasKey(op, "update"):
			b.kind = "update"
			idx, iok := intOf(op.Lookup("update"))
			if !iok || idx < 0 || idx >= len(namespaces) {
				return nil, false
			}
			b.ns = namespaces[idx]
			b.stmt = updateStatement{
				filter:    subDoc(op.Lookup("filter")),
				update:    op.Lookup("updateMods"),
				multi:     boolField(op, "multi"),
				upsert:    boolField(op, "upsert"),
				collation: docField(op, "collation"),
			}
			b.stmt.arrayFilters = op.Lookup("arrayFilters")
		case hasKey(op, "delete"):
			b.kind = "delete"
			idx, iok := intOf(op.Lookup("delete"))
			if !iok || idx < 0 || idx >= len(namespaces) {
				return nil, false
			}
			b.ns = namespaces[idx]
			b.stmt = updateStatement{
				filter:    subDoc(op.Lookup("filter")),
				collation: docField(op, "collation"),
				multi:     boolField(op, "multi"),
			}
		default:
			// An operation shape this router does not know. Guarding what it
			// cannot read would be pretending, so the whole command is left
			// alone and said so.
			return nil, false
		}
		out = append(out, b)
	}
	return out, true
}

func hasKey(doc bson.Raw, key string) bool {
	_, err := doc.LookupErr(key)
	return err == nil
}

func boolField(doc bson.Raw, key string) bool {
	b, ok := doc.Lookup(key).BooleanOK()
	return ok && b
}

// guardBulkWrite applies the ordinary write guards to a bulkWrite.
//
// It reports true when it has answered the client itself, which happens only
// when the command is refused.
func (c *conn) guardBulkWrite(ctx context.Context, msg *wire.Message, cmd bson.Raw) (*wire.Message, bool) {
	// Inside a transaction a key's owner is read as the transaction sees it,
	// as guardInsert does: an earlier statement of it may have freed the
	// value. See withTxnView.
	if t, in := txnOf(cmd); in {
		ctx = c.withTxnView(ctx, t)
	}
	ops, ok := parseBulkWrite(cmd, msg)
	if !ok {
		c.log.Warn("could not read this bulkWrite, forwarding it unguarded",
			"effect", "cross-tier unique keys and archived documents are not protected for it")
		return nil, false
	}

	touches := false
	for _, op := range ops {
		if c.r.cfg.Manages(op.ns) {
			touches = true
			break
		}
	}
	if !touches {
		return nil, false
	}

	// The same rule as every other write path, asked of the ops rather than of
	// a command shape this one does not have. See reserved.go: a document
	// carrying this system's own mark is written to both tiers and is then
	// invisible to every read that falls through.
	//
	// NOT YET MEASURED end to end: bulkWrite is an 8.0 command and the
	// throwaway host pair these were measured on is 6.0, so this one is the
	// same two pure functions the other paths use, reached from here by
	// reading. It is checked by testkeys.sh against the 8.0 cluster.
	for _, op := range ops {
		if !c.r.cfg.Manages(op.ns) {
			continue
		}
		var field string
		switch op.kind {
		case "insert":
			if f, bad := reservedTopLevel(op.doc); bad {
				field = f
			}
		case "update":
			if f, bad := reservedInUpdate(op.stmt.update); bad {
				field = f
			}
			if field == "" && op.stmt.upsert {
				// An upsert writes its filter's equality fields into the
				// document it creates, so the filter is a write path here too.
				if f, bad := reservedInUpsertFilter(op.stmt.filter); bad {
					field = f
				}
			}
		}
		if field == "" {
			continue
		}
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a bulkWrite that would put this system's own mark on a document",
			"ns", op.ns, "field", field)
		out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
			"thermocline: refusing this bulkWrite because an operation on "+op.ns+" sets "+field+
				", which is the field this system marks archived documents deleted with. A document "+
				"carrying it is written to both tiers and is then invisible to every read that "+
				"falls through to the archive, and a fast tier rebuilt from the archive does not "+
				"have it at all. Rename the field; it is reserved only in collections this system "+
				"tiers.")
		if rerr != nil {
			return nil, false
		}
		return out, true
	}

	// And the _id's size, for the same reason as the ordinary paths. See
	// largeid.go.
	for _, op := range ops {
		if !c.r.cfg.Manages(op.ns) {
			continue
		}
		n, bad := 0, false
		switch op.kind {
		case "insert":
			n, bad = largeIDInDoc(op.doc)
		case "update":
			n, bad = largeIDInUpdate(op.stmt.update, op.stmt.filter, op.stmt.upsert)
		}
		if bad {
			return c.refuseLargeID(msg, op.ns, n)
		}
	}

	_, inTxn := txnOf(cmd)

	// Documents the write has to be able to see have to be here before it runs.
	// Before, not after: repeating a write after a copy-up would apply $inc
	// twice, which is why the ordinary path does it in this order too.
	for _, op := range ops {
		if op.kind == "insert" || !c.r.cfg.Manages(op.ns) {
			continue
		}
		if len(op.stmt.filter) == 0 {
			continue
		}
		// An upsert also asks for the _id it would create. See
		// upsertStatement.filters.
		filters := []bson.Raw{op.stmt.filter}
		if op.stmt.upsert {
			filters = upsertStatement{filter: op.stmt.filter, collation: op.stmt.collation,
				update: op.stmt.update}.filters()
		}
		for _, filter := range filters {
			if inTxn {
				switch out, res := c.warmForTxn(ctx, op.ns, filter, copyUpOptions{let: letOf(cmd), collation: op.stmt.collation, budget: maxTimeBudget(cmd)}, msg.Header.RequestID, msg.Header.RequestID,
					"the documents this transaction writes"); res {
				case txnWarmReplay:
					c.r.Stats.TxnReplays.Add(1)
					c.cls = ClassTxnWarm
					return out, true
				case txnWarmComplete:
					continue
				}
				c.r.Stats.Refused.Add(1)
				c.cls = ClassRefused
				out, terr := c.r.txnCannotSpanTiers(msg.Header.RequestID, msg.Header.RequestID, op.ns,
					"a bulkWrite inside a transaction cannot reach documents the fast tier does not hold")
				if terr != nil {
					return nil, false
				}
				return out, true
			}
			if n, cerr := c.r.copyUpMatching(ctx, op.ns, filter, copyUpOptions{let: letOf(cmd), collation: op.stmt.collation, budget: maxTimeBudget(cmd)}); cerr != nil {
				// The caller's own deadline, reported the way MongoDB reports it
				// rather than as a refusal by this router.
				if out, isDeadline, ok := c.deadlineExpired(msg.Header.RequestID, msg.Header.RequestID, op.ns, "bulkWrite", cerr); isDeadline {
					return out, ok
				}
				c.r.Stats.Refused.Add(1)
				c.cls = ClassRefused
				c.log.Warn("refused a bulkWrite that would only partly apply", "ns", op.ns, "err", cerr)
				out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
					"thermocline: refusing this bulkWrite because it matches documents the fast tier does not hold, "+
						"and applying it to only some of them would report success for a change that did not happen: "+
						cerr.Error())
				if rerr != nil {
					return nil, false
				}
				return out, true
			} else if n > 0 {
				c.log.Info("copied documents up so a bulkWrite could reach them", "ns", op.ns, "documents", n)
			}
		}
	}

	// The _id check an ordinary insert gets, because bulkWrite is a second way
	// to send an insert and it was not getting it.
	//
	// An insert carrying an _id that lives only in the archive hides a
	// collision the application relies on, and the feeder then mirrors the new
	// document over the archived one. Measured through this command before this
	// existed: {_id: 500, v: "archived, precious", tier: "gold"} in the archive,
	// a bulkWrite inserting {_id: 500, v: "clobber via bulkWrite"} accepted with
	// nInserted 1, and six seconds later the archive held the clobber. The same
	// insert sent the ordinary way was refused. See insertid.go, whose guard
	// this reuses whole: the check belongs to the command shape, not to the
	// command name.
	//
	// Before the key claims below, because mongod reports _id first and because
	// a refusal here has nothing to give back yet.
	idDocs := map[string][]bson.Raw{}
	idAt := map[string][]int{}
	var idOrder []string
	for i, op := range ops {
		if op.kind != "insert" || !c.r.cfg.Manages(op.ns) {
			continue
		}
		if _, seen := idDocs[op.ns]; !seen {
			idOrder = append(idOrder, op.ns)
		}
		idDocs[op.ns] = append(idDocs[op.ns], op.doc)
		idAt[op.ns] = append(idAt[op.ns], i)
	}
	// Every operation refused here, by its position and the index it would
	// have duplicated. See applyOtherOps.
	refused := map[int]string{}
	for _, ns := range idOrder {
		clashes, err := c.r.archivedIDClashes(ctx, ns, idDocs[ns])
		// Counted and logged by refuseStale. See archivedIDClashes.
		if out, done := c.refusedAsBehind(ctx, msg, err, nil); done {
			return out, true
		}
		if err != nil {
			// An insert that cannot be checked is refused, the way the ordinary
			// path refuses one, and for the same reason: what this check stops
			// destroys data that is already safe. The exception is the same too,
			// a fast tier claiming all of history, where its own _id index is
			// the whole answer.
			if mark, known := c.r.completeness(ctx, ns); known && mark.Claimed && mark.CoversAll() {
				c.log.Warn("could not check a bulkWrite's ids against the archive; the fast tier "+
					"claims all of history, so its own _id index is the whole answer", "ns", ns, "err", err)
				continue
			}
			c.r.Stats.Refused.Add(1)
			c.cls = ClassRefused
			c.log.Warn("refused a bulkWrite whose ids could not be checked against the archive",
				"ns", ns, "err", err)
			out, rerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
				"thermocline: refusing this bulkWrite because the archive could not be asked whether it "+
					"already holds one of these _ids, and accepting it would overwrite an archived "+
					"document: "+err.Error())
			if rerr != nil {
				return nil, false
			}
			return out, true
		}
		for _, cl := range clashes {
			if cl.doc >= 0 && cl.doc < len(idAt[ns]) {
				refused[idAt[ns][cl.doc]] = "_id_"
			}
		}
	}
	ordered := true
	if v, lerr := cmd.LookupErr("ordered"); lerr == nil {
		if b, bok := v.BooleanOK(); bok {
			ordered = b
		}
	}
	// An ordered bulkWrite stops at its first error, so nothing after the first
	// refusal runs, and nothing after it claims a key.
	stopAt := len(ops)
	if ordered {
		for i := range ops {
			if _, bad := refused[i]; bad {
				stopAt = i
				break
			}
		}
	}

	// Then the unique keys, for the same reason and in the same way as an
	// ordinary insert or update.
	var claimed []string
	for oi := 0; oi < stopAt; oi++ {
		op := ops[oi]
		if _, bad := refused[oi]; bad || !c.r.cfg.Manages(op.ns) {
			continue
		}
		// This write may be what creates the collection on the fast tier, and a
		// collection created by a write carries none of the archive's options.
		// See Indexer.EnsureCollection.
		c.r.ensureTieredCollection(ctx, op.ns)
		idxs := c.r.uniqueIndexes(ctx, op.ns)
		if len(idxs) == 0 {
			continue
		}

		switch op.kind {
		case "insert":
			got, clashes, err := c.r.claimKeys(ctx, op.ns, []bson.Raw{op.doc}, idxs, true)
			claimed = append(claimed, got...)
			if out, done := c.refusedAsBehind(ctx, msg, err, claimed); done {
				return out, true
			}
			if err != nil {
				c.log.Warn("could not check unique keys for this bulkWrite, letting it through",
					"ns", op.ns, "err", err)
				continue
			}
			if len(clashes) > 0 {
				c.r.releaseKeys(ctx, got)
				refused[oi] = clashes[0].index
				if ordered {
					stopAt = oi
				}
			}
		case "update":
			sh := parseUpdate(op.stmt.update)
			for i := range idxs {
				// An upsert is checked whatever it appears to touch, because
				// the document it creates takes its key from the filter as
				// much as from the update. See affects.
				if !sh.affects(idxs[i]) && !op.stmt.upsert {
					continue
				}
				clash, kids, cerr := c.claimStatement(ctx, op.ns, idxs[i], sh, op.stmt)
				claimed = append(claimed, kids...)
				if out, done := c.refusedAsBehind(ctx, msg, cerr, claimed); done {
					return out, true
				}
				if cerr != nil {
					c.log.Warn("could not check unique keys for this bulkWrite, letting it through",
						"ns", op.ns, "err", cerr)
					continue
				}
				if clash {
					c.r.releaseKeys(ctx, kids)
					refused[oi] = idxs[i].name
					if ordered {
						stopAt = oi
					}
					break
				}
			}
		}
	}

	if len(refused) == 0 {
		// Held until the reply, so a command the fast tier rejects gives its
		// keys back rather than blocking those values until something collides.
		c.pendingKeys = append(c.pendingKeys, claimed...)
		return nil, false
	}
	c.r.Stats.DuplicateKeys.Add(int64(len(refused)))
	if out, done := c.applyOtherOps(ctx, msg, cmd, ops, refused, stopAt, ordered, claimed); done {
		return out, true
	}
	var first string
	for i := range ops {
		if idx, bad := refused[i]; bad {
			first = idx
			break
		}
	}
	return c.refuseBulk(ctx, msg, claimed, ops[0].ns, first)
}

// applyOtherOps runs the operations of a bulkWrite that were not refused and
// answers with mongod's results for them and a duplicate key error for each
// that was, at the positions the client sent.
//
// The whole command used to be refused over one archived duplicate. mongod
// runs the rest and reports the one, ordered stopping at it and unordered going
// on, and an application loading a thousand documents expects the other nine
// hundred and ninety-nine to land. done false means nothing was sent.
func (c *conn) applyOtherOps(ctx context.Context, msg *wire.Message, cmd bson.Raw, ops []bulkOp,
	refused map[int]string, stopAt int, ordered bool, claimed []string) (*wire.Message, bool) {
	var keep bson.A
	var keepAt []int
	for i := 0; i < stopAt; i++ {
		if _, bad := refused[i]; bad {
			continue
		}
		keep = append(keep, ops[i].raw)
		keepAt = append(keepAt, i)
	}
	errorsOnly := boolField(cmd, "errorsOnly")

	var up bson.Raw
	var results []bson.D
	if len(keep) > 0 {
		elems, err := cmd.Elements()
		if err != nil {
			return nil, false
		}
		trimmed := bson.D{}
		for _, e := range elems {
			switch e.Key() {
			case "ops", "cursor":
				continue
			}
			trimmed = append(trimmed, bson.E{Key: e.Key(), Value: e.Value()})
		}
		// Every result in the first batch, so each can be put back at its own
		// position; a later batch would come back by getMore with positions
		// this would not see.
		trimmed = append(trimmed, bson.E{Key: "ops", Value: keep},
			bson.E{Key: "cursor", Value: bson.D{{Key: "batchSize", Value: int32(len(keep) + 1)}}})
		trimmed = withStmtIDs(trimmed, cmd, keepAt)
		fwd, err := wire.NewMsg(msg.Header.RequestID, 0, trimmed)
		if err != nil {
			return nil, false
		}
		if werr := wire.Write(c.upstream, fwd); werr != nil {
			c.upstreamBroke()
			return nil, false
		}
		reply, rerr := wire.Read(c.upstream)
		if rerr != nil {
			c.upstreamBroke()
			out, _ := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
				"thermocline: lost the fast tier's reply to the operations of this bulkWrite that did not "+
					"take an archived key; they may have been applied")
			return out, true
		}
		c.r.noteOpTime(reply)
		up, err = reply.ReplyDoc()
		if err != nil || !replyOK(up) {
			out := reply
			out.Header.ResponseTo = msg.Header.RequestID
			return out, true
		}
		if id, ok := intOf64(up.Lookup("cursor", "id")); ok && id != 0 {
			c.log.Warn("a split bulkWrite's results did not fit one batch; positions after it are mongod's",
				"results", len(keep))
		}
		if arr, ok := up.Lookup("cursor", "firstBatch").ArrayOK(); ok {
			vals, _ := arr.Values()
			for _, v := range vals {
				d, dok := v.DocumentOK()
				if !dok {
					continue
				}
				r := bson.D{}
				els, _ := d.Elements()
				for _, el := range els {
					if el.Key() == "idx" {
						i, _ := intOf(el.Value())
						if i >= 0 && i < len(keepAt) {
							r = append(r, bson.E{Key: "idx", Value: int32(keepAt[i])})
						}
						continue
					}
					r = append(r, bson.E{Key: el.Key(), Value: el.Value()})
				}
				results = append(results, r)
			}
		}
		c.pendingKeys = append(c.pendingKeys, claimed...)
	}

	nErrors, _ := intOf(up.Lookup("nErrors"))
	// An ordered batch that failed before the first refusal never reached it.
	if !(ordered && nErrors > 0) {
		for i := range ops {
			idx, bad := refused[i]
			if !bad {
				continue
			}
			results = append(results, bson.D{
				{Key: "ok", Value: 0.0},
				{Key: "idx", Value: int32(i)},
				{Key: "code", Value: int32(11000)},
				{Key: "codeName", Value: "DuplicateKey"},
				{Key: "errmsg", Value: "E11000 duplicate key error collection: " + ops[i].ns + " index: " + idx +
					" :: thermocline: the existing document with this key is in the archive, where the fast " +
					"tier's unique index cannot see it"},
				{Key: "n", Value: int32(0)},
			})
			nErrors++
			if ordered {
				break
			}
		}
	}
	sort.SliceStable(results, func(i, j int) bool { return resultIdx(results[i]) < resultIdx(results[j]) })
	batch := bson.A{}
	for _, r := range results {
		if errorsOnly && resultOK(r) {
			continue
		}
		batch = append(batch, r)
	}
	count := func(k string) int32 {
		n, _ := intOf(up.Lookup(k))
		return int32(n)
	}
	body := bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "id", Value: int64(0)},
			{Key: "firstBatch", Value: batch},
			{Key: "ns", Value: "admin.$cmd.bulkWrite"},
		}},
		{Key: "nErrors", Value: int32(nErrors)},
		{Key: "nInserted", Value: count("nInserted")},
		{Key: "nMatched", Value: count("nMatched")},
		{Key: "nModified", Value: count("nModified")},
		{Key: "nUpserted", Value: count("nUpserted")},
		{Key: "nDeleted", Value: count("nDeleted")},
	}
	if up != nil {
		els, _ := up.Elements()
		for _, el := range els {
			switch el.Key() {
			case "cursor", "nErrors", "nInserted", "nMatched", "nModified", "nUpserted", "nDeleted", "ok":
				continue
			}
			body = append(body, bson.E{Key: el.Key(), Value: el.Value()})
		}
	}
	body = append(body, bson.E{Key: "ok", Value: 1.0})
	c.cls = ClassRefused
	out, err := wire.NewMsg(msg.Header.RequestID, msg.Header.RequestID, body)
	if err != nil {
		return nil, false
	}
	return out, true
}

// resultIdx and resultOK read a bulkWrite result built above.
func resultIdx(r bson.D) int32 {
	for _, e := range r {
		if e.Key == "idx" {
			switch v := e.Value.(type) {
			case int32:
				return v
			case bson.RawValue:
				n, _ := intOf(v)
				return int32(n)
			}
		}
	}
	return -1
}

func resultOK(r bson.D) bool {
	for _, e := range r {
		if e.Key == "ok" {
			switch v := e.Value.(type) {
			case float64:
				return v == 1
			case bson.RawValue:
				if f, ok := v.DoubleOK(); ok {
					return f == 1
				}
				n, _ := intOf(v)
				return n == 1
			}
		}
	}
	return false
}

// refuseBulk turns down a whole bulkWrite because one of its operations would
// duplicate a key an archived document holds.
//
// The whole command, not the one operation. bulkWrite reports per-operation
// results through a cursor, and rebuilding that reply to reject one entry while
// running the rest would be inventing results the server never produced. The
// application sees a duplicate key error, which is what it would have seen from
// a single cluster, and retries without the offending operation.
func (c *conn) refuseBulk(ctx context.Context, msg *wire.Message, claimed []string, ns, index string) (*wire.Message, bool) {
	c.r.releaseKeys(ctx, claimed)
	c.r.Stats.DuplicateKeys.Add(1)
	c.cls = ClassRefused
	c.log.Debug("bulkWrite would duplicate an archived document", "ns", ns, "index", index)
	out, err := c.r.duplicateKeyReply(msg.Header.RequestID, msg.Header.RequestID, ns, index)
	if err != nil {
		return nil, false
	}
	return out, true
}
