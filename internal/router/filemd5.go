package router

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/zelmario/thermocline/internal/meta"
	"github.com/zelmario/thermocline/internal/wire"
)

// filemd5 is dbHash with one file's chunks instead of a collection's documents,
// and it was unhandled for as long as dbHash was.
//
// GridFS stores a file as a row in <root>.files and a row per chunk in
// <root>.chunks, and filemd5 hashes the chunks of one file in order. A
// collection of file chunks that only grows and is read rarely is exactly what
// this system tiers, so the chunks of an old file are the ones the evictor
// moves, and the command runs on the fast tier.
//
// Measured on a four chunk file with two of its chunks evicted and both still
// live in the archive:
//
//	every chunk resident   3b5d3c7d207e37dceeedd301e35e2e58 over 4 chunks
//	two chunks evicted     70bc8f4b72a86921468bf8e8441dce51 over 2 chunks
//
// ok on both. An application verifying a file it stored would read that as the
// file being corrupt, which is the worst answer a verification command can
// give, and it is the same worst answer dbHash is refused for.
//
// So it is refused on the same terms, and by asking the same question the
// tiers can actually answer: does the archive hold chunks of this file that the
// fast tier does not. Two counts bounded by files_id, which the GridFS index
// covers, on a command an application sends rarely.

// guardFileMD5 refuses a filemd5 whose file the fast tier does not hold whole.
func (c *conn) guardFileMD5(ctx context.Context, msg *wire.Message, db string) (*wire.Message, bool) {
	cmd, err := msg.CommandDoc()
	if err != nil {
		return nil, false
	}
	fileID, err := cmd.LookupErr("filemd5")
	if err != nil {
		return nil, false
	}
	root := "fs"
	if v, rerr := cmd.LookupErr("root"); rerr == nil {
		if s, ok := v.StringValueOK(); ok && s != "" {
			root = s
		}
	}
	ns := db + "." + root + ".chunks"
	if !c.r.cfg.Manages(ns) {
		// Not tiered, so the fast tier is the whole of it and its answer is the
		// file's own.
		return nil, false
	}

	hot, cold, ok := c.chunkCounts(ctx, db, root+".chunks", fileID)
	if ok && cold <= hot {
		// The fast tier holds every chunk the archive does, so its hash is the
		// file's and the command is forwarded untouched.
		return nil, false
	}
	if !ok {
		// The question could not be asked, which for this command means the
		// archive is unreachable, and forwarding then answers a hash of
		// whatever the fast tier happens to hold. That is the defect this guard
		// exists for, arriving in the one state where nothing else would catch
		// it: unlike a find, there is no archive read below to fail and say so.
		//
		// Written this way after a review of my own guard, which returned false
		// here and failed open.
		c.r.Stats.Refused.Add(1)
		c.cls = ClassRefused
		c.log.Warn("refused a filemd5 because the archive could not be asked about the file",
			"ns", ns,
			"effect", "forwarding it would hash whatever chunks the fast tier holds and call that "+
				"the file's hash, which is what this guard exists to prevent",
			"action", "retry once the archive is reachable, or run filemd5 against the archive")
		out, cerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
			"thermocline: refusing this filemd5 because the archive could not be asked whether it "+
				"holds chunks of this file the fast tier does not, so the hash might be of part of "+
				"the file. Retry once the archive is reachable, or run filemd5 against the archive.")
		if cerr != nil {
			return nil, false
		}
		return out, true
	}

	c.r.Stats.Refused.Add(1)
	c.cls = ClassRefused
	c.log.Warn("refused a filemd5 over a file the fast tier does not hold whole",
		"ns", ns, "chunks_on_the_fast_tier", hot, "chunks_in_the_archive", cold,
		"reason", "filemd5 hashes the chunks of one file in order, and the fast tier has only "+
			"some of this file's chunks, so the hash would be of those",
		"effect", "answering would return a hash that looks like the file's and is not, which an "+
			"application verifying what it stored reads as the file being corrupt",
		"action", "warm this namespace so the file's chunks are all on the fast tier, or run "+
			"filemd5 against the archive directly")
	out, cerr := c.r.commandError(msg.Header.RequestID, msg.Header.RequestID,
		"thermocline: refusing this filemd5 because the fast tier holds "+itoa(int(hot))+
			" of this file's "+itoa(int(cold))+" chunks, so the hash would be of that part rather "+
			"than of the file. A hash cannot be corrected across two tiers the way a count can. "+
			"Warm "+ns+", or run filemd5 against the archive directly.")
	if cerr != nil {
		return nil, false
	}
	return out, true
}

// chunkCounts counts one file's chunks on each tier.
//
// Bounded by files_id, which every GridFS bucket indexes, so this is two index
// lookups rather than a walk of the collection. The archive's count excludes
// the copies of chunks the application deleted, the way every other read of it
// does.
func (c *conn) chunkCounts(ctx context.Context, db, coll string, fileID bson.RawValue) (hot, cold int64, ok bool) {
	filter := bson.D{{Key: "files_id", Value: fileID}}
	h, err := c.r.hot.Database(db).Collection(coll).CountDocuments(ctx, filter)
	if err != nil {
		return 0, 0, false
	}
	cfilter := bson.D{{Key: "files_id", Value: fileID}, meta.NotArchivedDeleted()}
	cd, err := c.r.cold.Database(db).Collection(coll).CountDocuments(ctx, cfilter)
	if err != nil {
		return 0, 0, false
	}
	return h, cd, true
}
