// Package pairing records which archive a fast tier has been working with, so
// a deployment pointed at a different one is refused rather than served.
//
// cold.uri can be repointed by a typo, a config copied from another
// environment, or a DNS name that now resolves somewhere else, and nothing used
// to notice: measured before this, ten documents mirrored into one archive,
// then the URI changed to a second, and a client counted the four the second
// happened to hold. The replica set name cannot tell two archives apart,
// because every environment calls its set rs0, so the identity is one this
// system writes: a random id in the archive's bookkeeping database, and the
// same id recorded on the fast tier the first time the two are used together.
package pairing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/zelmario/thermocline/internal/meta"
)

// identityColl is where the archive keeps its own id, and pairingColl where
// the fast tier keeps the id of the archive it has been working with.
const (
	identityColl = "identity"
	pairingColl  = "pairing"
	docID        = "archive"
)

type record struct {
	ID string    `bson:"id"`
	At time.Time `bson:"at"`
}

// ErrMismatch is a fast tier recorded against one archive and pointed at
// another.
var ErrMismatch = errors.New("the fast tier has been working with a different archive")

// Check compares the archive this fast tier recorded with the archive it is
// connected to, and records the pair when there is nothing to compare yet.
//
//   - Neither has an id: the first start of this deployment. Both are written.
//   - The archive has one and the fast tier does not: a fast tier new or
//     rebuilt from the archive, which is what rebuilding looks like. Recorded.
//   - Both, and the same: the pair it has always been.
//   - The fast tier has one and the archive a different one, or none: cold.uri
//     points at another cluster. Refused, because serving it answers from
//     somebody else's data, and a feeder would mirror into it.
//
// Accept is the deliberate way through the last case. A write that cannot be
// made for lack of privilege is logged and let through: this is a guard
// against a mistake, and an account that cannot write the bookkeeping
// database is a configuration the rest of the system reports on its own.
func Check(ctx context.Context, hot, cold *mongo.Client, log *slog.Logger) error {
	arch, archOK, err := read(ctx, cold, identityColl)
	if err != nil {
		log.Warn("could not read the archive's identity, so which archive this is was not checked", "err", err)
		return nil
	}
	seen, seenOK, err := read(ctx, hot, pairingColl)
	if err != nil {
		log.Warn("could not read which archive the fast tier has been working with, so it was not checked", "err", err)
		return nil
	}
	switch {
	case seenOK && (!archOK || arch.ID != seen.ID):
		found := "has no identity, so it is not an archive this system has used"
		if archOK {
			found = "is the archive " + arch.ID + " recorded " + arch.At.UTC().Format(time.RFC3339)
		}
		return fmt.Errorf("%w: the fast tier has been working with archive %s since %s, and cold.uri now %s. "+
			"Serving from it would answer from another deployment's data, and mirroring would write into it. "+
			"If cold.uri changed by mistake, put it back. If this is the archive you mean to use from now "+
			"on, run: cline pairing --accept",
			ErrMismatch, seen.ID, seen.At.UTC().Format(time.RFC3339), found)
	case seenOK:
		return nil
	}
	if !archOK {
		id, werr := createIdentity(ctx, cold)
		if werr != nil {
			log.Warn("could not give the archive an identity, so a later change of cold.uri will not be noticed", "err", werr)
			return nil
		}
		arch = record{ID: id}
	}
	if werr := write(ctx, hot, arch.ID); werr != nil {
		log.Warn("could not record on the fast tier which archive it works with, so a later change of cold.uri will not be noticed", "err", werr)
		return nil
	}
	log.Info("recorded which archive this fast tier works with", "archive", arch.ID)
	return nil
}

// Accept records the archive the fast tier is connected to now as the one it
// works with, giving that archive an identity if it has none, and returns it.
func Accept(ctx context.Context, hot, cold *mongo.Client) (string, error) {
	arch, archOK, err := read(ctx, cold, identityColl)
	if err != nil {
		return "", fmt.Errorf("read the archive's identity: %w", err)
	}
	id := arch.ID
	if !archOK {
		if id, err = createIdentity(ctx, cold); err != nil {
			return "", fmt.Errorf("give the archive an identity: %w", err)
		}
	}
	if err := write(ctx, hot, id); err != nil {
		return "", fmt.Errorf("record it on the fast tier: %w", err)
	}
	return id, nil
}

func read(ctx context.Context, c *mongo.Client, coll string) (record, bool, error) {
	var r record
	err := c.Database(meta.DB).Collection(coll).FindOne(ctx, bson.D{{Key: "_id", Value: docID}}).Decode(&r)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return record{}, false, nil
	}
	if err != nil {
		return record{}, false, err
	}
	return r, r.ID != "", nil
}

// createIdentity writes the archive's id if it has none, and returns whichever
// id it has afterwards, so two daemons starting at once agree on one.
func createIdentity(ctx context.Context, cold *mongo.Client) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	c := cold.Database(meta.DB).Collection(identityColl)
	_, err := c.UpdateOne(ctx, bson.D{{Key: "_id", Value: docID}},
		bson.D{{Key: "$setOnInsert", Value: bson.D{{Key: "id", Value: hex.EncodeToString(b)}, {Key: "at", Value: time.Now().UTC()}}}},
		options.UpdateOne().SetUpsert(true))
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return "", err
	}
	r, ok, err := read(ctx, cold, identityColl)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errors.New("the archive's identity was written and cannot be read back")
	}
	return r.ID, nil
}

func write(ctx context.Context, hot *mongo.Client, id string) error {
	_, err := hot.Database(meta.DB).Collection(pairingColl).ReplaceOne(ctx, bson.D{{Key: "_id", Value: docID}},
		bson.D{{Key: "id", Value: id}, {Key: "at", Value: time.Now().UTC()}},
		options.Replace().SetUpsert(true))
	return err
}
