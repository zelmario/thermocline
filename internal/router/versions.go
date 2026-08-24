package router

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// The archive runs the client's own queries, not a rewritten shape of them.
//
// That is deliberate and it is what makes a fall-through correct: whatever the
// application asked for, the archive is asked the same thing. It also means the
// archive has to understand everything the application says, and in the shape
// this system is built for it is the *older* cluster. An adoption points the
// fast tier at a new replica set and leaves the existing one as the archive, so
// the two are almost never the same version.
//
// A pipeline stage, an operator or an option newer than the archive then works
// until the day the documents it reads have aged out of the fast tier, and
// fails with the archive's own error, naming a stage the application has always
// used. That reads as a Thermocline bug and is not one, and nothing said the
// versions differed.
//
// Nothing can be done about it from here: rewriting a client's pipeline into
// an older dialect is not something a proxy can do correctly. So it is named,
// once, at startup, where an operator planning an adoption can act on it.

// warnAboutVersionSkew says what each tier is running, and what an older
// archive costs.
func (r *Router) warnAboutVersionSkew(ctx context.Context) {
	hot, hotOK := serverVersion(ctx, r.hot)
	cold, coldOK := serverVersion(ctx, r.cold)
	if !hotOK || !coldOK {
		// Not worth failing a start over, and the log says which one could not
		// be asked so the silence is not mistaken for agreement.
		r.log.Debug("could not read both tiers' versions",
			"fast_tier_known", hotOK, "archive_known", coldOK)
		return
	}

	r.log.Info("tier versions", "fast_tier", hot.text, "archive", cold.text)
	if compareVersions(cold.parts, hot.parts) >= 0 {
		return
	}
	r.log.Warn("the archive is older than the fast tier",
		"fast_tier", hot.text, "archive", cold.text,
		"reason", "a read the fast tier cannot answer in full is sent to the archive as the "+
			"application wrote it, so the archive has to understand every stage, operator and "+
			"option the application uses",
		"effect", "a query using anything newer than the archive works until the documents it "+
			"reads age out of the fast tier, and then fails with the archive's own error naming "+
			"a stage the application has always used",
		"action", "upgrade the archive to at least the fast tier's version, or confirm the "+
			"application uses nothing newer than the archive understands")
}

// version is a server's version, kept both ways: the text an operator reads and
// the numbers a comparison needs.
type version struct {
	text  string
	parts []int32
}

// serverVersion asks a tier what it is running.
//
// versionArray rather than the string, because the string carries release
// candidate and pre-release suffixes that no ordering here should try to
// interpret. The string is kept for the log, where a human reads it.
func serverVersion(ctx context.Context, c *mongo.Client) (version, bool) {
	var res struct {
		Version      string  `bson:"version"`
		VersionArray []int32 `bson:"versionArray"`
	}
	err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&res)
	if err != nil || len(res.VersionArray) == 0 {
		return version{}, false
	}
	text := res.Version
	if text == "" {
		text = fmt.Sprint(res.VersionArray)
	}
	return version{text: text, parts: res.VersionArray}, true
}

// compareVersions orders two versionArrays, comparing only the parts both
// carry. MongoDB's fourth element is a build counter and says nothing about
// what the server understands, so this stops at the first difference in the
// three that do.
func compareVersions(a, b []int32) int {
	n := 3
	for i := 0; i < n; i++ {
		var x, y int32
		if i < len(a) {
			x = a[i]
		}
		if i < len(b) {
			y = b[i]
		}
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		}
	}
	return 0
}
