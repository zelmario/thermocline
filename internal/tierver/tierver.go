// Package tierver checks that both tiers run the same MongoDB release.
//
// The archive runs the client's own queries, not a rewritten shape of them.
//
// That is deliberate and it is what makes a fall-through correct: whatever the
// application asked for, the archive is asked the same thing. It also means the
// archive has to understand everything the application says. A pipeline stage,
// an operator or an option newer than the archive then works until the day the
// documents it reads have aged out of the fast tier, and fails with the
// archive's own error, naming a stage the application has always used. That
// reads as a Thermocline bug and is not one.
//
// Nothing can be done about it from here: rewriting a client's pipeline into an
// older dialect is not something a proxy can do correctly. So the requirement
// is the configuration, not the code. Both tiers run the same release, and a
// daemon that finds otherwise refuses to start rather than serving right
// answers until the data ages.
//
// An adoption is where this bites, because the existing cluster becomes the
// archive and is usually the older side. Upgrading it is part of the adoption,
// and this is what says so, on the day the pair is first pointed at each other
// rather than months later.
package tierver

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Version is a server's version, kept both ways: the text an operator reads and
// the numbers a comparison needs.
type Version struct {
	Text  string
	Parts []int32
}

// Release is the major.minor part, which is the unit MongoDB adds a stage, an
// operator or an option in. 8.0.4 and 8.0.26 understand the same dialect;
// 7.0.30 and 8.0.4 do not.
func (v Version) Release() string {
	if len(v.Parts) < 2 {
		return v.Text
	}
	return fmt.Sprintf("%d.%d", v.Parts[0], v.Parts[1])
}

// Read asks a tier what it is running.
//
// versionArray rather than the string, because the string carries release
// candidate and pre-release suffixes that no ordering here should try to
// interpret. The string is kept for the log, where a human reads it.
func Read(ctx context.Context, c *mongo.Client) (Version, bool) {
	var res struct {
		Version      string  `bson:"version"`
		VersionArray []int32 `bson:"versionArray"`
	}
	err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&res)
	if err != nil || len(res.VersionArray) == 0 {
		return Version{}, false
	}
	text := res.Version
	if text == "" {
		text = fmt.Sprint(res.VersionArray)
	}
	return Version{Text: text, Parts: res.VersionArray}, true
}

// Compare orders two versionArrays, comparing only the parts both carry.
// MongoDB's fourth element is a build counter and says nothing about what the
// server understands, so this stops at the first difference in the three that
// do.
func Compare(a, b []int32) int {
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

// SameRelease reports whether two versionArrays are the same major.minor.
//
// The patch is deliberately not part of this. One tier is always upgraded
// before the other, so demanding an identical patch level would refuse to start
// in the middle of every rolling upgrade, which is an outage in exchange for
// nothing: a patch release adds no stage, operator or option for the archive to
// fail on.
func SameRelease(a, b []int32) bool {
	for i := 0; i < 2; i++ {
		var x, y int32
		if i < len(a) {
			x = a[i]
		}
		if i < len(b) {
			y = b[i]
		}
		if x != y {
			return false
		}
	}
	return true
}

// ReadFCV asks a tier what feature compatibility version it is running.
//
// A release is only half of what a server understands. Two 8.0 binaries with
// different feature compatibility versions accept different commands, which is
// the same failure with the same silence: the fast tier takes the query, the
// archive refuses it, and only the documents' age decides which one was asked.
//
// A server too old to have the parameter, or an account not allowed to read it,
// returns false rather than an error. See Check.
func ReadFCV(ctx context.Context, c *mongo.Client) (string, bool) {
	var res struct {
		FCV struct {
			Version       string `bson:"version"`
			TargetVersion string `bson:"targetVersion"`
		} `bson:"featureCompatibilityVersion"`
	}
	err := c.Database("admin").RunCommand(ctx, bson.D{
		{Key: "getParameter", Value: 1},
		{Key: "featureCompatibilityVersion", Value: 1},
	}).Decode(&res)
	if err != nil || res.FCV.Version == "" {
		return "", false
	}
	if res.FCV.TargetVersion != "" {
		// Mid-transition: setFeatureCompatibilityVersion is still running. Say
		// so rather than reporting the version it is leaving as settled.
		return res.FCV.Version + "->" + res.FCV.TargetVersion, true
	}
	return res.FCV.Version, true
}

// FCVMismatch is the error for two tiers on the same release that still do not
// understand the same things.
type FCVMismatch struct {
	Hot, Cold Version
	HotFCV    string
	ColdFCV   string
}

func (m *FCVMismatch) Error() string {
	return fmt.Sprintf("the tiers are both on %s but disagree on featureCompatibilityVersion: "+
		"fast tier %s, archive %s. A read the fast tier cannot answer in full is sent to the "+
		"archive as the application wrote it, and a lower feature compatibility version refuses "+
		"commands the binary supports, so the archive can reject what the fast tier accepted. "+
		"Run db.adminCommand({setFeatureCompatibilityVersion: \"%s\"}) on the tier that is behind, "+
		"or set allow_version_skew: true to start anyway",
		m.Hot.Release(), m.HotFCV, m.ColdFCV, higherFCV(m.HotFCV, m.ColdFCV))
}

// higherFCV is the version to bring the tier that is behind up to.
//
// A tier in the middle of setFeatureCompatibilityVersion reports both the
// version it is leaving and the one it is heading for, and the advice names
// where it is going: that is the version the pair will agree on, and
// "7.0->8.0" is not something anybody can pass to the command.
func higherFCV(a, b string) string {
	if fcvOrder(destination(a)) < fcvOrder(destination(b)) {
		return destination(b)
	}
	return destination(a)
}

// destination is where an FCV in transition is heading, or the version itself.
func destination(s string) string {
	if _, to, ok := strings.Cut(s, "->"); ok {
		return to
	}
	return s
}

// fcvOrder orders one major.minor FCV string against another.
func fcvOrder(s string) float64 {
	var maj, min int
	if _, err := fmt.Sscanf(s, "%d.%d", &maj, &min); err != nil {
		return 0
	}
	return float64(maj) + float64(min)/1000
}

// Mismatch is the error a daemon refuses to start with. It names both versions,
// because the whole point is that nothing said they differed.
type Mismatch struct {
	Hot, Cold Version
}

func (m *Mismatch) Error() string {
	return fmt.Sprintf("the tiers run different MongoDB releases: fast tier %s, archive %s. "+
		"Both tiers must run the same major.minor release, because a read the fast tier cannot "+
		"answer in full is sent to the archive as the application wrote it, so the archive has to "+
		"understand every stage, operator and option the application uses. "+
		"Upgrade the older tier to %s, or set allow_version_skew: true to start anyway and accept "+
		"that a query can fail once the documents it reads age out of the fast tier",
		m.Hot.Text, m.Cold.Text, newer(m.Hot, m.Cold).Release())
}

// newer is the release the older tier would be upgraded to.
func newer(hot, cold Version) Version {
	if Compare(cold.Parts, hot.Parts) > 0 {
		return cold
	}
	return hot
}

// Check reads both tiers' versions and reports whether a daemon may run.
//
// It never fails a start over a version it could not read. An unreadable
// buildInfo is a connection problem, and whatever needs that connection will
// say so in its own words a moment later; refusing here would only name the
// wrong cause. The log says the check did not happen, so silence is not
// mistaken for agreement.
func Check(ctx context.Context, hotClient, coldClient *mongo.Client, allowSkew bool, log *slog.Logger) error {
	hot, hotOK := Read(ctx, hotClient)
	cold, coldOK := Read(ctx, coldClient)
	if !hotOK || !coldOK {
		log.Warn("could not read both tiers' versions, so they were not checked",
			"fast_tier_known", hotOK, "archive_known", coldOK)
		return nil
	}

	log.Info("tier versions", "fast_tier", hot.Text, "archive", cold.Text)
	if SameRelease(hot.Parts, cold.Parts) {
		if Compare(hot.Parts, cold.Parts) != 0 {
			// The ordinary state in the middle of a rolling upgrade. Worth a
			// line, because it is also what a stalled upgrade looks like.
			log.Info("the tiers are on different patch releases of the same version",
				"fast_tier", hot.Text, "archive", cold.Text,
				"note", "allowed: a patch release adds nothing for the archive to fail on")
		}
		return checkFCV(ctx, hotClient, coldClient, hot, cold, allowSkew, log)
	}

	err := &Mismatch{Hot: hot, Cold: cold}
	if allowSkew {
		log.Warn("the tiers run different MongoDB releases, allowed by allow_version_skew",
			"fast_tier", hot.Text, "archive", cold.Text,
			"effect", "a query using anything the older tier does not understand works until the "+
				"documents it reads age out of the fast tier, and then fails with that tier's own "+
				"error naming a stage the application has always used",
			"action", "upgrade the older tier to "+newer(hot, cold).Release())
		return nil
	}
	return err
}

// checkFCV holds the same pair to the same feature compatibility version, for
// the same reason the releases have to match.
func checkFCV(ctx context.Context, hotClient, coldClient *mongo.Client,
	hot, cold Version, allowSkew bool, log *slog.Logger) error {
	hotFCV, hotOK := ReadFCV(ctx, hotClient)
	coldFCV, coldOK := ReadFCV(ctx, coldClient)
	if !hotOK || !coldOK {
		// Reading it needs a privilege the router's account may not have, and
		// a server old enough not to carry the parameter is one the release
		// check has already dealt with. Neither is worth failing a start over.
		log.Debug("could not read both tiers' featureCompatibilityVersion",
			"fast_tier_known", hotOK, "archive_known", coldOK)
		return nil
	}
	if hotFCV == coldFCV {
		log.Info("tier feature compatibility", "fcv", hotFCV)
		return nil
	}

	err := &FCVMismatch{Hot: hot, Cold: cold, HotFCV: hotFCV, ColdFCV: coldFCV}
	if allowSkew {
		log.Warn("the tiers disagree on featureCompatibilityVersion, allowed by allow_version_skew",
			"fast_tier", hotFCV, "archive", coldFCV,
			"effect", "the archive can refuse a command the fast tier accepted, and which tier "+
				"answers is decided by how old the documents are",
			"action", "setFeatureCompatibilityVersion to "+higherFCV(hotFCV, coldFCV)+
				" on the tier that is behind")
		return nil
	}
	return err
}
