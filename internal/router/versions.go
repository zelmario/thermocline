package router

import (
	"context"
	"strings"

	"github.com/zelmario/thermocline/internal/pairing"
	"github.com/zelmario/thermocline/internal/tierver"
)

// The requirement, and why it is a refusal rather than a warning, is in
// internal/tierver. This is where the router applies it: before the listener
// opens, because a router that starts on a mismatched pair answers correctly
// for months and then fails on the day the data ages.

// checkTierVersions stops the router when the tiers run different MongoDB
// releases, unless the configuration says to allow it.
func (r *Router) checkTierVersions(ctx context.Context) error {
	r.rememberReleases(ctx)
	if err := tierver.Check(ctx, r.hot, r.cold, r.cfg.AllowVersionSkew, r.log); err != nil {
		return err
	}
	return pairing.Check(ctx, r.hot, r.cold, r.log)
}

// rememberReleases records the two releases when the archive's is the older one.
//
// Only allow_version_skew gets this far, so this is the state of a deployment in
// the middle of an upgrade somebody is watching. What it is for is the error
// message: see archiveDialectNote.
func (r *Router) rememberReleases(ctx context.Context) {
	hot, hotOK := tierver.Read(ctx, r.hot)
	cold, coldOK := tierver.Read(ctx, r.cold)
	if !hotOK || !coldOK || tierver.SameRelease(hot.Parts, cold.Parts) {
		return
	}
	if tierver.Compare(cold.Parts, hot.Parts) >= 0 {
		// The archive is the newer one, which is the safe direction: it
		// understands everything the fast tier can be sent.
		return
	}
	r.olderArchive.Store(&releaseGap{hot: hot.Release(), cold: cold.Release()})
}

// releaseGap is the pair of releases, when the archive is behind.
type releaseGap struct {
	hot, cold string
}

// olderArchiveGap returns the gap, or nil when there is none.
func (r *Router) olderArchiveGap() *releaseGap {
	return r.olderArchive.Load()
}

// archiveDialectNote explains an archive error that looks like the older release
// refusing the query, and is empty for anything else.
//
// This is the failure the version requirement exists to prevent, and it only
// happens on a deployment that has switched the requirement off: the archive
// runs the client's own query, so one using a stage, operator or option the older
// release does not have works for as long as its documents are on the fast tier,
// and then fails with the archive's own parse error naming something the
// application has always used. Nothing in that error reads as a version problem.
//
// Measured on 8.0.26 over 7.0.37, with allow_version_skew on: the same
// $convert with format answered from fast tier documents and, over a collection
// the archive had to answer, came back "$convert found an unknown argument:
// format" with no mention of either release.
//
// Hedged on purpose. The router cannot know that this query would have parsed on
// the newer release, only that the pair disagrees and that the error is the shape
// that disagreement makes.
func (r *Router) archiveDialectNote(code int32, message string) string {
	gap := r.olderArchiveGap()
	if gap == nil || !dialectRefusal(code, message) {
		return ""
	}
	return " (thermocline: this came from the archive, which runs MongoDB " + gap.cold +
		" while the fast tier runs " + gap.hot + ". A read the fast tier cannot answer in full is " +
		"sent to the archive as the application wrote it, so a query using anything " + gap.cold +
		" does not understand fails only when it reaches back that far. Upgrade the archive, or " +
		"remove allow_version_skew to have the daemons refuse the pair again.)"
}

// dialectRefusal reports an error that looks like a server refusing to recognise
// something rather than failing to do it.
//
// Deliberately narrow. FailedToParse is not in the list, even though a release
// gap can produce it, because it is also what a genuinely malformed query gets
// from any release, and a version note on that sends a developer after a ghost.
// What is left is a server saying it does not know a name: InvalidPipelineOperator
// and the unrecognized-stage and unknown-field codes, plus the messages for the
// ones that arrive wrapped in a generic Location code. "$convert found an unknown
// argument: format" is one of those, which is how the case that prompted this
// arrives.
func dialectRefusal(code int32, message string) bool {
	switch code {
	case 168, 40324, 40415, 31325:
		return true
	}
	low := strings.ToLower(message)
	for _, pat := range []string{
		"unknown argument", "unrecognized expression", "unrecognized pipeline stage",
		"unknown operator", "unknown top level operator",
		"unrecognized field", "unknown modifier", "unknown expression",
	} {
		if strings.Contains(low, pat) {
			return true
		}
	}
	return false
}
