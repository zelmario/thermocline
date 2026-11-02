package router

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"regexp"
	"strings"
	"testing"
)

// The page's one inline script has to match the hash the policy allows, or the
// browser refuses to run it and the dashboard is a blank page.
func TestDashboardPolicyAllowsItsOwnScript(t *testing.T) {
	start := bytes.Index(dashboardHTML, []byte("<script>"))
	end := bytes.Index(dashboardHTML, []byte("</script>"))
	if start < 0 || end < start {
		t.Fatal("the dashboard has no inline script")
	}
	if bytes.Count(dashboardHTML, []byte("<script")) != 1 {
		t.Fatal("the dashboard has more than one script, and the policy allows one hash")
	}
	sum := sha256.Sum256(dashboardHTML[start+len("<script>") : end])
	want := "script-src 'sha256-" + base64.StdEncoding.EncodeToString(sum[:]) + "'"
	if !strings.Contains(dashboardCSP, want) {
		t.Fatalf("policy %q does not allow the page's script (%s)", dashboardCSP, want)
	}
	if strings.Contains(dashboardCSP, "script-src 'unsafe-inline'") {
		t.Fatal("the policy allows any inline script")
	}
}

// A collection name is chosen by whoever creates the collection, and the
// namespace table and its tooltip put it into markup. Unescaped, a collection
// named <img src=x onerror=...> ran in the operator's browser.
func TestDashboardEscapesServerStringsInMarkup(t *testing.T) {
	page := string(dashboardHTML)
	// A server field concatenated straight into a string, such as
	// "<td>" + x.ns + "</td>", rather than through esc().
	raw := regexp.MustCompile(`"\s*\+\s*\(?\s*[a-z]\.(ns|listen|fast_tier|completeness|feeder_id)\b`)
	for _, m := range raw.FindAllStringIndex(page, -1) {
		// The namespace chart's label is shortened and then set as
		// textContent by txt(), which is not markup.
		if strings.HasPrefix(page[m[1]:], ".slice(") {
			continue
		}
		line := strings.Count(page[:m[0]], "\n") + 1
		t.Errorf("dashboard.html:%d puts a server string into markup without esc(): %s",
			line, strings.TrimSpace(page[m[0]:m[1]]))
	}
}
