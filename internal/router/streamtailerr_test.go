package router

import (
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// A fast tier that steps down while the router runs a stream's own stages is
// what a driver resumes after, from the last token it was given; mongod's own
// error there would carry the label. Without it the stream ended on an
// election. A stage's own refusal is not resumable: resuming would hit it again.
func TestStreamTailErrorIsResumableOnlyForTheFastTier(t *testing.T) {
	c := &conn{r: &Router{}}
	labels := func(err error) (int32, []string) {
		out, rerr := c.streamTailErrorTo(1, err)
		if rerr != nil {
			t.Fatal(rerr)
		}
		doc, derr := out.ReplyDoc()
		if derr != nil {
			t.Fatal(derr)
		}
		code, _ := doc.Lookup("code").Int32OK()
		var ls []string
		if v, lerr := doc.LookupErr("errorLabels"); lerr == nil {
			arr, _ := v.ArrayOK()
			vals, _ := arr.Values()
			for _, x := range vals {
				ls = append(ls, x.StringValue())
			}
		}
		return code, ls
	}
	if code, ls := labels(mongo.CommandError{Code: 189, Name: "PrimarySteppedDown"}); len(ls) != 1 || ls[0] != "ResumableChangeStreamError" {
		t.Errorf("a stepped down fast tier came back as %d %v, want the resumable label", code, ls)
	}
	if code, ls := labels(mongo.CommandError{Code: 16412, Name: "Location16412"}); len(ls) != 0 || code != 16412 {
		t.Errorf("a stage's own refusal came back as %d %v, want 16412 and no label", code, ls)
	}
	if code, ls := labels(errResumeTokenModified); code != 280 || len(ls) != 0 {
		t.Errorf("a changed resume token came back as %d %v, want 280 and no label", code, ls)
	}
	if _, ls := labels(errors.New("x")); len(ls) != 0 {
		t.Errorf("an unknown error was labelled resumable: %v", ls)
	}
}
