package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// A collection a client created is named in commands an operator pastes into
// mongosh. Whatever the name, it has to stay one string literal: decoding the
// literal gives the name back, and nothing outside the quotes came from it.
func TestNamesStayStringLiterals(t *testing.T) {
	for _, name := range []string{
		"orders",
		"x;db.dropDatabase();db.x",
		`x");db.dropDatabase();("`,
		"x'),db.dropDatabase(),('",
		" line separators",
		"emoji \U0001F600",
		"\x1b]0;title\x07",
	} {
		ref := collRef("shop", name)
		const head = `db.getSiblingDB("shop").getCollection(`
		if !strings.HasPrefix(ref, head) || !strings.HasSuffix(ref, ")") {
			t.Fatalf("%q rendered as %s", name, ref)
		}
		lit := ref[len(head) : len(ref)-1]
		var back string
		if err := json.Unmarshal([]byte(lit), &back); err != nil || back != name {
			t.Errorf("%q rendered as %s, which does not decode back to it (%v)", name, lit, err)
		}
		if strings.ContainsAny(lit, "\x1b  \n") {
			t.Errorf("%q rendered with a raw control or separator character: %s", name, lit)
		}
	}
}

func TestPrintableEscapesControlCharacters(t *testing.T) {
	got := printable("shop.orders {\x1b]0;owned\x07: 1}")
	if strings.ContainsAny(got, "\x1b\x07") {
		t.Fatalf("an escape sequence reached the terminal: %q", got)
	}
	if printable("shop.orders {status: ?}") != "shop.orders {status: ?}" {
		t.Fatal("an ordinary shape was changed")
	}
}
