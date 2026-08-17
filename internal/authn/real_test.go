package authn

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/xdg-go/scram"
)

// Verify a real password against credentials mongod actually wrote, rather than
// against credentials this package derived itself.
//
// The unit tests prove the encoding round trips, but they generate both halves,
// so they would pass just as well against a private convention that no MongoDB
// server shares. This one is handed the credentials document out of
// admin.system.users and the password that belongs to it, and it fails if the
// router would have rejected a user the database accepts.
//
//	docker exec <node> mongosh ... --eval \
//	  'print(JSON.stringify(db.getSiblingDB("admin").system.users.findOne(
//	     {user:"dba",db:"admin"},{credentials:1}).credentials))' > creds.json
//	THERMOCLINE_CREDS=creds.json THERMOCLINE_CREDS_USER=dba \
//	  THERMOCLINE_CREDS_PASS=secret go test ./internal/authn/ -run Real -v
func TestRealMongoCredentials(t *testing.T) {
	path := os.Getenv("THERMOCLINE_CREDS")
	user := os.Getenv("THERMOCLINE_CREDS_USER")
	pass := os.Getenv("THERMOCLINE_CREDS_PASS")
	if path == "" || user == "" || pass == "" {
		t.Skip("set THERMOCLINE_CREDS, THERMOCLINE_CREDS_USER and THERMOCLINE_CREDS_PASS to run this")
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var doc map[string]struct {
		Salt           string `json:"salt"`
		StoredKey      string `json:"storedKey"`
		ServerKey      string `json:"serverKey"`
		IterationCount int    `json:"iterationCount"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	for _, mech := range []string{SHA256, SHA1} {
		c, ok := doc[mech]
		if !ok {
			continue
		}
		t.Run(mech, func(t *testing.T) {
			st := Stored{Salt: c.Salt, StoredKey: c.StoredKey, ServerKey: c.ServerKey, Iters: c.IterationCount}
			creds, err := st.Credentials()
			if err != nil {
				t.Fatalf("convert: %v", err)
			}
			hgf, err := HashFor(mech)
			if err != nil {
				t.Fatalf("hash: %v", err)
			}

			// MongoDB does not derive SCRAM-SHA-1 credentials from the password
			// itself. It uses the hex MD5 of "user:mongo:password", a survival
			// from the days when that digest was the whole of authentication.
			// SCRAM-SHA-256 uses the password as given, SASLprepped.
			//
			// The router never has to know any of this, which is the point worth
			// recording here: it verifies a proof against stored keys and never
			// derives anything, so the difference lives entirely in the client.
			secret := pass
			if mech == SHA1 {
				sum := md5.Sum([]byte(user + ":mongo:" + pass))
				secret = hex.EncodeToString(sum[:])
			}

			server, err := hgf.NewServer(func(string) (scram.StoredCredentials, error) { return creds, nil })
			if err != nil {
				t.Fatalf("server: %v", err)
			}
			client, err := hgf.NewClient(user, secret, "")
			if err != nil {
				t.Fatalf("client: %v", err)
			}

			cc, sc := client.NewConversation(), server.NewConversation()
			first, err := cc.Step("")
			if err != nil {
				t.Fatalf("client first: %v", err)
			}
			challenge, err := sc.Step(first)
			if err != nil {
				t.Fatalf("server first: %v", err)
			}
			proof, err := cc.Step(challenge)
			if err != nil {
				t.Fatalf("client proof: %v", err)
			}
			verify, err := sc.Step(proof)
			if err != nil {
				t.Fatalf("the router rejected a password mongod accepts: %v", err)
			}
			if _, err := cc.Step(verify); err != nil {
				t.Fatalf("client could not verify the router: %v", err)
			}
			if !sc.Valid() {
				t.Fatal("conversation did not end valid")
			}
		})
	}
}
