package authn

import (
	"encoding/base64"
	"testing"

	"github.com/xdg-go/scram"
)

// mongoStyle produces the credentials MongoDB would have stored for a password,
// in the encoding it stores them in: base64 for the three byte strings, plain
// for the iteration count.
//
// This is the whole point of the test. The values that reach Stored come out of
// a BSON document written by mongod, and every one of them is base64. Deriving
// them here the way the specification says, encoding them the way mongod does
// and then asking the router to verify a real client proof against them is the
// only way to know the decoding is right without a cluster.
func mongoStyle(t *testing.T, hgf scram.HashGeneratorFcn, user, pass string, kf scram.KeyFactors) Stored {
	t.Helper()
	cl, err := hgf.NewClient(user, pass, "")
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	sc := cl.GetStoredCredentials(kf)
	return Stored{
		Salt:      base64.StdEncoding.EncodeToString([]byte(kf.Salt)),
		StoredKey: base64.StdEncoding.EncodeToString(sc.StoredKey),
		ServerKey: base64.StdEncoding.EncodeToString(sc.ServerKey),
		Iters:     kf.Iters,
	}
}

func TestStoredCredentialsVerifyARealProof(t *testing.T) {
	for _, mech := range []string{SHA256, SHA1} {
		t.Run(mech, func(t *testing.T) {
			hgf, err := HashFor(mech)
			if err != nil {
				t.Fatalf("hash: %v", err)
			}
			const user, pass = "dba", "secret"
			kf := scram.KeyFactors{Salt: "0123456789abcdef", Iters: 15000}

			st := mongoStyle(t, hgf, user, pass, kf)
			creds, err := st.Credentials()
			if err != nil {
				t.Fatalf("convert: %v", err)
			}

			server, err := hgf.NewServer(func(string) (scram.StoredCredentials, error) {
				return creds, nil
			})
			if err != nil {
				t.Fatalf("server: %v", err)
			}
			client, err := hgf.NewClient(user, pass, "")
			if err != nil {
				t.Fatalf("client: %v", err)
			}

			cc := client.NewConversation()
			sc := server.NewConversation()

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
				t.Fatalf("server rejected a correct password: %v", err)
			}
			if _, err := cc.Step(verify); err != nil {
				t.Fatalf("client could not verify the server: %v", err)
			}
			if !sc.Valid() || !cc.Valid() {
				t.Fatal("conversation did not end valid")
			}
		})
	}
}

// A wrong password must be refused. Without this the test above would pass just
// as happily against a server that accepted anything.
func TestWrongPasswordIsRefused(t *testing.T) {
	hgf := scram.SHA256
	kf := scram.KeyFactors{Salt: "0123456789abcdef", Iters: 15000}
	st := mongoStyle(t, hgf, "dba", "secret", kf)
	creds, err := st.Credentials()
	if err != nil {
		t.Fatalf("convert: %v", err)
	}

	server, _ := hgf.NewServer(func(string) (scram.StoredCredentials, error) { return creds, nil })
	client, _ := hgf.NewClient("dba", "wrong", "")

	cc, sc := client.NewConversation(), server.NewConversation()
	first, _ := cc.Step("")
	challenge, err := sc.Step(first)
	if err != nil {
		t.Fatalf("server first: %v", err)
	}
	proof, err := cc.Step(challenge)
	if err != nil {
		t.Fatalf("client proof: %v", err)
	}
	if _, err := sc.Step(proof); err == nil {
		t.Fatal("the wrong password was accepted")
	}
}

// The salt must be decoded before it reaches KeyFactors. Passing MongoDB's
// base64 form straight through salts the proof with the wrong bytes, and every
// correct password is then rejected for what looks like a credential problem.
func TestSaltIsDecoded(t *testing.T) {
	kf := scram.KeyFactors{Salt: "0123456789abcdef", Iters: 15000}
	st := mongoStyle(t, scram.SHA256, "dba", "secret", kf)
	creds, err := st.Credentials()
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if creds.Salt != kf.Salt {
		t.Fatalf("salt not decoded: got %q want %q", creds.Salt, kf.Salt)
	}
}
