# The exhaust probe

The Go driver does not expose exhaust cursors and `mongosh` does not use them,
so no suite here can set the OP_MSG `exhaustAllowed` bit. This speaks the wire
protocol directly, which is the only way to find out what the router does with
it. It is the same approach that proved the compression finding: a purpose-built
client that does the one thing no driver in the harness will do.

What it showed, against an unauthenticated pair started with `make dev`, which
here was mongod 6.0. The server version matters only for the first line: the
defect is that the router reads one reply per request, whatever the server sends.

    mongod       getMore produced 5 replies, connection in step afterwards
    router       getMore produced 1 reply,   ping answered with responseTo=75
                 carrying documents 4 and 5 of the cursor

The router reads one reply per request. The server sends several when the bit is
set, so the rest stayed in the socket and answered whatever command came next.
With upstream connections pooled, that connection goes back to the pool and the
next borrower is a different client.

The bit is now cleared on the way through, and the same probe reports one reply
with `moreToCome=false` and `inStep=true`.

Run it on the host, not in a node, because it needs a socket to a server with no
authentication in front of it:

    make dev
    go run ./test/exhaust 127.0.0.1:28017 mongod        # what the server does
    ./bin/cline router --config <a config pointing at 28017 and 28018> &
    go run ./test/exhaust 127.0.0.1:27600 router        # what the router does
    make dev-down

`inStep=false` is the failure. It means every command after that one is answered
by the one before it.
