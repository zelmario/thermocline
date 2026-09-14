# The unacknowledged read probe

The router treats the OP_MSG `moreToCome` flag as "no reply is coming": it
writes the message upstream and returns without reading one. That is right for a
write, which is the only thing any driver sets the flag on, and it is right for
anything else only if the server also sends nothing.

If the server answers a `moreToCome` `find`, the reply stays in the socket and
the answer to whatever command comes next is somebody else's documents. That is
exactly the shape of the exhaust finding in the directory beside this one, and
that one was real. No driver here can set the flag on a read, and `mongosh`
sets it only through `writeConcern: {w: 0}` on a write, so the only way to ask
the question is to speak the protocol directly.

    go run ./test/unackread 127.0.0.1:28017 direct      # what mongod does
    go run ./test/unackread 127.0.0.1:27600 router      # what the router does

Run it on the host, not in a node, because it needs a socket to a server with no
authentication in front of it. `make dev` starts the pair; pass the cache cap by
hand if the six containers are up.

`inStep=false` is the failure. It means every command after that one is answered
by the one before it. `the find WAS answered` against mongod is what would make
the router's behaviour a defect rather than a matching pair of assumptions.
