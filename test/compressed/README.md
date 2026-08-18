# The uninvited compression probe

The handshake never agrees a compressor: the list is removed from the client's
hello and from the reply, in both directions, so a conforming driver never sends
OP_COMPRESSED. `testconcerns.sh` proves that much, with a `compressors=zlib`
client whose writes are still guarded.

What it cannot prove is what happens when a client compresses anyway, because
`mongosh` will not. This does, with zlib, compressor id 2.

Measured before the fix, against a collection whose documents are only in the
archive:

    compressors the peer agreed to: none
    plain insert:      refused: E11000 duplicate key error
    compressed insert: ACCEPTED n=1
    what landed on the fast tier: [{"_id":6,"via":"zlib"}]

The same insert, refused when readable and accepted when not. That is the
original compression finding still live, reached by ignoring the negotiation
rather than by winning it, and the document would then have been mirrored over
the archived record.

A compressed message is refused now, and the connection ends with it:

    compressed insert: refused: "thermocline: refusing a compressed message..."
    what landed on the fast tier: []

Run it against a router in front of an unauthenticated pair, as with the other
probes here. `probe.tiered` must be listed in `feeder.namespaces` and hold
`_id` 5 and 6 on the cold side only.
