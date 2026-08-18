# The cursor ownership probe

A cursor over an archive answer is served by the router from a registry shared
by every connection. That much is right: a real MongoDB cursor is server-scoped,
and a driver's pool sends the getMore from whichever connection is free. What was
missing is everything else mongod scopes it by.

The ids were handed out in sequence from a fixed start, so the first was always
2^62+1. This connects twice. A opens a cursor over a collection whose documents
are only in the archive; B, which has opened nothing, sends a getMore for that id
and names a collection of its own beside it.

Measured before the fix:

    A opened cursor 4611686018427387905 (router owned: true)
    B READ ANOTHER CLIENT'S CURSOR: ns="probe.tiered"
      batch=[{"_id":2,"v":"archived2"},{"_id":3,"v":"archived3"}]

The collection B named counted for nothing: the batch comes from the cursor. With
`terminate_auth` on, the authorization gate judges the name B supplied, so this
reads past it. With authentication proxied, the archive cursor was opened on the
router's own credentials, so B need not have access to that collection either.

After the fix, ids are random and a cursor belongs to whoever opened it:

    A opened cursor 6688429141544535120 (router owned: true)
    B was refused: code=13 "cursor id ... was not created by the authenticated
      user or was created in another session"

Run it the same way as `test/exhaust`, against a router in front of an
unauthenticated pair, with `probe.tiered` listed in `feeder.namespaces` and its
documents present only on the cold side.
