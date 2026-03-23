# Config

Currently the only ghs specific configuration parameter is
`gc.prunePreservedExpire`. This is intended to be similar to `prunePackExpire`
in principle, but applies to the preserved directory instead.

It's configured like any other git/jgit parameter, i.e. 500.seconds.ago, similar
to [gc.pruneExpire](https://git-scm.com/docs/git-gc#Documentation/git-gc.txt-gcpruneExpire)
in the gc section of the repo config or via the usual hierarchy levels. The
parameter is ignored by git/jgit itself.

NOTE: The functionality is based on the last modified time of a file, however, the last modified
time is not updated when a file is moved to a different directory, which is what happens in our
use case, i.e. from packs to preserved directory. Therefore this setting should be set higher than
the `prunePackExpire`, which is what is used to determine when a pack is moved to the preserved dir.

Defaults to 2 hours.
