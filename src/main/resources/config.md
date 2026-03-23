# Config

Currently the only ghs specific configuration parameter is `prunePreservedExpire`.
This is intended to be similar to `prunePackExpire` in principal, but applies to the preserved directory instead.

It's configured like any other jgit parameter in the gc section of the repo config or via the usual hierarchy levels.
The parameter is ignored by git/jgit itself.

