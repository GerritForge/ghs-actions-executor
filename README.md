# GHS Actions Executor

## Overview

Executor of Git actions as part of the GerritForge Health Service project.
Standalone self-contained jar that allows to run the regular maintenance operations
on one Git repository.

## How to build

The project is built as a standard Gerrit in-tree build, even though the generated
artifact isn't a plugin and does not depend on Gerrit Code Review.

Example:

```build
git clone https://gerrit.googlesource.com/gerrit
git clone https://github.com/GerritForge/ghs-actions-executor
cd gerrit/plugins
ln -sf ../../ghs-actions-executor
```

The project requires a custom-built JGit library, therefore requires a specific
change to be checked out before running the Bazel build of the plugin.

The coordination of the JGit checkout and the Bazel build is automated through
the Makefile in the project root directory, executed from the root path of the
Gerrit in-tree build source code.

Example from the Gerrit source root:

```build
make -f plugins/ghs-actions-executor/Makefile
```

The output is a fully executable self-contained jar under `bazel-bin/plugins/ghs-actions-executor/ghs-actions-executor.jar`.

## How to run tests

The project contains a series of tests that are fully compatible with the way
tests are executed in a standard Gerrit in-tree build plugins.

Example from the Gerrit source root:

```build
bazelisk test plugins/ghs-actions-executor/...
```
