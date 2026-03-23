.PHONY: clean

JGIT_REF=refs/changes/57/1233557/5

TARGET=bazel-bin/plugins/ghs-actions/executor/ghs-actions-executor.jar

SRCS=$(wildcard plugins/ghs-actions-executor/src/**/*.java)

JGIT_ORIGIN=https://review.gerrithub.io/GerritForge/jgit

build: ${TARGET}

${TARGET}: ${SRCS} jgit
	bazelisk build plugins/ghs-actions-executor

jgit:
	cd modules/jgit && git fetch $(JGIT_ORIGIN) ${JGIT_REF} && git checkout FETCH_HEAD
	REPIN=1 bazelisk run @external_deps//:pin

clean:
	-rm -f ${TARGET}
