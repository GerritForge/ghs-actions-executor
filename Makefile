.PHONY: clean

JGIT_REF=refs/changes/41/1228741/1

TARGET=bazel-bin/plugins/ghs-actions/executor/ghs-actions-executor.jar

SRCS=$(wildcard plugins/ghs-actions-executor/src/**/*.java)

build: ${TARGET}

${TARGET}: ${SRCS} jgit
	bazelisk build plugins/ghs-actions-executor

jgit:
	cd modules/jgit && git fetch origin ${JGIT_REF} && git checkout FETCH_HEAD

clean:
	-rm -f ${TARGET}
