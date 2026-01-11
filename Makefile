.PHONY: clean

JGIT_REF=refs/changes/38/1228838/2

TARGET=bazel-bin/plugins/ghs-actions/executor/ghs-actions-executor.jar

SRCS=$(wildcard plugins/ghs-actions-executor/src/**/*.java)

JGIT_ORIGIN=https://eclipse.gerrithub.io/eclipse-jgit/jgit

build: ${TARGET}

${TARGET}: ${SRCS} jgit
	bazelisk build plugins/ghs-actions-executor

jgit:
	cd modules/jgit && git fetch $(JGIT_ORIGIN) ${JGIT_REF} && git checkout FETCH_HEAD

clean:
	-rm -f ${TARGET}
