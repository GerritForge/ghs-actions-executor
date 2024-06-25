.PHONY: clean

JGIT_REF=refs/changes/75/1194975/2
GERRIT_REF=refs/heads/stable-3.10
GERRIT_REFERENCE=/home/jenkins/gerrit-reference.git
BUILD_IMAGE=gerritforge/gerrit-ci-agent-bazel-docker:debian-bullseye-latest

GERRIT_CLONE=cd /home/jenkins && git clone --recursive --reference ${GERRIT_REFERENCE} https://gerrit.googlesource.com/gerrit && cd gerrit && \
        git fetch origin ${GERRIT_REF} && git checkout FETCH_HEAD && git submodule update && \
        pushd modules/jgit && git fetch https://eclipse.gerrithub.io/eclipse-jgit/jgit ${JGIT_REF} && git checkout FETCH_HEAD && popd

LINK_EXECUTOR_SOURCE=ln -s /home/jenkins/ghs-actions-executor /home/jenkins/gerrit/plugins

ACTIONS_EXECUTOR_BUILD=${GERRIT_CLONE} && ${LINK_EXECUTOR_SOURCE} && cd /home/jenkins/gerrit && \
	. set-java.sh 17 && \
	bazelisk build //plugins/ghs-actions-executor:ghs-actions-executor_deploy.jar && cp -r bazel-bin/plugins/ghs-actions-executor/ghs-actions-executor_deploy.jar /home/jenkins/ghs-actions-executor/output/ && \
	shasum bazel-bin/plugins/ghs-actions-executor/ghs-actions-executor_deploy.jar

TARGET=output/ghs-actions-executor_deploy.jar

SRCS=$(wildcard src/**/*.java)

build: ${TARGET}

${TARGET}: Makefile BUILD ${SRCS}
	-mkdir -p output && chmod a+rw output
	docker run --rm -v $(shell pwd):/home/jenkins/ghs-actions-executor -u jenkins -ti $(BUILD_IMAGE) bash -c '${ACTIONS_EXECUTOR_BUILD}'

clean:
	-rm -f ${TARGET}
