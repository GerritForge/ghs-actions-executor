load("@rules_java//java:defs.bzl", "java_library")
load("//tools/bzl:junit.bzl", "junit_tests")
load(
    "//tools/bzl:plugin.bzl",
    "PLUGIN_DEPS",
)

genrule(
    name = "ghs-actions-executor",
    srcs = [":ghs-actions-executor-bin_deploy.jar"],
    outs = ["ghs-actions-executor.jar"],
    cmd = "cp $< $@",
)

java_binary(
    name = "ghs-actions-executor-bin",
    srcs = glob(["src/main/java/**/Main.java"]),
    main_class = "com.gerritforge.ghs.actions.Main",
    deps = [
        ":ghs-actions-executor_lib",
        "@flogger//jar",
    ],
)

junit_tests(
    name = "ghs-actions-executor_test",
    srcs = glob(["src/test/java/**/*.java"]),
    deps = [
        ":ghs-actions-executor_lib",
        "//lib/truth",
    ] + PLUGIN_DEPS,
)

java_library(
    name = "ghs-actions-executor_lib",
    srcs = glob(["src/main/java/**/*.java"]),
    deps = [
        "//lib:gson",
        "//lib:jgit",
        "//lib/flogger:api",
        "//lib/log:impl-log4j",
        "//lib/log:log4j",
    ],
)
