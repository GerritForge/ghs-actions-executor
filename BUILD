load("@rules_java//java:defs.bzl", "java_library")
load("//tools/bzl:junit.bzl", "junit_tests")
load(
    "//tools/bzl:plugin.bzl",
    "PLUGIN_DEPS",
    "PLUGIN_TEST_DEPS",
)

java_binary(
    name = "actions-executor",
    srcs = glob(["src/main/java/**/Main.java"]),
    main_class = "com.gerritforge.ghs.actions.Main",
    deps = [
        ":actions-executor_lib",
        "@flogger//jar",
    ],
)

junit_tests(
    name = "actions-executor_test",
    srcs = glob(["src/test/java/**/*.java"]),
    deps = [
        ":actions-executor_lib",
    ] + PLUGIN_DEPS + PLUGIN_TEST_DEPS,
)

java_library(
    name = "actions-executor_lib",
    srcs = glob(["src/main/java/**/*.java"]),
    deps = PLUGIN_DEPS,
)
