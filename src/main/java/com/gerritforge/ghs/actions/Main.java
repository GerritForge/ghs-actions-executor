// Copyright (C) 2024 GerritForge, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.gerritforge.ghs.actions;

import static java.lang.System.exit;

import com.gerritforge.ghs.actions.stats.StatsCollector;
import com.gerritforge.ghs.actions.stats.StatsResult;
import com.google.common.flogger.FluentLogger;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

public class Main {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static boolean verbose = false;
  private static boolean sequentialBitmapGeneration = false;

  private static final Set<String> AVAILABLE_FLAGS = Set.of("-v", "--sequential-bitmap-generation");

  public static void main(String[] args) {
    if (args.length < 2) {
      printUse();
      exit(-1);
    }

    if (args[0].equals("-v")) {
      verbose = true;
      args = removeFirstElement(args);
    }

    while (AVAILABLE_FLAGS.contains(args[0])) {
      switch (args[0]) {
        case "-v":
          verbose = true;
          args = removeFirstElement(args);
          break;
        case "--sequential-bitmap-generation":
          sequentialBitmapGeneration = true;
          args = removeFirstElement(args);
          break;
      }
    }

    String action = args[0];
    String repositoryPath = args[1];
    String outputPath = args.length > 2 ? args[2] : null;
    String className = Main.class.getPackageName() + "." + action;

    try {
      Class<Action> actionClass = (Class<Action>) Class.forName(className);
      Action instance = actionClass.getDeclaredConstructor().newInstance();
      instance.setVerbose(verbose);
      instance.setSequentialBitmapGeneration(sequentialBitmapGeneration);

      StatsCollector statsCollector = StatsCollector.start();

      ActionResult result = instance.apply(repositoryPath);

      StatsResult statsResult = statsCollector.stop();

      logger.atInfo().log("%s", result.toString());
      logger.atInfo().log("%s", statsResult.toString());

      persistExecutionResult(new ExecutionResult(result, statsResult), outputPath);
    } catch (ClassNotFoundException e) {
      logger.atSevere().withCause(e).log("Cannot find action class for action name:%s", action);
    } catch (InstantiationException | IllegalAccessException e) {
      logger.atSevere().withCause(e).log("Cannot instantiate action class %s", className);
    } catch (Exception e) {
      logger.atSevere().withCause(e).log(
          "Exception during the action execution. Action class: %s", className);
    }
  }

  private static void printUse() {
    String msgFormat =
        String.format(
            "Use: java %%s <actionName> <repositoryPath> (outputFile)\n"
                + "\n\t-v - enable verbose logging"
                + "\n\toutputFile - file to store the JSON output (defaults to: %s)",
            defaultOutputLocation("$PID"));
    logger.atInfo().log(msgFormat, Main.class.getName());
    System.err.printf(msgFormat + "%n", Main.class.getName());
  }

  private static void persistExecutionResult(ExecutionResult result, String outputPath)
      throws IOException {
    String filePath =
        outputPath != null
            ? outputPath
            : defaultOutputLocation(String.valueOf(ProcessHandle.current().pid()));
    logger.atInfo().log("storing execution results in: %s", filePath);

    try (PrintWriter out = new PrintWriter(filePath)) {
      out.println(result.toJson());
    }
  }

  private static String defaultOutputLocation(String pid) {
    return String.format("/tmp/ghs-action-execution-%s.json", pid);
  }

  private static String[] removeFirstElement(String[] array) {
    String[] newArray = new String[array.length - 1];
    System.arraycopy(array, 1, newArray, 0, array.length - 1);
    return newArray;
  }
}
