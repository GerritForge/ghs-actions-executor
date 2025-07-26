// Copyright (C) 2025 GerritForge, Inc.
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

import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.eclipse.jgit.lib.ObjectId;
import org.junit.Test;

public class PruneOutdatedBitmapsActionTest extends GitActionTest {
  @Test
  public void applyPruneOutdatedBitmapsActionShouldDoNothingWhenNoLog() throws Exception {
    // when no bitmap is generated
    pushNewCommitToBranch();
    File pack =
        Files.list(testRepoPath.resolve("objects/pack"))
            .filter(p -> p.toString().endsWith(".pack"))
            .findFirst()
            .get()
            .toFile();

    // and prune outdated bitmap action is called
    assertThat(new PruneOutdatedBitmapsAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();

    // then repository state doesn't change
    assertThat(pack.isFile()).isTrue();
    File preserved = testRepoPath.resolve("objects/pack/preserved").toFile();
    assertThat(preserved.isFile()).isFalse();
  }

  @Test
  public void applyPruneOutdatedBitmapsActionShouldPreserveSecondToLastAndPruneOutdatedLog()
      throws Exception {
    // when >two bitmapa are generated
    pushNewCommitToBranch();
    Path oldestPackPath =
        Files.list(testRepoPath.resolve("objects/pack"))
            .filter(p -> p.toString().endsWith(".pack"))
            .findFirst()
            .get();
    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
    pushNewCommitToBranch();
    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
    pushNewCommitToBranch();
    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
    Path bitmapsLogPath = oldestPackPath.getParent().resolve(".ghs-packs.log");
    String[] logPackIds = logEntries(bitmapsLogPath).toArray(String[]::new);

    // and prune outdated bitmap action is called
    assertThat(new PruneOutdatedBitmapsAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();

    // then second to last pack is preserved
    Path preservedPath = testRepoPath.resolve("objects/pack/preserved");
    String secondToLastPackId = logPackIds[1];
    assertThat(
            Files.list(preservedPath)
                .filter(p -> p.toString().contains(secondToLastPackId))
                .findFirst())
        .isPresent();

    // and then the newest pack is not modified
    String lastPackId = logPackIds[2];
    assertThat(
            Files.list(testRepoPath.resolve("objects/pack"))
                .filter(p -> p.toString().contains(lastPackId))
                .findFirst())
        .isPresent();

    // and then the oldest pack is pruned
    assertThat(oldestPackPath.toFile().isFile()).isFalse();

    // and then log file contains only second to last and last ids
    List<String> postPruneLogPackIds = logEntries(bitmapsLogPath);
    assertThat(postPruneLogPackIds).containsExactly(secondToLastPackId, lastPackId).inOrder();

    // and then repo is still operable
    pushNewCommitToBranch();
    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
  }

  @Test
  public void applyPruneOutdatedBitmapsActionShouldKeepSecondToLastAndLastInLog() throws Exception {
    // when >=two bitmapa are generated
    pushNewCommitToBranch();
    Path oldestPackPath =
        Files.list(testRepoPath.resolve("objects/pack"))
            .filter(p -> p.toString().endsWith(".pack"))
            .findFirst()
            .get();
    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
    pushNewCommitToBranch();
    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
    Path bitmapsLogPath = oldestPackPath.getParent().resolve(".ghs-packs.log");
    String[] logPackIds = logEntries(bitmapsLogPath).toArray(String[]::new);

    // and prune outdated bitmap action is called
    assertThat(new PruneOutdatedBitmapsAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();

    // then second to last pack is preserved
    Path preservedPath = testRepoPath.resolve("objects/pack/preserved");
    String secondToLastPackId = logPackIds[0];
    assertThat(
            Files.list(preservedPath)
                .filter(p -> p.toString().contains(secondToLastPackId))
                .findFirst())
        .isPresent();

    // and then the newest pack is not modified
    String lastPackId = logPackIds[1];
    assertThat(
            Files.list(testRepoPath.resolve("objects/pack"))
                .filter(p -> p.toString().contains(lastPackId))
                .findFirst())
        .isPresent();

    // and then repo advances and new bitmap is generated
    pushNewCommitToBranch();
    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
    String newLastPackId = logEntries(bitmapsLogPath).toArray(String[]::new)[2];

    // and then prune outdated bitmap action is called again
    assertThat(new PruneOutdatedBitmapsAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();

    // then old second to last pack is pruned
    assertThat(
            Files.list(preservedPath)
                .filter(p -> p.toString().contains(secondToLastPackId))
                .findFirst())
        .isEmpty();

    // and then old last is preserved
    assertThat(Files.list(preservedPath).filter(p -> p.toString().contains(lastPackId)).findFirst())
        .isPresent();

    // and new last is not modified
    assertThat(
            Files.list(testRepoPath.resolve("objects/pack"))
                .filter(p -> p.toString().contains(newLastPackId))
                .findFirst())
        .isPresent();

    // and then log file contains only last and new last ids
    List<String> postSecondPruneLogPackIds = logEntries(bitmapsLogPath);
    assertThat(postSecondPruneLogPackIds).containsExactly(lastPackId, newLastPackId).inOrder();

    // and then repo is still operable
    pushNewCommitToBranch();
    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
  }

  @Test
  public void reapplyPruneOutdatedBitmapsActionWhenRepoDidntProgressShoulBeIdempotent()
      throws Exception {
    // when >two bitmapa are generated
    pushNewCommitToBranch();
    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
    pushNewCommitToBranch();
    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
    pushNewCommitToBranch();
    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
    Path bitmapsLogPath = testRepoPath.resolve("objects/pack/.ghs-packs.log");
    String[] logPackIds = logEntries(bitmapsLogPath).toArray(String[]::new);

    // and prune outdated bitmap action is called >once
    assertThat(new PruneOutdatedBitmapsAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();
    assertThat(new PruneOutdatedBitmapsAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();

    // then second to last pack is still in preserved
    Path preservedPath = testRepoPath.resolve("objects/pack/preserved");
    String secondToLastPackId = logPackIds[1];
    assertThat(
            Files.list(preservedPath)
                .filter(p -> p.toString().contains(secondToLastPackId))
                .findFirst())
        .isPresent();

    // and then the newest pack is still not modified
    String lastPackId = logPackIds[2];
    assertThat(
            Files.list(testRepoPath.resolve("objects/pack"))
                .filter(p -> p.toString().contains(lastPackId))
                .findFirst())
        .isPresent();

    // and then log file still contains only second to last and last ids
    List<String> postPruneLogPackIds = logEntries(bitmapsLogPath);
    assertThat(postPruneLogPackIds).containsExactly(secondToLastPackId, lastPackId).inOrder();

    // and then repo is still operable
    pushNewCommitToBranch();
    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
  }

  private List<String> logEntries(Path bitmapsLogPath) throws IOException {
    byte[] content = Files.readAllBytes(bitmapsLogPath);
    int chunkSize = 20;
    if (content.length < chunkSize || content.length % chunkSize != 0) {
      return Collections.emptyList();
    }

    return Stream.iterate(0, i -> i < content.length, i -> i + chunkSize)
        .map(
            i -> {
              int end = Math.min(i + chunkSize, content.length);
              return Arrays.copyOfRange(content, i, end);
            })
        .map(id -> ObjectId.fromRaw(id).getName())
        .collect(toList());
  }
}
