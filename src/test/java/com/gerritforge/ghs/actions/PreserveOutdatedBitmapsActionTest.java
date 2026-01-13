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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.file.PackFile;
import org.eclipse.jgit.lib.ObjectId;
import org.junit.Test;

public class PreserveOutdatedBitmapsActionTest extends GitActionTest {

  private static final ObjectId PACK_ID_A =
      ObjectId.fromString("a3f5c9e8b7d6421f0e9a4c3b2d1e6f8a9b0c7d5e");
  private static final ObjectId PACK_ID_B =
      ObjectId.fromString("7b2e4a6c9d1f8e5a3c0b6d4f2e9a1c7b8d5f0e3a");

  @Test
  public void applyPreserveOutdatedBitmapsActionShouldDoNothingWhenNoLog() throws Exception {
    // when no bitmap is generated
    pushNewCommitToBranch();
    File pack = findMostRecentPack();

    // and preserve outdated bitmap action is called
    assertThat(new PreserveOutdatedBitmapsAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();

    // then repository state doesn't change
    assertThat(pack.isFile()).isTrue();
    assertThat(preservedPath.toFile().isFile()).isFalse();
  }

  @Test
  public void applyPreserveOutdatedBitmapsActionShouldPreserveOutdated() throws Exception {
    // when two bitmaps are generated
    PackFile olderPack = pushAndGenerateNewBitmap();
    PackFile newestPack = pushAndGenerateNewBitmap();

    // and preserve outdated bitmap action is called
    assertThat(new PreserveOutdatedBitmapsAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();

    // then the older pack is preserved
    assertThat(
            Files.list(preservedPath)
                .filter(p -> p.toString().contains(olderPack.getName()))
                .findFirst())
        .isPresent();

    // and then the newest pack is not modified
    assertThat(
            Files.list(packPath)
                .filter(p -> p.toString().contains(newestPack.getName()))
                .findFirst())
        .isPresent();

    // and then log file contains newest pack id
    assertBitmapsLogContainsOnly(newestPack.getId());

    // and then repo is still operable
    pushAndGenerateNewBitmap();
  }

  @Test
  public void applyPreserveOutdatedBitmapsActionShouldKeepTheLastInLog() throws Exception {
    // when >=one bitmap is generated
    PackFile lastPack = pushAndGenerateNewBitmap();

    // and preserve outdated bitmap action is called
    assertThat(new PreserveOutdatedBitmapsAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();

    // and then the newest pack is not modified
    assertThat(lastPack.isFile()).isTrue();

    // and then repo advances and new bitmap is generated
    PackFile newLastPack = pushAndGenerateNewBitmap();

    // and then preserve outdated bitmap action is called again
    assertThat(new PreserveOutdatedBitmapsAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();

    // then old last pack id is preserved
    assertThat(
            Files.list(preservedPath)
                .filter(p -> p.toString().contains(lastPack.getId()))
                .findFirst())
        .isPresent();

    // and new last is not modified
    assertThat(
            Files.list(packPath)
                .filter(p -> p.toString().contains(newLastPack.getId()))
                .findFirst())
        .isPresent();

    // and then log file contains only new last id
    assertBitmapsLogContainsOnly(newLastPack.getId());

    // and then repo is still operable
    pushAndGenerateNewBitmap();
  }

  @Test
  public void reapplyPreserveOutdatedBitmapsActionWhenRepoDidntProgressShouldBeIdempotent()
      throws Exception {
    // when >two bitmaps are generated
    PackFile secondToLastPack = pushAndGenerateNewBitmap();
    PackFile lastPack = pushAndGenerateNewBitmap();

    // and preserve outdated bitmap action is called >once
    assertThat(new PreserveOutdatedBitmapsAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();
    assertThat(new PreserveOutdatedBitmapsAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();

    // then second to last pack is still in preserved
    assertThat(
            Files.list(preservedPath)
                .filter(p -> p.toString().contains(secondToLastPack.getId()))
                .findFirst())
        .isPresent();

    // and then the newest pack is still not modified
    assertThat(
            Files.list(packPath).filter(p -> p.toString().contains(lastPack.getId())).findFirst())
        .isPresent();

    // and then log file contains last id
    assertBitmapsLogContainsOnly(lastPack.getId());

    // and then repo is still operable
    pushAndGenerateNewBitmap();
  }

  @Test
  public void shouldRemoveLogWhenLogExistsAndIsEmpty() throws IOException {
    Path logPath = BitmapGenerationLog.logPath(testRepoPath.toString());
    ensureBitmapsLogContainsExactly(List.of());

    assertThat(new PreserveOutdatedBitmapsAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();

    assertThat(Files.exists(logPath)).isFalse();
  }

  @Test
  public void shouldRemoveLogWhenNoBitmapExists() throws IOException {
    Path logPath = BitmapGenerationLog.logPath(testRepoPath.toString());
    ensureBitmapsLogContainsExactly(List.of(PACK_ID_A, PACK_ID_B));

    assertThat(new PreserveOutdatedBitmapsAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();

    assertThat(Files.exists(logPath)).isFalse();
  }

  @Test
  public void shouldOverwriteSingleObjectIdInLogEvenWhenItIsNotInTheLastPosition()
      throws IOException, GitAPIException {
    pushNewCommitToBranch();
    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();

    ObjectId mostRecentBitmapPackId = getMostRecentBitmapPackId();
    Path logPath = BitmapGenerationLog.logPath(testRepoPath.toString());

    ensureBitmapsLogContainsExactly(List.of(PACK_ID_A, mostRecentBitmapPackId, PACK_ID_B));

    assertThat(new PreserveOutdatedBitmapsAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();

    assertThat(Files.exists(logPath)).isTrue();
    assertBitmapsLogContainsOnly(mostRecentBitmapPackId.getName());
  }
}
