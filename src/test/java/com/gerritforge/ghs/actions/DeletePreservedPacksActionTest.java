// Copyright (C) 2026 GerritForge, Inc.
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

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;

public class DeletePreservedPacksActionTest extends GitActionTest {

  @Test
  public void applyDeletePreservedPacksActionShouldDoNothingWhenNoPreservedDir() throws Exception {
    assertThat(preservedPath.toFile().exists()).isFalse();

    ActionResult result = new DeletePreservedPacksAction().apply(testRepoPath.toString());

    assertThat(result.isSuccessful()).isTrue();
    assertThat(preservedPath.toFile().exists()).isFalse();
  }

  @Test
  public void applyDeletePreservedPacksActionShouldDeleteEmptyPreservedDir() throws Exception {
    Files.createDirectories(preservedPath);
    assertThat(preservedPath.toFile().isDirectory()).isTrue();

    ActionResult result = new DeletePreservedPacksAction().apply(testRepoPath.toString());

    assertThat(result.isSuccessful()).isTrue();
    assertThat(preservedPath.toFile().exists()).isFalse();
  }

  @Test
  public void applyDeletePreservedPacksActionShouldDeletePreservedPackFiles() throws Exception {
    setPrunePackExpire("now");

    pushAndGenerateNewBitmap();
    pushAndGenerateNewBitmap();
    new PreserveOutdatedBitmapsAction().apply(testRepoPath.toString());

    assertThat(preservedPath.toFile().isDirectory()).isTrue();
    long packCountBefore = Files.list(packPath).count();

    ActionResult result = new DeletePreservedPacksAction().apply(testRepoPath.toString());

    assertThat(result.isSuccessful()).isTrue();
    assertThat(preservedPath.toFile().exists()).isFalse();
    assertThat(Files.list(packPath).count()).isEqualTo(packCountBefore);
  }

  @Test
  public void applyDeletePreservedPacksActionIsIdempotent() throws Exception {
    Files.createDirectories(preservedPath);

    assertThat(new DeletePreservedPacksAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();
    assertThat(preservedPath.toFile().exists()).isFalse();

    ActionResult result = new DeletePreservedPacksAction().apply(testRepoPath.toString());

    assertThat(result.isSuccessful()).isTrue();
  }
}
