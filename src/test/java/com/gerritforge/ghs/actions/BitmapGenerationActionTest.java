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

import static com.google.common.truth.Truth.assertThat;
import static org.eclipse.jgit.internal.storage.pack.PackExt.BITMAP_INDEX;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.eclipse.jgit.internal.storage.file.GC;
import org.eclipse.jgit.internal.storage.file.PackFile;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.junit.Test;

public class BitmapGenerationActionTest extends GitActionTest {

  @Test
  public void applyBitmapGenerationActionShouldGenerateBitMap() throws Exception {
    Ref branch = pushNewCommitToBranch();
    PackFile packFile = findMostRecentPack();

    PackFile bitmapFile = packFile.create(BITMAP_INDEX);

    assertThat(bitmapFile.exists()).isFalse();

    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
    assertThat(bitmapFile.exists()).isTrue();
  }

  @Test
  public void applyBitmapGenerationActionShouldGenerateLog() throws Exception {
    File bitmapsLogFile = bitmapsLogPath.toFile();
    assertThat(bitmapsLogFile.exists()).isFalse();

    PackFile packFile = pushAndGenerateNewBitmap();

    assertThat(bitmapsLogFile.exists()).isTrue();
    assertBitmapsLogContainsOnly(packFile.getId());
  }

  @Test
  public void applyBitmapGenerationActionShouldUpdateLog() throws Exception {
    PackFile packFile1 = pushAndGenerateNewBitmap();
    PackFile packFile2 = pushAndGenerateNewBitmap();

    ensureBitmapsLogContainsExactly(
        List.of(ObjectId.fromString(packFile1.getId()), ObjectId.fromString(packFile2.getId())));
  }

  @Test
  public void applyBitmapGenerationActionShouldNotUpdateLogWithDuplicates() throws Exception {
    PackFile packFile = pushAndGenerateNewBitmap();
    BitmapGenerationAction action = new BitmapGenerationAction();

    assertThat(action.apply(testRepoPath.toString()).isSuccessful()).isTrue();

    assertBitmapsLogContainsOnly(packFile.getId());
  }

  @Test
  public void applyBitmapGenerationActionShouldNotGenerateBitMapIfAlreadyRunning()
      throws Exception {
    GC gc = new GC(repo);
    GC.PidLock lock = gc.new PidLock();
    lock.lock();

    long bitmapsCountBefore =
        Files.list(testRepoPath.resolve("objects/pack"))
            .filter(p -> p.toString().endsWith(".bitmap"))
            .count();

    ActionResult result = new BitmapGenerationAction().apply(testRepoPath.toString());

    long bitmapsCountAfter =
        Files.list(testRepoPath.resolve("objects/pack"))
            .filter(p -> p.toString().endsWith(".bitmap"))
            .count();

    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getMessage())
        .startsWith(
            "Skipped bitmap generation for repository "
                + testRepoGit.getRepository().getIdentifier());
    assertThat(bitmapsCountBefore).isEqualTo(bitmapsCountAfter);
  }
}
