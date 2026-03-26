// Copyright (c) 2024 GERRITFORGE, INC. All rights reserved.

// This software and associated documentation files (the "Software") are protected
// by intellectual property rights and are the exclusive property of GERRITFORGE,
// INC. Unauthorized copying, use, or distribution of this software is strictly
// prohibited.

// GERRITFORGE, GHS and GERRITFORGE HEALTH SERVICE are registered
// trademarks of GERRITFORGE, INC. The use of these trademarks is subject to the
// approval and regulation of GERRITFORGE, INC.

// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to use the
// Software solely for evaluation purposes, subject to the following conditions:

// 1. The Software may not be used for any commercial purposes.
// 2. The Software may not be copied, modified, merged, published, distributed,
//    sublicensed, and/or sold, except as expressly provided in this notice.
// 3. The above copyright notice and this permission notice shall be included in
//    all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

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

    PackFile bitmapFile = findMostRecentPack().create(BITMAP_INDEX);

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
  public void applySequentialBitmapGenerationActionShouldNotGenerateBitMapIfAlreadyRunning()
      throws Exception {
    setAllowConcurrentBitmapGeneration(false);

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

  @Test
  public void applyConcurrentBitmapGenerationActionShouldGenerateBitMapIfAlreadyRunning()
      throws Exception {
    setAllowConcurrentBitmapGeneration(true);

    pushNewCommitToBranch();
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

    assertThat(result.isSuccessful()).isTrue();
    assertThat(bitmapsCountBefore + 1).isEqualTo(bitmapsCountAfter);
  }
}
