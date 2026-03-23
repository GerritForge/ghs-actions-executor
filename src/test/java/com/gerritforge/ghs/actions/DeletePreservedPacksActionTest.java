// Copyright (c) 2026 GERRITFORGE, INC. All rights reserved.

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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import org.junit.Test;

public class DeletePreservedPacksActionTest extends GitActionTest {

  @Test
  public void applyDeletePreservedPacksActionShouldDoNothingWhenNoPreservedDir() {
    assertThat(preservedPath.toFile().exists()).isFalse();

    ActionResult result = new DeletePreservedPacksAction().apply(testRepoPath.toString());

    assertThat(result.isSuccessful()).isTrue();
  }

  @Test
  public void applyDeletePreservedPacksActionShouldSucceedWhenPreservedDirIsEmpty()
      throws Exception {
    Files.createDirectories(preservedPath);

    ActionResult result = new DeletePreservedPacksAction().apply(testRepoPath.toString());

    assertThat(result.isSuccessful()).isTrue();
    assertThat(preservedPath.toFile().isDirectory()).isTrue();
  }

  @Test
  public void applyDeletePreservedPacksActionShouldDeleteOnlyExpiredPreservedPackFiles()
      throws Exception {
    setPrunePreservedExpire("1.hour.ago");
    Files.createDirectories(preservedPath);
    Path expiredFile = Files.createFile(preservedPath.resolve("old-pack.pack"));
    Files.setLastModifiedTime(expiredFile, FileTime.from(Instant.now().minusSeconds(7200)));
    Path recentFile = Files.createFile(preservedPath.resolve("recent-pack.pack"));

    ActionResult result = new DeletePreservedPacksAction().apply(testRepoPath.toString());

    assertThat(result.isSuccessful()).isTrue();
    assertThat(Files.exists(expiredFile)).isFalse();
    assertThat(Files.exists(recentFile)).isTrue();
  }
}
