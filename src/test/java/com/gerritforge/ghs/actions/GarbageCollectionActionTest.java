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

import java.io.IOException;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.internal.storage.file.GC;
import org.eclipse.jgit.lib.ConfigConstants;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.StoredConfig;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.junit.Test;

public class GarbageCollectionActionTest extends GitActionTest {

  @Test
  public void applyGarbageCollectionActionShouldRepackRepo() throws Exception {

    setPrunepackexpire("now");

    pushNewCommitToBranch();
    pushNewCommitToBranch();

    assertThat(getNumberOfPackFiles()).isGreaterThan(1);

    assertThat(new GarbageCollectionAction().apply(testRepoPath.toString()).isSuccessful())
        .isTrue();

    assertThat(getNumberOfPackFiles()).isEqualTo(1);
  }

  private long getNumberOfPackFiles() throws IOException {
    return new GC((FileRepository) testRepoGit.getRepository()).getStatistics().numberOfPackFiles;
  }

  private void setPrunepackexpire(String value) throws IOException {
    Repository repository =
        new FileRepositoryBuilder()
            .setGitDir(testRepoPath.toFile())
            .readEnvironment()
            .findGitDir()
            .build();

    StoredConfig config = repository.getConfig();
    config.setString("gc", null, ConfigConstants.CONFIG_KEY_PRUNEPACKEXPIRE, value);
    config.save();
  }
}
