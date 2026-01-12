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

import static com.gerritforge.ghs.actions.BitmapGenerator.GC_LOCK_FILE;
import static com.google.common.truth.Truth.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.junit.Test;

public class BitmapGenerationActionTest extends GitActionTest {

  @Test
  public void applyBitmapGenerationActionShouldGenerateBitMap() throws Exception {
    Ref branch = pushNewCommitToBranch();
    Optional<Path> packFile =
        Files.list(testRepoPath.resolve("objects/pack"))
            .filter(p -> p.toString().endsWith(".pack"))
            .findFirst();
    assertThat(packFile).isPresent();
    String bitmapFileName = packFile.get().toString().replace(".pack", ".bitmap");
    File bitmapFile = new File(bitmapFileName);
    assertThat(bitmapFile.exists()).isFalse();

    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
    assertThat(bitmapFile.exists()).isTrue();
  }

  @Test
  public void applyBitmapGenerationActionShouldGenerateLog() throws Exception {
    pushNewCommitToBranch();
    Optional<Path> packFile =
        Files.list(testRepoPath.resolve("objects/pack"))
            .filter(p -> p.toString().endsWith(".pack"))
            .findFirst();

    Path bitmapsLogPath = packFile.get().getParent().resolve(".ghs-packs.log");
    File bitmapsLogFile = bitmapsLogPath.toFile();
    assertThat(bitmapsLogFile.exists()).isFalse();

    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
    assertThat(bitmapsLogFile.exists()).isTrue();

    String packFilename = packFile.get().getFileName().toString();
    String packId =
        packFilename.substring("pack-".length(), packFilename.length() - ".pack".length());
    assertThat(
            logEntries(bitmapsLogPath)
                .anyMatch(id -> ObjectId.fromRaw(id).getName().equals(packId)))
        .isTrue();
  }

  @Test
  public void applyBitmapGenerationActionShouldUpdateLog() throws Exception {
    pushNewCommitToBranch();
    Optional<Path> packFile =
        Files.list(testRepoPath.resolve("objects/pack"))
            .filter(p -> p.toString().endsWith(".pack"))
            .findFirst();

    Path bitmapsLogPath = packFile.get().getParent().resolve(".ghs-packs.log");
    File bitmapsLogFile = bitmapsLogPath.toFile();
    assertThat(bitmapsLogFile.exists()).isFalse();

    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
    assertThat(bitmapsLogFile.exists()).isTrue();

    pushNewCommitToBranch();
    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
    assertThat(logEntries(bitmapsLogPath).count()).isEqualTo(2L);
  }

  @Test
  public void applyBitmapGenerationActionShouldNotUpdateLogWithDuplicates() throws Exception {
    pushNewCommitToBranch();
    Path logPath = testRepoPath.resolve("objects/pack/.ghs-packs.log");
    BitmapGenerationAction action = new BitmapGenerationAction();

    assertThat(action.apply(testRepoPath.toString()).isSuccessful()).isTrue();
    assertThat(action.apply(testRepoPath.toString()).isSuccessful()).isTrue();

    assertThat(logEntries(logPath).count()).isEqualTo(1);
  }

  @Test
  public void applyBitmapGenerationActionShouldNotGenerateBitMapIfAlreadyRunning() throws Exception {
    File lockFile = new File(testRepoPath.resolve(GC_LOCK_FILE).toString());
    lockFile.createNewFile();

    try(FileChannel channel =
        FileChannel.open(
            lockFile.toPath(),
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);
    FileLock lock = channel.lock()) {
      assertThat(lockFile.exists()).isTrue();

      ActionResult result = new BitmapGenerationAction().apply(testRepoPath.toString());
      assertThat(result.isSuccessful()).isTrue();
      assertThat(result.getMessage()).startsWith("Bitmap generation already ongoing");
    }
  }

  private Stream<byte[]> logEntries(Path logPath) throws IOException {
    byte[] content = Files.readAllBytes(logPath);
    int chunkSize = 20;
    if (content.length < chunkSize || content.length % chunkSize != 0) {
      return Stream.empty();
    }

    return Stream.iterate(0, i -> i < content.length, i -> i + chunkSize)
        .map(
            i -> {
              int end = Math.min(i + chunkSize, content.length);
              return Arrays.copyOfRange(content, i, end);
            });
  }
}
