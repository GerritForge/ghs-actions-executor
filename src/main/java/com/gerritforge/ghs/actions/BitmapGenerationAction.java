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

import com.google.common.flogger.FluentLogger;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.internal.storage.file.Pack;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;

public class BitmapGenerationAction implements Action {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  public Collection<Pack> prepareBitmap(FileRepository repo) throws IOException {
    BitmapGenerator repack = new BitmapGenerator(repo, isVerbose());
    return repack.repackAndGenerateBitmap();
  }

  @Override
  public ActionResult apply(String repositoryPath) {
    FileRepositoryBuilder repositoryBuilder =
        new FileRepositoryBuilder()
            .setGitDir(new File(repositoryPath))
            .readEnvironment()
            .findGitDir();

    try (FileRepository repository = (FileRepository) repositoryBuilder.build()) {
      Collection<Pack> packFiles = prepareBitmap(repository);
      updateBitmapGenerationLog(packFiles, repository.getObjectsDirectory().toPath());
    } catch (IOException e) {
      logger.atSevere().withCause(e).log(
          "Bitmap generation failed for the repository path %s", repositoryPath);
      return new ActionResult(
          false, String.format("Bitmap generation action failed, message: %s", e.getCause()));
    }

    return new ActionResult(true);
  }

  private void updateBitmapGenerationLog(Collection<Pack> packfiles, Path objectsPath)
      throws IOException {
    if (packfiles.isEmpty()) {
      return;
    }

    Path logPath = objectsPath.resolve("pack").resolve("packs.log");
    try (FileChannel channel =
            FileChannel.open(
                logPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
        FileLock lock = channel.lock()) {
      for (Pack packFile : packfiles) {
        byte[] packBytes =
            String.format("%s%s", packFile.getPackName(), System.lineSeparator())
                .getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.wrap(packBytes);
        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }
      }
      channel.force(true);
    }
  }
}
