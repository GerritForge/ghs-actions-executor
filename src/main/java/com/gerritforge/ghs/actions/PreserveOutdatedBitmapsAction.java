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

import com.gerritforge.ghs.actions.BitmapGenerationLog.BytesChunk;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.stream.Stream;
import org.eclipse.jgit.lib.ObjectId;

class PreserveOutdatedBitmapsAction implements Action {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final PreserveInfo EMPTY = new PreserveInfo(Optional.empty(), 0L);

  @Override
  public ActionResult apply(String repositoryPath) {
    try {
      Optional<Path> snapshot = BitmapGenerationLog.snapshotLog(repositoryPath);
      snapshot.ifPresentOrElse(
          snapshotPath -> {
            try {
              Path preservedDir = ensurePreservedDir(repositoryPath);
              Path packsDir = preservedDir.getParent();
              PreserveInfo info =
                  BitmapGenerationLog.read(
                      snapshotPath,
                      packInfos -> preserveOldPacks(preservedDir, packsDir, packInfos));
              logger.atInfo().log(
                  "PreserveOutdatedBitmapsAction processed %d files in %s repository",
                  info.files(), repositoryPath);
              Files.deleteIfExists(snapshotPath);
              keepLastInLog(info, repositoryPath);
            } catch (IOException e) {
              logger.atSevere().withCause(e).log(
                  "Preserving pack files for log %s failed.", snapshotPath);
              throw new UncheckedIOException(e);
            }
          },
          () -> logger.atInfo().log("No packs to preserve in %s repository", repositoryPath));
    } catch (IOException | UncheckedIOException e) {
      logger.atSevere().withCause(e).log(
          "Preserve packs failed for the repository path %s", repositoryPath);
      return new ActionResult(
          false, String.format("Preserve packs action failed, message: %s", e.getMessage()));
    }
    return new ActionResult(true);
  }

  private void keepLastInLog(PreserveInfo info, String repositoryPath) throws IOException {
    if (info != EMPTY) {
      Path tempFile = Files.createTempFile(".ghs-packs", ".tmp");
      try (FileChannel logChannel = BitmapGenerationLog.openLogChannel(repositoryPath);
          FileLock lock = logChannel.lock();
          FileChannel tempChannel =
              FileChannel.open(tempFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
        info.last().ifPresent(last -> writePackId(last, tempChannel));
        FileChannel sourceChannel = (FileChannel) lock.acquiredBy();
        long transferred = 0L;
        long size = sourceChannel.size();

        while (transferred < size) {
          transferred += sourceChannel.transferTo(transferred, size - transferred, tempChannel);
        }
        tempChannel.force(true);
        BitmapGenerationLog.moveWithReplace(tempFile, BitmapGenerationLog.logPath(repositoryPath));
      } finally {
        Files.deleteIfExists(tempFile);
      }
    }
  }

  @CanIgnoreReturnValue
  private static int writePackId(byte[] packId, FileChannel channel) {
    ByteBuffer buffer = ByteBuffer.wrap(packId);
    int written = 0;
    while (buffer.hasRemaining()) {
      try {
        written += channel.write(buffer);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return written;
  }

  private PreserveInfo preserveOldPacks(
      Path preservedDir, Path packsDir, Stream<BytesChunk> packInfos) {
    return packInfos
        .map(
            packInfo -> {
              if (packInfo.isLast()) { // do nothing
                return new PreserveInfo(Optional.of(packInfo.data()), 0L);
              }

              String packId = ObjectId.fromRaw(packInfo.data()).getName();
              String packFilesMask = String.format("pack-%s.*", packId);
              long counter = 0L;

              try (DirectoryStream<Path> packFiles =
                  Files.newDirectoryStream(packsDir, packFilesMask)) {
                for (Path packFilePath : packFiles) {
                  Path targetPath = preservedDir.resolve(packFilePath.getFileName());
                  BitmapGenerationLog.move(packFilePath, targetPath);
                  counter++;
                }
              } catch (IOException e) {
                logger.atSevere().withCause(e).log("Moving pack files for %s id failed.", packId);
                throw new UncheckedIOException(e);
              }

              return new PreserveInfo(Optional.empty(), counter);
            })
        .reduce(
            EMPTY,
            (prev, next) ->
                new PreserveInfo(next.last().or(prev::last), prev.files() + next.files()));
  }

  private Path ensurePreservedDir(String repositoryPath) throws IOException {
    Path preservedPath = Path.of(repositoryPath, "objects", "pack", "preserved");
    if (preservedPath.toFile().isDirectory()) {
      return preservedPath;
    }

    Files.createDirectories(preservedPath);
    return preservedPath;
  }

  private record PreserveInfo(Optional<byte[]> last, long files) {}
}
