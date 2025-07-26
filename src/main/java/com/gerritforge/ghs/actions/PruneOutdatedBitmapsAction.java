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

class PruneOutdatedBitmapsAction implements Action {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final PruneInfo EMPTY = new PruneInfo(Optional.empty(), Optional.empty(), 0L);

  @Override
  public ActionResult apply(String repositoryPath) {
    try {
      Optional<Path> snapshot = BitmapGenerationLog.snapshotLog(repositoryPath);
      snapshot.ifPresentOrElse(
          snapshotPath -> {
            try {
              Path preservedDir = ensurePreservedDir(repositoryPath);
              Path packsDir = preservedDir.getParent();
              PruneInfo info =
                  BitmapGenerationLog.read(
                      snapshotPath, packInfos -> pruneOldPacks(preservedDir, packsDir, packInfos));
              logger.atInfo().log(
                  "PruneOutdatedBitmapsAction processed %d files in %s repository",
                  info.files(), repositoryPath);
              Files.deleteIfExists(snapshotPath);
              keepLastTwoPacksInLog(info, repositoryPath);
            } catch (IOException e) {
              logger.atSevere().withCause(e).log(
                  "Preserving bitmap files for log %s failed.", snapshotPath);
              throw new UncheckedIOException(e);
            }
          },
          () -> logger.atInfo().log("No bitmaps to preserve in %s repository", repositoryPath));
    } catch (IOException e) {
      logger.atSevere().withCause(e).log(
          "Preserve bitmaps failed for the repository path %s", repositoryPath);
      return new ActionResult(
          false, String.format("Preserve bitmaps action failed, message: %s", e.getMessage()));
    }
    return new ActionResult(true);
  }

  private void keepLastTwoPacksInLog(PruneInfo info, String repositoryPath) throws IOException {
    if (info != EMPTY) {
      Path tempFile = Files.createTempFile(".ghs-packs", ".tmp");
      try (FileLock lock = BitmapGenerationLog.acquaireLogLock(repositoryPath);
          FileChannel tempChannel =
              FileChannel.open(tempFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
        info.secondToLast()
            .ifPresent(secondToLastPack -> writePackId(secondToLastPack, tempChannel));
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

  private static void writePackId(byte[] packId, FileChannel channel) {
    ByteBuffer buffer = ByteBuffer.wrap(packId);
    while (buffer.hasRemaining()) {
      try {
        channel.write(buffer);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private PruneInfo pruneOldPacks(Path preservedDir, Path packsDir, Stream<BytesChunk> packInfos) {
    return packInfos
        .filter(packInfo -> packInfo.data().length > 0)
        .map(
            packInfo -> {
              if (packInfo.isLast()) { // do nothing
                return new PruneInfo(Optional.empty(), Optional.of(packInfo.data()), 0L);
              }

              String packId = ObjectId.fromRaw(packInfo.data()).getName();
              String bitmapFilesMask = String.format("pack-%s.*", packId);
              long counter = 0L;

              try (DirectoryStream<Path> bitmapFiles =
                  Files.newDirectoryStream(packsDir, bitmapFilesMask)) {
                for (Path bitmapFilePath : bitmapFiles) {
                  Path targetPath = preservedDir.resolve(bitmapFilePath.getFileName());
                  if (packInfo.isSecondToLast()) { // move to preserved dir
                    BitmapGenerationLog.move(bitmapFilePath, targetPath);
                  } else { // delete
                    Files.deleteIfExists(bitmapFilePath);
                  }
                  counter++;
                }
              } catch (IOException e) {
                logger.atSevere().withCause(e).log("Moving bitmap files for %s id failed.", packId);
                throw new UncheckedIOException(e);
              }

              // also remove previous `prune` actions preserved files
              if (!packInfo.isSecondToLast()) {
                try (DirectoryStream<Path> preservedBitmapFiles =
                    Files.newDirectoryStream(preservedDir, bitmapFilesMask)) {
                  for (Path preservedBitmapFilePath : preservedBitmapFiles) {
                    Files.deleteIfExists(preservedBitmapFilePath);
                    counter++;
                  }
                } catch (IOException e) {
                  logger.atSevere().withCause(e).log(
                      "Pruning preserved bitmap files for %s id failed.", packId);
                  throw new UncheckedIOException(e);
                }
              }

              return new PruneInfo(
                  packInfo.isSecondToLast() ? Optional.of(packInfo.data()) : Optional.empty(),
                  Optional.empty(),
                  counter);
            })
        .reduce(
            EMPTY,
            (prev, next) ->
                new PruneInfo(
                    prev.secondToLast().or(next::secondToLast),
                    next.last().or(prev::last),
                    prev.files() + next.files()));
  }

  private Path ensurePreservedDir(String repositoryPath) throws IOException {
    Path preservedPath = Path.of(repositoryPath, "objects", "pack", "preserved");
    if (preservedPath.toFile().isDirectory()) {
      return preservedPath;
    }

    Files.createDirectories(preservedPath);
    return preservedPath;
  }

  private static record PruneInfo(
      Optional<byte[]> secondToLast, Optional<byte[]> last, long files) {}
}
