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

import static com.gerritforge.ghs.actions.BitmapGenerationLog.ID_LENGTH;
import static org.eclipse.jgit.internal.storage.pack.PackExt.BITMAP_INDEX;
import static org.eclipse.jgit.internal.storage.pack.PackExt.INDEX;
import static org.eclipse.jgit.internal.storage.pack.PackExt.PACK;

import com.google.common.flogger.FluentLogger;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.eclipse.jgit.annotations.Nullable;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.internal.storage.file.GC;
import org.eclipse.jgit.internal.storage.pack.PackExt;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;

class PreserveOutdatedBitmapsAction implements Action {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final PreserveInfo EMPTY = new PreserveInfo(Optional.empty(), 0L);

  private static final Comparator<Path> BY_LAST_MODIFIED =
      (a, b) -> {
        try {
          return Files.getLastModifiedTime(a).compareTo(Files.getLastModifiedTime(b));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      };

  @Override
  public ActionResult apply(String repositoryPath) {
    Optional<Path> snapshot;
    try {
      snapshot = BitmapGenerationLog.snapshotLog(repositoryPath);
    } catch (IOException e) {
      logger.atSevere().withCause(e).log(
          "Preserve packs failed for the repository path %s", repositoryPath);
      return new ActionResult(
          false, String.format("Preserve packs action failed, message: %s", e.getMessage()));
    }

    if (snapshot.isEmpty()) {
      logger.atInfo().log("No packs to preserve in %s repository", repositoryPath);
      return new ActionResult(true);
    }

    Path snapshotPath = snapshot.get();
    FileRepositoryBuilder repositoryBuilder =
        new FileRepositoryBuilder()
            .setGitDir(new File(repositoryPath))
            .readEnvironment()
            .findGitDir();

    try (FileRepository repo = (FileRepository) repositoryBuilder.build()) {
      Path preservedDir = ensurePreservedDir(repositoryPath);
      Path packsDir = preservedDir.getParent();
      Path logPath = BitmapGenerationLog.logPath(repositoryPath);

      GC gc = new GC(repo);
      try (GC.PidLock lock = gc.new PidLock()) {
        if (!lock.lock()) {
          throw new GcLockHeldException(lock.getPidFile());
        }

        Set<ObjectId> entriesFromLog = BitmapGenerationLog.readAllEntriesFromLog(snapshotPath);
        PreserveInfo info = preserveOldPacks(preservedDir, packsDir, entriesFromLog);

        logger.atInfo().log(
            "PreserveOutdatedBitmapsAction processed %d files in %s repository",
            info.files(), repositoryPath);

        Files.deleteIfExists(snapshotPath);
        if (info.last.isPresent()) {
          overwriteSingleObjectIdInLog(info.last.get(), repositoryPath);
        } else {
          logger.atInfo().log("No entries to keep in %s. Removing.", logPath.getFileName());
          Files.deleteIfExists(logPath);
        }
      }

      return new ActionResult(true);
    } catch (GcLockHeldException e) {
      logger.atWarning().withCause(e).log(
          "Skipped preserve packs for repository %s", repositoryPath);
      return new ActionResult(
          false,
          String.format(
              "Skipped preserve packs for repository %s. Cannot lock gc.pid.", repositoryPath));
    } catch (IOException e) {
      logger.atSevere().withCause(e).log("Preserving pack files for log %s failed.", snapshotPath);
      return new ActionResult(
          false, String.format("Preserve packs action failed, message: %s", e.getMessage()));
    }
  }

  private static void overwriteSingleObjectIdInLog(ObjectId last, String repositoryPath)
      throws IOException {
    Path tempFile = Files.createTempFile(".ghs-packs", ".tmp");
    try (FileChannel logChannel = BitmapGenerationLog.openLogChannel(repositoryPath);
        FileLock lock = logChannel.lock();
        FileChannel tempChannel =
            FileChannel.open(tempFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {

      byte[] raw = new byte[(int) ID_LENGTH];
      last.copyRawTo(raw, 0);
      BitmapGenerationLog.writePackId(raw, tempChannel);

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

  @Nullable
  static Path getMostRecentExistingBitmap(Path packsDir) throws IOException {
    Path latest = null;

    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(
            packsDir,
            entry -> {
              String name = entry.getFileName().toString();
              return name.startsWith("pack-") && name.endsWith(".bitmap");
            })) {

      for (Path p : stream) {
        if (latest == null || BY_LAST_MODIFIED.compare(p, latest) > 0) {
          latest = p;
        }
      }
    }

    return latest;
  }

  private PreserveInfo preserveOldPacks(
      Path preservedDir, Path packsDir, Set<ObjectId> packEntriesInLog) throws IOException {
    if (packEntriesInLog.isEmpty()) {
      logger.atFine().log("No entries found in log. Nothing to preserve.");
      return EMPTY;
    }

    Path mostRecentBitmap = getMostRecentExistingBitmap(packsDir);
    if (mostRecentBitmap == null) {
      logger.atFine().log("Could not find any existing bitmap in %s.", packsDir);
      /* don't return here, we still want to move log-referenced packs to `preserved`, if any exist.*/
    }

    Optional<ObjectId> maybePackToKeepInLog = Optional.empty();
    long numberOfFilesMovedToPreserved = 0L;
    for (ObjectId packId : packEntriesInLog) {
      String packName = String.format("pack-%s", packId.getName());
      Path packBitmapPath =
          packsDir.resolve(String.format("%s.%s", packName, BITMAP_INDEX.getExtension()));

      if (mostRecentBitmap != null
          && packBitmapPath.getFileName().equals(mostRecentBitmap.getFileName())) {
        logger.atInfo().log(
            "%s is associated to the most recent bitmap. Do not move to preserved.", packName);
        maybePackToKeepInLog = Optional.of(packId);
        continue;
      }

      numberOfFilesMovedToPreserved += movePacksToPreserved(packsDir, preservedDir, packId);
    }
    return new PreserveInfo(maybePackToKeepInLog, numberOfFilesMovedToPreserved);
  }

  private static long movePacksToPreserved(Path packsDir, Path preservedDir, ObjectId packId)
      throws IOException {
    long numberOfFilesMovedToPreserved = 0L;
    for (PackExt ext : List.of(BITMAP_INDEX, INDEX, PACK)) {
      Path sourcePackFile =
          packsDir.resolve(String.format("pack-%s.%s", packId.getName(), ext.getExtension()));
      Path destinationPackFile = preservedDir.resolve(sourcePackFile.getFileName());
      if (BitmapGenerationLog.move(sourcePackFile, destinationPackFile)) {
        numberOfFilesMovedToPreserved++;
      }
    }
    return numberOfFilesMovedToPreserved;
  }

  private Path ensurePreservedDir(String repositoryPath) throws IOException {
    Path preservedPath = Path.of(repositoryPath, "objects", "pack", "preserved");
    if (preservedPath.toFile().isDirectory()) {
      return preservedPath;
    }

    Files.createDirectories(preservedPath);
    return preservedPath;
  }

  private record PreserveInfo(Optional<ObjectId> last, long files) {}
}
