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
import static org.eclipse.jgit.lib.ConfigConstants.CONFIG_GC_SECTION;
import static org.eclipse.jgit.lib.ConfigConstants.CONFIG_KEY_PRUNEPACKEXPIRE;

import com.google.common.base.MoreObjects;
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
import java.text.ParseException;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.eclipse.jgit.annotations.Nullable;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.internal.storage.file.FileSnapshot;
import org.eclipse.jgit.internal.storage.pack.PackExt;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.util.GitTimeParser;
import org.eclipse.jgit.util.SystemReader;

class PreserveOutdatedBitmapsAction implements Action {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final PreserveInfo EMPTY = new PreserveInfo(Set.of(), 0L);
  private static final int PRUNE_PACK_EXPIRE_SECONDS_DEFAULT = 3600;
  private static final String PRUNE_PACK_EXPIRE_DEFAULT =
      PRUNE_PACK_EXPIRE_SECONDS_DEFAULT + ".seconds.ago";

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
    try {
      Instant packfilePruneCutoff = getPrunePackExpire(repositoryPath);
      Optional<Path> snapshot = BitmapGenerationLog.snapshotLog(repositoryPath);
      snapshot.ifPresentOrElse(
          snapshotPath -> {
            try {
              Path preservedDir = ensurePreservedDir(repositoryPath);
              Path packsDir = preservedDir.getParent();
              Path logPath = BitmapGenerationLog.logPath(repositoryPath);
              Set<ObjectId> entriesFromLog =
                  BitmapGenerationLog.readAllEntriesFromLog(snapshotPath);

              PreserveInfo info =
                  preserveOldPacks(preservedDir, packsDir, entriesFromLog, packfilePruneCutoff);
              logger.atInfo().log(
                  "PreserveOutdatedBitmapsAction processed %d files in %s repository",
                  info.files(), repositoryPath);
              Files.deleteIfExists(snapshotPath);
              if (!info.packIds.isEmpty()) {
                overwriteObjectIdsInLog(info.packIds, repositoryPath);
              } else {
                logger.atInfo().log("No entries to keep in %s. Removing.", logPath.getFileName());
                Files.deleteIfExists(logPath);
              }
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

  private static Instant getPrunePackExpire(String repositoryPath) {
    try {
      try (FileRepository repository = new FileRepository(repositoryPath)) {
        String prunePackExpire =
            repository.getConfig().getString(CONFIG_GC_SECTION, null, CONFIG_KEY_PRUNEPACKEXPIRE);
        return GitTimeParser.parseInstant(
            MoreObjects.firstNonNull(prunePackExpire, PRUNE_PACK_EXPIRE_DEFAULT));
      }
    } catch (IOException | ParseException e) {
      logger.atWarning().withCause(e).log(
          "Unable to read gc.prunePackExpire from Git config: defaulting to %s",
          PRUNE_PACK_EXPIRE_DEFAULT);
      // Cannot use GitTimeParser.parseInstant() as it would still throw IOException and
      // ParseException
      return SystemReader.getInstance()
          .civilNow()
          .atZone(SystemReader.getInstance().getTimeZoneId())
          .toInstant()
          .minusSeconds(PRUNE_PACK_EXPIRE_SECONDS_DEFAULT);
    }
  }

  private static void overwriteObjectIdsInLog(Set<ObjectId> packIds, String repositoryPath)
      throws IOException {
    Path tempFile = Files.createTempFile(".ghs-packs", ".tmp");
    try (FileChannel logChannel = BitmapGenerationLog.openLogChannel(repositoryPath);
        FileLock lock = logChannel.lock();
        FileChannel tempChannel =
            FileChannel.open(tempFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {

      byte[] raw = new byte[(int) ID_LENGTH * packIds.size()];
      int o = 0;
      for (ObjectId packId : packIds) {
        packId.copyRawTo(raw, o);
        o += (int) ID_LENGTH;
      }
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
      Path preservedDir, Path packsDir, Set<ObjectId> packEntriesInLog, Instant packfilePruneCutoff)
      throws IOException {
    if (packEntriesInLog.isEmpty()) {
      logger.atFine().log("No entries found in log. Nothing to preserve.");
      return EMPTY;
    }

    Path mostRecentBitmap = getMostRecentExistingBitmap(packsDir);
    if (mostRecentBitmap == null) {
      logger.atFine().log("Could not find any existing bitmap in %s.", packsDir);
      /* don't return here, we still want to move log-referenced packs to `preserved`, if any exist.*/
    }

    Set<ObjectId> packsToKeepInLog = new HashSet<>();
    long numberOfFilesMovedToPreserved = 0L;
    for (ObjectId packId : packEntriesInLog) {
      String packName = String.format("pack-%s", packId.getName());
      Path packBitmapPath =
          packsDir.resolve(String.format("%s.%s", packName, BITMAP_INDEX.getExtension()));

      if (mostRecentBitmap != null
          && packBitmapPath.getFileName().equals(mostRecentBitmap.getFileName())) {
        logger.atInfo().log(
            "%s is associated to the most recent bitmap. Do not move to preserved.", packName);
        packsToKeepInLog.add(packId);
        continue;
      }

      File packFile =
          packsDir.resolve(String.format("%s.%s", packName, PACK.getExtension())).toFile();
      FileSnapshot packSnapshot = FileSnapshot.save(packFile);
      Instant instant = packSnapshot.lastModifiedInstant();
      if (instant.isAfter(packfilePruneCutoff)) {
        logger.atInfo().log("%s has not expired. Do not move to preserved.", packName);
        packsToKeepInLog.add(packId);
        continue;
      }

      numberOfFilesMovedToPreserved += movePacksToPreserved(packsDir, preservedDir, packId);
    }
    return new PreserveInfo(packsToKeepInLog, numberOfFilesMovedToPreserved);
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

  private record PreserveInfo(Set<ObjectId> packIds, long files) {}
}
