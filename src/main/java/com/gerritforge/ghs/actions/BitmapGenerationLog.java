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

import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.eclipse.jgit.lib.ObjectId;

/** Manages thread-safe log for bitmap pack generation operations. */
class BitmapGenerationLog {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final String GHS_PACKS_LOG = ".ghs-packs.log";
  static final long ID_LENGTH = 20L;

  /**
   * Updates the bitmap generation log by appending the given pack names, ensuring that duplicate
   * entries are not written.
   *
   * <p>The log file ({@code .ghs-packs.log}) is accessed under an exclusive file lock to guarantee
   * thread safety. Existing entries are read first and used only for membership checks; the file
   * contents and ordering of existing entries are preserved.
   *
   * <p>pack names that are already present in the log are skipped. New pack names are appended in
   * the order provided by the {@code packIds} iterable.
   *
   * @param objectsPath the Git objects directory path
   * @param packIds the pack ObjectIds to record in the bitmap generation log
   * @throws IOException if the log file is corrupted or if file operations fail
   */
  static void update(Path objectsPath, Iterable<ObjectId> packIds) throws IOException {
    Path logPath = objectsPath.resolve("pack").resolve(GHS_PACKS_LOG);
    try (FileChannel channel =
            FileChannel.open(
                logPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        FileLock lock = channel.lock()) {

      Set<ObjectId> existing = readAllEntriesFromLog(channel);

      channel.position(channel.size());
      byte[] raw = new byte[(int) ID_LENGTH];
      for (ObjectId id : packIds) {
        if (existing.add(id)) {
          logger.atFine().log("Adding packFile %s to %s.", id, GHS_PACKS_LOG);
          id.copyRawTo(raw, 0);
          writePackId(raw, channel);
        } else {
          logger.atInfo().log("%s already contains packFile %s: skipping.", GHS_PACKS_LOG, id);
        }
      }

      channel.force(true);
    }
  }

  public static Set<ObjectId> readAllEntriesFromLog(Path logPath) throws IOException {
    try (FileChannel channel =
            FileChannel.open(
                logPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        FileLock lock = channel.lock()) {
      return readAllEntriesFromLog(channel);
    }
  }

  public static Set<ObjectId> readAllEntriesFromLog(FileChannel channel) throws IOException {
    long startNanos = System.nanoTime();
    logger.atInfo().log("Starting read of existing packfiles entries from %s.", GHS_PACKS_LOG);

    long size = channel.size();
    if (size % ID_LENGTH != 0) {
      throw new IOException(
          "Corrupted " + GHS_PACKS_LOG + " (size not multiple of " + ID_LENGTH + ")");
    }

    channel.position(0);

    int entries = (int) (size / ID_LENGTH);
    Set<ObjectId> ids = new HashSet<>(entries);
    ByteBuffer buf = ByteBuffer.allocate((int) ID_LENGTH);
    byte[] raw = new byte[(int) ID_LENGTH];

    for (int i = 0; i < entries; i++) {
      buf.clear();
      int read = 0;
      while (read < ID_LENGTH) {
        int n = channel.read(buf);
        if (n < 0) {
          throw new IOException("Corrupted " + GHS_PACKS_LOG + " (unexpected EOF)");
        }
        read += n;
      }
      buf.flip();
      buf.get(raw);
      ids.add(ObjectId.fromRaw(raw));
    }

    long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
    logger.atInfo().log(
        "Finished reading %d packfiles entries from %s in %d ms.",
        ids.size(), GHS_PACKS_LOG, elapsedMillis);

    return ids;
  }

  @CanIgnoreReturnValue
  public static int writePackId(byte[] packId, FileChannel channel) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(packId);
    int written = 0;
    while (buffer.hasRemaining()) {
      written += channel.write(buffer);
    }
    return written;
  }

  /**
   * Creates a timestamped snapshot of the log file.
   *
   * @param repositoryPath the repository path
   * @return the snapshot file path, or empty if no log exists
   * @throws IOException if file operations fail
   */
  static Optional<Path> snapshotLog(String repositoryPath) throws IOException {
    Path logPath = logPath(repositoryPath);
    if (!logPath.toFile().exists()) {
      logger.atInfo().log("No packs.log file found at [%s]; skipping snapshot", logPath);
      return Optional.empty();
    }

    Path snapshotPath =
        logPath
            .getParent()
            .resolve(String.format("packs.log.%d.snapshot", System.currentTimeMillis()));
    try (FileChannel channel =
            FileChannel.open(logPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        FileLock lock = channel.lock()) {
      move(logPath, snapshotPath);
    }

    return Optional.of(snapshotPath);
  }

  /**
   * Opens a file channel for log operations.
   *
   * @param repositoryPath the repository path
   * @return the file channel
   * @throws IOException if channel creation fails
   */
  static FileChannel openLogChannel(String repositoryPath) throws IOException {
    Path logPath = logPath(repositoryPath);
    FileChannel channel =
        FileChannel.open(
            logPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    return channel;
  }

  /**
   * Constructs the log file path for the given repository.
   *
   * @param repositoryPath the repository path
   * @return the log file path
   */
  static Path logPath(String repositoryPath) {
    return Path.of(repositoryPath, "objects", "pack", GHS_PACKS_LOG);
  }

  /**
   * Moves a file with atomic operation support, if the file exists.
   *
   * @param source the source file
   * @param target the target file
   * @return a boolean indicating whether the source file existed.
   * @throws IOException if move operation fails
   */
  static boolean move(Path source, Path target) throws IOException {
    if (Files.exists(source)) {
      try {
        Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(source, target);
      }
      logger.atFine().log("Moves source: %s to target: %s.", source, target);
      return true;
    }
    logger.atFine().log(
        "Cannot move source: %s to target: %s, because source does not exist. Skipping.",
        source, target);
    return false;
  }

  /**
   * Moves a file with replace option and atomic operation support.
   *
   * @param source the source file
   * @param target the target file
   * @throws IOException if move operation fails
   */
  static void moveWithReplace(Path source, Path target) throws IOException {
    if (Files.exists(source)) {
      try {
        Files.move(
            source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
      }
    }
  }

  private BitmapGenerationLog() {}
}
