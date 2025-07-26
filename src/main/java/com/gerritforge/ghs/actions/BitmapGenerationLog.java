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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/** Manages thread-safe log for bitmap pack generation operations. */
class BitmapGenerationLog {
  /** Functional interface for performing log update operations. */
  @FunctionalInterface
  interface Updater {
    void writeContent(FileChannel channel) throws IOException;
  }

  /** Functional interface for reading log content in chunks. */
  @FunctionalInterface
  interface Reader<T> {
    T read(Stream<BytesChunk> chunks) throws IOException;
  }

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final String GHS_PACKS_LOG = ".ghs-packs.log";
  static final long ID_LENGTH = 20L;

  /**
   * Updates the bitmap generation log with file locking to ensure thread safety.
   *
   * @param objectsPath the Git objects directory path
   * @param updater the operation to perform on the log file
   * @throws IOException if file operations fail
   */
  static void update(Path objectsPath, Updater updater) throws IOException {
    Path logPath = objectsPath.resolve("pack").resolve(GHS_PACKS_LOG);
    try (FileChannel channel =
            FileChannel.open(
                logPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
        FileLock lock = channel.lock()) {
      updater.writeContent(channel);
      channel.force(true);
    }
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
   * Reads log file content in chunks with file locking.
   *
   * @param fileToRead the file to read
   * @param reader the reader to process chunks
   * @return the result from the reader
   * @throws IOException if file operations fail
   */
  static <T> T read(Path fileToRead, Reader<T> reader) throws IOException {
    try (FileChannel channel =
            FileChannel.open(fileToRead, StandardOpenOption.READ, StandardOpenOption.WRITE);
        InputStream input = Channels.newInputStream(channel);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));
        FileLock lock = channel.lock()) {
      long fileSize = channel.size();
      long totalChunks = fileSize / ID_LENGTH;

      Stream<BytesChunk> chunks =
          LongStream.rangeClosed(0, totalChunks)
              .mapToObj(
                  index -> {
                    try {
                      long position = ID_LENGTH * index;
                      long size = Math.min(ID_LENGTH, fileSize - position);

                      MappedByteBuffer buffer =
                          channel.map(FileChannel.MapMode.READ_ONLY, position, size);
                      boolean isLast = index == totalChunks - 1;

                      byte[] chunk = new byte[(int) size];
                      buffer.get(chunk);
                      return new BytesChunk(chunk, isLast);
                    } catch (IOException e) {
                      logger.atSevere().withCause(e).log("Reading chunks failed.");
                      throw new UncheckedIOException(e);
                    }
                  });
      return reader.read(chunks);
    }
  }

  /**
   * Moves a file with atomic operation support.
   *
   * @param source the source file
   * @param target the target file
   * @throws IOException if move operation fails
   */
  static void move(Path source, Path target) throws IOException {
    if (Files.exists(source)) {
      try {
        Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(source, target);
      }
    }
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

  /** Represents a chunk of bytes from the log file. */
  record BytesChunk(byte[] data, boolean isLast) {}

  private BitmapGenerationLog() {}
}
