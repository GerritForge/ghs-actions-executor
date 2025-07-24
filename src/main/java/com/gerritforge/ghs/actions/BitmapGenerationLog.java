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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Manages thread-safe log for bitmap pack generation operations. */
class BitmapGenerationLog {
  /** Functional interface for performing log update operations. */
  @FunctionalInterface
  interface Updater {
    void writeContent(FileChannel channel) throws IOException;
  }

  /**
   * Updates the bitmap generation log with file locking to ensure thread safety.
   *
   * @param objectsPath the Git objects directory path
   * @param updater the operation to perform on the log file
   * @throws IOException if file operations fail
   */
  static void update(Path objectsPath, Updater updater) throws IOException {
    Path logPath = objectsPath.resolve("pack").resolve(".ghs-packs.log");
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

  private BitmapGenerationLog() {}
}
