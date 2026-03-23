// Copyright (c) 2026 GERRITFORGE, INC. All rights reserved.

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

import static org.eclipse.jgit.lib.ConfigConstants.CONFIG_GC_SECTION;

import com.google.common.base.MoreObjects;
import com.google.common.flogger.FluentLogger;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Instant;

import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.util.FileUtils;
import org.eclipse.jgit.util.GitTimeParser;
import org.eclipse.jgit.util.SystemReader;

class DeletePreservedPacksAction implements Action {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  static final String CONFIG_KEY_PRUNE_PRESERVED_EXPIRE = "prunePreservedExpire";
  private static final int PRUNE_PRESERVED_EXPIRE_SECONDS_DEFAULT = 7200; // 2 hours
  private static final String PRUNE_PRESERVED_EXPIRE_DEFAULT =
      PRUNE_PRESERVED_EXPIRE_SECONDS_DEFAULT + ".seconds.ago";

  @Override
  public ActionResult apply(String repositoryPath) {
    Path preservedDir = Path.of(repositoryPath, "objects", "pack", "preserved");

    if (!preservedDir.toFile().isDirectory()) {
      logger.atInfo().log(
          "Preserved directory %s does not exist, nothing to delete.", preservedDir);
      return new ActionResult(true);
    }

    try {
      Instant cutoff = getPrunePreservedExpireCutoff(repositoryPath);
      long deleted = deleteExpiredFiles(preservedDir, cutoff);
      logger.atInfo().log(
          "Deleted %d expired file(s) from preserved directory %s in %s repository.",
          deleted, preservedDir, repositoryPath);
      return new ActionResult(true);
    } catch (IOException e) {
      logger.atSevere().withCause(e).log(
          "Failed to delete expired files from preserved directory %s in %s repository.",
          preservedDir, repositoryPath);
      return new ActionResult(
          false, "Delete preserved packs action failed, message: " + e.getMessage());
    }
  }

  private static Instant getPrunePreservedExpireCutoff(String repositoryPath) {
    try {
      try (FileRepository repository = new FileRepository(repositoryPath)) {
        String configured =
            repository
                .getConfig()
                .getString(CONFIG_GC_SECTION, null, CONFIG_KEY_PRUNE_PRESERVED_EXPIRE);
        return GitTimeParser.parseInstant(
            MoreObjects.firstNonNull(configured, PRUNE_PRESERVED_EXPIRE_DEFAULT));
      }
    } catch (IOException | ParseException e) {
      logger.atWarning().withCause(e).log(
          "Unable to read gc.%s from git config: defaulting to %s",
          CONFIG_KEY_PRUNE_PRESERVED_EXPIRE, PRUNE_PRESERVED_EXPIRE_DEFAULT);
      return SystemReader.getInstance()
          .civilNow()
          .atZone(SystemReader.getInstance().getTimeZoneId())
          .toInstant()
          .minusSeconds(PRUNE_PRESERVED_EXPIRE_SECONDS_DEFAULT);
    }
  }

  private static long deleteExpiredFiles(Path preservedDir, Instant cutoff) throws IOException {
    long deleted = 0;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(preservedDir)) {
      for (Path file : stream) {
        if (Files.isRegularFile(file)
            && Files.getLastModifiedTime(file).toInstant().isBefore(cutoff)) {
          FileUtils.delete(file.toFile(), FileUtils.RETRY);
          logger.atFine().log("Deleted expired preserved file %s.", file);
          deleted++;
        }
      }
    }
    return deleted;
  }
}
