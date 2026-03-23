// Copyright (C) 2026 GerritForge, Inc.
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
import java.io.IOException;
import java.nio.file.Path;
import org.eclipse.jgit.util.FileUtils;

class DeletePreservedPacksAction implements Action {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  @Override
  public ActionResult apply(String repositoryPath) {
    Path preservedDir = Path.of(repositoryPath, "objects", "pack", "preserved");

    try {
      FileUtils.delete(preservedDir.toFile(), FileUtils.RECURSIVE | FileUtils.RETRY |  FileUtils.SKIP_MISSING);
      logger.atInfo().log(
          "Deleted preserved directory %s in %s repository.", preservedDir, repositoryPath);
      return new ActionResult(true);
    } catch (IOException e) {
      logger.atSevere().withCause(e).log(
          "Failed to delete preserved directory %s in %s repository.", preservedDir, repositoryPath);
      return new ActionResult(
          false, "Delete preserved packs action failed, message: " + e.getMessage());
    }
  }
}
