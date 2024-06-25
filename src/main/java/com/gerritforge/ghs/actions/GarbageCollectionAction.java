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
import java.text.ParseException;
import java.util.concurrent.ExecutionException;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.internal.storage.file.GC;
import org.eclipse.jgit.lib.NullProgressMonitor;
import org.eclipse.jgit.lib.TextProgressMonitor;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.storage.pack.PackConfig;

public class GarbageCollectionAction implements Action {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  @Override
  public ActionResult apply(String repositoryPath) {
    FileRepositoryBuilder repositoryBuilder =
        new FileRepositoryBuilder()
            .setGitDir(new File(repositoryPath))
            .readEnvironment()
            .findGitDir();

    try (FileRepository repository = (FileRepository) repositoryBuilder.build()) {
      runGarbageCollection(repository);
    } catch (IOException | ParseException | ExecutionException | InterruptedException e) {
      logger.atSevere().withCause(e).log(
          "Garbage collection action failed for the repository path %s", repositoryPath);
      return new ActionResult(
          false, String.format("Garbage collection action failed, message: %s", e.getCause()));
    }

    return new ActionResult(true);
  }

  private void runGarbageCollection(FileRepository repo)
      throws IOException, ParseException, ExecutionException, InterruptedException {
    GC gc = new GC(repo);
    gc.setPackConfig(new PackConfig(repo));
    gc.setProgressMonitor(isVerbose() ? new TextProgressMonitor() : NullProgressMonitor.INSTANCE);
    gc.gc().get();
  }
}
