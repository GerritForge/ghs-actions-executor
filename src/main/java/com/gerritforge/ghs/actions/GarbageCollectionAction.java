// Copyright (c) 2024 GERRITFORGE, INC. All rights reserved.

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

import com.google.common.flogger.FluentLogger;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.ExecutionException;
import org.eclipse.jgit.api.errors.GitAPIException;
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
    } catch (IOException
        | ParseException
        | ExecutionException
        | InterruptedException
        | GitAPIException e) {
      logger.atSevere().withCause(e).log(
          "Garbage collection action failed for the repository path %s", repositoryPath);
      return new ActionResult(
          false, String.format("Garbage collection action failed, message: %s", e.getCause()));
    }

    return new ActionResult(true);
  }

  private void runGarbageCollection(FileRepository repo)
      throws IOException,
          ParseException,
          ExecutionException,
          InterruptedException,
          GitAPIException {
    GC gc = new GC(repo);
    gc.setPackConfig(new PackConfig(repo));
    gc.setProgressMonitor(isVerbose() ? new TextProgressMonitor() : NullProgressMonitor.INSTANCE);
    gc.gc().get();
  }
}
