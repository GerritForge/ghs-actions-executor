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

import static com.gerritforge.ghs.actions.FileRepositoryFactory.create;

import com.google.common.flogger.FluentLogger;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.eclipse.jgit.internal.storage.file.FileReftableDatabase;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.internal.storage.file.RefDirectory;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.RefDatabase;

public class PackRefsAction implements Action {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  @Override
  public ActionResult apply(String repositoryPath) {
    try (FileRepository repo = create(new File(repositoryPath))) {
      RefDatabase refDb = repo.getRefDatabase();

      if (refDb instanceof FileReftableDatabase) {
        ((FileReftableDatabase) refDb).compactFully();
        return new ActionResult(true);
      }

      Collection<Ref> refs = refDb.getRefsByPrefix(Constants.R_REFS);
      List<String> refsToBePacked =
          refs.stream()
              .filter(r -> !r.isSymbolic())
              .filter(r -> r.getStorage().isLoose())
              .map(Ref::getName)
              .collect(Collectors.toList());
      ((RefDirectory) repo.getRefDatabase()).pack(refsToBePacked);
      return new ActionResult(true);
    } catch (IOException e) {
      logger.atSevere().withCause(e).log(
          "Packed refs generation failed for the repository path %s", repositoryPath);
      return new ActionResult(
          false, String.format("Packed refs generation action failed, message: %s", e.getCause()));
    }
  }
}
