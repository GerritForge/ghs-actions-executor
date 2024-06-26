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

import java.util.Optional;
import org.eclipse.jgit.annotations.Nullable;

public class ActionResult {
  private final boolean successful;
  @Nullable private final String message;

  public ActionResult(boolean isSuccessful) {
    this(isSuccessful, null);
  }

  public ActionResult(boolean isSuccessful, @Nullable String message) {
    this.successful = isSuccessful;
    this.message = message;
  }

  public boolean isSuccessful() {
    return successful;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return "ActionResult{"
        + "successful="
        + successful
        + ", message="
        + Optional.ofNullable(message).orElse("")
        + '}';
  }
}
