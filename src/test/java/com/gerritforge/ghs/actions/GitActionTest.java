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

import static com.gerritforge.ghs.actions.PreserveOutdatedBitmapsAction.getMostRecentExistingBitmap;
import static com.google.common.truth.Truth.assertThat;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.internal.storage.file.PackFile;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.storage.file.FileBasedConfig;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.RefSpec;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TestName;

@Ignore
public abstract class GitActionTest {
  protected static final Path TEST_TMP_PREFIX = Path.of("/tmp");
  protected static final String TEST_AUTHOR = "Test Author";
  protected static final String TEST_AUTHOR_EMAIL = "test@author.com";
  protected static final String TEST_COMMIT_MESSAGE = "Test commit message";
  protected static final String TEST_BRANCH = "test-branch";
  public static final String ALL_REFS_SPEC = "refs/*:refs/*";

  @Rule public TestName name = new TestName();
  protected Path testRepoPath;
  protected FileRepository repo;
  protected Git testRepoGit;
  protected String gitRepoUri;
  protected Path objectsPath;
  protected Path packPath;
  protected Path preservedPath;
  protected Path bitmapsLogPath;

  @Before
  public void setup() throws Exception {
    testRepoPath = Files.createTempDirectory(TEST_TMP_PREFIX, sanitisedTestName());
    gitRepoUri = "file://" + testRepoPath;

    testRepoGit = Git.init().setBare(true).setDirectory(testRepoPath.toFile()).call();

    FileRepositoryBuilder repositoryBuilder =
        new FileRepositoryBuilder()
            .setGitDir(new File(testRepoPath.toString()))
            .readEnvironment()
            .findGitDir();
    repo = (FileRepository) repositoryBuilder.build();

    objectsPath = testRepoPath.resolve("objects");
    packPath = objectsPath.resolve("pack");
    preservedPath = packPath.resolve("preserved");
    bitmapsLogPath = packPath.resolve(".ghs-packs.log");
  }

  @After
  public void teardown() throws Exception {
    testRepoGit.close();
  }

  protected void setPrunePackExpire(String prunePackExpire) throws IOException {
    FileBasedConfig repoConfig = repo.getConfig();
    repoConfig.setString("gc", null, "prunePackExpire", prunePackExpire);
    repoConfig.save();
  }

  protected String sanitisedTestName() {
    return this.getClass().getSimpleName() + "_" + name.getMethodName();
  }

  protected Ref pushNewCommitToBranch() throws GitAPIException, IOException {
    Path testCloneRepoPath = Files.createTempDirectory(TEST_TMP_PREFIX, sanitisedTestName());
    Git git =
        Git.cloneRepository().setURI(gitRepoUri).setDirectory(testCloneRepoPath.toFile()).call();
    RevCommit commit =
        git.commit()
            .setAuthor(TEST_AUTHOR, TEST_AUTHOR_EMAIL)
            .setMessage(TEST_COMMIT_MESSAGE)
            .call();
    Ref branch = git.branchCreate().setName(TEST_BRANCH).setStartPoint(commit.getName()).call();
    git.push().setForce(true).setRefSpecs(new RefSpec(ALL_REFS_SPEC)).setRemote(gitRepoUri).call();
    return branch;
  }

  protected void ensureBitmapsLogContainsExactly(List<ObjectId> entries) throws IOException {
    deleteGHSLog();
    Path logPath = BitmapGenerationLog.logPath(testRepoPath.toString());

    BitmapGenerationLog.update(objectsPath, entries);

    assertThat(Files.exists(logPath)).isTrue();
    assertThat(BitmapGenerationLog.readAllEntriesFromLog(logPath))
        .containsExactlyElementsIn(entries);
  }

  protected void assertBitmapsLogContainsOnly(String entry) throws IOException {
    assertThat(BitmapGenerationLog.readAllEntriesFromLog(bitmapsLogPath))
        .containsExactly(ObjectId.fromString(entry));
  }

  protected void deleteGHSLog() throws IOException {
    Files.deleteIfExists(BitmapGenerationLog.logPath(testRepoPath.toString()));
  }

  protected ObjectId getMostRecentBitmapPackId() throws IOException {
    Path mostRecentExistingBitmap = getMostRecentExistingBitmap(packPath);
    assertThat(mostRecentExistingBitmap).isNotNull();
    return ObjectId.fromString(new PackFile(mostRecentExistingBitmap.toFile()).getId());
  }

  protected PackFile findMostRecentPack() throws IOException {
    try (Stream<Path> s = Files.list(packPath)) {
      return s.filter(p -> p.toString().endsWith(".pack"))
          .max(Comparator.comparingLong(p -> p.toFile().lastModified()))
          .map(p -> new PackFile(p.toFile()))
          .orElseThrow();
    }
  }

  @CanIgnoreReturnValue
  protected PackFile pushAndGenerateNewBitmap() throws GitAPIException, IOException {
    pushNewCommitToBranch();
    assertThat(new BitmapGenerationAction().apply(testRepoPath.toString()).isSuccessful()).isTrue();
    return findMostRecentPack();
  }
}
