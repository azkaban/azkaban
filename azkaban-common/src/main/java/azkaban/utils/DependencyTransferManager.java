/*
 * Copyright 2019 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.utils;

import azkaban.Constants;
import azkaban.spi.DependencyFile;
import azkaban.spi.Storage;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static azkaban.Constants.ConfigurationKeys.*;
import static azkaban.utils.ThinArchiveUtils.*;

/**
 * Handles downloading of dependencies. Used during the thin archive upload process, and upon starting the execution
 * of a flow defined in a thin archive (to download necessary dependencies). Provides a thin layer of retry logic,
 * checksum validation and parallelism on top of the base Storage::getDependency().
 */
@Singleton
public class DependencyTransferManager {
  private static final int DEFAULT_NUM_THREADS = 32;

  public final int dependencyMaxDownloadTries;

  public final int dependencyDownloadMaxTimeout;

  private final Storage storage;

  private final ExecutorService threadPool;

  private static final Logger logger = LoggerFactory.getLogger(DependencyTransferManager.class);

  @Inject
  public DependencyTransferManager(final Props props, final Storage storage) {
    this.storage = storage;
    this.threadPool = Executors.newFixedThreadPool(
        props.getInt(AZKABAN_DEPENDENCY_DOWNLOAD_THREADPOOL_SIZE, DEFAULT_NUM_THREADS),
        new ThreadFactoryBuilder().setNameFormat("azk-dependency-pool-%d").build());
    this.dependencyMaxDownloadTries =
        props.getInt(Constants.ConfigurationKeys.AZKABAN_DEPENDENCY_MAX_DOWNLOAD_TRIES, 2);
    this.dependencyDownloadMaxTimeout = props.getInt(AZKABAN_DEPENDENCY_DOWNLOAD_TIMEOUT_SECONDS, 300);
  }

  /**
   * Return if the DependencyTransferManager will be able to download dependencies from the current
   * Storage instance.
   * @return if this class is enabled and can download dependencies.
   */
  public boolean isEnabled() { return this.storage.dependencyFetchingEnabled(); }

  private void ensureIsEnabled() {
    if (!isEnabled()) {
      throw new UnsupportedOperationException("Thin archive support is not enabled!");
    }
  }

  /**
   * downloads a set of dependencies from an origin. Each downloaded dependency is stored in the file returned by
   * DependencyFile::getFile.
   *
   * @param deps        set of DependencyFile to download
   * @param projectName name of the project for which the dependencies are being downloaded
   */
  public void downloadAllDependencies(final Set<DependencyFile> deps, String projectName) {
    if (deps.isEmpty()) {
      // Nothing for us to do!
      return;
    }
    long startTime = System.currentTimeMillis();

    ensureIsEnabled();

    CompletableFuture[] taskFutures = deps
        .stream()
        .map(f -> CompletableFuture.runAsync(() -> downloadDependency(f, projectName), threadPool))
        .toArray(CompletableFuture[]::new);

    try {
      waitForAllToSucceedOrOneToFail(taskFutures);
      logger.info("Time taken to download all thin archive dependencies for project {} in {} ms.",
          projectName, System.currentTimeMillis() - startTime);
    } catch (InterruptedException e) {
      // No point in continuing, let's stop any future downloads and try to interrupt currently running ones.
      logger.error("Download interrupted. Cancelling all pending downloads for project {}.", projectName);
      cancelPendingTasks(taskFutures, projectName);
      throw new DependencyTransferException("Download interrupted.", e);
    } catch (ExecutionException e) {
      // ^^^ see above comment ^^^
      logger.error("Download failed. Cancelling all pending downloads for project {}.", projectName);
      cancelPendingTasks(taskFutures, projectName);
      if (e.getCause() instanceof DependencyTransferException) {
        throw (DependencyTransferException) e.getCause();
      }
      throw new RuntimeException(e.getCause());
    } catch (TimeoutException e) {
      logger.error("Timed out while downloading dependencies for project {}.", projectName, e);
      cancelPendingTasks(taskFutures, projectName);
      throw new DependencyTransferException("Timed out while downloading dependencies.", e);
    }
  }

  private void downloadDependency(final DependencyFile f, String projectName) {
    try {
      downloadDependency(f, 0, projectName);
      logger.info("Successfully downloaded dependency {} for project {}.", f.getFileName(), projectName);
    } catch (IOException e) {
      logger.error("Error while downloading dependency {} for project {}.", f.getFileName(), projectName, e);
      throw new DependencyTransferException("Error while downloading dependency " + f.getFileName(), e);
    } catch (HashNotMatchException e) {
      logger.error("Checksum did not match when downloading dependency {} for project {}.", f.getFileName(), projectName, e);
      throw new DependencyTransferException("Checksum did not match when downloading dependency " + f.getFileName(), e);
    }
  }

  /* Cancel all the not-started tasks which are possibly waiting in the queue */
  private static void cancelPendingTasks(CompletableFuture[] taskFutures, String projectName) {
    logger.info("cancelling the pending tasks for project {} because one of the downloads failed.", projectName);
    boolean cancelled = true;
    for (CompletableFuture future : taskFutures) {
      cancelled &= future.cancel(false);
    }
    logger.info("{} cancel the pending tasks for project {}.", cancelled ? "Successfully" : "Failed to", projectName);
  }

  private void downloadDependency(final DependencyFile f, final int retries, String projectName)
      throws HashNotMatchException, IOException {
    FileOutputStream outputStream = null;
    InputStream inputStream = null;
    try {
      // Make any necessary directories
      f.getFile().getParentFile().mkdirs();

      outputStream = new FileOutputStream(f.getFile());
      inputStream = this.storage.getDependency(f);
      IOUtils.copy(inputStream, outputStream);
    } catch (IOException e) {
      if (retries + 1 < dependencyMaxDownloadTries) {
        // downloadDependency will overwrite our destination file if attempted again
        exponentialBackoffDelay(retries);
        downloadDependency(f, retries + 1, projectName);
        return;
      }
      logger.error("Failed to download dependency file {} for project {} with {} max reties.",
          f.getFileName(), projectName, dependencyMaxDownloadTries, e);
      throw e;
    } finally {
      IOUtils.closeQuietly(inputStream);
      IOUtils.closeQuietly(outputStream);
    }

    try {
      validateDependencyHash(f);
    } catch (HashNotMatchException e) {
      if (retries + 1 < dependencyMaxDownloadTries) {
        // downloadDependency will overwrite our destination file if attempted again
        exponentialBackoffDelay(retries);
        downloadDependency(f, retries + 1, projectName);
        return;
      }
      logger.error("Failed to complete hash validation for dependency file {} for project {} with {} max download reties."
          + " Failing Download", f.getFileName(), projectName, dependencyMaxDownloadTries);
      throw e;
    }
  }

  private static void exponentialBackoffDelay(final int retries) {
    try {
      // Will wait for 1, 2, 4, 8... seconds
      Thread.sleep((long) (Math.pow(2, retries) * 1000));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Add timeout to the download process.
   * @throws TimeoutException, if the download process takes longer than the timeout.
   * */
  private void waitForAllToSucceedOrOneToFail(final CompletableFuture<?>[] futures)
      throws InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<?> failure = new CompletableFuture();
    for (CompletableFuture<?> f : futures) {
      // f = f is redundant, but bug checker throws error if we don't do it because it doesn't like us ignoring a
      // returned future...but we're still going to ignore it.
      f = f.exceptionally(ex -> {
        logger.error("Download failed with an exception: " + ex);
        failure.completeExceptionally(ex);
        return null;
      });
    }
    // Wait for either the failure future to complete, or all of the actual futures to complete.
    CompletableFuture.anyOf(failure, CompletableFuture.allOf(futures)).get(dependencyDownloadMaxTimeout, TimeUnit.SECONDS);
  }
}
