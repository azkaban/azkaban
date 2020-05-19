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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.io.IOUtils;

import static azkaban.utils.ThinArchiveUtils.*;

/**
 * Handles downloading of dependencies. Used during the thin archive upload process, and upon starting the execution
 * of a flow defined in a thin archive (to download necessary dependencies). Provides a thin layer of retry logic,
 * checksum validation and parallelism on top of the base Storage::getDependency().
 */
@Singleton
public class DependencyTransferManager {
  private static final int NUM_THREADS = 8;

  public final int dependencyMaxDownloadTries;

  private final Storage storage;

  @Inject
  public DependencyTransferManager(final Props props, final Storage storage) {
    this.storage = storage;

    this.dependencyMaxDownloadTries =
        props.getInt(Constants.ConfigurationKeys.AZKABAN_DEPENDENCY_MAX_DOWNLOAD_TRIES, 2);
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
   * downloads a set of dependencies from an origin. Each downloaded dependency is stored in the file
   * returned by DependencyFile::getFile.
   *
   * @param deps set of DependencyFile to download
   */
  public void downloadAllDependencies(final Set<DependencyFile> deps) {
    if (deps.isEmpty()) {
      // Nothing for us to do!
      return;
    }

    ensureIsEnabled();

    ExecutorService threadPool = Executors.newFixedThreadPool(NUM_THREADS,
        new ThreadFactoryBuilder().setNameFormat("azk-dependency-pool-%d").build());
    CompletableFuture[] taskFutures = deps
        .stream()
        .map(f -> CompletableFuture.runAsync(() -> downloadDependency(f), threadPool))
        .toArray(CompletableFuture[]::new);

    try {
      waitForAllToSucceedOrOneToFail(taskFutures);
    } catch (InterruptedException e) {
      // No point in continuing, let's stop any future downloads and try to interrupt currently running ones.
      threadPool.shutdownNow();
      throw new DependencyTransferException("Download interrupted.", e);
    } catch (ExecutionException e) {
      // ^^^ see above comment ^^^
      threadPool.shutdownNow();
      if (e.getCause() instanceof DependencyTransferException) {
        throw (DependencyTransferException) e.getCause();
      }
      throw new RuntimeException(e.getCause());
    }
  }

  private void downloadDependency(final DependencyFile f) {
    try {
      downloadDependency(f, 0);
    } catch (IOException e) {
      throw new DependencyTransferException("Error while downloading dependency " + f.getFileName(), e);
    } catch (HashNotMatchException e) {
      throw new DependencyTransferException("Checksum did not match when downloading dependency " + f.getFileName(), e);
    }
  }

  private void downloadDependency(final DependencyFile f, final int retries)
      throws HashNotMatchException, IOException {
    try {
      // Make any necessary directories
      f.getFile().getParentFile().mkdirs();

      FileOutputStream fos = new FileOutputStream(f.getFile());
      IOUtils.copy(this.storage.getDependency(f), fos);
    } catch (IOException e) {
      if (retries + 1 < dependencyMaxDownloadTries) {
        // downloadDependency will overwrite our destination file if attempted again
        exponentialBackoffDelay(retries);
        downloadDependency(f, retries + 1);
        return;
      }
      throw e;
    }

    try {
      validateDependencyHash(f);
    } catch (HashNotMatchException e) {
      if (retries + 1 < dependencyMaxDownloadTries) {
        // downloadDependency will overwrite our destination file if attempted again
        exponentialBackoffDelay(retries);
        downloadDependency(f, retries + 1);
        return;
      }
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

  private static void waitForAllToSucceedOrOneToFail(final CompletableFuture<?>[] futures)
      throws InterruptedException, ExecutionException {
    CompletableFuture<?> failure = new CompletableFuture();
    for (CompletableFuture<?> f : futures) {
      // f = f is redundant, but bug checker throws error if we don't do it because it doesn't like us ignoring a
      // returned future...but we're still going to ignore it.
      f = f.exceptionally(ex -> {
        failure.completeExceptionally(ex);
        return null;
      });
    }
    // Wait for either the failure future to complete, or all of the actual futures to complete.
    CompletableFuture.anyOf(failure, CompletableFuture.allOf(futures)).get();
  }
}
