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

import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import azkaban.spi.FileIOStatus;
import azkaban.spi.FileOrigin;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.storage.LocalStorage;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import static azkaban.Constants.ConfigurationKeys.*;
import static azkaban.utils.ThinArchiveUtils.*;

/**
 * handles uploading and downloading of dependencies from/to specified FileOrigin origins. Used during the thin archive
 * upload process, and upon starting the execution of a flow defined in a thin archive (to download necessary
 * dependencies from STORAGE)
 */
public class DependencyTransferManager {
  public static final int MAX_DEPENDENCY_DOWNLOAD_TRIES = 2;

  private static final int NUM_THREADS = 8;

  private static final int REMOTE_DOWNLOAD_READ_TIMEOUT = 5 * 60; // 5 minute timeout
  private static final int REMOTE_DOWNLOAD_CONNECTION_TIMEOUT = 10; // 10 second timeout

  private final Props props;
  private final Storage storage;

  private final boolean storageEnabled;
  private final boolean remoteEnabled;

  @Inject
  public DependencyTransferManager(final Props props, final Storage storage) {
    this.props = props;
    this.storage = storage;

    // Make sure the necessary azkaban properties were set to enable the respective origins
    this.storageEnabled = props.containsKey(AZKABAN_STORAGE_HDFS_DEPENDENCY_ROOT_URI) || storage instanceof LocalStorage;
    this.remoteEnabled = props.containsKey(AZKABAN_STARTUP_DEPENDENCIES_REMOTE_DOWNLOAD_BASE_URL);
  }

  public boolean storageOriginEnabled() {
    return this.storageEnabled;
  }

  public boolean remoteOriginEnabled() {
    return this.remoteEnabled;
  }

  private void ensureOriginIsEnabled(FileOrigin o) {
    if ((o == FileOrigin.STORAGE && !this.storageEnabled) || (o == FileOrigin.REMOTE && !this.remoteEnabled)) {
      throw new UnsupportedOperationException("Thin archive support is not enabled for origin " + o.name());
    }
  }

  /**
   * uploads a set of dependencies to an origin and returns the subset of dependencies that are guaranteed
   * to be successfully persisted to that origin.
   *
   * @param deps set of DependencyFile to upload
   * @param origin where to upload the dependency to, currently only supports FileOrigin.STORAGE
   *
   * @return Set of DependencyFile that are guaranteed to be successfully persisted to storage
   * (i.e. they have a file status of FileIOStatus.CLOSED)
   */
  public Set<DependencyFile> uploadAllDependencies(final Set<DependencyFile> deps, final FileOrigin origin) {
    if (deps.size() == 0) {
      // Nothing for us to do! Just return the empty set.
      return deps;
    }

    ensureOriginIsEnabled(origin);

    if (origin != FileOrigin.STORAGE) {
      throw new UnsupportedOperationException("Invalid upload origin. Can only upload to STORAGE!");
    }

    ExecutorService threadPool = Executors.newFixedThreadPool(NUM_THREADS);
    Map<DependencyFile, CompletableFuture> taskFutures = deps
        .stream()
        .collect(Collectors.toMap(
            f -> f,
            f -> CompletableFuture.supplyAsync(() -> uploadDependencyToStorage(f), threadPool)));

    try {
      waitForAllToSucceedOrOneToFail(taskFutures.values().stream().toArray(CompletableFuture[]::new));
    } catch (InterruptedException e) {
      // We use .shutdown() instead of .shutdownNow() because we're fine letting currently running uploads
      // finish. We just don't want to start any new uploads.
      threadPool.shutdown();
      throw new DependencyTransferException("Upload interrupted.", e);
    } catch (ExecutionException e) {
      // ^^^ see above comment ^^^
      threadPool.shutdown();
      throw (DependencyTransferException) e.getCause();
    }

    return taskFutures
        .keySet()
        .stream()
        .filter(d -> (Boolean) getFromFuture(taskFutures.get(d)))
        .collect(Collectors.toSet());
  }

  private boolean uploadDependencyToStorage(final DependencyFile f) {
    try {
      // We return true if the dependency has a status of closed,
      // so we are guaranteed that it persisted successfully to storage.
      return this.storage.putDependency(f) == FileIOStatus.CLOSED;
    } catch (IOException | StorageException e) {
      throw new DependencyTransferException("Error while uploading dependency " + f.getFileName(), e);
    }
  }

  /**
   * downloads a set of dependencies from an origin. Each downloaded dependency is stored in the file
   * returned by DependencyFile::getFile.
   *
   * @param deps set of DependencyFile to upload
   * @param origin where to upload the dependency to, currently only supports FileOrigin.STORAGE
   *
   * @return Set of DependencyFile that are guaranteed to be successfully persisted to storage
   * (i.e. they have a file status of FileIOStatus.CLOSED)
   */
  public void downloadAllDependencies(final Set<DependencyFile> deps, final FileOrigin origin) {
    if (deps.size() == 0) {
      // Nothing for us to do!
      return;
    }

    ensureOriginIsEnabled(origin);

    ExecutorService threadPool = Executors.newFixedThreadPool(NUM_THREADS);
    CompletableFuture[] taskFutures = deps
        .stream()
        .map(f -> CompletableFuture.runAsync(() -> downloadDependency(f, origin), threadPool))
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
      throw (DependencyTransferException) e.getCause();
    }
  }

  private void downloadDependency(final DependencyFile f, final FileOrigin origin) {
    try {
      downloadDependency(f, origin, 0);
    } catch (IOException e) {
      throw new DependencyTransferException("Error while downloading dependency " + f.getFileName(), e);
    } catch (HashNotMatchException e) {
      throw new DependencyTransferException("Checksum did not match when downloading dependency " + f.getFileName(), e);
    }
  }

  private void downloadDependency(final DependencyFile f, final FileOrigin origin, final int retries)
      throws HashNotMatchException, IOException {
    try {
      // Make any necessary directories
      f.getFile().getParentFile().mkdirs();

      if (origin == FileOrigin.REMOTE) {
        FileUtils.copyURLToFile(
            getUrlForDependency(f),
            f.getFile(),
            REMOTE_DOWNLOAD_CONNECTION_TIMEOUT * 1000,
            REMOTE_DOWNLOAD_READ_TIMEOUT * 1000);
      } else if (origin == FileOrigin.STORAGE) {
        FileOutputStream fos = new FileOutputStream(f.getFile());
        IOUtils.copy(this.storage.getDependency(f), fos);
      } else {
        throw new RuntimeException("Unrecognized origin type for dependency download: " + origin.toString());
      }
    } catch (IOException e) {
      if (retries + 1 < MAX_DEPENDENCY_DOWNLOAD_TRIES) {
        // downloadDependency will overwrite our destination file if attempted again
        downloadDependency(f, origin, retries + 1);
        return;
      }
      throw e;
    }

    try {
      validateDependencyHash(f);
    } catch (HashNotMatchException e) {
      if (retries + 1 < MAX_DEPENDENCY_DOWNLOAD_TRIES) {
        // downloadDependency will overwrite our destination file if attempted again
        downloadDependency(f, origin, retries + 1);
        return;
      }
      throw e;
    }
  }

  private URL getUrlForDependency(final Dependency d) throws MalformedURLException {
    return new URL(
        new URL(this.props.getString(AZKABAN_STARTUP_DEPENDENCIES_REMOTE_DOWNLOAD_BASE_URL)),
        convertIvyCoordinateToPath(d));
  }

  // Helper to get object from Future while converting checked exceptions to RuntimeExceptions
  private static Object getFromFuture(final Future f) {
    try {
      return f.get();
    } catch (Exception e) {
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
