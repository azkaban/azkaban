/*
 * Copyright 2018 LinkedIn Corp.
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

package azkaban.dag;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread safe and non blocking service for DAG processing.
 *
 * <p>Since only one thread is used to progress the DAG, thread synchronization is avoided.
 */
@SuppressWarnings("FutureReturnValueIgnored")
@Singleton
class DagService {

  private static final long SHUTDOWN_WAIT_TIMEOUT = 60;
  private static final Logger logger = LoggerFactory.getLogger(DagService.class);

  private final ExecutorService executorService;

  DagService() {
    // Give the thread a name to make debugging easier.
    final ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("Dag-service").build();
    this.executorService = Executors.newSingleThreadExecutor(namedThreadFactory);
  }

  void startDag(final Dag dag) {
    this.executorService.submit(dag::start);
  }

  /**
   * Transitions the node to the success state.
   */
  void markNodeSuccess(final Node node) {
    this.executorService.submit(node::markSuccess);
  }

  /**
   * Transitions the node from the killing state to the killed state.
   */
  void markNodeKilled(final Node node) {
    this.executorService.submit(node::markKilled);
  }

  /**
   * Transitions the node to the failure state.
   */
  void markNodeFailed(final Node node) {
    this.executorService.submit(node::markFailed);
  }

  /**
   * Kills a Dag.
   */
  void killDag(final Dag dag) {
    this.executorService.submit(dag::kill);
  }

  /**
   * Shuts down the service and wait for the tasks to finish.
   *
   * <p>Adopted from
   * <a href="https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html">
   *   the Oracle JAVA Documentation.
   * </a>
   */
  void shutdownAndAwaitTermination() {
    this.executorService.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!this.executorService.awaitTermination(SHUTDOWN_WAIT_TIMEOUT, TimeUnit.SECONDS)) {
        this.executorService.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!this.executorService.awaitTermination(SHUTDOWN_WAIT_TIMEOUT, TimeUnit.SECONDS)) {
          logger.error("The DagService did not terminate.");
        }
      }
    } catch (final InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      this.executorService.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }
}
