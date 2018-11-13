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

import azkaban.utils.ExecutorServiceUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread safe and non blocking service for DAG processing.
 *
 * <p>Allow external inputs to be given to a dag or node to allow the dag to transition states
 * . Since only one thread is used to progress the DAG, thread synchronization is avoided.
 */
@SuppressWarnings("FutureReturnValueIgnored")
@Singleton
public class DagService {

  private static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.ofSeconds(10);
  private static final Logger logger = LoggerFactory.getLogger(DagService.class);

  private final ExecutorServiceUtils executorServiceUtils;
  private final ExecutorService executorService;

  @Inject
  public DagService(final ExecutorServiceUtils executorServiceUtils) {
    // Give the thread a name to make debugging easier.
    this.executorServiceUtils = executorServiceUtils;
    final ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("Dag-service").build();
    this.executorService = Executors.newSingleThreadExecutor(namedThreadFactory);
  }

  public void startDag(final Dag dag) {
    this.executorService.submit(dag::start);
  }

  /**
   * Transitions the node to the success state.
   */
  public void markNodeSuccess(final Node node) {
    this.executorService.submit(node::markSuccess);
  }

  /**
   * Transitions the node from the killing state to the killed state.
   */
  public void markNodeKilled(final Node node) {
    this.executorService.submit(node::markKilled);
  }

  /**
   * Transitions the node to the failure state.
   */
  public void markNodeFailed(final Node node) {
    this.executorService.submit(node::markFailed);
  }

  /**
   * Kills a DAG.
   */
  public void killDag(final Dag dag) {
    this.executorService.submit(dag::kill);
  }

  /**
   * Shuts down the service and waits for the tasks to finish.
   */
  public void shutdownAndAwaitTermination() throws InterruptedException {
    logger.info("DagService is shutting down.");
    this.executorServiceUtils.gracefulShutdown(this.executorService, SHUTDOWN_WAIT_TIMEOUT);
  }

  @VisibleForTesting
  ExecutorService getExecutorService() {
    return this.executorService;
  }
}
