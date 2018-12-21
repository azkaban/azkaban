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

package azkaban.executor;

import azkaban.executor.selector.ExecutionControllerUtils;
import javax.annotation.Nullable;
import javax.inject.Inject;
import org.apache.log4j.Logger;

/**
 * Handles removing of running executions (after they have been deemed to be be done or orphaned).
 */
public class ExecutionFinalizer {

  private static final Logger logger = Logger.getLogger(ExecutionFinalizer.class);

  private final ExecutorLoader executorLoader;
  private final ExecutorManagerUpdaterStage updaterStage;
  private final AlerterHolder alerterHolder;
  private final RunningExecutions runningExecutions;

  @Inject
  public ExecutionFinalizer(final ExecutorLoader executorLoader,
      final ExecutorManagerUpdaterStage updaterStage,
      final AlerterHolder alerterHolder, final RunningExecutions runningExecutions) {
    this.executorLoader = executorLoader;
    this.updaterStage = updaterStage;
    this.alerterHolder = alerterHolder;
    this.runningExecutions = runningExecutions;
  }

  /**
   * If the current status of the execution is not one of the finished statuses, marks the execution
   * as failed in the DB. Removes the execution from the running executions cache.
   *
   * @param flow the execution
   * @param reason reason for finalizing the execution
   * @param originalError the cause, if execution is being finalized because of an error
   */
  public void finalizeFlow(final ExecutableFlow flow, final String reason,
      @Nullable final Throwable originalError) {

    final int execId = flow.getExecutionId();
    boolean alertUser = true;
    this.updaterStage.set("finalizing flow " + execId);
    // First we check if the execution in the datastore is complete
    try {
      final ExecutableFlow dsFlow;
      if (ExecutionControllerUtils.isFinished(flow)) {
        dsFlow = flow;
      } else {
        this.updaterStage.set("finalizing flow " + execId + " loading from db");
        dsFlow = this.executorLoader.fetchExecutableFlow(execId);

        // If it's marked finished, we're good. If not, we fail everything and
        // then mark it finished.
        if (!ExecutionControllerUtils.isFinished(dsFlow)) {
          this.updaterStage.set("finalizing flow " + execId + " failing the flow");
          ExecutionControllerUtils.failEverything(dsFlow);
          this.executorLoader.updateExecutableFlow(dsFlow);
        }
      }

      this.updaterStage.set("finalizing flow " + execId + " deleting active reference");

      // Delete the executing reference.
      if (flow.getEndTime() == -1) {
        flow.setEndTime(System.currentTimeMillis());
        this.executorLoader.updateExecutableFlow(dsFlow);
      }
      this.executorLoader.removeActiveExecutableReference(execId);

      this.updaterStage.set("finalizing flow " + execId + " cleaning from memory");
      this.runningExecutions.get().remove(execId);
    } catch (final ExecutorManagerException e) {
      alertUser = false; // failed due to azkaban internal error, not to alert user
      logger.error(e);
    }

    // TODO append to the flow log that we marked this flow as failed + the extraReasons

    this.updaterStage.set("finalizing flow " + execId + " alerting and emailing");
    if (alertUser) {
      ExecutionControllerUtils.alertUser(flow, this.alerterHolder,
          ExecutionControllerUtils.getFinalizeFlowReasons(reason,
              originalError));
    }
  }

}
