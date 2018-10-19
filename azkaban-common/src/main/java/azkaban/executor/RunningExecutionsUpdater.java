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

import azkaban.alert.Alerter;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

/**
 * Updates running executions.
 */
public class RunningExecutionsUpdater {

  private static final Logger logger = Logger.getLogger(RunningExecutionsUpdater.class);

  // TODO this is a temporary property until problems have been fixed - not adding it to Constants
  // Default is true. If set to false, executions on unresponsive executors are not evicted and
  // email alerts are sent instead
  static final String EVICT_UNRESPONSIVE_EXECUTIONS_KEY = "azkaban.evictUnresponsiveExecutions";

  // When we have an http error, for that flow, we'll check every 10 secs (errorThreshold), 360
  // times (3600 seconds = 1 hour) before we send an email about unresponsive executor.
  private final int numErrorsBetweenUnresponsiveEmail = 360;
  // After 1 minute of unresponsiveness the execution is either evicted or the first email is sent
  final int numErrorsBeforeUnresponsive = 6;
  final long errorThreshold = 10000;
  private final ExecutorManagerUpdaterStage updaterStage;
  private final AlerterHolder alerterHolder;
  private final CommonMetrics commonMetrics;
  private final ExecutorApiGateway apiGateway;
  private final RunningExecutions runningExecutions;
  private final ExecutionFinalizer executionFinalizer;
  private final Props props;

  @Inject
  public RunningExecutionsUpdater(final ExecutorManagerUpdaterStage updaterStage,
      final AlerterHolder alerterHolder, final CommonMetrics commonMetrics,
      final ExecutorApiGateway apiGateway, final RunningExecutions runningExecutions,
      final ExecutionFinalizer executionFinalizer, final Props props) {
    this.updaterStage = updaterStage;
    this.alerterHolder = alerterHolder;
    this.commonMetrics = commonMetrics;
    this.apiGateway = apiGateway;
    this.runningExecutions = runningExecutions;
    this.executionFinalizer = executionFinalizer;
    this.props = props;
  }

  /**
   * Updates running executions.
   */
  @SuppressWarnings("unchecked")
  public void updateExecutions() {
    this.updaterStage.set("Starting update all flows.");
    final Map<Optional<Executor>, List<ExecutableFlow>> exFlowMap = getFlowToExecutorMap();
    final ArrayList<ExecutableFlow> finalizeFlows =
        new ArrayList<>();

    for (final Map.Entry<Optional<Executor>, List<ExecutableFlow>> entry : exFlowMap
        .entrySet()) {

      final Optional<Executor> executorOption = entry.getKey();
      if (!executorOption.isPresent()) {
        for (final ExecutableFlow flow : entry.getValue()) {
          logger.warn("Finalizing execution " + flow.getExecutionId()
              + ". Executor id of this execution doesn't exist");
          finalizeFlows.add(flow);
        }
        continue;
      }
      final Executor executor = executorOption.get();

      this.updaterStage.set("Starting update flows on " + executor.getHost() + ":"
          + executor.getPort());

      Map<String, Object> results = null;
      try {
        results = this.apiGateway.updateExecutions(executor, entry.getValue());
      } catch (final ExecutorManagerException e) {
        handleException(entry, executor, e);
      }

      if (results != null) {
        final List<Map<String, Object>> executionUpdates =
            (List<Map<String, Object>>) results
                .get(ConnectorParams.RESPONSE_UPDATED_FLOWS);
        for (final Map<String, Object> updateMap : executionUpdates) {
          try {
            final ExecutableFlow flow = updateExecution(updateMap);

            this.updaterStage.set("Updated flow " + flow.getExecutionId());

            if (ExecutorManager.isFinished(flow)) {
              finalizeFlows.add(flow);
            }
          } catch (final ExecutorManagerException e) {
            final ExecutableFlow flow = e.getExecutableFlow();
            logger.error(e);

            if (flow != null) {
              logger.warn("Finalizing execution " + flow.getExecutionId());
              finalizeFlows.add(flow);
            }
          }
        }
      }
    }

    this.updaterStage.set("Finalizing " + finalizeFlows.size() + " error flows.");

    for (final ExecutableFlow flow : finalizeFlows) {
      this.executionFinalizer
          .finalizeFlow(flow, "Not running on the assigned executor (any more)", null);
    }

    this.updaterStage.set("Updated all active flows. Waiting for next round.");
  }

  private void handleException(final Entry<Optional<Executor>, List<ExecutableFlow>> entry,
      final Executor executor, final ExecutorManagerException e) {
    logger.error("Failed to get update from executor " + executor.getHost(), e);
    boolean sendUnresponsiveEmail = false;
    for (final ExecutableFlow flow : entry.getValue()) {
      final Pair<ExecutionReference, ExecutableFlow> pair =
          this.runningExecutions.get().get(flow.getExecutionId());
      // TODO can runningFlows.get ever return null, causing NPE below?

      this.updaterStage
          .set("Failed to get update for flow " + pair.getSecond().getExecutionId());

      final ExecutionReference ref = pair.getFirst();
      ref.setNextCheckTime(DateTime.now().getMillis() + this.errorThreshold);
      ref.setNumErrors(ref.getNumErrors() + 1);

      if (this.props.getBoolean(EVICT_UNRESPONSIVE_EXECUTIONS_KEY, true)) {
        if (ref.getNumErrors() >= this.numErrorsBeforeUnresponsive) {
          this.executionFinalizer
              .finalizeFlow(flow, "Evicting execution because executor is unresponsive", e);
        }
      } else if (ref.getNumErrors() == this.numErrorsBeforeUnresponsive
          || ref.getNumErrors() % this.numErrorsBetweenUnresponsiveEmail == 0) {
        // if any of the executions has failed many enough updates, alert
        sendUnresponsiveEmail = true;
      }
    }
    if (sendUnresponsiveEmail) {
      final Alerter mailAlerter = this.alerterHolder.get("email");
      mailAlerter.alertOnFailedUpdate(executor, entry.getValue(), e);
    }
  }

  /* Group Executable flow by Executors to reduce number of REST calls */
  private Map<Optional<Executor>, List<ExecutableFlow>> getFlowToExecutorMap() {
    final HashMap<Optional<Executor>, List<ExecutableFlow>> exFlowMap =
        new HashMap<>();

    for (final Pair<ExecutionReference, ExecutableFlow> runningFlow : this.runningExecutions.get()
        .values()) {
      final ExecutionReference ref = runningFlow.getFirst();
      final ExecutableFlow flow = runningFlow.getSecond();
      final Optional<Executor> executor = ref.getExecutor();

      // We can set the next check time to prevent the checking of certain
      // flows.
      if (ref.getNextCheckTime() >= DateTime.now().getMillis()) {
        continue;
      }

      List<ExecutableFlow> flows = exFlowMap.get(executor);
      if (flows == null) {
        flows = new ArrayList<>();
        exFlowMap.put(executor, flows);
      }

      flows.add(flow);
    }

    return exFlowMap;
  }

  private ExecutableFlow updateExecution(final Map<String, Object> updateData)
      throws ExecutorManagerException {

    final Integer execId =
        (Integer) updateData.get(ConnectorParams.UPDATE_MAP_EXEC_ID);
    if (execId == null) {
      throw new ExecutorManagerException(
          "Response is malformed. Need exec id to update.");
    }

    final Pair<ExecutionReference, ExecutableFlow> refPair =
        this.runningExecutions.get().get(execId);
    if (refPair == null) {
      // this shouldn't ever happen on real azkaban runtime.
      // but this can easily happen in unit tests if there's some inconsistent mocking.
      throw new ExecutorManagerException(
          "No execution found in the map with the execution id any more. Removing " + execId);
    }

    final ExecutionReference ref = refPair.getFirst();
    final ExecutableFlow flow = refPair.getSecond();
    if (updateData.containsKey("error")) {
      // The flow should be finished here.
      throw new ExecutorManagerException((String) updateData.get("error"), flow);
    }

    // Reset errors.
    ref.setNextCheckTime(0);
    ref.setNumErrors(0);
    final Status oldStatus = flow.getStatus();
    flow.applyUpdateObject(updateData);
    final Status newStatus = flow.getStatus();

    if (oldStatus != newStatus && newStatus == Status.FAILED) {
      this.commonMetrics.markFlowFail();
    }

    final ExecutionOptions options = flow.getExecutionOptions();
    if (oldStatus != newStatus && newStatus.equals(Status.FAILED_FINISHING)) {
      // We want to see if we should give an email status on first failure.
      if (options.getNotifyOnFirstFailure()) {
        final Alerter mailAlerter = this.alerterHolder.get("email");
        try {
          mailAlerter.alertOnFirstError(flow);
        } catch (final Exception e) {
          logger.error("Failed to send first error email." + e.getMessage(), e);
        }
      }
      if (options.getFlowParameters().containsKey("alert.type")) {
        final String alertType = options.getFlowParameters().get("alert.type");
        final Alerter alerter = this.alerterHolder.get(alertType);
        if (alerter != null) {
          try {
            alerter.alertOnFirstError(flow);
          } catch (final Exception e) {
            logger.error("Failed to alert by " + alertType, e);
          }
        } else {
          logger.error("Alerter type " + alertType
              + " doesn't exist. Failed to alert.");
        }
      }
    }

    return flow;
  }

}
