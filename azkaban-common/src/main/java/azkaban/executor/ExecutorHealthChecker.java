/*
* Copyright 2019 LinkedIn Corp.
*
* Licensed under the Apache License, Version 2.0 (the “License”); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/
package azkaban.executor;

import azkaban.Constants.ConfigurationKeys;
import azkaban.DispatchMethod;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically checks the health of executors. Finalizes flows or sends alert emails when needed.
 */
@SuppressWarnings("FutureReturnValueIgnored")
@Singleton
public class ExecutorHealthChecker {

  private static final Logger logger = LoggerFactory.getLogger(ExecutorHealthChecker.class);
  // Max number of executor failures before sending out alert emails.
  private static final int DEFAULT_EXECUTOR_MAX_FAILURE_COUNT = 6;
  // Web server checks executor health every 5 min by default.
  private static final Duration DEFAULT_EXECUTOR_HEALTHCHECK_INTERVAL = Duration.ofMinutes(5);
  private final long healthCheckIntervalMin;
  private final int executorMaxFailureCount;
  private final List<String> alertEmails;
  private final ScheduledExecutorService scheduler;
  private final ExecutorLoader executorLoader;
  private final ExecutorApiGateway apiGateway;
  private final AlerterHolder alerterHolder;
  private final Map<Integer, Integer> executorFailureCount = new HashMap<>();
  private final int executorPingTimeout;

  @Inject
  public ExecutorHealthChecker(final Props azkProps, final ExecutorLoader executorLoader,
      final ExecutorApiGateway apiGateway, final AlerterHolder alerterHolder) {
    this.healthCheckIntervalMin = azkProps
        .getLong(ConfigurationKeys.AZKABAN_EXECUTOR_HEALTHCHECK_INTERVAL_MIN,
            DEFAULT_EXECUTOR_HEALTHCHECK_INTERVAL.toMinutes());
    this.executorMaxFailureCount = azkProps.getInt(ConfigurationKeys
        .AZKABAN_EXECUTOR_MAX_FAILURE_COUNT, DEFAULT_EXECUTOR_MAX_FAILURE_COUNT);
    this.alertEmails = azkProps.getStringList(ConfigurationKeys.AZKABAN_ADMIN_ALERT_EMAIL);
    this.scheduler = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("azk-health-checker").build());
    this.executorLoader = executorLoader;
    this.apiGateway = apiGateway;
    this.alerterHolder = alerterHolder;
    this.executorPingTimeout =
        azkProps.getInt(ConfigurationKeys.AZKABAN_EXECUTOR_PING_TIMEOUT, 5000);
  }

  public void start() {
    logger.info("Starting executor health checker.");
    this.scheduler.scheduleAtFixedRate(this::checkExecutorHealthQuietly, 0L,
        this.healthCheckIntervalMin,
        TimeUnit.MINUTES);
  }

  public void shutdown() {
    logger.info("Shutting down executor health checker.");
    this.scheduler.shutdown();
    try {
      if (!this.scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
        this.scheduler.shutdownNow();
      }
    } catch (final InterruptedException ex) {
      this.scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Wrapper for capturing and logging any exceptions thrown during healthcheck.
   * {@code ScheduledExecutorService} stops the scheduled invocations of a given method in
   * case it throws an exception.
   * Any exceptions are not expected at this stage however in case any unchecked exceptions
   * do occur, we still don't want subsequent healthchecks to stop.
   */
  public void checkExecutorHealthQuietly() {
    try {
      logger.info("Begin executor healthcheck routine.");
      checkExecutorHealth();
    } catch (final RuntimeException e) {
      logger.error("Unexepected error during executor healthcheck.", e);
    } finally {
      logger.info("End executor healthcheck routine.");
    }
  }

  private static String executorDetailString(Executor executor) {
    return String.format("executor-id: %d, executor-host: %s, executor-port: %d",
        executor.getId(),
        executor.getHost(),
        executor.getPort());
  }

  /**
   * Checks executor health. Finalizes the flow if its executor is already removed from DB or
   * sends alert emails if the executor isn't alive any more.
   */
  @VisibleForTesting
  void checkExecutorHealth() {
    final Map<Optional<Executor>, List<ExecutableFlow>> exFlowMap = getFlowToExecutorMap();
    for (final Map.Entry<Optional<Executor>, List<ExecutableFlow>> entry : exFlowMap.entrySet()) {
      final Optional<Executor> executorOption = entry.getKey();
      if (!executorOption.isPresent()) {
        final String finalizeReason = "Executor id of this execution doesn't exist.";
        finalizeFlows(entry.getValue(), finalizeReason);
        continue;
      }

      final Executor executor = executorOption.get();
      Optional<ExecutorManagerException> healthcheckException = Optional.empty();
      Map<String, Object> results = null;
      try {
        long pingTime = System.currentTimeMillis();
        results = this.apiGateway
            .callWithExecutionId(executor.getHost(), executor.getPort(),
                ConnectorParams.PING_ACTION, null, null, null,
                Optional.of(executorPingTimeout));
        pingTime = System.currentTimeMillis() - pingTime;
        logger.info("Got ping response from " + executorDetailString(executor)
            + " in " + pingTime + "ms");
      } catch (final ExecutorManagerException e) {
        healthcheckException = Optional.of(e);
      } catch (final RuntimeException re) {
        logger.error("Unexpected exception while reaching executor - "
            + executorDetailString(executor), re);
      }
      if (!healthcheckException.isPresent()) {
        if (results == null || results.containsKey(ConnectorParams.RESPONSE_ERROR) || !results
            .containsKey(ConnectorParams.STATUS_PARAM) || !results.get(ConnectorParams.STATUS_PARAM)
            .equals(ConnectorParams.RESPONSE_ALIVE)) {
          healthcheckException = Optional.of(
              new ExecutorManagerException("Status of executor - " + executorDetailString(executor)
                  + " is not alive."));
        }
      }

      if (healthcheckException.isPresent()){
        try {
          handleExecutorNotAliveCase(executor, entry.getValue(), healthcheckException.get());
        } catch (RuntimeException re) {
          logger.error("Unchecked exception during failure handling for executor - "
              + executorDetailString(executor), re);
        }
      } else {
        // Executor is alive. Clear the failure count.
        if (this.executorFailureCount.containsKey(executor.getId())) {
          this.executorFailureCount.put(executor.getId(), 0);
        }
      }
    }
  }

  /**
   * Finalize given flows with the provided reason.
   *
   * @param flows
   * @param finalizeReason
   */
  @VisibleForTesting
  void finalizeFlows(List<ExecutableFlow> flows, String finalizeReason) {
    for (ExecutableFlow flow: flows) {
      logger.warn(
          String.format("Finalizing execution %s, %s", flow.getExecutionId(), finalizeReason));
      try {
        ExecutionControllerUtils
            .finalizeFlow(this.executorLoader, this.alerterHolder, flow, finalizeReason, null,
                Status.FAILED);
      } catch (RuntimeException e) {
        logger.error("Unchecked exception while finalizing execution: " + flow.getExecutionId(), e);
      }
    }
  }

  /**
   * Groups Executable flow by Executors to reduce number of REST calls.
   *
   * @return executor to list of flows map
   */
  private Map<Optional<Executor>, List<ExecutableFlow>> getFlowToExecutorMap() {
    final HashMap<Optional<Executor>, List<ExecutableFlow>> exFlowMap = new HashMap<>();
    try {
      for (final Pair<ExecutionReference, ExecutableFlow> runningFlow : this
          .executorLoader.fetchActiveFlows(DispatchMethod.POLL).values()) {
        final Optional<Executor> executor = runningFlow.getFirst().getExecutor();
        List<ExecutableFlow> flows = exFlowMap.get(executor);
        if (flows == null) {
          flows = new ArrayList<>();
          exFlowMap.put(executor, flows);
        }
        flows.add(runningFlow.getSecond());
      }
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to get flow to executor map. Exception reported: ", e);
    }
    return exFlowMap;
  }

  /**
   * Increments executor failure count. If it reaches max failure count, sends alert emails to AZ
   * admin and executes any cleanup actions for flows on those executors.
   *
   * @param executor the executor
   * @param flows flows assigned to the executor
   * @param e Exception thrown when the executor is not alive
   */
  private void handleExecutorNotAliveCase(final Executor executor, final List<ExecutableFlow> flows,
      final ExecutorManagerException e) {
    logger.error("Failed to get update from executor - " + executorDetailString(executor), e);
    this.executorFailureCount.put(executor.getId(), this.executorFailureCount.getOrDefault
        (executor.getId(), 0) + 1);
    if (this.executorFailureCount.get(executor.getId()) % this.executorMaxFailureCount == 0) {
      if (!this.alertEmails.isEmpty()) {
        logger.info(String.format("Executor failure count is %d. Sending alert emails to %s.",
            this.executorFailureCount.get(executor.getId()), this.alertEmails));
        try {
          this.alerterHolder.get("email")
              .alertOnFailedExecutorHealthCheck(executor, flows, e,
                  this.alertEmails);
        } catch (final RuntimeException re) {
          logger.error("Unchecked exception while sending admin alert mails for executor - "
              + executorDetailString(executor), re);
        }
      }
      this.cleanupForMissingExecutor(executor, flows);
    }
  }

  /**
   * Perform any cleanup required for an unreachable executor.
   *
   * Note that ideally we would like to disable the executor such that not further executions are
   * 'assigned' to it. However with the pull/polling based model there is currently no direct way
   * of doing this other than the hitting the corresponding ajax endpoint for the executor.
   * That endpoint is most likely not reachable (hence the repeated healthcheck failures).
   * Updating the active status or removing the executor from db will not have an impact on any
   * executor that is still alive and was unreachable temporarily.
   * For now we limit the action to finalizing any flows assigned to the the unreachable executor.
   *
   * @param executor
   * @param executions
   */
  @VisibleForTesting
  void cleanupForMissingExecutor(Executor executor, List<ExecutableFlow> executions) {
    String finalizeReason =
        String.format("Executor was unreachable, executor-id: %s, executor-host: %s, "
                + "executor-port: %d", executor.getId(), executor.getHost(), executor.getPort());
    finalizeFlows(executions, finalizeReason);
  }
}
