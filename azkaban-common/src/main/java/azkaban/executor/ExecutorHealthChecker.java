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
import azkaban.utils.Pair;
import azkaban.utils.Props;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorHealthChecker.class);
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

  @Inject
  public ExecutorHealthChecker(final Props azkProps, final ExecutorLoader executorLoader,
      final ExecutorApiGateway apiGateway, final AlerterHolder alerterHolder) {
    this.healthCheckIntervalMin = azkProps
        .getLong(ConfigurationKeys.AZKABAN_EXECUTOR_HEALTHCHECK_INTERVAL_MIN,
            DEFAULT_EXECUTOR_HEALTHCHECK_INTERVAL.toMinutes());
    this.executorMaxFailureCount = azkProps.getInt(ConfigurationKeys
        .AZKABAN_EXECUTOR_MAX_FAILURE_COUNT, DEFAULT_EXECUTOR_MAX_FAILURE_COUNT);
    this.alertEmails = azkProps.getStringList(ConfigurationKeys.AZKABAN_ADMIN_ALERT_EMAIL);
    this.scheduler = Executors.newSingleThreadScheduledExecutor();
    this.executorLoader = executorLoader;
    this.apiGateway = apiGateway;
    this.alerterHolder = alerterHolder;
  }

  public void start() {
    LOG.info("Starting executor health checker.");
    this.scheduler.scheduleAtFixedRate(() -> checkExecutorHealth(), 0L, this.healthCheckIntervalMin,
        TimeUnit.MINUTES);
  }

  public void shutdown() {
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
   * Checks executor health. Finalizes the flow if its executor is already removed from DB or
   * sends alert emails if the executor isn't alive any more.
   */
  public void checkExecutorHealth() {
    final Map<Optional<Executor>, List<ExecutableFlow>> exFlowMap = getFlowToExecutorMap();
    for (final Map.Entry<Optional<Executor>, List<ExecutableFlow>> entry : exFlowMap.entrySet()) {
      final Optional<Executor> executorOption = entry.getKey();
      if (!executorOption.isPresent()) {
        final String finalizeReason = "Executor id of this execution doesn't exist.";
        for (final ExecutableFlow flow : entry.getValue()) {
          LOG.warn(
              String.format("Finalizing execution %s, %s", flow.getExecutionId(), finalizeReason));
          ExecutionControllerUtils
              .finalizeFlow(this.executorLoader, this.alerterHolder, flow, finalizeReason, null);
        }
        continue;
      }

      final Executor executor = executorOption.get();
      try {
        // Todo jamiesjc: add metrics to monitor the http call return time
        final Map<String, Object> results = this.apiGateway
            .callWithExecutionId(executor.getHost(), executor.getPort(),
                ConnectorParams.PING_ACTION, null, null);
        if (results == null || results.containsKey(ConnectorParams.RESPONSE_ERROR) || !results
            .containsKey(ConnectorParams.STATUS_PARAM) || !results.get(ConnectorParams.STATUS_PARAM)
            .equals(ConnectorParams.RESPONSE_ALIVE)) {
          throw new ExecutorManagerException("Status of executor " + executor.getId() + " is "
              + "not alive.");
        } else {
          // Executor is alive. Clear the failure count.
          if (this.executorFailureCount.containsKey(executor.getId())) {
            this.executorFailureCount.put(executor.getId(), 0);
          }
        }
      } catch (final ExecutorManagerException e) {
        handleExecutorNotAliveCase(entry, executor, e);
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
          .executorLoader.fetchActiveFlows().values()) {
        final Optional<Executor> executor = runningFlow.getFirst().getExecutor();
        List<ExecutableFlow> flows = exFlowMap.get(executor);
        if (flows == null) {
          flows = new ArrayList<>();
          exFlowMap.put(executor, flows);
        }
        flows.add(runningFlow.getSecond());
      }
    } catch (final ExecutorManagerException e) {
      LOG.error("Failed to get flow to executor map");
    }
    return exFlowMap;
  }

  /**
   * Increments executor failure count. If it reaches max failure count, sends alert emails to AZ
   * admin.
   *
   * @param entry executor to list of flows map entry
   * @param executor the executor
   * @param e Exception thrown when the executor is not alive
   */
  private void handleExecutorNotAliveCase(
      final Entry<Optional<Executor>, List<ExecutableFlow>> entry, final Executor executor,
      final ExecutorManagerException e) {
    LOG.error("Failed to get update from executor " + executor.getId(), e);
    this.executorFailureCount.put(executor.getId(), this.executorFailureCount.getOrDefault
        (executor.getId(), 0) + 1);
    if (this.executorFailureCount.get(executor.getId()) % this.executorMaxFailureCount == 0
        && !this.alertEmails.isEmpty()) {
      entry.getValue().stream().forEach(flow -> flow
          .getExecutionOptions().setFailureEmails(this.alertEmails));
      LOG.info(String.format("Executor failure count is %d. Sending alert emails to %s.",
          this.executorFailureCount.get(executor.getId()), this.alertEmails));
      this.alerterHolder.get("email").alertOnFailedUpdate(executor, entry.getValue(), e);
    }
  }
}
