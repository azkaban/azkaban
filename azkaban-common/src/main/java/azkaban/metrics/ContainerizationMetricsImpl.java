/*
 * Copyright 2021 LinkedIn Corp.
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

package azkaban.metrics;

import azkaban.executor.Status;
import azkaban.utils.Props;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class implements ContainerMetrics and emit metrics for containerized executions
 */
public class ContainerizationMetricsImpl implements ContainerizationMetrics {

  private static final Logger logger = LoggerFactory.getLogger(ContainerizationMetricsImpl.class);
  private final MetricsManager metricsManager;
  private Meter podCompleted, podRequested, podScheduled, initContainerRunning,
      appContainerStarting, podReady, podInitFailure, podAppFailure;
  private Meter flowSubmitToExecutor, flowSubmitToContainer;
  private Meter executionStopped, oomKilled, containerDispatchFail, vpaRecommenderFail,
      yarnGetApplicationsFail, yarnApplicationKillFail;
  private Meter cleanupStaleFlowHeartBeat, cleanupContainerHeartBeat, cleanupYarnAppHeartBeat;
  private Timer cleanupStaleFlowTimer, cleanupContainerTimer, cleanupYarnAppTimer;
  private Histogram timeToDispatch;
  private volatile boolean isInitialized = false;
  private Map<String, CounterGauge> cleanupStaleFlowCounterGauges;

  @Inject
  public ContainerizationMetricsImpl(MetricsManager metricsManager) {
    this.metricsManager = metricsManager;
  }

  @Override
  public void setUp() {
    logger.info(String.format("Setting up container metrics."));
    this.podCompleted = this.metricsManager.addMeter("Pod-Completed-Meter");
    this.podRequested = this.metricsManager.addMeter("Pod-Requested-Meter");
    this.podScheduled = this.metricsManager.addMeter("Pod-Scheduled-Meter");
    this.initContainerRunning = this.metricsManager.addMeter("Init-Container-Running-Meter");
    this.appContainerStarting = this.metricsManager.addMeter("App-Container-Starting-Meter");
    this.podReady = this.metricsManager.addMeter("Pod-Ready-Meter");
    this.podInitFailure = this.metricsManager.addMeter("Pod-Init-Failure-Meter");
    this.podAppFailure = this.metricsManager.addMeter("Pod-App-Failure-Meter");
    this.flowSubmitToExecutor = this.metricsManager.addMeter("Flow-Submit-To-Executor-Meter");
    this.flowSubmitToContainer = this.metricsManager.addMeter("Flow-Submit-To-Container-Meter");
    this.timeToDispatch = this.metricsManager.addHistogram("Time-To-Dispatch-Pod-Histogram");
    this.executionStopped = this.metricsManager.addMeter("Execution-Stopped-Meter");
    this.oomKilled = this.metricsManager.addMeter("OOM-Killed-Meter");
    this.containerDispatchFail = this.metricsManager.addMeter("Container-Dispatch-Fail-Meter");
    this.vpaRecommenderFail = this.metricsManager.addMeter("VPA-Recommender-Fail-Meter");
    this.yarnGetApplicationsFail = this.metricsManager.addMeter("Yarn-Get-Applications-Fail-Meter");
    this.yarnApplicationKillFail = this.metricsManager.addMeter("Yarn-Application-Kill-Fail-Meter");
    this.cleanupStaleFlowHeartBeat = this.metricsManager.addMeter("Cleanup-Stale-Flow-Heartbeat"
        + "-Meter");
    this.cleanupContainerHeartBeat = this.metricsManager.addMeter("Cleanup-Container-Heartbeat"
        + "-Meter");
    this.cleanupYarnAppHeartBeat = this.metricsManager.addMeter("Cleanup-Yarn-Application-Heartbeat"
        + "-Meter");
    this.cleanupStaleFlowTimer = this.metricsManager.addTimer("Cleanup-Stale-Flow-Timer");
    this.cleanupContainerTimer = this.metricsManager.addTimer("Cleanup-Container-Timer");
    this.cleanupYarnAppTimer = this.metricsManager.addTimer("Cleanup-Yarn-Application-Timer");

    this.cleanupStaleFlowCounterGauges = new HashMap<>();
    cleanupStaleFlowCounterGauges.put(Status.DISPATCHING.name(),
        this.metricsManager.addCounterGauge("Cleanup-Stale-Dispatching-Flow-Number"));
    cleanupStaleFlowCounterGauges.put(Status.PREPARING.name(),
        this.metricsManager.addCounterGauge("Cleanup-Stale-Preparing-Flow-Number"));
    cleanupStaleFlowCounterGauges.put(Status.RUNNING.name(),
        this.metricsManager.addCounterGauge("Cleanup-Stale-Running-Flow-Number"));
    cleanupStaleFlowCounterGauges.put(Status.PAUSED.name(),
        this.metricsManager.addCounterGauge("Cleanup-Stale-Paused-Flow-Number"));
    cleanupStaleFlowCounterGauges.put(Status.KILLING.name(),
        this.metricsManager.addCounterGauge("Cleanup-Stale-Killing-Flow-Number"));
    cleanupStaleFlowCounterGauges.put(Status.FAILED_FINISHING.name(),
        this.metricsManager.addCounterGauge("Cleanup-Stale-Failed_Finishing-Flow-Number"));
  }

  @Override
  public synchronized void startReporting(Props props) {
    logger.info(String.format("Start reporting container metrics"));
    this.metricsManager.startReporting(props);
    this.isInitialized = true;
  }

  @Override
  public boolean isInitialized() {
    return isInitialized;
  }

  /**
   * Mark the occurrence of various pod statuses, defined by {@link azkaban.executor.container.watch.AzPodStatus}
   */

  @Override
  public void markPodCompleted() {
    this.podCompleted.mark();
  }

  @Override
  public void markPodRequested() {
    this.podRequested.mark();
  }

  @Override
  public void markPodScheduled() {
    this.podScheduled.mark();
  }

  @Override
  public void markInitContainerRunning() {
    this.initContainerRunning.mark();
  }

  @Override
  public void markAppContainerStarting() {
    this.appContainerStarting.mark();
  }

  @Override
  public void markPodReady() {
    this.podReady.mark();
  }

  @Override
  public void markPodInitFailure() {
    this.podInitFailure.mark();
  }

  @Override
  public void markPodAppFailure() {
    this.podAppFailure.mark();
  }


  @Override
  public void addTimeToDispatch(final long time) {
    timeToDispatch.update(time);
  }

  @Override
  public void markFlowSubmitToExecutor() {
    flowSubmitToExecutor.mark();
  }

  @Override
  public void markFlowSubmitToContainer() {
    flowSubmitToContainer.mark();
  }

  @Override
  public void markExecutionStopped() {
    executionStopped.mark();
  }

  @Override
  public void markOOMKilled() {
    oomKilled.mark();
  }

  @Override
  public void markContainerDispatchFail() {
    containerDispatchFail.mark();
  }

  @Override
  public void markVPARecommenderFail() {
    vpaRecommenderFail.mark();
  }

  @Override
  public void markYarnGetApplicationsFail() {
    yarnGetApplicationsFail.mark();
  }

  @Override
  public void markYarnApplicationKillFail(long n) {
    yarnApplicationKillFail.mark(n);
  }

  @Override
  public void sendCleanupStaleFlowHeartBeat() {
    cleanupStaleFlowHeartBeat.mark();
  }

  @Override
  public void addCleanupStaleStatusFlowNumber(String status, long n){
    if (this.cleanupStaleFlowCounterGauges.containsKey(status)) {
      this.cleanupStaleFlowCounterGauges.get(status).add(n);
    }
  }

  @Override
  public void sendCleanupContainerHeartBeat() {
    cleanupContainerHeartBeat.mark();
  }

  @Override
  public void sendCleanupYarnApplicationHeartBeat() {
    cleanupYarnAppHeartBeat.mark();
  }

  @Override
  public void recordCleanupStaleFlowTimer(long duration, TimeUnit unit){
    cleanupStaleFlowTimer.update(duration, unit);
  }

  @Override
  public void recordCleanupContainerTimer(long duration, TimeUnit unit){
    cleanupContainerTimer.update(duration, unit);
  }

  @Override
  public void recordCleanupYarnApplicationTimer(long duration, TimeUnit unit){
    cleanupYarnAppTimer.update(duration, unit);
  }
}
