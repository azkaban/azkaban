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

import azkaban.utils.Props;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.google.inject.Inject;
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
  private Meter executionStopped, containerDispatchFail;
  private Histogram timeToDispatch;
  private boolean isInitialized = false;

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
    this.containerDispatchFail = this.metricsManager.addMeter("Container-Dispatch-Fail-Meter");
  }

  @Override
  public void startReporting(Props props) {
    logger.info(String.format("Start reporting container metrics"));
    this.metricsManager.startReporting("AZ-WEB", props);
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
  public void markPodReady() { this.podReady.mark(); }

  @Override
  public void markPodInitFailure() {
    this.podInitFailure.mark();
  }

  @Override
  public void markPodAppFailure() {
    this.podAppFailure.mark();
  }


  @Override
  public void addTimeToDispatch(final long time) { timeToDispatch.update(time); }

  @Override
  public void markFlowSubmitToExecutor() { flowSubmitToExecutor.mark(); }

  @Override
  public void markFlowSubmitToContainer() { flowSubmitToContainer.mark(); }

  @Override
  public void markExecutionStopped() { executionStopped.mark(); }

  @Override
  public void markContainerDispatchFail() { containerDispatchFail.mark(); }
}
