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
//Todo haqin: setup timeToDispatch, flowSubmitToExecutor, flowSubmitToContainer, implement
//corresponding methods
public class ContainerizationMetricsImpl implements ContainerizationMetrics {

  private static final Logger logger = LoggerFactory.getLogger(ContainerizationMetricsImpl.class);
  private final MetricsManager metricsManager;
  private Meter podCompleted, podRequested, podScheduled, initContainerRunning,
      appContainerStarting, podReady, podInitFailure, podAppFailure;
  private Meter flowSubmitToExecutor, flowSubmitToContainer;
  private Histogram timeToDispatch;

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
  }

  @Override
  public void startReporting(Props props) {
    logger.info(String.format("Start reporting container metrics"));
    this.metricsManager.startReporting("AZ-WEB", props);
  }

  /**
   * Mark the occurrence of various pod statuses, defined by {@link azkaban.executor.container.watch.AzPodStatus}
   */
  // Increment in the number of pod whose application containers exited without errors
  @Override
  public void markPodCompleted() {
    this.podCompleted.mark();
  }

  // Increment in the number of pod creation received by Kubernetes api-server
  @Override
  public void markPodRequested() {
    this.podRequested.mark();
  }

  // Increment in the number of pod scheduled
  @Override
  public void markPodScheduled() {
    this.podScheduled.mark();
  }

  // Increment in the number of pod whose init containers are executing
  @Override
  public void markInitContainerRunning() {
    this.initContainerRunning.mark();
  }

  // Increment in the number of pod whose at least 1 application container has started
  @Override
  public void markAppContainerStarting() {
    this.appContainerStarting.mark();
  }

  // Increment in the number of pod whose application containers alll started
  @Override
  public void markPodReady() { this.podReady.mark(); }

  // Increment in the number of pod failed during initialization
  @Override
  public void markPodInitFailure() {
    this.podInitFailure.mark();
  }

  // Increment in the number of pod failed during application containers running
  @Override
  public void markPodAppFailure() {
    this.podAppFailure.mark();
  }

  /**
   * Add a time duration of dispatching a pod from ready to preparing to the histogram
   */
  @Override
  public void addTimeToDispatch(final long time) {
    //Todo haqin: implement metric that records time taken to dispatch flow to a container
  }

  /**
   * Record a flow dispatched to executor
   */
  @Override
  public void markFlowSubmitToExecutor() {
    //Todo haqin: implement metric that records number of flows dispatched to bare metal executor
  }

  /**
   * Record a flow dispatched to container
   */
  @Override
  public void markFlowSubmitToContainer() {
    //Todo haqin: implement metric that records number of flows dispatched to a container
  }
}
