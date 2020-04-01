/*
 * Copyright 2017 LinkedIn Corp.
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

package azkaban.execapp;

import azkaban.execapp.metric.ProjectCacheHitRatio;
import azkaban.metrics.MetricsManager;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * This class ExecMetrics is in charge of collecting metrics from executors.
 */
@Singleton
public class ExecMetrics {
  public static final String NUM_RUNNING_FLOWS_NAME = "EXEC-NumRunningFlows";
  public static final String NUM_QUEUED_FLOWS_NAME = "EXEC-NumQueuedFlows";
  public static final String PROJECT_DIR_CACHE_HIT_RATIO_NAME = "project-dir-cache-hit-ratio";
  public static final String FLOW_SETUP_TIMER_NAME = "flow-setup-timer";
  public static final String QUEUE_WAIT_HISTOGRAM_NAME = "queue-wait-histogram";
  public static final String FLOW_TIME_TO_START_HISTOGRAM_NAME= "flow-time-to-start-histogram";
  public static final String FLOW_KILLING_METER_NAME = "flow-killing-meter";
  public static final String FLOW_TIME_TO_KILL_HISTOGRAM_NAME = "flow-time-to-kill-histogram";
  public static final String FLOW_KILLED_METER_NAME = "flow-killed-meter";
  public static final String FLOW_SUCCESS_METER_NAME = "flow-success-meter";
  public static final String JOB_FAIL_METER_NAME = "job-fail-meter";
  public static final String JOB_SUCCESS_METER_NAME = "job-success-meter";
  public static final String JOB_KILLED_METER_NAME = "job-killed-meter";

  private final MetricsManager metricsManager;
  private Timer flowSetupTimer;
  private final ProjectCacheHitRatio projectCacheHitRatio;
  private Histogram queueWaitHistogram;
  private Histogram flowTimeToStartHistogram;
  private Meter flowKillingMeter;
  private Histogram flowTimeToKillHistogram;
  private Meter flowKilledMeter;
  private Meter flowSuccessMeter;
  private Meter jobFailMeter;
  private Meter jobSuccessMeter;
  private Meter jobKilledMeter;

  @Inject
  ExecMetrics(final MetricsManager metricsManager) {
    this.metricsManager = metricsManager;
    // setup project cache ratio metrics
    this.projectCacheHitRatio = new ProjectCacheHitRatio();
    this.metricsManager.addGauge(PROJECT_DIR_CACHE_HIT_RATIO_NAME,
        this.projectCacheHitRatio::getValue);
    this.flowSetupTimer = this.metricsManager.addTimer(FLOW_SETUP_TIMER_NAME);
    this.queueWaitHistogram = this.metricsManager.addHistogram(QUEUE_WAIT_HISTOGRAM_NAME);
    this.flowTimeToStartHistogram =
        this.metricsManager.addHistogram(FLOW_TIME_TO_START_HISTOGRAM_NAME);
    this.flowKillingMeter = this.metricsManager.addMeter(FLOW_KILLING_METER_NAME);
    this.flowTimeToKillHistogram =
        this.metricsManager.addHistogram(FLOW_TIME_TO_KILL_HISTOGRAM_NAME);
    this.flowKilledMeter = this.metricsManager.addMeter(FLOW_KILLED_METER_NAME);
    this.flowSuccessMeter = this.metricsManager.addMeter(FLOW_SUCCESS_METER_NAME);
    this.jobFailMeter = this.metricsManager.addMeter(JOB_FAIL_METER_NAME);
    this.jobSuccessMeter = this.metricsManager.addMeter(JOB_SUCCESS_METER_NAME);
    this.jobKilledMeter = this.metricsManager.addMeter(JOB_KILLED_METER_NAME);
  }

  ProjectCacheHitRatio getProjectCacheHitRatio() {
    return this.projectCacheHitRatio;
  }

  public void addFlowRunnerManagerMetrics(final FlowRunnerManager flowRunnerManager) {
    this.metricsManager
        .addGauge(NUM_RUNNING_FLOWS_NAME, flowRunnerManager::getNumRunningFlows);
    this.metricsManager
        .addGauge(NUM_QUEUED_FLOWS_NAME, flowRunnerManager::getNumQueuedFlows);
  }

  /**
   * @return the {@link Timer.Context} for the timer.
   */
  public Timer.Context getFlowSetupTimerContext() {
    return this.flowSetupTimer.time();
  }

  /**
   * Add the time between submission and when the flow preparation starts.
   *
   * @param time queue wait time for a flow.
   */
  public void addQueueWait(final long time) {
    this.queueWaitHistogram.update(time);
  }

  /**
   * Add the time between submission and start of execution for a flow.
   *
   * @param time submission-to-start time for a flow
   */
  public void addFlowTimeToStart(final long time) {
    this.flowTimeToStartHistogram.update(time);
  }

  /**
   * Record the start of the flow killing procedure.
   */
  public void markFlowKilling() {
    this.flowKillingMeter.mark();
  }

  /**
   * Add the time it took to kill all the jobs in an execution.
   *
   * @param time killing-to-killed time for a flow
   */
  public void addFlowTimeToKill(final long time) {
    this.flowTimeToKillHistogram.update(time);
  }

  /**
   * Record a killed flow execution event.
   */
  public void markFlowKilled() { this.flowKilledMeter.mark(); }

  /**
   * Record a successful flow execution event.
   */
  public void markFlowSuccess() { this.flowSuccessMeter.mark(); }

  /**
   * Record a failed job execution event.
   */
  public void markJobFail() { this.jobFailMeter.mark(); }

  /**
   * Record a successful job execution event.
   */
  public void markJobSuccess() { this.jobSuccessMeter.mark(); }

  /**
   * Record a killed job execution event.
   */
  public void markJobKilled() { this.jobKilledMeter.mark(); }
}
