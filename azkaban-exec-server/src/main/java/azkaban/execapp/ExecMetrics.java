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

import azkaban.metrics.MetricsManager;
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
  public static final String PROJECT_DIR_CACHE_HIT_RATIO_NAME = "EXEC-ProjectDirCacheHitRatio";
  public static final String FLOW_SETUP_TIMER_NAME = "EXEC-flow-setup-timer";

  private final MetricsManager metricsManager;
  private Timer flowSetupTimer;

  @Inject
  ExecMetrics(final MetricsManager metricsManager) {
    this.metricsManager = metricsManager;
    setupStaticMetrics();
  }

  public void setupStaticMetrics() {
    this.flowSetupTimer = this.metricsManager.addTimer(FLOW_SETUP_TIMER_NAME);
  }

  public void addFlowRunnerManagerMetrics(final FlowRunnerManager flowRunnerManager) {
    this.metricsManager
        .addGauge(NUM_RUNNING_FLOWS_NAME, flowRunnerManager::getNumRunningFlows);
    this.metricsManager
        .addGauge(NUM_QUEUED_FLOWS_NAME, flowRunnerManager::getNumQueuedFlows);
    this.metricsManager
        .addGauge(PROJECT_DIR_CACHE_HIT_RATIO_NAME, flowRunnerManager::getProjectDirCacheHitRatio);
  }

  /**
   * @return the {@link Timer.Context} for the timer.
   */
  public Timer.Context getFlowSetupTimerContext() { return this.flowSetupTimer.time(); }
}
