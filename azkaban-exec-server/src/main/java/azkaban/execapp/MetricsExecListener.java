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

import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.execapp.FlowRunnerManager;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Gauge;

import org.apache.log4j.Logger;


/**
 * This class MetricsExecRegister is in charge of collecting metrics from executors.
 */
public enum MetricsExecListener implements EventListener {
  INSTANCE;

  private static final Logger logger = Logger.getLogger(MetricsExecListener.class);

  private FlowRunnerManager _flowRunnerManager;

  public void registerFlowRunnerManagerMetrics(MetricRegistry metrics, FlowRunnerManager flowRunnerManager) throws Exception {
    this._flowRunnerManager = flowRunnerManager;

    if (_flowRunnerManager == null)
      throw new Exception("flowRunnerManager has not yet been initialized.");

    logger.info("Registering executor specific metrics.");
    metrics.register("EXEC-NumRunningFlows", new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return _flowRunnerManager.getNumRunningFlows();
      }
    });


    metrics.register("EXEC-NumQueuedFlows", new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return _flowRunnerManager.getNumQueuedFlows();
      }
    });
  }

  @Override
  public synchronized void handleEvent(Event event) {
    /**
     * TODO: Adding Web Server Specific metrics here.
     */
  }
}
