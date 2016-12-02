/*
 * Copyright 2016 LinkedIn Corp.
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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Gauge;

import azkaban.execapp.FlowRunnerManager;
import org.apache.log4j.Logger;


/**
 * This class MetricsExecRegister is in charge of collecting metrics from executors.
 */
public class MetricsExecRegister {
  private static final Logger logger = Logger.getLogger(MetricsExecRegister.class);

  private String endpointName;
  private FlowRunnerManager _flowRunnerManager;

  public MetricsExecRegister(MetricsExecRegisterBuilder builder) {
    this.endpointName = builder.endpointName;
    this._flowRunnerManager = builder._flowRunnerManager;
  }

  public void addExecutorManagerMetrics(MetricRegistry metrics) throws Exception {
    if (_flowRunnerManager == null)
      throw new Exception("flowRunnerManager has not yet been initialized.");

    logger.info("register executor metrics.");
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

  public static class MetricsExecRegisterBuilder {
    private FlowRunnerManager _flowRunnerManager;
    private String endpointName;

    public MetricsExecRegisterBuilder(String endpointName) {
      this.endpointName = endpointName;
    }

    public MetricsExecRegisterBuilder addFlowRunnerManager(FlowRunnerManager flowRunnerManager) {
      this._flowRunnerManager = flowRunnerManager;
      return this;
    }

    public MetricsExecRegister build() {
      return new MetricsExecRegister(this);
    }
  }

}
