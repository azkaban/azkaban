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

package azkaban.webapp;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Gauge;

import azkaban.executor.ExecutorManager;

/**
 * This class MetricsWebRegister is in charge of collecting metrics from web server.
 */
public class MetricsWebRegister {
  private ExecutorManager _executorManager;
  private String endpointName;

  public MetricsWebRegister(MetricsWebRegisterBuilder builder) {
    this.endpointName = builder.endpointName;
    this._executorManager = builder._executorManager;
  }

  public void addExecutorManagerMetrics(MetricRegistry metrics) throws Exception {
    if (_executorManager == null)
      throw new Exception("Can not find executorManager.");

    metrics.register("WEB-NumRunningFlows", new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return _executorManager.getRunningFlows().size();
      }
    });

    metrics.register("WEB-NumQueuedFlows", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return _executorManager.getQueuedFlowSize();
      }
    });
  }

  public static class MetricsWebRegisterBuilder {
    private ExecutorManager _executorManager;
    private String endpointName;

    public MetricsWebRegisterBuilder(String endpointName) {
      this.endpointName = endpointName;
    }

    public MetricsWebRegisterBuilder addExecutorManager(ExecutorManager executorManager) {
      this._executorManager = executorManager;
      return this;
    }

    public MetricsWebRegister build() {
      return new MetricsWebRegister(this);
    }
  }

}
