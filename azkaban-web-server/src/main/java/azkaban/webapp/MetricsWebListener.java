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

import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.executor.ExecutorManager;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Gauge;

import static java.lang.Math.toIntExact;

import org.apache.log4j.Logger;

/**
 * This singleton Object MetricsWebListener is in charge of collecting metrics from web server.
 */
public enum MetricsWebListener implements EventListener {
  INSTANCE;

  private ExecutorManager _executorManager;
  private Meter getCallMeter;
  private Meter postCallMeter;

  private static final Logger logger = Logger.getLogger(MetricsWebListener.class);

  // Private constructor for Singleton Class.
  private MetricsWebListener() {
  }

  public void registerExecutorManagerMetrics(MetricRegistry metrics, ExecutorManager executorManager) throws Exception {
    this._executorManager = executorManager;

    if (_executorManager == null)
      throw new Exception("Can not find executorManager.");

    metrics.register("WEB-NumRunningFlows", new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        // TODO: Executor Manager doesn't leave an good interface to have number of running flow
        try {
          return _executorManager.getRunningFlows().size() - toIntExact(_executorManager.getQueuedFlowSize());
        } catch (ArithmeticException ae) {
          logger.error("The long number overflows int limits.");
        }
        return -1;
      }
    });

    metrics.register("WEB-NumQueuedFlows", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return  _executorManager.getQueuedFlowSize();
      }
    });
  }

  public void addAPIMetrics(MetricRegistry metrics) throws Exception {
    getCallMeter = metrics.meter("get-call-mater");
    postCallMeter = metrics.meter("post-call-mater");

    metrics.register("get-call-mater-Rate", new Gauge<Double>() {
      @Override
      public Double getValue() {
        return getCallMeter.getOneMinuteRate();
      }
    });

    metrics.register("post-call-meter-Rate", new Gauge<Double>() {
      @Override
      public Double getValue() {
        return postCallMeter.getOneMinuteRate();
      }
    });

  }

  @Override
  public synchronized void handleEvent(Event event) {

    /**
     * TODO: Use switch to select event type.
     *
     */
    if (event.getType() == Event.Type.GET_CALL && getCallMeter != null) {
      getCallMeter.mark();
    } else if (event.getType() == Event.Type.POST_CALL && postCallMeter != null) {
      postCallMeter.mark();
    }
  }
}
