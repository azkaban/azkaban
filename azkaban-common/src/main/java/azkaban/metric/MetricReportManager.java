/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.metric;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;


public class MetricReportManager {
  private static final int MAX_EMITTER_THREADS = 2;
  private static final Logger logger = Logger.getLogger(MetricReportManager.class);

  private List<IMetric<?>> metrics;
  private IMetricEmitter metricEmitter;
  private ExecutorService executorService;
  private static volatile MetricReportManager instance = null;

  // For singleton
  private MetricReportManager() {
    executorService = Executors.newFixedThreadPool(MAX_EMITTER_THREADS);
    metrics = new ArrayList<IMetric<?>>();
  }

  public static boolean isInstantiated() {
    return instance != null;
  }

  public static MetricReportManager getInstance() {
    if (instance == null) {
      synchronized (MetricReportManager.class) {
        if (instance == null) {
          logger.info("Instantiating MetricReportManager");
          instance = new MetricReportManager();
        }
      }
    }
    return instance;
  }

  // each element of metrics List is responsible to call this method and report metrics
  public void reportMetric(final IMetric<?> metric) {
    if (metric != null) {
      logger.debug(String.format("Submitting %s metric for metric emission pool", metric.getName()));
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            metricEmitter.reportMetric(metric);
          } catch (Exception ex) {
            logger.error(String.format("Failed to report %s metric due to %s", metric.getName(), ex.toString()));
          }
        }
      });
    }
  }

  public void setMetricEmitter(final IMetricEmitter emitter) {
    metricEmitter = emitter;
  }

  public IMetricEmitter getMetricEmitter() {
    return metricEmitter;
  }

  public void AddMetric(final IMetric<?> metric) {
    // metric null or already present
    if (metric != null && getMetricFromName(metric.getName()) == null) {
      logger.debug(String.format("Adding %s metric in Metric Manager", metric.getName()));
      metrics.add(metric);
      metric.setMetricManager(this);
    }
  }

  public IMetric<?> getMetricFromName(final String name) {
    IMetric<?> metric = null;
    if (name != null) {
      for (IMetric<?> currentMetric : metrics) {
        if (currentMetric.getName().equals(name)) {
          metric = currentMetric;
          break;
        }
      }
    }
    return metric;
  }

  protected void finalize() {
    executorService.shutdown();
  }
}
