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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

/**
 * Manager for access or updating metric related functionality of Azkaban
 */
public class MetricReportManager {
  /**
   * Maximum number of metrics reporting threads
   */
  private static final int MAX_EMITTER_THREADS = 4;
  private static final Logger logger = Logger.getLogger(MetricReportManager.class);

  /**
   * List of all the metrics that Azkaban is tracking
   */
  private List<IMetric<?>> metrics;
  /**
   * List of all the emitter listening all the metrics
   */
  private List<IMetricEmitter> metricEmitters;
  private ExecutorService executorService;
  // Singleton variable
  private static volatile MetricReportManager instance = null;
  boolean isManagerEnabled;

  private MetricReportManager() {
    logger.debug("Instantiating Metric Manager");
    executorService = Executors.newFixedThreadPool(MAX_EMITTER_THREADS);
    metrics = new ArrayList<IMetric<?>>();
    metricEmitters = new LinkedList<IMetricEmitter>();
    enableManager();
  }

  /**
   * @return true, if we have enabled metric manager from Azkaban exec server
   */
  public static boolean isInstantiated() {
    return instance != null;
  }

  /**
   * Get a singleton object for Metric Manager
   */
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
    if (metric != null && isManagerEnabled) {

      // Report metric to all the emitters
      synchronized (metric) {
        logger.debug(String.format("Submitting %s metric for metric emission pool", metric.getName()));
        for (final IMetricEmitter metricEmitter : metricEmitters) {
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
    }
  }

  /**
   * Add a metric emitter to report metric
   * @param emitter
   */
  public void addMetricEmitter(final IMetricEmitter emitter) {
    metricEmitters.add(emitter);
  }

  /**
   * remove a metric emitter
   * @param emitter
   */
  public void removeMetricEmitter(final IMetricEmitter emitter) {
    metricEmitters.remove(emitter);
  }

  /**
   * Get all the metric emitters
   * @return
   */
  public List<IMetricEmitter> getMetricEmitters() {
    return metricEmitters;
  }

  /**
   * Add a metric to be managed by Metric Manager
   * @param metric
   */
  public void addMetric(final IMetric<?> metric) {
    // metric null or already present
    if (metric != null && getMetricFromName(metric.getName()) == null) {
      logger.debug(String.format("Adding %s metric in Metric Manager", metric.getName()));
      metrics.add(metric);
      metric.updateMetricManager(this);
    } else {
      logger.error("Failed to add metric");
    }
  }

  /**
   * Get metric object for a given metric name
   * @param name metricName
   * @return metric Object, if found. Otherwise null.
   */
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

  /**
   * Get all the emitters
   * @return
   */
  public List<IMetric<?>> getAllMetrics() {
    return metrics;
  }

  public void enableManager() {
    logger.info("Enabling Metric Manager");
    isManagerEnabled = true;
  }

  /**
   * Disable Metric Manager and ask all emitters to purge all available data.
   */
  public void disableManager() {
    logger.info("Disabling Metric Manager");
    if(isManagerEnabled) {
      isManagerEnabled = false;
      for(IMetricEmitter emitter: metricEmitters) {
        try {
          emitter.purgeAllData();
        } catch (Exception ex) {
          logger.error("Failed to purge data "  + ex.toString());
        }
      }
    }
  }

  /**
   * Shutdown execution service
   * {@inheritDoc}
   * @see java.lang.Object#finalize()
   */
  protected void finalize() {
    executorService.shutdown();
  }
}
