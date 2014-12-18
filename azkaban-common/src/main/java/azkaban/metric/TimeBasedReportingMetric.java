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

import java.util.Timer;
import java.util.TimerTask;

/**
 * Metrics tracked after every interval using timer
 * @param <T> Type of Value of a given metric
 */
public abstract class TimeBasedReportingMetric<T> extends AbstractMetric<T> {
  private Timer timer;

  /**
   * @param metricName Name of metric
   * @param metricType Metric type. For display purposes.
   * @param initialValue Initial Value of a metric
   * @param manager Metric Manager whom a metric will report to
   * @param interval Time interval for metric tracking
   */
  public TimeBasedReportingMetric(String metricName, String metricType, T initialValue, MetricReportManager manager,
      long interval) {
    super(metricName, metricType, initialValue, manager);
    timer = new Timer();
    timer.schedule(getTimerTask(), interval, interval);
  }

  /**
   * Get a TimerTask
   * @return An anonymous TimerTask class
   */
  private TimerTask getTimerTask() {
    TimerTask recurringReporting = new TimerTask() {
      @Override
      public void run() {
        preTrackingEventMethod();
        notifyManager();
        postTrackingEventMethod();
      }
    };
    return recurringReporting;
  }

  /**
   * Method to change tracking interval
   * @param interval
   */
  public void updateInterval(final long interval) {
    logger.debug(String.format("Updating tracking interval to %d milisecond for %s metric", interval, getName()));
    timer.cancel();
    timer = new Timer();
    timer.schedule(getTimerTask(), interval, interval);
  }

  /**
   * This method is responsible for making any last minute update to value, if any
   */
  protected abstract void preTrackingEventMethod();

  /**
   * This method is responsible for making any post processing after tracking
   */
  protected abstract void postTrackingEventMethod();

}
