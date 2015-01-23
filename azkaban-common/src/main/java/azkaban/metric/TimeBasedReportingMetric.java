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
  protected long MAX_MILISEC_INTERVAL = 60 * 60 * 1000;
  protected long MIN_MILISEC_INTERVAL = 3 * 1000;

  /**
   * @param metricName Name of metric
   * @param metricType Metric type. For display purposes.
   * @param initialValue Initial Value of a metric
   * @param manager Metric Manager whom a metric will report to
   * @param interval Time interval for metric tracking
   * @throws MetricException
   */
  public TimeBasedReportingMetric(String metricName, String metricType, T initialValue, MetricReportManager manager,
      long interval) throws MetricException {
    super(metricName, metricType, initialValue, manager);
    if(!isValidInterval(interval)) {
      throw new MetricException("Invalid interval: Cannot instantiate timer");
    }
    timer = new Timer();
    timer.schedule(getTimerTask(), interval, interval);
  }

  /**
   * Get a TimerTask to reschedule Timer
   * @return An anonymous TimerTask class
   */
  private TimerTask getTimerTask() {
    final TimeBasedReportingMetric<T> lockObject = this;
    TimerTask recurringReporting = new TimerTask() {
      @Override
      public void run() {
        synchronized (lockObject) {
          preTrackingEventMethod();
          notifyManager();
          postTrackingEventMethod();
        }
      }
    };
    return recurringReporting;
  }

  /**
   * Method to change tracking interval
   * @param interval
   * @throws MetricException
   */
  public void updateInterval(final long interval) throws MetricException {
    if(!isValidInterval(interval)) {
      throw new MetricException("Invalid interval: Cannot update timer");
    }
    logger.debug(String.format("Updating tracking interval to %d milisecond for %s metric", interval, getName()));
    timer.cancel();
    timer = new Timer();
    timer.schedule(getTimerTask(), interval, interval);
  }

  private boolean isValidInterval(final long interval) {
    return interval >= MIN_MILISEC_INTERVAL && interval <= MAX_MILISEC_INTERVAL;
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
