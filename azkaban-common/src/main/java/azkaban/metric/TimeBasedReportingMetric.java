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


public abstract class TimeBasedReportingMetric<T> extends AbstractMetric<T> {
  private Timer timer;

  public TimeBasedReportingMetric(String metricName, String metricType, T initialValue, MetricReportManager manager,
      long interval) {
    super(metricName, metricType, initialValue, manager);
    timer = new Timer();
    timer.schedule(getTimerTask(), interval, interval);
  }

  private TimerTask getTimerTask() {
    TimerTask recurringReporting = new TimerTask() {
      @Override
      public void run() {
        finalizeValue();
        notifyManager();
      }
    };
    return recurringReporting;
  }

  public void updateInterval(long interval) {
    timer.cancel();
    timer = new Timer();
    timer.schedule(getTimerTask(), interval, interval);
  }

  /**
   * This method is responsible for making a final update to value, if any
   */
  protected abstract void finalizeValue();

}
