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

package azkaban.execapp.metric;

import azkaban.event.Event;
import azkaban.event.Event.Type;
import azkaban.event.EventListener;
import azkaban.metric.MetricException;
import azkaban.metric.MetricReportManager;
import azkaban.metric.TimeBasedReportingMetric;

/**
 * Metric to keep track of number of running jobs in Azkaban exec server
 */
public class NumRunningJobMetric extends TimeBasedReportingMetric<Integer> implements EventListener {
  public static final String NUM_RUNNING_JOB_METRIC_NAME = "NumRunningJobMetric";
  private static final String NUM_RUNNING_JOB_METRIC_TYPE = "uint16";

  /**
   * @param manager metric manager
   * @param interval reporting interval
   * @throws MetricException
   */
  public NumRunningJobMetric(MetricReportManager manager, long interval) throws MetricException {
    super(NUM_RUNNING_JOB_METRIC_NAME, NUM_RUNNING_JOB_METRIC_TYPE, 0, manager, interval);
    logger.debug("Instantiated NumRunningJobMetric");
  }

  /**
   * Listen for events to maintain correct value of number of running jobs
   * {@inheritDoc}
   * @see azkaban.event.EventListener#handleEvent(azkaban.event.Event)
   */
  @Override
  public synchronized void handleEvent(Event event) {
    if (event.getType() == Type.JOB_STARTED) {
      value = value + 1;
    } else if (event.getType() == Type.JOB_FINISHED) {
      value = value - 1;
    }
  }

  @Override
  protected void preTrackingEventMethod() {
    // nothing to finalize value is already updated
  }

  @Override
  protected void postTrackingEventMethod() {
    // nothing to post process
  }

}
