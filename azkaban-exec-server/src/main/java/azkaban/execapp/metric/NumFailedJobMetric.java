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
import azkaban.event.EventData;
import azkaban.event.EventListener;
import azkaban.execapp.JobRunner;
import azkaban.executor.Status;
import azkaban.metric.MetricException;
import azkaban.metric.MetricReportManager;
import azkaban.metric.TimeBasedReportingMetric;

/**
 * Metric to keep track of number of failed jobs in between the tracking events
 */
public class NumFailedJobMetric extends TimeBasedReportingMetric<Integer> implements EventListener {
  public static final String NUM_FAILED_JOB_METRIC_NAME = "NumFailedJobMetric";
  private static final String NUM_FAILED_JOB_METRIC_TYPE = "uint16";

  public NumFailedJobMetric(MetricReportManager manager, long interval) throws MetricException {
    super(NUM_FAILED_JOB_METRIC_NAME, NUM_FAILED_JOB_METRIC_TYPE, 0, manager, interval);
    logger.debug("Instantiated NumFailedJobMetric");
  }

  /**
   * Listen for events to maintain correct value of number of failed jobs
   * {@inheritDoc}
   * @see azkaban.event.EventListener#handleEvent(azkaban.event.Event)
   */
  @Override
  public synchronized void handleEvent(Event event) {
    if (event.getType() == Type.JOB_FINISHED && Status.FAILED.equals(event.getData().getStatus())) {
      value = value + 1;
    }
  }

  @Override
  protected void preTrackingEventMethod() {
    // Nothing to finalize before tracking event
  }

  @Override
  protected synchronized void postTrackingEventMethod() {
    value = 0;
  }

}
