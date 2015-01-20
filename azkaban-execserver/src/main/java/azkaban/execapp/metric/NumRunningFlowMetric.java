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

import azkaban.execapp.FlowRunnerManager;
import azkaban.metric.MetricException;
import azkaban.metric.MetricReportManager;
import azkaban.metric.TimeBasedReportingMetric;

/**
 * Metric to keep track of number of running flows in Azkaban exec server
 */
public class NumRunningFlowMetric extends TimeBasedReportingMetric<Integer> {
  public static final String NUM_RUNNING_FLOW_METRIC_NAME = "NumRunningFlowMetric";
  private static final String NUM_RUNNING_FLOW_METRIC_TYPE = "uint16";

  private FlowRunnerManager flowManager;

  /**
   * @param flowRunnerManager Flow runner manager
   * @param manager metric report manager
   * @param interval reporting interval
   * @throws MetricException
   */
  public NumRunningFlowMetric(FlowRunnerManager flowRunnerManager, MetricReportManager manager, long interval) throws MetricException {
    super(NUM_RUNNING_FLOW_METRIC_NAME, NUM_RUNNING_FLOW_METRIC_TYPE, 0, manager, interval);
    logger.debug("Instantiated NumRunningFlowMetric");
    flowManager = flowRunnerManager;
  }

  /**
   * Update value using flow manager
   * {@inheritDoc}
   * @see azkaban.metric.TimeBasedReportingMetric#preTrackingEventMethod()
   */
  @Override
  protected synchronized void preTrackingEventMethod() {
    value = flowManager.getNumRunningFlows();
  }

  @Override
  protected void postTrackingEventMethod() {
    // nothing to post process
  }

}
