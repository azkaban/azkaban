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

import java.util.Timer;
import java.util.TimerTask;
import azkaban.execapp.FlowRunnerManager;
import azkaban.metric.AbstractMetric;


public class NumRunningFlowMetric extends AbstractMetric<Integer> {
  public static final String NUM_RUNNING_FLOW_METRIC_NAME = "NumRunningFlowMetric";
  public static final String NUM_RUNNING_FLOW_METRIC_TYPE = "uint16";
  private static final int NUM_RUNNING_FLOW_INTERVAL = 5 * 1000; //milliseconds TODO: increase frequency

  private FlowRunnerManager flowManager;
  private Timer timer = new Timer();

  public NumRunningFlowMetric(FlowRunnerManager flowRunnerManager) {
    super(NUM_RUNNING_FLOW_METRIC_NAME, NUM_RUNNING_FLOW_METRIC_TYPE, 0);
    logger.debug("Instantiated NumRunningFlowMetric");
    flowManager = flowRunnerManager;

    // schedule timer to trigger UpdateValueAndNotifyManager
    timer.schedule(new TimerTask() {

      @Override
      public void run() {
        UpdateValueAndNotifyManager();
      }
    }, NUM_RUNNING_FLOW_INTERVAL, NUM_RUNNING_FLOW_INTERVAL);

  }

  public synchronized void UpdateValueAndNotifyManager() {
    value = flowManager.getNumRunningFlows();
    notifyManager();
  }
}
