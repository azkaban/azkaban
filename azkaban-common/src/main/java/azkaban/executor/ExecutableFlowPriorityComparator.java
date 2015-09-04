/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.executor;

import java.util.Comparator;

import org.apache.log4j.Logger;

import azkaban.utils.Pair;

public final class ExecutableFlowPriorityComparator implements
  Comparator<Pair<ExecutionReference, ExecutableFlow>> {
  private static Logger logger = Logger
    .getLogger(ExecutableFlowPriorityComparator.class);

  @Override
  public int compare(Pair<ExecutionReference, ExecutableFlow> pair1,
    Pair<ExecutionReference, ExecutableFlow> pair2) {
    ExecutableFlow exflow1 = null, exflow2 = null;
    if (pair1 != null && pair1.getSecond() != null) {
      exflow1 = pair1.getSecond();
    }
    if (pair2 != null && pair2.getSecond() != null) {
      exflow2 = pair2.getSecond();
    }
    if (exflow1 == null && exflow2 == null)
      return 0;
    else if (exflow1 == null)
      return -1;
    else if (exflow2 == null)
      return 1;
    else {
      // descending order of priority
      int diff = getPriority(exflow2) - getPriority(exflow1);
      if (diff == 0) {
        // increasing order of update time, if same priority
        diff = (int) (exflow1.getUpdateTime() - exflow2.getUpdateTime());
      }
      if (diff == 0) {
        // increasing order of execution id, if same priority and updateTime
        diff = exflow1.getExecutionId() - exflow2.getExecutionId();
      }
      return diff;
    }
  }

  /* Helper method to fetch flow priority from flow props */
  private int getPriority(ExecutableFlow exflow) {
    ExecutionOptions options = exflow.getExecutionOptions();
    int priority = ExecutionOptions.DEFAULT_FLOW_PRIORITY;
    if (options != null
      && options.getFlowParameters() != null
      && options.getFlowParameters()
        .containsKey(ExecutionOptions.FLOW_PRIORITY)) {
      try {
        priority =
          Integer.valueOf(options.getFlowParameters().get(
            ExecutionOptions.FLOW_PRIORITY));
      } catch (NumberFormatException ex) {
        priority = ExecutionOptions.DEFAULT_FLOW_PRIORITY;
        logger.error("Failed to parse flow priority for exec_id = "
          + exflow.getExecutionId());
      }
    }
    return priority;
  }
}
