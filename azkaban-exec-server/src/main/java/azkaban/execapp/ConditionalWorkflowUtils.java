/*
* Copyright 2018 LinkedIn Corp.
*
* Licensed under the Apache License, Version 2.0 (the “License”); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/
package azkaban.execapp;

import static azkaban.flow.ConditionOnJobStatus.ALL_FAILED;
import static azkaban.flow.ConditionOnJobStatus.ALL_SUCCESS;
import static azkaban.flow.ConditionOnJobStatus.ONE_FAILED;
import static azkaban.flow.ConditionOnJobStatus.ONE_FAILED_ALL_DONE;
import static azkaban.flow.ConditionOnJobStatus.ONE_SUCCESS;
import static azkaban.flow.ConditionOnJobStatus.ONE_SUCCESS_ALL_DONE;

import azkaban.executor.ExecutableNode;
import azkaban.executor.Status;
import azkaban.flow.ConditionOnJobStatus;

public class ConditionalWorkflowUtils {

  public static final String SATISFIED = "satisfied";
  public static final String PENDING = "pending";
  public static final String FAILED = "failed";

  public static String checkConditionOnJobStatus(final ExecutableNode node) {
    final ConditionOnJobStatus conditionOnJobStatus = node.getConditionOnJobStatus();
    switch (conditionOnJobStatus) {
      case ALL_SUCCESS:
      case ALL_FAILED:
      case ALL_DONE:
        return checkAllStatus(node, conditionOnJobStatus);
      case ONE_FAILED:
      case ONE_SUCCESS:
        return checkOneStatus(node, conditionOnJobStatus);
      case ONE_FAILED_ALL_DONE:
      case ONE_SUCCESS_ALL_DONE:
        return checkOneStatusAllDone(node, conditionOnJobStatus);
      default:
        return checkAllStatus(node, ALL_SUCCESS);
    }
  }

  private static String checkAllStatus(final ExecutableNode node, final ConditionOnJobStatus
      condition) {
    String result = SATISFIED;
    for (final String dependency : node.getInNodes()) {
      final ExecutableNode dependencyNode = node.getParentFlow().getExecutableNode(dependency);
      final Status depStatus = dependencyNode.getStatus();
      if (!Status.isStatusFinished(depStatus)) {
        return PENDING;
      } else if ((condition.equals(ALL_SUCCESS) && Status.isStatusFailed(depStatus)) ||
          (condition.equals(ALL_FAILED) && Status.isStatusSucceeded(depStatus))) {
        result = FAILED;
      }
    }
    return result;
  }

  private static String checkOneStatus(final ExecutableNode node, final ConditionOnJobStatus
      condition) {
    boolean finished = true;
    for (final String dependency : node.getInNodes()) {
      final ExecutableNode dependencyNode = node.getParentFlow().getExecutableNode(dependency);
      final Status depStatus = dependencyNode.getStatus();
      if ((condition.equals(ONE_SUCCESS) && Status.isStatusSucceeded(depStatus)) ||
          (condition.equals(ONE_FAILED) && Status.isStatusFailed(depStatus))) {
        return SATISFIED;
      } else if (!Status.isStatusFinished(depStatus)) {
        finished = false;
      }
    }
    return finished ? FAILED : PENDING;
  }

  private static String checkOneStatusAllDone(final ExecutableNode node, final ConditionOnJobStatus
      condition) {
    String result = FAILED;
    for (final String dependency : node.getInNodes()) {
      final ExecutableNode dependencyNode = node.getParentFlow().getExecutableNode(dependency);
      final Status depStatus = dependencyNode.getStatus();
      if (!Status.isStatusFinished(depStatus)) {
        return PENDING;
      } else if ((condition.equals(ONE_SUCCESS_ALL_DONE) && Status.isStatusSucceeded(depStatus)) ||
          (condition.equals(ONE_FAILED_ALL_DONE) && Status.isStatusFailed(depStatus))) {
        result = SATISFIED;
      }
    }
    return result;
  }

}
