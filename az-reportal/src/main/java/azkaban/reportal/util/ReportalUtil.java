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

package azkaban.reportal.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.reportal.util.Reportal.Variable;
import azkaban.utils.Props;

public class ReportalUtil {
  public static IStreamProvider getStreamProvider(String fileSystem) {
    if (fileSystem.equalsIgnoreCase("hdfs")) {
      return new StreamProviderHDFS();
    }
    return new StreamProviderLocal();
  }

  /**
   * Returns a list of the executable nodes in the specified flow in execution
   * order. Assumes that the flow is linear.
   *
   * @param nodes
   * @return
   */
  public static List<ExecutableNode> sortExecutableNodes(ExecutableFlow flow) {
    List<ExecutableNode> sortedNodes = new ArrayList<ExecutableNode>();

    if (flow != null) {
      List<String> startNodeIds = flow.getStartNodes();

      String nextNodeId = startNodeIds.isEmpty() ? null : startNodeIds.get(0);

      while (nextNodeId != null) {
        ExecutableNode node = flow.getExecutableNode(nextNodeId);
        sortedNodes.add(node);

        Set<String> outNodes = node.getOutNodes();
        nextNodeId = outNodes.isEmpty() ? null : outNodes.iterator().next();
      }
    }

    return sortedNodes;
  }

  /**
   * Get runtime variables to be set in unscheduled mode of execution.
   * Returns empty list, if no runtime variable is found
   *
   * @param variables
   * @return
   */
  public static List<Variable> getRunTimeVariables(
    Collection<Variable> variables) {
    List<Variable> runtimeVariables =
      ReportalUtil.getVariablesByRegex(variables,
        Reportal.REPORTAL_CONFIG_PREFIX_NEGATION_REGEX);

    return runtimeVariables;
  }

  /**
   * Shortlist variables which match a given regex. Returns empty empty list, if no
   * eligible variable is found
   *
   * @param variables
   * @param regex
   * @return
   */
  public static List<Variable> getVariablesByRegex(
    Collection<Variable> variables, String regex) {
    List<Variable> shortlistedVariables = new ArrayList<Variable>();
    if (variables != null && regex != null) {
      for (Variable var : variables) {
        if (var.getTitle().matches(regex)) {
          shortlistedVariables.add(var);
        }
      }
    }
    return shortlistedVariables;
  }

  /**
   * Shortlist variables which match a given prefix. Returns empty map, if no
   * eligible variable is found.
   *
   * @param variables
   *          variables to be processed
   * @param prefix
   *          prefix to be matched
   * @return a map with shortlisted variables and prefix removed
   */
  public static Map<String, String> getVariableMapByPrefix(
    Collection<Variable> variables, String prefix) {
    Map<String, String> shortlistMap = new HashMap<String, String>();
    if (variables!=null && prefix != null) {
      for (Variable var : getVariablesByRegex(variables,
        Reportal.REPORTAL_CONFIG_PREFIX_REGEX)) {
        shortlistMap
          .put(var.getTitle().replaceFirst(prefix, ""), var.getName());
      }
    }
    return shortlistMap;
  }
}
