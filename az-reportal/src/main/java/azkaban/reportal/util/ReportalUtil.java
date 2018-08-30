/*
 * Copyright 2018 LinkedIn Corp.
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

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.reportal.util.Reportal.Variable;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReportalUtil {

  public static IStreamProvider getStreamProvider(final String fileSystem) {
    if (fileSystem.equalsIgnoreCase("hdfs")) {
      return new StreamProviderHDFS();
    }
    return new StreamProviderLocal();
  }

  /**
   * Returns a list of the executable nodes in the specified flow in execution
   * order. Assumes that the flow is linear.
   */
  public static List<ExecutableNode> sortExecutableNodes(final ExecutableFlow flow) {
    final List<ExecutableNode> sortedNodes = new ArrayList<>();

    if (flow != null) {
      final List<String> startNodeIds = flow.getStartNodes();

      String nextNodeId = startNodeIds.isEmpty() ? null : startNodeIds.get(0);

      while (nextNodeId != null) {
        final ExecutableNode node = flow.getExecutableNode(nextNodeId);
        sortedNodes.add(node);

        final Set<String> outNodes = node.getOutNodes();
        nextNodeId = outNodes.isEmpty() ? null : outNodes.iterator().next();
      }
    }

    return sortedNodes;
  }

  /**
   * Get runtime variables to be set in unscheduled mode of execution.
   * Returns empty list, if no runtime variable is found
   */
  public static List<Variable> getRunTimeVariables(
      final Collection<Variable> variables) {
    final List<Variable> runtimeVariables =
        ReportalUtil.getVariablesByRegex(variables,
            Reportal.REPORTAL_CONFIG_PREFIX_NEGATION_REGEX);

    return runtimeVariables;
  }

  /**
   * Shortlist variables which match a given regex. Returns empty empty list, if no
   * eligible variable is found
   */
  public static List<Variable> getVariablesByRegex(
      final Collection<Variable> variables, final String regex) {
    final List<Variable> shortlistedVariables = new ArrayList<>();
    if (variables != null && regex != null) {
      for (final Variable var : variables) {
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
   * @param variables variables to be processed
   * @param prefix prefix to be matched
   * @return a map with shortlisted variables and prefix removed
   */
  public static Map<String, String> getVariableMapByPrefix(
      final Collection<Variable> variables, final String prefix) {
    final Map<String, String> shortlistMap = new HashMap<>();
    if (variables != null && prefix != null) {
      for (final Variable var : getVariablesByRegex(variables,
          Reportal.REPORTAL_CONFIG_PREFIX_REGEX)) {
        shortlistMap
            .put(var.getTitle().replaceFirst(prefix, ""), var.getName());
      }
    }
    return shortlistMap;
  }

  private static String formatValue(final String value) {
    return "\"" + value.replace("\"", "") + "\"";
  }

  public static void outputQueryResult(final ResultSet result, final OutputStream outputStream)
      throws SQLException {
    final PrintStream outFile = new PrintStream(outputStream);
    final String delim = ",";
    boolean isHeaderPending = true;
    if (result != null) {
      while (result.next()) {
        final int numColumns = result.getMetaData().getColumnCount();
        final StringBuilder dataString = new StringBuilder();

        if (isHeaderPending) {
          final StringBuilder headerString = new StringBuilder();
          for (int j = 1; j <= numColumns; j++) {
            final String colName = formatValue(result.getMetaData().getColumnName(j));
            if (j > 1) {
              headerString.append(delim).append(colName);
            } else {
              headerString.append(colName);
            }
          }
          isHeaderPending = false;
          outFile.println(headerString.toString());
        }

        for (int j = 1; j <= numColumns; j++) {
          String colVal = result.getString(j);
          if (colVal == null) {
            colVal = "\"null\"";
          } else {
            colVal = formatValue(colVal);
          }

          if (j > 1) {
            dataString.append(delim).append(colVal);
          } else {
            dataString.append(colVal);
          }
        }

        outFile.println(dataString.toString());
      }
    }
    outFile.close();
  }
}
