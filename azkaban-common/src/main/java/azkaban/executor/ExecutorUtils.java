package azkaban.executor;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;

/** Executor utility functions */
public class ExecutorUtils {

  /** Private constructor. */
  private ExecutorUtils() {}

  /** @return the maximum number of concurrent runs for one flow */
  public static int getMaxConcurrentRunsOneFlow(final Props azkProps) {
    // The default threshold is set to 30 for now, in case some users are affected. We may
    // decrease this number in future, to better prevent DDos attacks.
    return azkProps.getInt(ConfigurationKeys.MAX_CONCURRENT_RUNS_ONEFLOW,
        Constants.DEFAULT_MAX_ONCURRENT_RUNS_ONEFLOW);
  }

  /** @return a map of (project name, flow name) to max number of concurrent runs for the flow. */
  public static Map<Pair<String, String>, Integer> getMaxConcurentRunsPerFlowMap(
      final Props azkProps) {
    Map<Pair<String, String>, Integer> map = new HashMap<>();
    String perFlowSettings = azkProps.get(ConfigurationKeys.CONCURRENT_RUNS_ONEFLOW_WHITELIST);
    if (perFlowSettings != null) {
      // settings for flows are delimited by semicolon, so split on semicolon to to get the list
      // of flows with custom max concurrent runs
      String[] flowSettings = perFlowSettings.split(";");
      for (String flowSetting: flowSettings) {
        // fields for a flow are delimited by comma, so split on comma to get the list of fields:
        // project name, flow name, and max number of concurrent runs.
        String[] setting = flowSetting.split(",");
        Preconditions.checkState(setting.length == 3,
            "setting value must be specified as <project name>,<flow name>,<max concurrent runs>");
        Pair<String, String> key = new Pair(setting[0], setting[1]);
        Integer maxRuns = Integer.parseInt(setting[2]);
        map.put(key, maxRuns);
      }
    }
    return map;
  }

  /**
   * Get the maximum number of concurrent runs for the specified flow, using the value in
   * azkaban.concurrent.runs.oneflow.whitelist if explictly specified for the flow, and otherwise
   * azkaban.max.concurrent.runs.oneflow or the default.
   *
   * @param projectName project name
   * @param flowName flow name
   * @param defaultMaxConcurrentRuns default max number of concurrent runs for one flow, if not
   * explcitly specified for the flow.
   * @param maxConcurrentRunsFlowMap map of (project, flow) to max number of concurrent runs for
   * flow for which the value is explicitly specified via whitelist.
   * @return the maximum number of concurrent runs for the flow.
   */
  public static int getMaxConcurrentRunsForFlow(String projectName, String flowName,
      int defaultMaxConcurrentRuns, Map<Pair<String, String>, Integer> maxConcurrentRunsFlowMap) {
      return maxConcurrentRunsFlowMap.getOrDefault(new Pair(projectName, flowName),
          defaultMaxConcurrentRuns);
  }
}
