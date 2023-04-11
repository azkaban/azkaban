/*
 * Copyright 2017 LinkedIn Corp.
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

package azkaban.flow;

import static java.util.Objects.requireNonNull;

import azkaban.executor.DisabledJob;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.utils.Props;
import com.google.gson.Gson;
import java.util.List;
import java.util.UUID;
import org.joda.time.DateTime;
import org.slf4j.Logger;

public class FlowUtils {

  public static Props addCommonFlowProperties(final Props parentProps,
      final ExecutableFlowBase flow) {
    final Props props = new Props(parentProps);

    props.put(CommonJobProperties.FLOW_ID, flow.getFlowId());
    props.put(CommonJobProperties.EXEC_ID, flow.getExecutionId());
    props.put(CommonJobProperties.PROJECT_ID, flow.getProjectId());
    props.put(CommonJobProperties.PROJECT_NAME, flow.getProjectName());
    props.put(CommonJobProperties.PROJECT_VERSION, flow.getVersion());
    props.put(CommonJobProperties.FLOW_UUID, UUID.randomUUID().toString());
    props.put(CommonJobProperties.PROJECT_LAST_CHANGED_BY, flow.getLastModifiedByUser());
    props.put(CommonJobProperties.PROJECT_LAST_CHANGED_DATE, flow.getLastModifiedTimestamp());
    props.put(CommonJobProperties.SUBMIT_USER, flow.getExecutableFlow().getSubmitUser());
    props.put(CommonJobProperties.EXECUTION_SOURCE, flow.getExecutionSource());

    final DateTime loadTime = new DateTime();

    props.put(CommonJobProperties.FLOW_START_TIMESTAMP, loadTime.toString());
    props.put(CommonJobProperties.FLOW_START_YEAR, loadTime.toString("yyyy"));
    props.put(CommonJobProperties.FLOW_START_MONTH, loadTime.toString("MM"));
    props.put(CommonJobProperties.FLOW_START_DAY, loadTime.toString("dd"));
    props.put(CommonJobProperties.FLOW_START_HOUR, loadTime.toString("HH"));
    props.put(CommonJobProperties.FLOW_START_MINUTE, loadTime.toString("mm"));
    props.put(CommonJobProperties.FLOW_START_SECOND, loadTime.toString("ss"));
    props.put(CommonJobProperties.FLOW_START_MILLISSECOND,
        loadTime.toString("SSS"));
    props.put(CommonJobProperties.FLOW_START_TIMEZONE,
        loadTime.toString("ZZZZ"));

    return props;
  }

  /**
   * Change job status to disabled in exflow if the job is in disabledJobs
   */
  public static void applyDisabledJobs(final List<DisabledJob> disabledJobs,
      final ExecutableFlowBase exflow) {
    for (final DisabledJob disabled : disabledJobs) {
      if (disabled.isEmbeddedFlow()) {
        final ExecutableNode node = exflow.getExecutableNode(disabled.getName());
        if (node != null && node instanceof ExecutableFlowBase) {
          applyDisabledJobs(disabled.getChildren(), (ExecutableFlowBase) node);
        }
       } else { // job
        final ExecutableNode node = exflow.getExecutableNode(disabled.getName());
        if (node != null) {
          node.setStatus(Status.DISABLED);
        }
      }
    }
  }

  public static Project getProject(final ProjectManager projectManager, final int projectId) {
    final Project project = projectManager.getProject(projectId);
    if (project == null) {
      throw new RuntimeException("Error finding the project to execute "
          + projectId);
    }
    return project;
  }

  public static Flow getFlow(final Project project, final String flowName) {
    final Project nonNullProj = requireNonNull(project);
    final Flow flow = nonNullProj.getFlow(flowName);
    if (flow == null) {
      throw new RuntimeException("Error finding the flow to execute " + flowName);
    }
    return flow;
  }

  // if pass in executorManagerAdapter, then use it to populate the flow's ExecutionOption
  public static ExecutableFlow createExecutableFlow(final Project project, final Flow flow,
   ExecutorManagerAdapter executorManagerAdapter, Logger logger) {
    ExecutableFlow exFlow = new ExecutableFlow(project, flow);
    exFlow.addAllProxyUsers(project.getProxyUsers());

    if (executorManagerAdapter == null) {
      return exFlow;
    }
    try {
      executorManagerAdapter.preloadExecutionOptions(exFlow);
    } catch (Throwable e) {
      if(logger != null) {
        logger.warn("Fail to preload ExecutableFlow, continue without loading ExecutionOptions", e);
      }
      // having issue, fallback to without loading ExecutionOptions
      exFlow = new ExecutableFlow(project, flow);
      exFlow.addAllProxyUsers(project.getProxyUsers());
    }

    return exFlow;
  }

  public static String toJson(final Project proj) {
    final Gson gson = new Gson();
    final String jsonStr = gson.toJson(proj);
    return jsonStr;
  }

  public static Project toProject(final String json) {
    final Gson gson = new Gson();
    return gson.fromJson(json, Project.class);
  }
}
