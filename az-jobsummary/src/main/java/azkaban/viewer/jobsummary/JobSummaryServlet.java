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

package azkaban.viewer.jobsummary;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.server.session.Session;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.plugin.PluginRegistry;
import azkaban.webapp.plugin.ViewerPlugin;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import azkaban.webapp.servlet.Page;
import java.io.File;
import java.io.IOException;
import java.util.List;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

public class JobSummaryServlet extends LoginAbstractAzkabanServlet {
  private static final String PROXY_USER_SESSION_KEY =
      "hdfs.browser.proxy.user";
  private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM =
      "hadoop.security.manager.class";
  private static final Logger logger = Logger.getLogger(JobSummaryServlet.class);

  private final Props props;
  private final File webResourcesPath;

  private final String viewerName;
  private final String viewerPath;

  private ExecutorManagerAdapter executorManagerAdapter;
  private ProjectManager projectManager;

  private String outputDir;

  public JobSummaryServlet(final Props props) {
    this.props = props;
    this.viewerName = props.getString("viewer.name");
    this.viewerPath = props.getString("viewer.path");

    this.webResourcesPath =
        new File(new File(props.getSource()).getParentFile().getParentFile(),
            "web");
    this.webResourcesPath.mkdirs();
    setResourceDirectory(this.webResourcesPath);
  }

  private Project getProjectByPermission(final int projectId, final User user,
      final Permission.Type type) {
    final Project project = this.projectManager.getProject(projectId);
    if (project == null) {
      return null;
    }
    if (!hasPermission(project, user, type)) {
      return null;
    }
    return project;
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.executorManagerAdapter = server.getExecutorManagerAdapter();
    this.projectManager = server.getProjectManager();
  }

  private void handleViewer(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {

    final Page page =
        newPage(req, resp, session,
            "azkaban/viewer/jobsummary/velocity/jobsummary.vm");
    page.add("viewerPath", this.viewerPath);
    page.add("viewerName", this.viewerName);

    final User user = session.getUser();
    final int execId = getIntParam(req, "execid");
    final String jobId = getParam(req, "jobid");
    final int attempt = getIntParam(req, "attempt", 0);

    page.add("execid", execId);
    page.add("jobid", jobId);
    page.add("attempt", attempt);

    ExecutableFlow flow = null;
    ExecutableNode node = null;
    try {
      flow = this.executorManagerAdapter.getExecutableFlow(execId);
      if (flow == null) {
        page.add("errorMsg", "Error loading executing flow " + execId
            + ": not found.");
        page.render();
        return;
      }

      node = flow.getExecutableNodePath(jobId);
      if (node == null) {
        page.add("errorMsg",
            "Job " + jobId + " doesn't exist in " + flow.getExecutionId());
        return;
      }

      final List<ViewerPlugin> jobViewerPlugins =
          PluginRegistry.getRegistry().getViewerPluginsForJobType(
              node.getType());
      page.add("jobViewerPlugins", jobViewerPlugins);
    } catch (final ExecutorManagerException e) {
      page.add("errorMsg", "Error loading executing flow: " + e.getMessage());
      page.render();
      return;
    }

    final int projectId = flow.getProjectId();
    final Project project = getProjectByPermission(projectId, user, Type.READ);
    if (project == null) {
      page.render();
      return;
    }

    page.add("projectName", project.getName());
    page.add("flowid", flow.getId());
    page.add("parentflowid", node.getParentFlow().getFlowId());
    page.add("jobname", node.getId());

    page.render();
  }

  private void handleDefault(final HttpServletRequest request,
      final HttpServletResponse response, final Session session) throws ServletException,
      IOException {
    final Page page =
        newPage(request, response, session,
            "azkaban/viewer/jobsummary/velocity/jobsummary.vm");
    page.add("viewerPath", this.viewerPath);
    page.add("viewerName", this.viewerName);
    page.add("errorMsg", "No job execution specified.");
    page.render();
  }

  @Override
  protected void handleGet(final HttpServletRequest request,
      final HttpServletResponse response, final Session session) throws ServletException,
      IOException {
    if (hasParam(request, "execid") && hasParam(request, "jobid")) {
      handleViewer(request, response, session);
    } else {
      handleDefault(request, response, session);
    }
  }

  @Override
  protected void handlePost(final HttpServletRequest request,
      final HttpServletResponse response, final Session session) throws ServletException,
      IOException {
  }
}
