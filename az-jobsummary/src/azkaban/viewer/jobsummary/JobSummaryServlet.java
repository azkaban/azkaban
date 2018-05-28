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

package azkaban.viewer.jobsummary;

import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

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
import azkaban.utils.JSONUtils;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import azkaban.webapp.servlet.Page;
import azkaban.webapp.plugin.PluginRegistry;
import azkaban.webapp.plugin.ViewerPlugin;

public class JobSummaryServlet extends LoginAbstractAzkabanServlet {
  private static final String PROXY_USER_SESSION_KEY =
      "hdfs.browser.proxy.user";
  private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM =
      "hadoop.security.manager.class";
  private static Logger logger = Logger.getLogger(JobSummaryServlet.class);

  private Props props;
  private File webResourcesPath;

  private String viewerName;
  private String viewerPath;

  private ExecutorManagerAdapter executorManager;
  private ProjectManager projectManager;

  private String outputDir;

  public JobSummaryServlet(Props props) {
    this.props = props;
    viewerName = props.getString("viewer.name");
    viewerPath = props.getString("viewer.path");

    webResourcesPath =
        new File(new File(props.getSource()).getParentFile().getParentFile(),
            "web");
    webResourcesPath.mkdirs();
    setResourceDirectory(webResourcesPath);
  }

  private Project getProjectByPermission(int projectId, User user,
      Permission.Type type) {
    Project project = projectManager.getProject(projectId);
    if (project == null) {
      return null;
    }
    if (!hasPermission(project, user, type)) {
      return null;
    }
    return project;
  }

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    AzkabanWebServer server = (AzkabanWebServer) getApplication();
    executorManager = server.getExecutorManager();
    projectManager = server.getProjectManager();
  }

  private void handleViewer(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {

    Page page =
        newPage(req, resp, session,
            "azkaban/viewer/jobsummary/velocity/jobsummary.vm");
    page.add("viewerPath", viewerPath);
    page.add("viewerName", viewerName);

    User user = session.getUser();
    int execId = getIntParam(req, "execid");
    String jobId = getParam(req, "jobid");
    int attempt = getIntParam(req, "attempt", 0);

    page.add("execid", execId);
    page.add("jobid", jobId);
    page.add("attempt", attempt);

    ExecutableFlow flow = null;
    ExecutableNode node = null;
    try {
      flow = executorManager.getExecutableFlow(execId);
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

      List<ViewerPlugin> jobViewerPlugins =
          PluginRegistry.getRegistry().getViewerPluginsForJobType(
              node.getType());
      page.add("jobViewerPlugins", jobViewerPlugins);
    } catch (ExecutorManagerException e) {
      page.add("errorMsg", "Error loading executing flow: " + e.getMessage());
      page.render();
      return;
    }

    int projectId = flow.getProjectId();
    Project project = getProjectByPermission(projectId, user, Type.READ);
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

  private void handleDefault(HttpServletRequest request,
      HttpServletResponse response, Session session) throws ServletException,
      IOException {
    Page page =
        newPage(request, response, session,
            "azkaban/viewer/jobsummary/velocity/jobsummary.vm");
    page.add("viewerPath", viewerPath);
    page.add("viewerName", viewerName);
    page.add("errorMsg", "No job execution specified.");
    page.render();
  }

  @Override
  protected void handleGet(HttpServletRequest request,
      HttpServletResponse response, Session session) throws ServletException,
      IOException {
    if (hasParam(request, "execid") && hasParam(request, "jobid")) {
      handleViewer(request, response, session);
    } else {
      handleDefault(request, response, session);
    }
  }

  @Override
  protected void handlePost(HttpServletRequest request,
      HttpServletResponse response, Session session) throws ServletException,
      IOException {
  }
}
