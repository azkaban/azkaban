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

package azkaban.webapp.servlet;

import azkaban.flowtrigger.quartz.FlowTriggerScheduler;
import azkaban.flowtrigger.quartz.FlowTriggerScheduler.ScheduledFlowTrigger;
import azkaban.project.CronSchedule;
import azkaban.project.FlowTrigger;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.server.AzkabanAPI;
import azkaban.server.session.Session;
import azkaban.user.Permission.Type;
import azkaban.utils.TimeUtils;
import azkaban.webapp.AzkabanWebServer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowTriggerServlet extends LoginAbstractAzkabanServlet {

  private static final String API_FETCH_TRIGGER = "fetchTrigger";
  private static final String API_PAUSE_TRIGGER = "pauseTrigger";
  private static final String API_RESUME_TRIGGER = "resumeTrigger";

  private static final long serialVersionUID = 1L;
  private FlowTriggerScheduler scheduler;
  private ProjectManager projectManager;
  private static final Logger logger = LoggerFactory.getLogger(FlowTriggerServlet.class);

  public FlowTriggerServlet() {
    super(createAPIEndpoints());
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = getApplication();
    this.scheduler = server.getFlowTriggerScheduler();
    this.projectManager = server.getProjectManager();
  }

  private static List<AzkabanAPI> createAPIEndpoints() {
    final List<AzkabanAPI> apiEndpoints = new ArrayList<>();
    apiEndpoints.add(new AzkabanAPI("ajax", API_FETCH_TRIGGER));
    apiEndpoints.add(new AzkabanAPI("ajax", API_PAUSE_TRIGGER));
    apiEndpoints.add(new AzkabanAPI("ajax", API_RESUME_TRIGGER));
    return apiEndpoints;
  }

  @Override
  protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else {
      handlePage(req, resp, session);
    }
  }

  private void ajaxFetchTrigger(final int projectId, final String flowId, final Session session,
      final HashMap<String,
          Object> ret) {
    final ScheduledFlowTrigger res = this.scheduler
        .getScheduledFlowTriggerJobs().stream().filter(
            scheduledFlowTrigger -> scheduledFlowTrigger.getFlowId().equals(flowId)
                && scheduledFlowTrigger.getProjectId
                () == projectId).findFirst().orElse(null);

    if (res != null) {
      final Map<String, Object> jsonObj = new HashMap<>();
      final FlowTrigger flowTrigger = res.getFlowTrigger();
      final CronSchedule schedule = flowTrigger.getSchedule();
      jsonObj.put("cronExpression", schedule.getCronExpression());
      jsonObj.put("submitUser", res.getSubmitUser());
      jsonObj.put("firstSchedTime",
          TimeUtils.formatDateTime(res.getQuartzTrigger().getStartTime().getTime()));
      jsonObj.put("nextExecTime",
          TimeUtils.formatDateTime(res.getQuartzTrigger().getNextFireTime().getTime()));

      Long maxWaitMin = null;
      if (flowTrigger.getMaxWaitDuration().isPresent()) {
        maxWaitMin = flowTrigger.getMaxWaitDuration().get().toMinutes();
      }
      jsonObj.put("maxWaitMin", maxWaitMin);

      if (!flowTrigger.getDependencies().isEmpty()) {
        jsonObj.put("dependencies", res.getDependencyListJson());
      }
      ret.put("flowTrigger", jsonObj);
    }
  }

  private boolean checkProjectIdAndFlowId(final HttpServletRequest req) {
    return hasParam(req, "projectId") && hasParam(req, "flowId");
  }

  private void handleAJAXAction(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException, IOException {
    final HashMap<String, Object> ret = new HashMap<>();
    final String ajaxName = getParam(req, "ajax");
    if (API_FETCH_TRIGGER.equals(ajaxName)) {
      if (checkProjectIdAndFlowId(req)) {
        final int projectId = getIntParam(req, "projectId");
        final String flowId = getParam(req, "flowId");
        ajaxFetchTrigger(projectId, flowId, session, ret);
      }
    } else if (API_PAUSE_TRIGGER.equals(ajaxName) || API_RESUME_TRIGGER.equals(ajaxName)) {
      if (checkProjectIdAndFlowId(req)) {
        final int projectId = getIntParam(req, "projectId");
        final String flowId = getParam(req, "flowId");
        final Project project = this.projectManager.getProject(projectId);

        if (project == null) {
          ret.put("error", "please specify a valid project id");
        } else if (!hasPermission(project, session.getUser(), Type.ADMIN)) {
          ret.put("error", "Permission denied. Need ADMIN access.");
        } else {
          try {
            if (API_PAUSE_TRIGGER.equals(ajaxName)) {
              if (this.scheduler.pauseFlowTriggerIfPresent(projectId, flowId)) {
                logger.info("Flow trigger for flow {}.{} is paused", project.getName(), flowId);
              } else {
                logger.warn("Flow trigger for flow {}.{} doesn't exist", project.getName(), flowId);
              }
            } else {
              if (this.scheduler.resumeFlowTriggerIfPresent(projectId, flowId)) {
                logger.info("Flow trigger for flow {}.{} is resumed", project.getName(), flowId);
              } else {
                logger.warn("Flow trigger for flow {}.{} doesn't exist", project.getName(), flowId);
              }
            }
            ret.put("status", "success");
          } catch (final SchedulerException ex) {
            ret.put("error", ex.getMessage());
          }
        }
      }
    }
    if (ret != null) {
      this.writeJSON(resp, ret);
    }
  }

  private void handlePage(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/flowtriggerspage.vm");

    page.add("flowTriggers", this.scheduler.getScheduledFlowTriggerJobs());
    page.render();
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
  }
}
