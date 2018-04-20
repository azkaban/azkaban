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
import azkaban.server.session.Session;
import azkaban.webapp.AzkabanWebServer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class FlowTriggerServlet extends LoginAbstractAzkabanServlet {

  private static final long serialVersionUID = 1L;
  private FlowTriggerScheduler scheduler;

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.scheduler = server.getScheduler();
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
      jsonObj.put("cronExpression", res.getFlowTrigger().getSchedule().getCronExpression());
      jsonObj.put("submitUser", res.getSubmitUser());
      jsonObj.put("firstSchedTime",
          utils.formatDateTime(res.getQuartzTrigger().getStartTime().getTime()));
      jsonObj.put("nextExecTime",
          utils.formatDateTime(res.getQuartzTrigger().getNextFireTime().getTime()));
      jsonObj.put("maxWaitMin", res.getFlowTrigger().getMaxWaitDuration().toMinutes());
      if (!res.getFlowTrigger().getDependencies().isEmpty()) {
        jsonObj.put("dependencies", res.getDependencyListJson());
      }
      ret.put("flowTrigger", jsonObj);
    }
  }

  private void handleAJAXAction(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final HashMap<String, Object> ret = new HashMap<>();
    final String ajaxName = getParam(req, "ajax");
    if (ajaxName.equals("fetchTrigger")) {
      if (hasParam(req, "projectId") && hasParam(req, "flowId")) {
        final int projectId = getIntParam(req, "projectId");
        final String flowId = getParam(req, "flowId");
        ajaxFetchTrigger(projectId, flowId, session, ret);
        if (ret != null) {
          this.writeJSON(resp, ret);
        }
      }
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
