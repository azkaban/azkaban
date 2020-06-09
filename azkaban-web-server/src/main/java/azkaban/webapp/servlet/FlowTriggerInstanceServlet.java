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

import azkaban.flowtrigger.CancellationCause;
import azkaban.flowtrigger.DependencyInstance;
import azkaban.flowtrigger.FlowTriggerService;
import azkaban.flowtrigger.TriggerInstance;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.server.session.Session;
import azkaban.user.Permission.Type;
import azkaban.webapp.AzkabanWebServer;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class FlowTriggerInstanceServlet extends LoginAbstractAzkabanServlet {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = Logger.getLogger(FlowTriggerInstanceServlet.class);
  private FlowTriggerService triggerService;
  private ProjectManager projectManager;

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = getApplication();
    this.triggerService = server.getFlowTriggerService();
    this.projectManager = server.getProjectManager();
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

  private void handlePage(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/executingflowtriggerspage.vm");

    page.add("runningTriggers", this.triggerService.getRunningTriggers());
    page.add("recentTriggers", this.triggerService.getRecentlyFinished());

    page.add("vmutils", new ExecutorVMHelper());
    page.render();
  }

  private void handleAJAXAction(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final HashMap<String, Object> ret = new HashMap<>();
    final String ajaxName = getParam(req, "ajax");

    //todo chengren311: add permission control
    if (ajaxName.equals("fetchRunningTriggers")) {
      ajaxFetchRunningTriggerInstances(ret);
    } else if (ajaxName.equals("killRunningTrigger")) {
      if (hasParam(req, "id")) {
        final String triggerInstanceId = getParam(req, "id");
        ajaxKillTriggerInstance(triggerInstanceId, session, ret);
      } else {
        ret.put("error", "please specify a valid running trigger instance id");
      }
    } else if (ajaxName.equals("showTriggerProperties")) {
      if (hasParam(req, "id")) {
        final String triggerInstanceId = getParam(req, "id");
        loadTriggerProperties(triggerInstanceId, ret);
      } else {
        ret.put("error", "please specify a valid running trigger instance id");
      }
    } else if (ajaxName.equals("fetchTriggerStatus")) {
      if (hasParam(req, "triggerinstid")) {
        final String triggerInstanceId = getParam(req, "triggerinstid");
        ajaxFetchTriggerInstanceByTriggerInstId(triggerInstanceId, session, ret);
      } else if (hasParam(req, "execid")) {
        final int execId = getIntParam(req, "execid");
        ajaxFetchTriggerInstanceByExecId(execId, session, ret);
      } else {
        ret.put("error", "please specify a valid trigger instance id or flow execution id");
      }
    } else if (ajaxName.equals("fetchTriggerInstances")) {
      if (hasParam(req, "project") && hasParam(req, "flow")) {
        final String projectName = getParam(req, "project");
        final String flowId = getParam(req, "flow");
        final Project project = this.projectManager.getProject(projectName);
        if (project == null) {
          ret.put("error", "please specify a valid project name");
        } else if (!hasPermission(project, session.getUser(), Type.READ)) {
          ret.put("error", "Permission denied. Need READ access.");
        } else {
          ajaxFetchTriggerInstances(project.getId(), flowId, ret, req);
        }
      } else {
        ret.put("error", "please specify project id and flow id");
      }
    }

    if (ret != null) {
      this.writeJSON(resp, ret);
    }
  }

  private void ajaxFetchTriggerInstances(
      final int projectId,
      final String flowId,
      final HashMap<String, Object> ret,
      final HttpServletRequest req)
      throws ServletException {

    final int from = Integer.valueOf(getParam(req, "start"));
    final int length = Integer.valueOf(getParam(req, "length"));

    final Collection<TriggerInstance> triggerInstances = this.triggerService
        .getTriggerInstances(projectId, flowId, from, length);

    ret.put("flow", flowId);
    ret.put("total", triggerInstances.size());
    ret.put("from", from);
    ret.put("length", length);

    final List<Object> history = new ArrayList<>();
    for (final TriggerInstance instance : triggerInstances) {
      final HashMap<String, Object> triggerInfo = new HashMap<>();
      triggerInfo.put("instanceId", instance.getId());
      triggerInfo.put("submitUser", instance.getSubmitUser());
      triggerInfo.put("startTime", instance.getStartTime());
      triggerInfo.put("endTime", instance.getEndTime());
      triggerInfo.put("status", instance.getStatus().toString());
      history.add(triggerInfo);
    }

    ret.put("executions", history);
  }

  private void loadTriggerProperties(final String triggerInstanceId,
      final HashMap<String, Object> ret) {
    final TriggerInstance triggerInstance = this.triggerService
        .findTriggerInstanceById(triggerInstanceId);
    if (triggerInstance != null) {
      ret.put("triggerProperties", triggerInstance.getFlowTrigger().toString());
    } else {
      ret.put("error", "the trigger instance doesn't exist");
    }
  }


  private void wrapTriggerInst(final TriggerInstance triggerInst,
      final HashMap<String, Object> ret) {
    final List<Map<String, Object>> dependencyOutput = new ArrayList<>();
    for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
      final Map<String, Object> depMap = new HashMap<>();
      depMap.put("triggerInstanceId", depInst.getTriggerInstance().getId());
      depMap.put("dependencyName", depInst.getDepName());
      depMap.put("dependencyType", depInst.getTriggerInstance().getFlowTrigger()
          .getDependencyByName(depInst.getDepName()).getType());
      depMap.put("dependencyStartTime", depInst.getStartTime());
      depMap.put("dependencyEndTime", depInst.getEndTime());
      depMap.put("dependencyStatus", depInst.getStatus());
      depMap.put("dependencyCancelCause", depInst.getCancellationCause());
      depMap.put("dependencyConfig", depInst.getTriggerInstance().getFlowTrigger()
          .getDependencyByName(depInst.getDepName()));
      dependencyOutput.add(depMap);
    }
    ret.put("items", dependencyOutput);

    ret.put("triggerId", triggerInst.getId());
    ret.put("triggerSubmitter", triggerInst.getSubmitUser());
    ret.put("triggerStartTime", triggerInst.getStartTime());
    ret.put("triggerEndTime", triggerInst.getEndTime());
    ret.put("triggerStatus", triggerInst.getStatus());
    final String flowTriggerJson = new GsonBuilder().setPrettyPrinting().create()
        .toJson(triggerInst.getFlowTrigger());
    ret.put("triggerProps", flowTriggerJson);
  }

  private void ajaxFetchTriggerInstanceByExecId(final int execId, final Session session,
      final HashMap<String, Object> ret) {
    final TriggerInstance triggerInst = this.triggerService
        .findTriggerInstanceByExecId(execId);
    if (triggerInst != null) {
      wrapTriggerInst(triggerInst, ret);
    }
  }

  private void ajaxFetchTriggerInstanceByTriggerInstId(final String triggerInstanceId,
      final Session session, final HashMap<String, Object> ret) {
    final TriggerInstance triggerInst = this.triggerService
        .findTriggerInstanceById(triggerInstanceId);
    if (triggerInst != null) {
      wrapTriggerInst(triggerInst, ret);
    }
  }

  private void ajaxKillTriggerInstance(final String triggerInstanceId, final Session session,
      final HashMap<String, Object> ret) {
    final TriggerInstance triggerInst = this.triggerService
        .findRunningTriggerInstById(triggerInstanceId);
    if (triggerInst != null) {
      if (hasPermission(triggerInst.getProject(), session.getUser(), Type.EXECUTE)) {
        this.triggerService.cancelTriggerInstance(triggerInst, CancellationCause.MANUAL);
      } else {
        ret.put("error", "no permission to kill the trigger");
      }
    } else {
      ret.put("error", "the trigger doesn't exist, might already finished or cancelled");
    }
  }

  private void ajaxFetchRunningTriggerInstances(final HashMap<String, Object> ret) throws
      ServletException {
    final Collection<TriggerInstance> triggerInstanceList = this.triggerService
        .getRunningTriggers();

    final List<HashMap<String, Object>> output = new ArrayList<>();
    ret.put("items", output);

    for (final TriggerInstance triggerInstance : triggerInstanceList) {
      writeTriggerInstancesData(output, triggerInstance);
    }
  }

  private void writeTriggerInstancesData(final List<HashMap<String, Object>> output,
      final TriggerInstance triggerInst) {

    final HashMap<String, Object> data = new HashMap<>();
    data.put("id", triggerInst.getId());
    data.put("starttime", triggerInst.getStartTime());
    data.put("endtime", triggerInst.getEndTime());
    data.put("status", triggerInst.getStatus());
    data.put("flowExecutionId", triggerInst.getFlowExecId());
    data.put("submitUser", triggerInst.getSubmitUser());
    data.put("flowTriggerConfig", triggerInst.getFlowTrigger());
    final List<Map<String, Object>> dependencyOutput = new ArrayList<>();
    for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
      final Map<String, Object> depMap = new HashMap<>();
      depMap.put("dependencyName", depInst.getDepName());
      depMap.put("dependencyStarttime", depInst.getStartTime());
      depMap.put("dependencyEndtime", depInst.getEndTime());
      depMap.put("dependencyStatus", depInst.getStatus());
      depMap.put("dependencyConfig", depInst.getTriggerInstance().getFlowTrigger()
          .getDependencyByName
              (depInst.getDepName()));
      dependencyOutput.add(depMap);
    }
    data.put("dependencies", dependencyOutput);
    output.add(data);
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    }
  }

  /**
   * @param cronTimezone represents the timezone from remote API call
   * @return if the string is equal to UTC, we return UTC; otherwise, we always return default
   * timezone.
   */
  private DateTimeZone parseTimeZone(final String cronTimezone) {
    if (cronTimezone != null && cronTimezone.equals("UTC")) {
      return DateTimeZone.UTC;
    }

    return DateTimeZone.getDefault();
  }

  private DateTime getPresentTimeByTimezone(final DateTimeZone timezone) {
    return new DateTime(timezone);
  }

  public class ExecutorVMHelper {

    public String getProjectName(final int id) {
      final Project project = FlowTriggerInstanceServlet.this.projectManager.getProject(id);
      if (project == null) {
        return String.valueOf(id);
      }

      return project.getName();
    }
  }
}
