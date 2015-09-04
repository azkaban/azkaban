/*
 * Copyright 2014 LinkedIn Corp.
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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import azkaban.executor.ConnectorParams;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerException;
import azkaban.server.session.Session;
import azkaban.user.Permission;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.utils.Pair;
import azkaban.webapp.AzkabanWebServer;

/**
 * User facing servlet for Azkaban default metric display
 */
public class StatsServlet extends LoginAbstractAzkabanServlet {
  private static final long serialVersionUID = 1L;
  private UserManager userManager;
  private ExecutorManager execManager;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    AzkabanWebServer server = (AzkabanWebServer) getApplication();
    userManager = server.getUserManager();
    execManager = server.getExecutorManager();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    if (hasParam(req, ConnectorParams.ACTION_PARAM)) {
      handleAJAXAction(req, resp, session);
    } else {
      handleStatePageLoad(req, resp, session);
    }
  }

  private void handleAJAXAction(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
    HashMap<String, Object> ret = new HashMap<String, Object>();
    int executorId = getIntParam(req, ConnectorParams.EXECUTOR_ID_PARAM);
    String actionName = getParam(req, ConnectorParams.ACTION_PARAM);

    if (actionName.equals(ConnectorParams.STATS_GET_METRICHISTORY)) {
      handleGetMetricHistory(executorId, req, ret, session.getUser());
    } else if (actionName.equals(ConnectorParams.STATS_GET_ALLMETRICSNAME)) {
      handleGetAllMetricName(executorId, req, ret);
    } else if (actionName.equals(ConnectorParams.STATS_SET_REPORTINGINTERVAL)) {
      handleChangeConfigurationRequest(executorId, ConnectorParams.STATS_SET_REPORTINGINTERVAL, req, ret);
    } else if (actionName.equals(ConnectorParams.STATS_SET_CLEANINGINTERVAL)) {
      handleChangeConfigurationRequest(executorId, ConnectorParams.STATS_SET_CLEANINGINTERVAL, req, ret);
    } else if (actionName.equals(ConnectorParams.STATS_SET_MAXREPORTERPOINTS)) {
      handleChangeConfigurationRequest(executorId, ConnectorParams.STATS_SET_MAXREPORTERPOINTS, req, ret);
    } else if (actionName.equals(ConnectorParams.STATS_SET_ENABLEMETRICS)) {
      handleChangeConfigurationRequest(executorId, ConnectorParams.STATS_SET_ENABLEMETRICS, req, ret);
    } else if (actionName.equals(ConnectorParams.STATS_SET_DISABLEMETRICS)) {
      handleChangeConfigurationRequest(executorId, ConnectorParams.STATS_SET_DISABLEMETRICS, req, ret);
    }

    writeJSON(resp, ret);
  }

  /**
   * Get all metrics tracked by the given executor
   *
   * @param executorId
   * @param req
   * @param ret
   */
  private void handleGetAllMetricName(int executorId, HttpServletRequest req,
    HashMap<String, Object> ret) throws IOException {
    Map<String, Object> result;
    try {
      result =
        execManager.callExecutorStats(executorId,
          ConnectorParams.STATS_GET_ALLMETRICSNAME,
          (Pair<String, String>[]) null);

      if (result.containsKey(ConnectorParams.RESPONSE_ERROR)) {
        ret.put("error", result.get(ConnectorParams.RESPONSE_ERROR).toString());
      } else {
        ret.put("metricList", result.get("data"));
      }
    } catch (ExecutorManagerException e) {
      ret.put("error", "Failed to fetch metric names for executor : "
        + executorId);
    }
  }

  /**
   * Generic method to facilitate actionName action using Azkaban exec server
   * @param executorId
   * @param actionName  Name of the action
   * @throws ExecutorManagerException
   */
  private void handleChangeConfigurationRequest(int executorId, String actionName, HttpServletRequest req, HashMap<String, Object> ret)
      throws ServletException, IOException {
    try {
      Map<String, Object> result =
        execManager
          .callExecutorStats(executorId, actionName, getAllParams(req));
      if (result.containsKey(ConnectorParams.RESPONSE_ERROR)) {
        ret.put(ConnectorParams.RESPONSE_ERROR,
          result.get(ConnectorParams.RESPONSE_ERROR).toString());
      } else {
        ret.put(ConnectorParams.STATUS_PARAM,
          result.get(ConnectorParams.STATUS_PARAM));
      }
    } catch (ExecutorManagerException ex) {
      ret.put("error", "Failed to change config change");
    }
  }

  /**
   * Get metric snapshots for a metric and date specification
   * @param executorId
   * @throws ServletException
   * @throws ExecutorManagerException
   */
  private void handleGetMetricHistory(int executorId, HttpServletRequest req,
    HashMap<String, Object> ret, User user) throws IOException,
    ServletException {
    try {
      Map<String, Object> result =
        execManager.callExecutorStats(executorId,
          ConnectorParams.STATS_GET_METRICHISTORY, getAllParams(req));
      if (result.containsKey(ConnectorParams.RESPONSE_ERROR)) {
        ret.put(ConnectorParams.RESPONSE_ERROR,
          result.get(ConnectorParams.RESPONSE_ERROR).toString());
      } else {
        ret.put("data", result.get("data"));
      }
    } catch (ExecutorManagerException ex) {
      ret.put("error", "Failed to fetch metric history");
    }
  }

  /**
   * @throws ExecutorManagerException
   *
   */
  private void handleStatePageLoad(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException {
    Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/statsPage.vm");
    if (!hasPermission(session.getUser(), Permission.Type.METRICS)) {
      page.add("errorMsg", "User " + session.getUser().getUserId() + " has no permission.");
      page.render();
      return;
    }

    try {
      Collection<Executor> executors = execManager.getAllActiveExecutors();
      page.add("executorList", executors);

      Map<String, Object> result =
        execManager.callExecutorStats(executors.iterator().next().getId(),
          ConnectorParams.STATS_GET_ALLMETRICSNAME,
          (Pair<String, String>[]) null);
      if (result.containsKey(ConnectorParams.RESPONSE_ERROR)) {
        page.add("errorMsg", result.get(ConnectorParams.RESPONSE_ERROR)
          .toString());
      } else {
        page.add("metricList", result.get("data"));
      }
    } catch (Exception e) {
      page.add("errorMsg", "Failed to get a response from Azkaban exec server");
    }

    page.render();
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException,
      IOException {
  }

  protected boolean hasPermission(User user, Permission.Type type) {
    for (String roleName : user.getRoles()) {
      Role role = userManager.getRole(roleName);
      if (role.getPermission().isPermissionSet(type) || role.getPermission().isPermissionSet(Permission.Type.ADMIN)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Parse all Http request params
   * @return
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private Pair<String, String>[] getAllParams(HttpServletRequest req) {
    List<Pair<String, String>> allParams = new LinkedList<Pair<String, String>>();

    Iterator it = req.getParameterMap().entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pairs = (Map.Entry) it.next();
      for (Object value : (String[]) pairs.getValue()) {
        allParams.add(new Pair<String, String>((String) pairs.getKey(), (String) value));
      }
    }

    return allParams.toArray(new Pair[allParams.size()]);
  }
}
