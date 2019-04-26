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

import azkaban.executor.ConnectorParams;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.server.session.Session;
import azkaban.user.User;
import azkaban.utils.Pair;
import azkaban.webapp.AzkabanWebServer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * User facing servlet for Azkaban default metric display
 */
public class StatsServlet extends LoginAbstractAzkabanServlet {

  private static final Logger LOG = LoggerFactory.getLogger(StatsServlet.class);

  private static final long serialVersionUID = 1L;
  private ExecutorManagerAdapter execManagerAdapter;

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.execManagerAdapter = server.getExecutorManager();
  }

  @Override
  protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session)
      throws ServletException,
      IOException {
    if (hasParam(req, ConnectorParams.ACTION_PARAM)) {
      handleAJAXAction(req, resp, session);
    } else {
      handleStatePageLoad(req, resp, session);
    }
  }

  private void handleAJAXAction(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session)
      throws ServletException, IOException {
    final HashMap<String, Object> ret = new HashMap<>();
    final int executorId = getIntParam(req, ConnectorParams.EXECUTOR_ID_PARAM);
    final String actionName = getParam(req, ConnectorParams.ACTION_PARAM);

    if (actionName.equals(ConnectorParams.STATS_GET_METRICHISTORY)) {
      handleGetMetricHistory(executorId, req, ret, session.getUser());
    } else if (actionName.equals(ConnectorParams.STATS_GET_ALLMETRICSNAME)) {
      handleGetAllMetricName(executorId, req, ret);
    } else if (actionName.equals(ConnectorParams.STATS_SET_REPORTINGINTERVAL)) {
      handleChangeConfigurationRequest(executorId, ConnectorParams.STATS_SET_REPORTINGINTERVAL, req,
          ret);
    } else if (actionName.equals(ConnectorParams.STATS_SET_CLEANINGINTERVAL)) {
      handleChangeConfigurationRequest(executorId, ConnectorParams.STATS_SET_CLEANINGINTERVAL, req,
          ret);
    } else if (actionName.equals(ConnectorParams.STATS_SET_MAXREPORTERPOINTS)) {
      handleChangeConfigurationRequest(executorId, ConnectorParams.STATS_SET_MAXREPORTERPOINTS, req,
          ret);
    } else if (actionName.equals(ConnectorParams.STATS_SET_ENABLEMETRICS)) {
      handleChangeConfigurationRequest(executorId, ConnectorParams.STATS_SET_ENABLEMETRICS, req,
          ret);
    } else if (actionName.equals(ConnectorParams.STATS_SET_DISABLEMETRICS)) {
      handleChangeConfigurationRequest(executorId, ConnectorParams.STATS_SET_DISABLEMETRICS, req,
          ret);
    }

    writeJSON(resp, ret);
  }

  /**
   * Get all metrics tracked by the given executor
   */
  private void handleGetAllMetricName(final int executorId, final HttpServletRequest req,
      final HashMap<String, Object> ret) throws IOException {
    final Map<String, Object> result;
    try {
      result =
          this.execManagerAdapter.callExecutorStats(executorId,
              ConnectorParams.STATS_GET_ALLMETRICSNAME,
              (Pair<String, String>[]) null);

      if (result.containsKey(ConnectorParams.RESPONSE_ERROR)) {
        ret.put("error", result.get(ConnectorParams.RESPONSE_ERROR).toString());
      } else {
        ret.put("metricList", result.get("data"));
      }
    } catch (final ExecutorManagerException e) {
      LOG.error(e.getMessage(), e);
      ret.put("error", "Failed to fetch metric names for executor : "
          + executorId);
    }
  }

  /**
   * Generic method to facilitate actionName action using Azkaban exec server
   *
   * @param actionName Name of the action
   */
  private void handleChangeConfigurationRequest(final int executorId, final String actionName,
      final HttpServletRequest req, final HashMap<String, Object> ret) throws IOException {
    try {
      final Map<String, Object> result =
          this.execManagerAdapter
              .callExecutorStats(executorId, actionName, getAllParams(req));
      if (result.containsKey(ConnectorParams.RESPONSE_ERROR)) {
        ret.put(ConnectorParams.RESPONSE_ERROR,
            result.get(ConnectorParams.RESPONSE_ERROR).toString());
      } else {
        ret.put(ConnectorParams.STATUS_PARAM,
            result.get(ConnectorParams.STATUS_PARAM));
      }
    } catch (final ExecutorManagerException ex) {
      LOG.error(ex.getMessage(), ex);
      ret.put("error", "Failed to change config change");
    }
  }

  /**
   * Get metric snapshots for a metric and date specification
   */
  private void handleGetMetricHistory(final int executorId, final HttpServletRequest req,
      final HashMap<String, Object> ret, final User user) throws IOException {
    try {
      final Map<String, Object> result =
          this.execManagerAdapter.callExecutorStats(executorId,
              ConnectorParams.STATS_GET_METRICHISTORY, getAllParams(req));
      if (result.containsKey(ConnectorParams.RESPONSE_ERROR)) {
        ret.put(ConnectorParams.RESPONSE_ERROR,
            result.get(ConnectorParams.RESPONSE_ERROR).toString());
      } else {
        ret.put("data", result.get("data"));
      }
    } catch (final ExecutorManagerException ex) {
      LOG.error(ex.getMessage(), ex);
      ret.put("error", "Failed to fetch metric history");
    }
  }

  /**
   * @throws ExecutorManagerException
   *
   */
  private void handleStatePageLoad(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) {
    final Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/statsPage.vm");

    try {
      final Collection<Executor> executors = this.execManagerAdapter.getAllActiveExecutors();
      page.add("executorList", executors);

      if (executors.isEmpty()) {
        throw new ExecutorManagerException("Executor list is empty.");
      }

      final Map<String, Object> result =
          this.execManagerAdapter.callExecutorStats(executors.iterator().next().getId(),
              ConnectorParams.STATS_GET_ALLMETRICSNAME,
              (Pair<String, String>[]) null);
      if (result.containsKey(ConnectorParams.RESPONSE_ERROR)) {
        page.add("errorMsg", result.get(ConnectorParams.RESPONSE_ERROR)
            .toString());
      } else {
        page.add("metricList", result.get("data"));
      }
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      page.add("errorMsg", "Failed to get a response from Azkaban exec server");
    }

    page.render();
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) {
  }

  /**
   * Parse all Http request params
   */
  private Pair<String, String>[] getAllParams(final HttpServletRequest req) {
    final List<Pair<String, String>> allParams = new LinkedList<>();

    final Iterator it = req.getParameterMap().entrySet().iterator();
    while (it.hasNext()) {
      final Map.Entry pairs = (Map.Entry) it.next();
      for (final Object value : (String[]) pairs.getValue()) {
        allParams.add(new Pair<>((String) pairs.getKey(), (String) value));
      }
    }

    return allParams.toArray(new Pair[allParams.size()]);
  }
}
