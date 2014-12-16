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

package azkaban.execapp;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.executor.ConnectorParams;
import azkaban.metric.IMetric;
import azkaban.metric.IMetricEmitter;
import azkaban.metric.InMemoryHistoryNode;
import azkaban.metric.InMemoryMetricEmitter;
import azkaban.metric.MetricReportManager;
import azkaban.metric.TimeBasedReportingMetric;
import azkaban.server.HttpRequestUtils;
import azkaban.server.ServerConstants;
import azkaban.utils.JSONUtils;


public class StatsServlet extends HttpServlet implements ConnectorParams {
  private static final long serialVersionUID = 2L;
  private static final Logger logger = Logger.getLogger(StatsServlet.class);
  private AzkabanExecutorServer server;

  public void init(ServletConfig config) throws ServletException {
    server =
        (AzkabanExecutorServer) config.getServletContext().getAttribute(ServerConstants.AZKABAN_SERVLET_CONTEXT_KEY);
  }

  public boolean hasParam(HttpServletRequest request, String param) {
    return HttpRequestUtils.hasParam(request, param);
  }

  public String getParam(HttpServletRequest request, String name) throws ServletException {
    return HttpRequestUtils.getParam(request, name);
  }

  public long getLongParam(HttpServletRequest request, String name) throws ServletException {
    return HttpRequestUtils.getLongParam(request, name);
  }

  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    Map<String, Object> ret = new HashMap<String, Object>();

    if (hasParam(req, ACTION_PARAM)) {
      String action = getParam(req, ACTION_PARAM);
      if (action.equals(STATS_SET_REPORTINGINTERVAL)) {
        handleChangeMetricInterval(req, ret);
      } else if (action.equals(STATS_GET_ALLMETRICSNAME)) {
        handleGetAllMMetricsName(req, ret);
      } else if (action.equals(STATS_GET_METRICHISTORY)) {
        handleGetMetricHistory(req, ret);
      }
    }

    JSONUtils.toJSON(ret, resp.getOutputStream(), true);
  }

  private void handleGetMetricHistory(HttpServletRequest req, Map<String, Object> ret) throws ServletException {
    if (MetricReportManager.isInstantiated()) {
      MetricReportManager metricManager = MetricReportManager.getInstance();
      InMemoryMetricEmitter memoryEmitter = null;

      for (IMetricEmitter emitter : metricManager.getMetricEmitters()) {
        if (emitter instanceof InMemoryMetricEmitter) {
          memoryEmitter = (InMemoryMetricEmitter) emitter;
          break;
        }
      }

      // if we have a memory emitter
      if (memoryEmitter != null) {
        try {
          List<InMemoryHistoryNode> result =
              memoryEmitter.getDrawMetric(getParam(req, STATS_MAP_METRICNAMEPARAM),
                  parseDate(getParam(req, STATS_MAP_STARTDATE)), parseDate(getParam(req, STATS_MAP_ENDDATE)));

          if (result != null && result.size() > 0) {
            ret.put("data", result);
          } else {
            ret.put(RESPONSE_ERROR, "No metric stats available");
          }

        } catch (ParseException ex) {
          ret.put(RESPONSE_ERROR, "Invalid Date filter");
        }
      } else {
        ret.put(RESPONSE_ERROR, "InMemoryMetricEmitter not instantiated");
      }
    } else {
      ret.put(RESPONSE_ERROR, "MetricReportManager not instantiated");
    }
  }

  private void handleGetAllMMetricsName(HttpServletRequest req, Map<String, Object> ret) {
    if (MetricReportManager.isInstantiated()) {
      MetricReportManager metricManager = MetricReportManager.getInstance();
      List<IMetric<?>> result = metricManager.getAllMetrics();
      if(result.size() == 0) {
        ret.put(RESPONSE_ERROR, "No Metric being tracked");
      } else {
        ret.put("data", result);
      }
    } else {
      ret.put(RESPONSE_ERROR, "MetricReportManager not instantiated");
    }
  }

  private void handleChangeMetricInterval(HttpServletRequest req, Map<String, Object> ret) throws ServletException {
    try {
      String metricName = getParam(req, STATS_MAP_METRICNAMEPARAM);
      long newInterval = getLongParam(req, STATS_MAP_REPORTINGINTERVAL);
      if (MetricReportManager.isInstantiated()) {
        MetricReportManager metricManager = MetricReportManager.getInstance();
        TimeBasedReportingMetric<?> metric = (TimeBasedReportingMetric<?>) metricManager.getMetricFromName(metricName);
        metric.updateInterval(newInterval);
      }
      ret.put(STATUS_PARAM, RESPONSE_SUCCESS);
    } catch (Exception e) {
      logger.error(e);
      ret.put(RESPONSE_ERROR, e.getMessage());
    }
  }

  private Date parseDate(String date) throws ParseException {
    DateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm a");
    return format.parse(date);
  }
}
