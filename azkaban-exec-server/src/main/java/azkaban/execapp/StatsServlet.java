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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.executor.ConnectorParams;
import azkaban.metric.IMetric;
import azkaban.metric.IMetricEmitter;
import azkaban.metric.MetricReportManager;
import azkaban.metric.TimeBasedReportingMetric;
import azkaban.metric.inmemoryemitter.InMemoryHistoryNode;
import azkaban.metric.inmemoryemitter.InMemoryMetricEmitter;
import azkaban.server.HttpRequestUtils;
import azkaban.utils.JSONUtils;

/**
 * Servlet to communicate with Azkaban exec server This servlet get requests
 * from stats servlet in Azkaban Web server
 */
public class StatsServlet extends HttpServlet implements ConnectorParams {
  private static final long serialVersionUID = 2L;
  private static final Logger logger = Logger.getLogger(StatsServlet.class);

  public boolean hasParam(HttpServletRequest request, String param) {
    return HttpRequestUtils.hasParam(request, param);
  }

  public String getParam(HttpServletRequest request, String name)
      throws ServletException {
    return HttpRequestUtils.getParam(request, name);
  }

  public Boolean getBooleanParam(HttpServletRequest request, String name)
      throws ServletException {
    return HttpRequestUtils.getBooleanParam(request, name);
  }

  public long getLongParam(HttpServletRequest request, String name)
      throws ServletException {
    return HttpRequestUtils.getLongParam(request, name);
  }

  /**
   * Handle all get request to Stats Servlet {@inheritDoc}
   *
   * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest,
   *      javax.servlet.http.HttpServletResponse)
   */
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    Map<String, Object> ret = new HashMap<String, Object>();

    if (hasParam(req, ACTION_PARAM)) {
      String action = getParam(req, ACTION_PARAM);
      if (action.equals(STATS_SET_REPORTINGINTERVAL)) {
        handleChangeMetricInterval(req, ret);
      } else if (action.equals(STATS_SET_CLEANINGINTERVAL)) {
        handleChangeCleaningInterval(req, ret);
      } else if (action.equals(STATS_SET_MAXREPORTERPOINTS)) {
        handleChangeEmitterPoints(req, ret);
      } else if (action.equals(STATS_GET_ALLMETRICSNAME)) {
        handleGetAllMMetricsName(req, ret);
      } else if (action.equals(STATS_GET_METRICHISTORY)) {
        handleGetMetricHistory(req, ret);
      } else if (action.equals(STATS_SET_ENABLEMETRICS)) {
        handleChangeManagerStatusRequest(req, ret, true);
      } else if (action.equals(STATS_SET_DISABLEMETRICS)) {
        handleChangeManagerStatusRequest(req, ret, false);
      } else {
        ret.put(RESPONSE_ERROR, "Invalid action");
      }
    }

    JSONUtils.toJSON(ret, resp.getOutputStream(), true);
  }

  /**
   * enable or disable metric Manager A disable will also purge all data from
   * all metric emitters
   */
  private void handleChangeManagerStatusRequest(HttpServletRequest req,
      Map<String, Object> ret, boolean enableMetricManager) {
    try {
      logger.info("Updating metric manager status");
      if ((enableMetricManager && MetricReportManager.isInstantiated())
          || MetricReportManager.isAvailable()) {
        MetricReportManager metricManager = MetricReportManager.getInstance();
        if (enableMetricManager) {
          metricManager.enableManager();
        } else {
          metricManager.disableManager();
        }
        ret.put(STATUS_PARAM, RESPONSE_SUCCESS);
      } else {
        ret.put(RESPONSE_ERROR, "MetricManager is not available");
      }
    } catch (Exception e) {
      logger.error(e);
      ret.put(RESPONSE_ERROR, e.getMessage());
    }
  }

  /**
   * Update number of display snapshots for /stats graphs
   */
  private void handleChangeEmitterPoints(HttpServletRequest req,
      Map<String, Object> ret) {
    try {
      long numInstance = getLongParam(req, STATS_MAP_EMITTERNUMINSTANCES);
      if (MetricReportManager.isAvailable()) {
        MetricReportManager metricManager = MetricReportManager.getInstance();
        InMemoryMetricEmitter memoryEmitter =
            extractInMemoryMetricEmitter(metricManager);
        memoryEmitter.setReportingInstances(numInstance);
        ret.put(STATUS_PARAM, RESPONSE_SUCCESS);
      } else {
        ret.put(RESPONSE_ERROR, "MetricManager is not available");
      }
    } catch (Exception e) {
      logger.error(e);
      ret.put(RESPONSE_ERROR, e.getMessage());
    }
  }

  /**
   * Update InMemoryMetricEmitter interval to maintain metric snapshots
   */
  private void handleChangeCleaningInterval(HttpServletRequest req,
      Map<String, Object> ret) {
    try {
      long newInterval = getLongParam(req, STATS_MAP_CLEANINGINTERVAL);
      if (MetricReportManager.isAvailable()) {
        MetricReportManager metricManager = MetricReportManager.getInstance();
        InMemoryMetricEmitter memoryEmitter =
            extractInMemoryMetricEmitter(metricManager);
        memoryEmitter.setReportingInterval(newInterval);
        ret.put(STATUS_PARAM, RESPONSE_SUCCESS);
      } else {
        ret.put(RESPONSE_ERROR, "MetricManager is not available");
      }
    } catch (Exception e) {
      logger.error(e);
      ret.put(RESPONSE_ERROR, e.getMessage());
    }
  }

  /**
   * Get metric snapshots for a metric and date specification
   *
   * @throws ServletException
   */
  private void handleGetMetricHistory(HttpServletRequest req,
      Map<String, Object> ret) throws ServletException {
    if (MetricReportManager.isAvailable()) {
      MetricReportManager metricManager = MetricReportManager.getInstance();
      InMemoryMetricEmitter memoryEmitter =
          extractInMemoryMetricEmitter(metricManager);

      // if we have a memory emitter
      if (memoryEmitter != null) {
        try {
          List<InMemoryHistoryNode> result =
              memoryEmitter.getMetrics(
                  getParam(req, STATS_MAP_METRICNAMEPARAM),
                  parseDate(getParam(req, STATS_MAP_STARTDATE)),
                  parseDate(getParam(req, STATS_MAP_ENDDATE)),
                  getBooleanParam(req, STATS_MAP_METRICRETRIEVALMODE));

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
      ret.put(RESPONSE_ERROR, "MetricReportManager is not available");
    }
  }

  /**
   * Get InMemoryMetricEmitter, if available else null
   */
  private InMemoryMetricEmitter extractInMemoryMetricEmitter(
      MetricReportManager metricManager) {
    InMemoryMetricEmitter memoryEmitter = null;
    for (IMetricEmitter emitter : metricManager.getMetricEmitters()) {
      if (emitter instanceof InMemoryMetricEmitter) {
        memoryEmitter = (InMemoryMetricEmitter) emitter;
        break;
      }
    }
    return memoryEmitter;
  }

  /**
   * Get all the metrics tracked by metric manager
   */
  private void handleGetAllMMetricsName(HttpServletRequest req,
      Map<String, Object> ret) {
    if (MetricReportManager.isAvailable()) {
      MetricReportManager metricManager = MetricReportManager.getInstance();
      List<IMetric<?>> result = metricManager.getAllMetrics();
      if (result.size() == 0) {
        ret.put(RESPONSE_ERROR, "No Metric being tracked");
      } else {
        List<String> metricNames = new LinkedList<String>();
        for (IMetric<?> metric : result) {
          metricNames.add(metric.getName());
        }
        ret.put("data", metricNames);
      }
    } else {
      ret.put(RESPONSE_ERROR, "MetricReportManager is not available");
    }
  }

  /**
   * Update tracking interval for a given metrics
   *
   * @throws ServletException
   */
  private void handleChangeMetricInterval(HttpServletRequest req,
      Map<String, Object> ret) throws ServletException {
    try {
      String metricName = getParam(req, STATS_MAP_METRICNAMEPARAM);
      long newInterval = getLongParam(req, STATS_MAP_REPORTINGINTERVAL);
      if (MetricReportManager.isAvailable()) {
        MetricReportManager metricManager = MetricReportManager.getInstance();
        TimeBasedReportingMetric<?> metric =
            (TimeBasedReportingMetric<?>) metricManager
                .getMetricFromName(metricName);
        metric.updateInterval(newInterval);
        ret.put(STATUS_PARAM, RESPONSE_SUCCESS);
      } else {
        ret.put(RESPONSE_ERROR, "MetricManager is not available");
      }
    } catch (Exception e) {
      logger.error(e);
      ret.put(RESPONSE_ERROR, e.getMessage());
    }
  }

  private Date parseDate(String date) throws ParseException {
    DateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
    return format.parse(date);
  }
}
