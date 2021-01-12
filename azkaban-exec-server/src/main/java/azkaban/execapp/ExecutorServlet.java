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

package azkaban.execapp;

import static azkaban.common.ServletUtils.*;
import static azkaban.server.HttpRequestUtils.*;
import static java.util.Objects.requireNonNull;

import azkaban.Constants;
import azkaban.executor.ConnectorParams;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.utils.FileIOUtils.JobMetaData;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.JSONUtils;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;


public class ExecutorServlet extends HttpServlet implements ConnectorParams {

  private static final Logger logger = Logger.getLogger(ExecutorServlet.class
      .getName());
  private static final long serialVersionUID = -3528600004096666451L;
  private AzkabanExecutorServer application;
  private FlowRunnerManager flowRunnerManager;

  public ExecutorServlet() {
    super();
  }

  @Override
  public void init(final ServletConfig config) {
    this.application =
        (AzkabanExecutorServer) config.getServletContext().getAttribute(
            Constants.AZKABAN_SERVLET_CONTEXT_KEY);

    if (this.application == null) {
      throw new IllegalStateException(
          "No batch application is defined in the servlet context!");
    }

    this.flowRunnerManager = this.application.getFlowRunnerManager();
  }



  /**
   * @deprecated GET available for seamless upgrade. azkaban-web now uses POST.
   */
  @Deprecated
  @Override
  public void doGet(final HttpServletRequest req, final HttpServletResponse resp)
      throws IOException {
    handleRequest(req, resp);
  }

  @Override
  public void doPost(final HttpServletRequest req, final HttpServletResponse resp)
      throws IOException {
    handleRequest(req, resp);
  }

  public void handleRequest(final HttpServletRequest req, final HttpServletResponse resp)
      throws IOException {
    final HashMap<String, Object> respMap = new HashMap<>();
    try {
      if (!hasParam(req, ConnectorParams.ACTION_PARAM)) {
        logger.error("Parameter action not set");
        respMap.put("error", "Parameter action not set");
      } else {
        final String action = getParam(req, ConnectorParams.ACTION_PARAM);
        if (action.equals(ConnectorParams.UPDATE_ACTION)) {
          handleAjaxUpdateRequest(req, respMap);
        } else if (action.equals(ConnectorParams.PING_ACTION)) {
          respMap.put(ConnectorParams.STATUS_PARAM, ConnectorParams.RESPONSE_ALIVE);
        } else if (action.equals(ConnectorParams.RELOAD_JOBTYPE_PLUGINS_ACTION)) {
          logger.info("Reloading Jobtype plugins");
          handleReloadJobTypePlugins(respMap);
        } else if (action.equals(ConnectorParams.ACTIVATE)) {
          logger.warn("Setting ACTIVE flag to true");
          setActive(true, respMap);
        } else if (action.equals(ConnectorParams.GET_STATUS)) {
          logger.debug("Get Executor Status: ");
          getStatus(respMap);
        } else if (action.equals(ConnectorParams.DEACTIVATE)) {
          logger.warn("Setting ACTIVE flag to false");
          setActive(false, respMap);
        } else if (action.equals(ConnectorParams.SHUTDOWN)) {
          shutdown(respMap);
        } else if (action.equals(ConnectorParams.MODIFY_PROPERTY_ACTION)) {
          String propName = getParam(req, ConnectorParams.PROPERTY_NAME_PARAM);
          String propValue = getParam(req, ConnectorParams.PROPERTY_VALUE_PARAM);
          handleModifyProperty(propName, propValue, respMap);
        } else {
          final int execid = Integer.parseInt(getParam(req, ConnectorParams.EXECID_PARAM));
          final String user = getParam(req, ConnectorParams.USER_PARAM, null);

          logger.info("User " + user + " has called action " + action + " on "
              + execid);
          if (action.equals(ConnectorParams.METADATA_ACTION)) {
            handleFetchMetaDataEvent(execid, req, resp, respMap);
          } else if (action.equals(ConnectorParams.LOG_ACTION)) {
            handleFetchLogEvent(execid, req, resp, respMap);
          } else if (action.equals(ConnectorParams.ATTACHMENTS_ACTION)) {
            handleFetchAttachmentsEvent(execid, req, resp, respMap);
          } else if (action.equals(ConnectorParams.EXECUTE_ACTION)) {
            handleAjaxExecute(req, respMap, execid);
          } else if (action.equals(ConnectorParams.STATUS_ACTION)) {
            handleAjaxFlowStatus(respMap, execid);
          } else if (action.equals(ConnectorParams.CANCEL_ACTION)) {
            logger.info("Cancel called.");
            handleAjaxCancel(respMap, execid, user);
          } else if (action.equals(ConnectorParams.PAUSE_ACTION)) {
            logger.info("Paused called.");
            handleAjaxPause(respMap, execid, user);
          } else if (action.equals(ConnectorParams.RESUME_ACTION)) {
            logger.info("Resume called.");
            handleAjaxResume(respMap, execid, user);
          } else if (action.equals(ConnectorParams.MODIFY_EXECUTION_ACTION)) {
            logger.info("Modify Execution Action");
            handleModifyExecutionRequest(respMap, execid, user, req);
          } else {
            logger.error("action: '" + action + "' not supported.");
            respMap.put("error", "action: '" + action + "' not supported.");
          }
        }
      }
    } catch (final Exception e) {
      logger.error(e.getMessage(), e);
      respMap.put(ConnectorParams.RESPONSE_ERROR, e.getMessage());
    }
    writeJSON(resp, respMap);
    resp.flushBuffer();
  }

  private void handleModifyProperty(@Nullable final String propName,
      @Nullable final String propValue, final HashMap<String, Object> respMap)
      throws ServletException {
    if (propName == null || propValue == null) {
      String errMsg =
          String.format("Both %s and %s need to be provided for action %s. "  +
              "(%s, %s) = (%s, %s)", ConnectorParams.PROPERTY_NAME_PARAM, ConnectorParams.PROPERTY_VALUE_PARAM,
          ConnectorParams.MODIFY_PROPERTY_ACTION, ConnectorParams.PROPERTY_NAME_PARAM,
          ConnectorParams.PROPERTY_VALUE_PARAM, propName, propValue);
      logger.error(errMsg);
      respMap.put(ConnectorParams.RESPONSE_ERROR, errMsg);
      return;
    }

    if (propName.equals(ConnectorParams.POLLING_INTERVAL_MILLIS_PROPERTY_NAME)) {
      modifyPollingInterval(propValue, respMap);
    } else {
      String errMsg = String.format("Modification of %s property is not supported.", propName);
      logger.error(errMsg);
      respMap.put(ConnectorParams.RESPONSE_ERROR, errMsg);
    }
  }

  /**
   * Modifies the time interval at which the executor is polling the queue for unassigned jobs, and
   * assigning a job(s) to itself.
   *
   * @param newPollingIntervalMillis The desired polling interval. It needs to be a positive
   *                                 integer (milliseconds) in String format.
   * @param respMap The response map.
   */

  private void modifyPollingInterval(@Nonnull final String newPollingIntervalMillis,
      final HashMap<String, Object> respMap) {
    int pollingIntervalMillis = 0;
    boolean isValidValue = false;
    try {
      pollingIntervalMillis = Integer.parseUnsignedInt(newPollingIntervalMillis);
      isValidValue =  (pollingIntervalMillis > 0);
    } catch (NumberFormatException ex) {
      // isValidValue will continue to be false, which we will used for error handling.
      // No need to do anything additional in the catch block.
    }

    if (!isValidValue) {
      String errMsg = String.format("%s doesn't look like a positive integer." +
              " (%s, %s) = (%s, %s)", newPollingIntervalMillis, ConnectorParams.PROPERTY_NAME_PARAM,
          ConnectorParams.PROPERTY_VALUE_PARAM, ConnectorParams.POLLING_INTERVAL_MILLIS_PROPERTY_NAME,
          newPollingIntervalMillis);
      logger.error(errMsg);
      respMap.put(ConnectorParams.RESPONSE_ERROR, errMsg);
      return;
    }

    if (flowRunnerManager.changePollingInterval(pollingIntervalMillis)) {
      respMap.put(ConnectorParams.STATUS_ACTION, String.format("Changed polling interval to %s ms",
          newPollingIntervalMillis));
    } else {
      respMap.put(ConnectorParams.RESPONSE_ERROR, "Failed to change polling interval. Please check"
          + " the logs for error messages from underlying functions. Please retry if existing "
          + "schedule got canceled and new one didn't start.");
    }
  }

  private void handleModifyExecutionRequest(final Map<String, Object> respMap,
      final int execId, final String user, final HttpServletRequest req) throws ServletException {
    if (!hasParam(req, ConnectorParams.MODIFY_EXECUTION_ACTION_TYPE)) {
      respMap.put(ConnectorParams.RESPONSE_ERROR, "Modification type not set.");
    }
    final String modificationType = getParam(req, ConnectorParams.MODIFY_EXECUTION_ACTION_TYPE);

    try {
      if (ConnectorParams.MODIFY_RETRY_FAILURES.equals(modificationType)) {
        this.flowRunnerManager.retryFailures(execId, user);
      }
    } catch (final ExecutorManagerException e) {
      logger.error(e.getMessage(), e);
      respMap.put("error", e.getMessage());
    }
  }

  private void handleFetchLogEvent(final int execId, final HttpServletRequest req,
      final HttpServletResponse resp, final Map<String, Object> respMap)
      throws ServletException {
    final String type = getParam(req, "type");
    final int startByte = getIntParam(req, "offset");
    final int length = getIntParam(req, "length");

    resp.setContentType("text/plain");
    resp.setCharacterEncoding("utf-8");

    if (type.equals("flow")) {
      final LogData result;
      try {
        result = this.flowRunnerManager.readFlowLogs(execId, startByte, length);
        respMap.putAll(result.toObject());
      } catch (final Exception e) {
        logger.error(e.getMessage(), e);
        respMap.put(ConnectorParams.RESPONSE_ERROR, e.getMessage());
      }
    } else {
      final int attempt = getIntParam(req, "attempt", 0);
      final String jobId = getParam(req, "jobId");
      try {
        final LogData result =
            this.flowRunnerManager.readJobLogs(execId, jobId, attempt, startByte,
                length);
        respMap.putAll(result.toObject());
      } catch (final Exception e) {
        logger.error(e.getMessage(), e);
        respMap.put("error", e.getMessage());
      }
    }
  }

  private void handleFetchAttachmentsEvent(final int execId, final HttpServletRequest req,
      final HttpServletResponse resp, final Map<String, Object> respMap)
      throws ServletException {

    final String jobId = getParam(req, "jobId");
    final int attempt = getIntParam(req, "attempt", 0);
    try {
      final List<Object> result =
          this.flowRunnerManager.readJobAttachments(execId, jobId, attempt);
      respMap.put("attachments", result);
    } catch (final Exception e) {
      logger.error(e.getMessage(), e);
      respMap.put("error", e.getMessage());
    }
  }

  private void handleFetchMetaDataEvent(final int execId, final HttpServletRequest req,
      final HttpServletResponse resp, final Map<String, Object> respMap)
      throws ServletException {
    final int startByte = getIntParam(req, "offset");
    final int length = getIntParam(req, "length");

    resp.setContentType("text/plain");
    resp.setCharacterEncoding("utf-8");

    final int attempt = getIntParam(req, "attempt", 0);
    final String jobId = getParam(req, "jobId");
    try {
      final JobMetaData result =
          this.flowRunnerManager.readJobMetaData(execId, jobId, attempt, startByte,
              length);
      respMap.putAll(result.toObject());
    } catch (final Exception e) {
      logger.error(e.getMessage(), e);
      respMap.put("error", e.getMessage());
    }
  }

  private void handleAjaxUpdateRequest(final HttpServletRequest req,
      final Map<String, Object> respMap) throws ServletException, IOException {
    final ArrayList<Object> updateTimesList =
        (ArrayList<Object>) JSONUtils.parseJSONFromString(getParam(req,
            ConnectorParams.UPDATE_TIME_LIST_PARAM));
    final ArrayList<Object> execIDList =
        (ArrayList<Object>) JSONUtils.parseJSONFromString(getParam(req,
            ConnectorParams.EXEC_ID_LIST_PARAM));

    final ArrayList<Object> updateList = new ArrayList<>();
    for (int i = 0; i < execIDList.size(); ++i) {
      final long updateTime = JSONUtils.getLongFromObject(updateTimesList.get(i));
      final int execId = (Integer) execIDList.get(i);

      final ExecutableFlowBase flow = this.flowRunnerManager.getExecutableFlow(execId);
      if (flow == null) {
        final Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put(ConnectorParams.RESPONSE_ERROR, "Flow does not exist");
        errorResponse.put(ConnectorParams.UPDATE_MAP_EXEC_ID, execId);
        updateList.add(errorResponse);
        continue;
      }

      if (flow.getUpdateTime() > updateTime) {
        updateList.add(flow.toUpdateObject(updateTime));
      }
    }

    respMap.put(ConnectorParams.RESPONSE_UPDATED_FLOWS, updateList);
  }

  private void handleAjaxExecute(final HttpServletRequest req,
      final Map<String, Object> respMap, final int execId) {
    try {
      this.flowRunnerManager.submitFlow(execId);
    } catch (final ExecutorManagerException e) {
      logger.error(e.getMessage(), e);
      respMap.put(ConnectorParams.RESPONSE_ERROR, e.getMessage());
    }
  }

  private void handleAjaxFlowStatus(final Map<String, Object> respMap, final int execid) {
    final ExecutableFlowBase flow = this.flowRunnerManager.getExecutableFlow(execid);
    if (flow == null) {
      respMap.put(ConnectorParams.STATUS_PARAM, ConnectorParams.RESPONSE_NOTFOUND);
    } else {
      respMap.put(ConnectorParams.STATUS_PARAM, flow.getStatus().toString());
      respMap.put(ConnectorParams.RESPONSE_UPDATETIME, flow.getUpdateTime());
    }
  }

  private void handleAjaxPause(final Map<String, Object> respMap, final int execid,
      final String user) {
    if (user == null) {
      respMap.put(ConnectorParams.RESPONSE_ERROR, "user has not been set");
      return;
    }

    try {
      this.flowRunnerManager.pauseFlow(execid, user);
      respMap.put(ConnectorParams.STATUS_PARAM, ConnectorParams.RESPONSE_SUCCESS);
    } catch (final ExecutorManagerException e) {
      logger.error(e.getMessage(), e);
      respMap.put(ConnectorParams.RESPONSE_ERROR, e.getMessage());
    }
  }

  private void handleAjaxResume(final Map<String, Object> respMap, final int execid,
      final String user) throws ServletException {
    if (user == null) {
      respMap.put(ConnectorParams.RESPONSE_ERROR, "user has not been set");
      return;
    }

    try {
      this.flowRunnerManager.resumeFlow(execid, user);
      respMap.put(ConnectorParams.STATUS_PARAM, ConnectorParams.RESPONSE_SUCCESS);
    } catch (final ExecutorManagerException e) {
      logger.error(e.getMessage(), e);
      respMap.put(ConnectorParams.RESPONSE_ERROR, e.getMessage());
    }
  }

  private void handleAjaxCancel(final Map<String, Object> respMap, final int execid,
      final String user) {
    if (user == null) {
      respMap.put(ConnectorParams.RESPONSE_ERROR, "user has not been set");
      return;
    }

    try {
      this.flowRunnerManager.cancelFlow(execid, user);
      respMap.put(ConnectorParams.STATUS_PARAM, ConnectorParams.RESPONSE_SUCCESS);
    } catch (final ExecutorManagerException e) {
      logger.error(e.getMessage(), e);
      respMap.put(ConnectorParams.RESPONSE_ERROR, e.getMessage());
    }
  }

  private void handleReloadJobTypePlugins(final Map<String, Object> respMap) {
    try {
      this.flowRunnerManager.reloadJobTypePlugins();
      respMap.put(ConnectorParams.STATUS_PARAM, ConnectorParams.RESPONSE_SUCCESS);
    } catch (final Exception e) {
      logger.error(e.getMessage(), e);
      respMap.put(ConnectorParams.RESPONSE_ERROR, e.getMessage());
    }
  }

  private void setActive(final boolean value, final Map<String, Object> respMap) {
    try {
      setActiveInternal(value);
      respMap.put(ConnectorParams.STATUS_PARAM, ConnectorParams.RESPONSE_SUCCESS);
    } catch (final Exception e) {
      logger.error(e.getMessage(), e);
      respMap.put(ConnectorParams.RESPONSE_ERROR, e.getMessage());
    }
  }

  private void setActiveInternal(final boolean value)
      throws ExecutorManagerException, InterruptedException {
    this.flowRunnerManager.setExecutorActive(value,
        this.application.getHost(), this.application.getPort());
  }

  /**
   * Prepare the executor for shutdown.
   *
   * @param respMap json response object
   */
  private void shutdown(final Map<String, Object> respMap) {
    try {
      logger.warn("Shutting down executor...");

      // Set the executor to inactive. Will receive no new flows.
      setActiveInternal(false);
      this.application.shutdown();
      respMap.put(ConnectorParams.STATUS_PARAM, ConnectorParams.RESPONSE_SUCCESS);
    } catch (final Exception e) {
      logger.error(e.getMessage(), e);
      respMap.put(ConnectorParams.RESPONSE_ERROR, e.getMessage());
    }
  }

  private void getStatus(final Map<String, Object> respMap) {
    try {
      final ExecutorLoader executorLoader = this.application.getExecutorLoader();
      final Executor executor = requireNonNull(
          executorLoader.fetchExecutor(this.application.getHost(), this.application.getPort()),
          "The executor can not be null");

      respMap.put("executor_id", Integer.toString(executor.getId()));
      respMap.put("isActive", String.valueOf(executor.isActive()));
      respMap.put(ConnectorParams.STATUS_PARAM, ConnectorParams.RESPONSE_SUCCESS);
    } catch (final Exception e) {
      logger.error(e.getMessage(), e);
      respMap.put(ConnectorParams.RESPONSE_ERROR, e.getMessage());
    }
  }


}
