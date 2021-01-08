/*
 * Copyright 2021 LinkedIn Corp.
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

package azkaban.container;

import azkaban.Constants;
import azkaban.executor.ConnectorParams;
import azkaban.executor.ExecutorManagerException;
import azkaban.server.HttpRequestUtils;
import azkaban.utils.FileIOUtils;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.map.ObjectMapper;

import static azkaban.common.ServletUtils.writeJSON;
import static azkaban.server.HttpRequestUtils.hasParam;
import static azkaban.server.HttpRequestUtils.getParam;
import static azkaban.server.HttpRequestUtils.getIntParam;


public class ContainerServlet extends HttpServlet implements ConnectorParams {

  public static final String JSON_MIME_TYPE = "application/json";
  private static final Logger logger = LoggerFactory.getLogger(ContainerServlet.class);
  private FlowContainer flowContainer;

  public ContainerServlet() { super(); }

  @Override
  public void init(final ServletConfig config) {
    this.flowContainer =
        (FlowContainer) config.getServletContext().getAttribute(
            Constants.AZKABAN_CONTAINER_CONTEXT_KEY);

    if (this.flowContainer == null) {
      throw new IllegalStateException(
          "No flow container defined in this servlet context!");
    }
  }

  @Override
  public void doGet(final HttpServletRequest req, final HttpServletResponse resp)
      throws ServletException, IOException {
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
      } else if (!hasParam(req, ConnectorParams.EXECID_PARAM)) {
        logger.error("Parameter execId not provided");
        respMap.put(ConnectorParams.RESPONSE_ERROR, "Parameter execId not provided");
      } else {
        final String action = getParam(req, ConnectorParams.ACTION_PARAM);
        if (!(action.equals(ConnectorParams.CANCEL_ACTION) ||
              action.equals(ConnectorParams.LOG_ACTION)) ||
              action.equals(ConnectorParams.METADATA_ACTION)) {
          // Only the above 3 actions are supported for Containerized ecosystem
          respMap.put(ConnectorParams.RESPONSE_ERROR, "Unsupported action type: " + action);
        } else {
          final int execid = Integer.parseInt(getParam(req, ConnectorParams.EXECID_PARAM));
          final String user = getParam(req, ConnectorParams.USER_PARAM, null);

          logger.info("User " + user + " has called action " + action + " on " + execid);

          if (action.equals(ConnectorParams.CANCEL_ACTION)) {
            handleAjaxCancel(respMap, execid, user);
          } else if (action.equals(ConnectorParams.METADATA_ACTION)) {
            handleFetchMetaDataEvent(execid, req, resp, respMap);
          } else {
            // action == LOG_ACTION
            handleFetchLogEvent(execid, req, resp, respMap);
          }
        }
      }
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      respMap.put(ConnectorParams.RESPONSE_ERROR, e.getMessage());
    }
    writeJSON(resp, respMap);
    resp.flushBuffer();
  }

  private void handleAjaxCancel(final Map<String, Object> respMap, final int execid,
      final String user) {
    if (user == null) {
      respMap.put(ConnectorParams.RESPONSE_ERROR, "user has not been set");
      return;
    }

    try {
      this.flowContainer.cancelFlow(execid, user);
      respMap.put(ConnectorParams.STATUS_PARAM, ConnectorParams.RESPONSE_SUCCESS);
    } catch (final ExecutorManagerException e) {
      logger.error(e.getMessage(), e);
      respMap.put(ConnectorParams.RESPONSE_ERROR, e.getMessage());
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
      final FileIOUtils.LogData result;
      try {
        result = this.flowContainer.readFlowLogs(execId, startByte, length);
        respMap.putAll(result.toObject());
      } catch (final Exception e) {
        logger.error(e.getMessage(), e);
        respMap.put(ConnectorParams.RESPONSE_ERROR, e.getMessage());
      }
    } else {
      final int attempt = getIntParam(req, "attempt", 0);
      final String jobId = getParam(req, "jobId");
      try {
        final FileIOUtils.LogData result =
            this.flowContainer.readJobLogs(execId, jobId, attempt, startByte,
                length);
        respMap.putAll(result.toObject());
      } catch (final Exception e) {
        logger.error(e.getMessage(), e);
        respMap.put("error", e.getMessage());
      }
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
      final FileIOUtils.JobMetaData result =
          this.flowContainer.readJobMetaData(execId, jobId, attempt, startByte,
              length);
      respMap.putAll(result.toObject());
    } catch (final Exception e) {
      logger.error(e.getMessage(), e);
      respMap.put("error", e.getMessage());
    }
  }
}
