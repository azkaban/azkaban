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

package azkaban.executor;

import azkaban.server.AbstractServiceServlet;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.JSONUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

public class ExecutorManagerServlet extends AbstractServiceServlet {

  public static final String URL = "executorManager";
  private static final long serialVersionUID = 1L;
  private static final Logger logger = Logger
      .getLogger(ExecutorManagerServlet.class);
  private final ExecutorManagerAdapter executorManager;

  public ExecutorManagerServlet(final ExecutorManagerAdapter executorManager) {
    this.executorManager = executorManager;
  }

  @Override
  public void doGet(final HttpServletRequest req, final HttpServletResponse resp)
      throws ServletException, IOException {
    final HashMap<String, Object> respMap = new HashMap<>();
    try {
      if (!hasParam(req, ExecutorManagerAdapter.INFO_ACTION)) {
        logger.error("Parameter action not set");
        respMap.put("error", "Parameter action not set");
      } else {
        final String action = getParam(req, ExecutorManagerAdapter.INFO_ACTION);
        if (action.equals(ExecutorManagerAdapter.ACTION_UPDATE)) {
          handleAjaxUpdateRequest(req, respMap);
        } else {
          final int execid =
              Integer.parseInt(getParam(req,
                  ExecutorManagerAdapter.INFO_EXEC_ID));
          final String user =
              getParam(req, ExecutorManagerAdapter.INFO_USER_ID, null);

          logger.info("User " + user + " has called action " + action + " on "
              + execid);
          if (action.equals(ExecutorManagerAdapter.ACTION_GET_FLOW_LOG)) {
            handleFetchFlowLogEvent(execid, req, resp, respMap);
          } else if (action.equals(ExecutorManagerAdapter.ACTION_GET_JOB_LOG)) {
            handleFetchJobLogEvent(execid, req, resp, respMap);
          } else if (action.equals(ExecutorManagerAdapter.ACTION_SUBMIT_FLOW)) {
            handleAjaxSubmitFlow(req, respMap, execid);
          } else if (action.equals(ExecutorManagerAdapter.ACTION_CANCEL_FLOW)) {
            logger.info("Cancel called.");
            handleAjaxCancelFlow(respMap, execid, user);
          } else if (action.equals(ExecutorManagerAdapter.ACTION_PAUSE_FLOW)) {
            logger.info("Paused called.");
            handleAjaxPauseFlow(respMap, execid, user);
          } else if (action.equals(ExecutorManagerAdapter.ACTION_RESUME_FLOW)) {
            logger.info("Resume called.");
            handleAjaxResumeFlow(respMap, execid, user);
          } else if (action
              .equals(ExecutorManagerAdapter.ACTION_MODIFY_EXECUTION)) {
            logger.info("Modify Execution Action");
            handleModifyExecution(respMap, execid, user, req);
          } else {
            logger.error("action: '" + action + "' not supported.");
            respMap.put("error", "action: '" + action + "' not supported.");
          }
        }
      }
    } catch (final Exception e) {
      logger.error(e);
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e.getMessage());
    }
    writeJSON(resp, respMap);
    resp.flushBuffer();
  }

  private void handleModifyExecution(final HashMap<String, Object> respMap,
      final int execid, final String user, final HttpServletRequest req) {
    if (!hasParam(req, ExecutorManagerAdapter.INFO_MODIFY_COMMAND)) {
      respMap.put(ExecutorManagerAdapter.INFO_ERROR,
          "Modification command not set.");
      return;
    }

    try {
      final String modificationType =
          getParam(req, ExecutorManagerAdapter.INFO_MODIFY_COMMAND);
      final ExecutableFlow exflow = this.executorManager.getExecutableFlow(execid);
      if (ExecutorManagerAdapter.COMMAND_MODIFY_RETRY_FAILURES
          .equals(modificationType)) {
        this.executorManager.retryFailures(exflow, user);
      }
    } catch (final Exception e) {
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }
  }

  private void handleAjaxResumeFlow(final HashMap<String, Object> respMap,
      final int execid, final String user) {
    try {
      final ExecutableFlow exFlow = this.executorManager.getExecutableFlow(execid);
      this.executorManager.resumeFlow(exFlow, user);
    } catch (final Exception e) {
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }

  }

  private void handleAjaxPauseFlow(final HashMap<String, Object> respMap, final int execid,
      final String user) {
    try {
      final ExecutableFlow exFlow = this.executorManager.getExecutableFlow(execid);
      this.executorManager.pauseFlow(exFlow, user);
    } catch (final Exception e) {
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }
  }

  private void handleAjaxCancelFlow(final HashMap<String, Object> respMap,
      final int execid, final String user) {
    try {
      final ExecutableFlow exFlow = this.executorManager.getExecutableFlow(execid);
      this.executorManager.cancelFlow(exFlow, user);
    } catch (final Exception e) {
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }
  }

  private void handleAjaxSubmitFlow(final HttpServletRequest req,
      final HashMap<String, Object> respMap, final int execid) {
    try {
      final String execFlowJson =
          getParam(req, ExecutorManagerAdapter.INFO_EXEC_FLOW_JSON);
      final ExecutableFlow exflow =
          ExecutableFlow.createExecutableFlowFromObject(JSONUtils
              .parseJSONFromString(execFlowJson));
      final String user = getParam(req, ExecutorManagerAdapter.INFO_USER_ID);
      this.executorManager.submitExecutableFlow(exflow, user);
      respMap.put(ExecutorManagerAdapter.INFO_EXEC_ID, exflow.getExecutionId());
    } catch (final Exception e) {
      e.printStackTrace();
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }
  }

  private void handleFetchJobLogEvent(final int execid, final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> respMap) {
    try {
      final ExecutableFlow exFlow = this.executorManager.getExecutableFlow(execid);
      final String jobId = getParam(req, ExecutorManagerAdapter.INFO_JOB_NAME);
      final int offset = getIntParam(req, ExecutorManagerAdapter.INFO_OFFSET);
      final int length = getIntParam(req, ExecutorManagerAdapter.INFO_LENGTH);
      final int attempt = getIntParam(req, ExecutorManagerAdapter.INFO_ATTEMPT);
      final LogData log =
          this.executorManager.getExecutionJobLog(exFlow, jobId, offset, length,
              attempt);
      respMap.put(ExecutorManagerAdapter.INFO_LOG,
          JSONUtils.toJSON(log.toObject()));
    } catch (final Exception e) {
      e.printStackTrace();
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }
  }

  private void handleFetchFlowLogEvent(final int execid, final HttpServletRequest req,
      final HttpServletResponse resp, final HashMap<String, Object> respMap) {
    try {
      final ExecutableFlow exFlow = this.executorManager.getExecutableFlow(execid);
      final int offset = getIntParam(req, ExecutorManagerAdapter.INFO_OFFSET);
      final int length = getIntParam(req, ExecutorManagerAdapter.INFO_LENGTH);
      final LogData log =
          this.executorManager.getExecutableFlowLog(exFlow, offset, length);
      respMap.put(ExecutorManagerAdapter.INFO_LOG,
          JSONUtils.toJSON(log.toObject()));
    } catch (final Exception e) {
      e.printStackTrace();
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }

  }

  private void handleAjaxUpdateRequest(final HttpServletRequest req,
      final HashMap<String, Object> respMap) {
    try {
      final ArrayList<Object> updateTimesList =
          (ArrayList<Object>) JSONUtils.parseJSONFromString(getParam(req,
              ExecutorManagerAdapter.INFO_UPDATE_TIME_LIST));
      final ArrayList<Object> execIDList =
          (ArrayList<Object>) JSONUtils.parseJSONFromString(getParam(req,
              ExecutorManagerAdapter.INFO_EXEC_ID_LIST));

      final ArrayList<Object> updateList = new ArrayList<>();
      for (int i = 0; i < execIDList.size(); ++i) {
        final long updateTime = JSONUtils.getLongFromObject(updateTimesList.get(i));
        final int execId = (Integer) execIDList.get(i);

        final ExecutableFlow flow = this.executorManager.getExecutableFlow(execId);
        if (flow == null) {
          final Map<String, Object> errorResponse = new HashMap<>();
          errorResponse.put(ExecutorManagerAdapter.INFO_ERROR,
              "Flow does not exist");
          errorResponse.put(ExecutorManagerAdapter.INFO_EXEC_ID, execId);
          updateList.add(errorResponse);
          continue;
        }

        if (flow.getUpdateTime() > updateTime) {
          updateList.add(flow.toUpdateObject(updateTime));
        }
      }

      respMap.put(ExecutorManagerAdapter.INFO_UPDATES, updateList);
    } catch (final Exception e) {
      e.printStackTrace();
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }
  }

}
