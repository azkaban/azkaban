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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.JSONUtils;
import azkaban.server.AbstractServiceServlet;

public class ExecutorManagerServlet extends AbstractServiceServlet {
  private final ExecutorManagerAdapter executorManager;

  public static final String URL = "executorManager";
  private static final long serialVersionUID = 1L;
  private static final Logger logger = Logger
      .getLogger(ExecutorManagerServlet.class);

  public ExecutorManagerServlet(ExecutorManagerAdapter executorManager) {
    this.executorManager = executorManager;
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    HashMap<String, Object> respMap = new HashMap<String, Object>();
    try {
      if (!hasParam(req, ExecutorManagerAdapter.INFO_ACTION)) {
        logger.error("Parameter action not set");
        respMap.put("error", "Parameter action not set");
      } else {
        String action = getParam(req, ExecutorManagerAdapter.INFO_ACTION);
        if (action.equals(ExecutorManagerAdapter.ACTION_UPDATE)) {
          handleAjaxUpdateRequest(req, respMap);
        } else {
          int execid =
              Integer.parseInt(getParam(req,
                  ExecutorManagerAdapter.INFO_EXEC_ID));
          String user =
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
    } catch (Exception e) {
      logger.error(e);
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e.getMessage());
    }
    writeJSON(resp, respMap);
    resp.flushBuffer();
  }

  private void handleModifyExecution(HashMap<String, Object> respMap,
      int execid, String user, HttpServletRequest req) {
    if (!hasParam(req, ExecutorManagerAdapter.INFO_MODIFY_COMMAND)) {
      respMap.put(ExecutorManagerAdapter.INFO_ERROR,
          "Modification command not set.");
      return;
    }

    try {
      String modificationType =
          getParam(req, ExecutorManagerAdapter.INFO_MODIFY_COMMAND);
      ExecutableFlow exflow = executorManager.getExecutableFlow(execid);
      if (ExecutorManagerAdapter.COMMAND_MODIFY_RETRY_FAILURES
          .equals(modificationType)) {
        executorManager.retryFailures(exflow, user);
      }
    } catch (Exception e) {
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }
  }

  private void handleAjaxResumeFlow(HashMap<String, Object> respMap,
      int execid, String user) {
    try {
      ExecutableFlow exFlow = executorManager.getExecutableFlow(execid);
      executorManager.resumeFlow(exFlow, user);
    } catch (Exception e) {
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }

  }

  private void handleAjaxPauseFlow(HashMap<String, Object> respMap, int execid,
      String user) {
    try {
      ExecutableFlow exFlow = executorManager.getExecutableFlow(execid);
      executorManager.pauseFlow(exFlow, user);
    } catch (Exception e) {
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }
  }

  private void handleAjaxCancelFlow(HashMap<String, Object> respMap,
      int execid, String user) {
    try {
      ExecutableFlow exFlow = executorManager.getExecutableFlow(execid);
      executorManager.cancelFlow(exFlow, user);
    } catch (Exception e) {
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }
  }

  private void handleAjaxSubmitFlow(HttpServletRequest req,
      HashMap<String, Object> respMap, int execid) {
    try {
      String execFlowJson =
          getParam(req, ExecutorManagerAdapter.INFO_EXEC_FLOW_JSON);
      ExecutableFlow exflow =
          ExecutableFlow.createExecutableFlowFromObject(JSONUtils
              .parseJSONFromString(execFlowJson));
      String user = getParam(req, ExecutorManagerAdapter.INFO_USER_ID);
      executorManager.submitExecutableFlow(exflow, user);
      respMap.put(ExecutorManagerAdapter.INFO_EXEC_ID, exflow.getExecutionId());
    } catch (Exception e) {
      e.printStackTrace();
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }
  }

  private void handleFetchJobLogEvent(int execid, HttpServletRequest req,
      HttpServletResponse resp, HashMap<String, Object> respMap) {
    try {
      ExecutableFlow exFlow = executorManager.getExecutableFlow(execid);
      String jobId = getParam(req, ExecutorManagerAdapter.INFO_JOB_NAME);
      int offset = getIntParam(req, ExecutorManagerAdapter.INFO_OFFSET);
      int length = getIntParam(req, ExecutorManagerAdapter.INFO_LENGTH);
      int attempt = getIntParam(req, ExecutorManagerAdapter.INFO_ATTEMPT);
      LogData log =
          executorManager.getExecutionJobLog(exFlow, jobId, offset, length,
              attempt);
      respMap.put(ExecutorManagerAdapter.INFO_LOG,
          JSONUtils.toJSON(log.toObject()));
    } catch (Exception e) {
      e.printStackTrace();
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }
  }

  private void handleFetchFlowLogEvent(int execid, HttpServletRequest req,
      HttpServletResponse resp, HashMap<String, Object> respMap) {
    try {
      ExecutableFlow exFlow = executorManager.getExecutableFlow(execid);
      int offset = getIntParam(req, ExecutorManagerAdapter.INFO_OFFSET);
      int length = getIntParam(req, ExecutorManagerAdapter.INFO_LENGTH);
      LogData log =
          executorManager.getExecutableFlowLog(exFlow, offset, length);
      respMap.put(ExecutorManagerAdapter.INFO_LOG,
          JSONUtils.toJSON(log.toObject()));
    } catch (Exception e) {
      e.printStackTrace();
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }

  }

  @SuppressWarnings("unchecked")
  private void handleAjaxUpdateRequest(HttpServletRequest req,
      HashMap<String, Object> respMap) {
    try {
      ArrayList<Object> updateTimesList =
          (ArrayList<Object>) JSONUtils.parseJSONFromString(getParam(req,
              ExecutorManagerAdapter.INFO_UPDATE_TIME_LIST));
      ArrayList<Object> execIDList =
          (ArrayList<Object>) JSONUtils.parseJSONFromString(getParam(req,
              ExecutorManagerAdapter.INFO_EXEC_ID_LIST));

      ArrayList<Object> updateList = new ArrayList<Object>();
      for (int i = 0; i < execIDList.size(); ++i) {
        long updateTime = JSONUtils.getLongFromObject(updateTimesList.get(i));
        int execId = (Integer) execIDList.get(i);

        ExecutableFlow flow = executorManager.getExecutableFlow(execId);
        if (flow == null) {
          Map<String, Object> errorResponse = new HashMap<String, Object>();
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
    } catch (Exception e) {
      e.printStackTrace();
      respMap.put(ExecutorManagerAdapter.INFO_ERROR, e);
    }
  }

}
