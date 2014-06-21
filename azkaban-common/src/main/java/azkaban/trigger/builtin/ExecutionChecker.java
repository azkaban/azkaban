/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.trigger.builtin;

import java.util.HashMap;
import java.util.Map;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.trigger.ConditionChecker;

public class ExecutionChecker implements ConditionChecker {

  public static final String type = "ExecutionChecker";
  public static ExecutorManagerAdapter executorManager;

  private String checkerId;
  private int execId;
  private String jobName;
  private Status wantedStatus;

  public ExecutionChecker(String checkerId, int execId, String jobName,
      Status wantedStatus) {
    this.checkerId = checkerId;
    this.execId = execId;
    this.jobName = jobName;
    this.wantedStatus = wantedStatus;
  }

  public static void setExecutorManager(ExecutorManagerAdapter em) {
    executorManager = em;
  }

  @Override
  public Object eval() {
    ExecutableFlow exflow;
    try {
      exflow = executorManager.getExecutableFlow(execId);
    } catch (ExecutorManagerException e) {
      e.printStackTrace();
      return Boolean.FALSE;
    }
    if (jobName != null) {
      ExecutableNode job = exflow.getExecutableNode(jobName);
      if (job != null) {
        return job.getStatus().equals(wantedStatus);
      } else {
        return Boolean.FALSE;
      }
    } else {
      return exflow.getStatus().equals(wantedStatus);
    }

  }

  @Override
  public Object getNum() {
    return null;
  }

  @Override
  public void reset() {
  }

  @Override
  public String getId() {
    return checkerId;
  }

  @Override
  public String getType() {
    return type;
  }

  public static ExecutionChecker createFromJson(HashMap<String, Object> jsonObj)
      throws Exception {
    if (!jsonObj.get("type").equals(type)) {
      throw new Exception("Cannot create checker of " + type + " from "
          + jsonObj.get("type"));
    }
    int execId = Integer.valueOf((String) jsonObj.get("execId"));
    String jobName = null;
    if (jsonObj.containsKey("jobName")) {
      jobName = (String) jsonObj.get("jobName");
    }
    String checkerId = (String) jsonObj.get("checkerId");
    Status wantedStatus = Status.valueOf((String) jsonObj.get("wantedStatus"));

    return new ExecutionChecker(checkerId, execId, jobName, wantedStatus);
  }

  @SuppressWarnings("unchecked")
  @Override
  public ConditionChecker fromJson(Object obj) throws Exception {
    return createFromJson((HashMap<String, Object>) obj);
  }

  @Override
  public Object toJson() {
    Map<String, Object> jsonObj = new HashMap<String, Object>();
    jsonObj.put("type", type);
    jsonObj.put("execId", String.valueOf(execId));
    if (jobName != null) {
      jsonObj.put("jobName", jobName);
    }
    jsonObj.put("wantedStatus", wantedStatus.toString());
    jsonObj.put("checkerId", checkerId);
    return jsonObj;
  }

  @Override
  public void stopChecker() {
  }

  @Override
  public void setContext(Map<String, Object> context) {
  }

  @Override
  public long getNextCheckTime() {
    return -1;
  }

}
