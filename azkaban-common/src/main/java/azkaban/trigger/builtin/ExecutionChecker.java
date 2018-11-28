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

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.trigger.ConditionChecker;
import java.util.HashMap;
import java.util.Map;

public class ExecutionChecker implements ConditionChecker {

  public static final String type = "ExecutionChecker";
  public static ExecutorManagerAdapter executorManagerAdapter;

  private final String checkerId;
  private final int execId;
  private final String jobName;
  private final Status wantedStatus;

  public ExecutionChecker(final String checkerId, final int execId, final String jobName,
      final Status wantedStatus) {
    this.checkerId = checkerId;
    this.execId = execId;
    this.jobName = jobName;
    this.wantedStatus = wantedStatus;
  }

  public static void setExecutorManagerAdapter(final ExecutorManagerAdapter em) {
    executorManagerAdapter = em;
  }

  public static ExecutionChecker createFromJson(final HashMap<String, Object> jsonObj)
      throws Exception {
    if (!jsonObj.get("type").equals(type)) {
      throw new Exception("Cannot create checker of " + type + " from "
          + jsonObj.get("type"));
    }
    final int execId = Integer.valueOf((String) jsonObj.get("execId"));
    String jobName = null;
    if (jsonObj.containsKey("jobName")) {
      jobName = (String) jsonObj.get("jobName");
    }
    final String checkerId = (String) jsonObj.get("checkerId");
    final Status wantedStatus = Status.valueOf((String) jsonObj.get("wantedStatus"));

    return new ExecutionChecker(checkerId, execId, jobName, wantedStatus);
  }

  @Override
  public Object eval() {
    final ExecutableFlow exflow;
    try {
      exflow = executorManagerAdapter.getExecutableFlow(this.execId);
    } catch (final ExecutorManagerException e) {
      e.printStackTrace();
      return Boolean.FALSE;
    }
    if (this.jobName != null) {
      final ExecutableNode job = exflow.getExecutableNode(this.jobName);
      if (job != null) {
        return job.getStatus().equals(this.wantedStatus);
      } else {
        return Boolean.FALSE;
      }
    } else {
      return exflow.getStatus().equals(this.wantedStatus);
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
    return this.checkerId;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public ConditionChecker fromJson(final Object obj) throws Exception {
    return createFromJson((HashMap<String, Object>) obj);
  }

  @Override
  public Object toJson() {
    final Map<String, Object> jsonObj = new HashMap<>();
    jsonObj.put("type", type);
    jsonObj.put("execId", String.valueOf(this.execId));
    if (this.jobName != null) {
      jsonObj.put("jobName", this.jobName);
    }
    jsonObj.put("wantedStatus", this.wantedStatus.toString());
    jsonObj.put("checkerId", this.checkerId);
    return jsonObj;
  }

  @Override
  public void stopChecker() {
  }

  @Override
  public void setContext(final Map<String, Object> context) {
  }

  @Override
  public long getNextCheckTime() {
    return -1;
  }

}
