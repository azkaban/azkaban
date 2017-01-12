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

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.sla.SlaOption;
import azkaban.trigger.ConditionChecker;
import azkaban.utils.Utils;

import static java.util.Objects.requireNonNull;


public class SlaChecker implements ConditionChecker {
  public static final String type = "SlaChecker";
  enum NodeType { FLOW, JOB }
  enum ConditionType { FINISH, SUCCEED }

  private static final Logger logger = Logger.getLogger(SlaChecker.class);
  private static ExecutorManagerAdapter executorManager;

  private final String id;
  private final SlaOption slaOption;
  private final int execId;

  private ExecutableFlow flow = null;
  private ExecutableNode job = null;
  private NodeType nodeType;
  private ConditionType conditionType;
  private long startTime = -1;
  private long slaExpireTime = -1;

  /**
   * TODO remove static linking
   * @param executorManagerAdapter
   */
  public static void setExecutorManager(ExecutorManagerAdapter executorManagerAdapter) {
    executorManager = executorManagerAdapter;
  }

  public SlaChecker(String id, SlaOption slaOption, int execId) {
    this.id = id;
    this.slaOption = slaOption;
    this.execId = execId;

    retrieveSlaTypeProperties();
  }

  private void retrieveSlaTypeProperties() {
    switch (slaOption.getType()) {
      case SlaOption.TYPE_FLOW_FINISH:
        nodeType = NodeType.FLOW;
        conditionType = ConditionType.FINISH;
        break;
      case SlaOption.TYPE_FLOW_SUCCEED:
        nodeType = NodeType.FLOW;
        conditionType = ConditionType.SUCCEED;
        break;
      case SlaOption.TYPE_JOB_FINISH:
        nodeType = NodeType.JOB;
        conditionType = ConditionType.FINISH;
        break;
      case SlaOption.TYPE_JOB_SUCCEED:
        nodeType = NodeType.JOB;
        conditionType = ConditionType.SUCCEED;
        break;
    }
  }

  private long getStartTime() {
    switch (nodeType) {
      case FLOW: return flow.getStartTime();
      case JOB: return job.getStartTime();
    }
    return -1;
  }

  private Status getStatus() {
    switch (nodeType) {
      case FLOW: return flow.getStatus();
      case JOB: return job.getStatus();
    }
    return null;
  }

  /**
   * Initialize the instance.
   *
   * Initializing the derived members depends on ExecutorManager to be set.
   * Also, initializing the start time is possible when the job has already started.
   *
   * @return false on caught exceptions else true.
   */
  private boolean init() {
    try {
      requireNonNull(executorManager);
      if (flow == null) {
        flow = executorManager.getExecutableFlow(execId);
        if (NodeType.JOB == nodeType) {
          String jobName = (String) slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
          job = flow.getExecutableNode(jobName);
        }
      }
      // Start time needs to be initialized if unset
      if (startTime < 0) {
        long t = getStartTime();
        if (t > 0) {
          this.startTime = t;
          ReadablePeriod duration = Utils.parsePeriodString((String) slaOption.getInfo().get(SlaOption.INFO_DURATION));
          this.slaExpireTime = new DateTime(startTime).plus(duration).getMillis();
        }
      }
    } catch (ExecutorManagerException e) {
      // Log errors but don't crash
      logger.error("Can't get executable flow.", e);
      return false;
    }
    return true;
  }

  private boolean hasSlaExpired() {
    return slaExpireTime < DateTime.now().getMillis();
  }

  // return true to trigger sla action
  @Override
  public Object eval() {
    logger.info("Checking sla for execution " + execId);
    return isSlaFailed();
  }

  /**
   * TODO Remove convoluted logic. Investigate the need for 2 separate methods for pass and fail test
   * DO NOT DELETE METHOD. This is used by some weird reflection code using JEXL expression
   *
   * @return
   */
  @SuppressWarnings("unused")
  public Object isSlaFailed() {
    if (!init()) {
      // Don't trigger SLA actions if unable to init
      return false;
    }
    if (startTime < 0) {
      return false;
    }

    /*
     * Fire the trigger (return true) if the desired terminal state isn't reached when
     *  - the SLA time has already expired, OR
     *  - the flow / job has already reached a terminal state.
     */
    Status status = getStatus();
    return (isTerminalState(status) || hasSlaExpired()) && !hasReachedDesiredTerminalState();
  }

  /**
   * TODO Remove convoluted logic. Investigate the need for 2 separate methods for pass and fail test
   * DO NOT DELETE METHOD. This is used by some weird reflection code using JEXL expression
   *
   * @return
   */
  @SuppressWarnings("unused")
  public Object isSlaPassed() {
    if (!init()) {
      return true;
    }
    if (startTime < 0) {
      return false;
    }
    return hasReachedDesiredTerminalState();
  }

  private boolean hasReachedDesiredTerminalState() {
    Status status = getStatus();
    switch (conditionType) {
      case FINISH:
        return isTerminalState(status);
      case SUCCEED:
        return hasSucceeded(status);
    }
    return false;
  }

  /**
   * Terminal States are final states post which there will not be any further changes in state.
   *
   * @param status status of flow / job
   * @return true if status is a terminal state
   */
  private boolean isTerminalState(Status status) {
    return Status.FAILED.equals(status) || Status.KILLED.equals(status) || hasSucceeded(status);
  }

  private boolean hasSucceeded(Status status) {
    return Status.SUCCEEDED.equals(status);
  }

  @Override
  public Object getNum() {
    return null;
  }

  @Override
  public void reset() { }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public ConditionChecker fromJson(Object obj) throws Exception {
    return createFromJson(obj);
  }

  public static SlaChecker createFromJson(Object obj) throws Exception {
    return createFromJson((Map<String, Object>) obj);
  }

  public static SlaChecker createFromJson(Map<String, Object> obj) throws Exception {
    if (!obj.get("type").equals(type)) {
      throw new Exception("Cannot create checker of " + type + " from " + obj.get("type"));
    }
    String id = (String) obj.get("id");
    SlaOption slaOption = SlaOption.fromObject(obj.get("slaOption"));
    int execId = Integer.valueOf((String) obj.get("execId"));
    return new SlaChecker(id, slaOption, execId);
  }

  @Override
  public Object toJson() {
    Map<String, Object> jsonObj = new HashMap<>();
    jsonObj.put("type", type);
    jsonObj.put("id", id);
    jsonObj.put("slaOption", slaOption.toObject());
    jsonObj.put("execId", String.valueOf(execId));

    return jsonObj;
  }

  @Override
  public void stopChecker() { }

  @Override
  public void setContext(Map<String, Object> context) { }

  @Override
  public long getNextCheckTime() {
    return slaExpireTime;
  }
}
