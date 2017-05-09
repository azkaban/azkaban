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

public class SlaChecker implements ConditionChecker {

  private static final Logger logger = Logger.getLogger(SlaChecker.class);
  public static final String type = "SlaChecker";

  private String id;
  private SlaOption slaOption;
  private int execId;
  private long checkTime = -1;

  private static ExecutorManagerAdapter executorManager;

  public SlaChecker(String id, SlaOption slaOption, int execId) {
    this.id = id;
    this.slaOption = slaOption;
    this.execId = execId;
  }

  public static void setExecutorManager(ExecutorManagerAdapter em) {
    executorManager = em;
  }

  private Boolean isSlaMissed(ExecutableFlow flow) {
    String type = slaOption.getType();
    if (flow.getStartTime() < 0) {
      return Boolean.FALSE;
    }
    Status status;
    if (type.equals(SlaOption.TYPE_FLOW_FINISH)) {
      if (checkTime < flow.getStartTime()) {
        ReadablePeriod duration =
            Utils.parsePeriodString((String) slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        DateTime startTime = new DateTime(flow.getStartTime());
        DateTime nextCheckTime = startTime.plus(duration);
        this.checkTime = nextCheckTime.getMillis();
      }
      status = flow.getStatus();
      if (checkTime < DateTime.now().getMillis()) {
        return !isFlowFinished(status);
      }
    } else if (type.equals(SlaOption.TYPE_FLOW_SUCCEED)) {
      if (checkTime < flow.getStartTime()) {
        ReadablePeriod duration =
            Utils.parsePeriodString((String) slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        DateTime startTime = new DateTime(flow.getStartTime());
        DateTime nextCheckTime = startTime.plus(duration);
        this.checkTime = nextCheckTime.getMillis();
      }
      status = flow.getStatus();
      if (checkTime < DateTime.now().getMillis()) {
        return !isFlowSucceeded(status);
      } else {
        return status.equals(Status.FAILED) || status.equals(Status.KILLED);
      }
    } else if (type.equals(SlaOption.TYPE_JOB_FINISH)) {
      String jobName =
          (String) slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
      ExecutableNode node = flow.getExecutableNode(jobName);
      if (node.getStartTime() < 0) {
        return Boolean.FALSE;
      }
      if (checkTime < node.getStartTime()) {
        ReadablePeriod duration =
            Utils.parsePeriodString((String) slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        DateTime startTime = new DateTime(node.getStartTime());
        DateTime nextCheckTime = startTime.plus(duration);
        this.checkTime = nextCheckTime.getMillis();
      }
      status = node.getStatus();
      if (checkTime < DateTime.now().getMillis()) {
        return !isJobFinished(status);
      }
    } else if (type.equals(SlaOption.TYPE_JOB_SUCCEED)) {
      String jobName =
          (String) slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
      ExecutableNode node = flow.getExecutableNode(jobName);
      if (node.getStartTime() < 0) {
        return Boolean.FALSE;
      }
      if (checkTime < node.getStartTime()) {
        ReadablePeriod duration =
            Utils.parsePeriodString((String) slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        DateTime startTime = new DateTime(node.getStartTime());
        DateTime nextCheckTime = startTime.plus(duration);
        this.checkTime = nextCheckTime.getMillis();
      }
      status = node.getStatus();
      if (checkTime < DateTime.now().getMillis()) {
        return !isJobFinished(status);
      } else {
        return status.equals(Status.FAILED) || status.equals(Status.KILLED);
      }
    }
    return Boolean.FALSE;
  }

  private Boolean isSlaGood(ExecutableFlow flow) {
    String type = slaOption.getType();
    if (flow.getStartTime() < 0) {
      return Boolean.FALSE;
    }
    Status status;
    if (type.equals(SlaOption.TYPE_FLOW_FINISH)) {
      if (checkTime < flow.getStartTime()) {
        ReadablePeriod duration =
            Utils.parsePeriodString((String) slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        DateTime startTime = new DateTime(flow.getStartTime());
        DateTime nextCheckTime = startTime.plus(duration);
        this.checkTime = nextCheckTime.getMillis();
      }
      status = flow.getStatus();
      return isFlowFinished(status);
    } else if (type.equals(SlaOption.TYPE_FLOW_SUCCEED)) {
      if (checkTime < flow.getStartTime()) {
        ReadablePeriod duration =
            Utils.parsePeriodString((String) slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        DateTime startTime = new DateTime(flow.getStartTime());
        DateTime nextCheckTime = startTime.plus(duration);
        this.checkTime = nextCheckTime.getMillis();
      }
      status = flow.getStatus();
      return isFlowSucceeded(status);
    } else if (type.equals(SlaOption.TYPE_JOB_FINISH)) {
      String jobName =
          (String) slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
      ExecutableNode node = flow.getExecutableNode(jobName);
      if (node.getStartTime() < 0) {
        return Boolean.FALSE;
      }
      if (checkTime < node.getStartTime()) {
        ReadablePeriod duration =
            Utils.parsePeriodString((String) slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        DateTime startTime = new DateTime(node.getStartTime());
        DateTime nextCheckTime = startTime.plus(duration);
        this.checkTime = nextCheckTime.getMillis();
      }
      status = node.getStatus();
      return isJobFinished(status);
    } else if (type.equals(SlaOption.TYPE_JOB_SUCCEED)) {
      String jobName =
          (String) slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
      ExecutableNode node = flow.getExecutableNode(jobName);
      if (node.getStartTime() < 0) {
        return Boolean.FALSE;
      }
      if (checkTime < node.getStartTime()) {
        ReadablePeriod duration =
            Utils.parsePeriodString((String) slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        DateTime startTime = new DateTime(node.getStartTime());
        DateTime nextCheckTime = startTime.plus(duration);
        this.checkTime = nextCheckTime.getMillis();
      }
      status = node.getStatus();
      return isJobSucceeded(status);
    }
    return Boolean.FALSE;
  }

  // return true to trigger sla action
  @Override
  public Object eval() {
    logger.info("Checking sla for execution " + execId);
    ExecutableFlow flow;
    try {
      flow = executorManager.getExecutableFlow(execId);
    } catch (ExecutorManagerException e) {
      logger.error("Can't get executable flow.", e);
      e.printStackTrace();
      // something wrong, send out alerts
      return Boolean.TRUE;
    }
    return isSlaMissed(flow);
  }

  public Object isSlaFailed() {
    ExecutableFlow flow;
    try {
      flow = executorManager.getExecutableFlow(execId);
    } catch (ExecutorManagerException e) {
      logger.error("Can't get executable flow.", e);
      // something wrong, send out alerts
      return Boolean.TRUE;
    }
    return isSlaMissed(flow);
  }

  public Object isSlaPassed() {
    ExecutableFlow flow;
    try {
      flow = executorManager.getExecutableFlow(execId);
    } catch (ExecutorManagerException e) {
      logger.error("Can't get executable flow.", e);
      // something wrong, send out alerts
      return Boolean.TRUE;
    }
    return isSlaGood(flow);
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

  @SuppressWarnings("unchecked")
  public static SlaChecker createFromJson(Object obj) throws Exception {
    return createFromJson((HashMap<String, Object>) obj);
  }

  public static SlaChecker createFromJson(HashMap<String, Object> obj)
      throws Exception {
    Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    if (!jsonObj.get("type").equals(type)) {
      throw new Exception("Cannot create checker of " + type + " from "
          + jsonObj.get("type"));
    }
    String id = (String) jsonObj.get("id");
    SlaOption slaOption = SlaOption.fromObject(jsonObj.get("slaOption"));
    int execId = Integer.valueOf((String) jsonObj.get("execId"));
    return new SlaChecker(id, slaOption, execId);
  }

  @Override
  public Object toJson() {
    Map<String, Object> jsonObj = new HashMap<String, Object>();
    jsonObj.put("type", type);
    jsonObj.put("id", id);
    jsonObj.put("slaOption", slaOption.toObject());
    jsonObj.put("execId", String.valueOf(execId));

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
    return checkTime;
  }

  private boolean isFlowFinished(Status status) {
    if (status.equals(Status.FAILED) || status.equals(Status.KILLED)
        || status.equals(Status.SUCCEEDED)) {
      return Boolean.TRUE;
    } else {
      return Boolean.FALSE;
    }
  }

  private boolean isFlowSucceeded(Status status) {
    return status.equals(Status.SUCCEEDED);
  }

  private boolean isJobFinished(Status status) {
    if (status.equals(Status.FAILED) || status.equals(Status.KILLED)
        || status.equals(Status.SUCCEEDED)) {
      return Boolean.TRUE;
    } else {
      return Boolean.FALSE;
    }
  }

  private boolean isJobSucceeded(Status status) {
    return status.equals(Status.SUCCEEDED);
  }
}
