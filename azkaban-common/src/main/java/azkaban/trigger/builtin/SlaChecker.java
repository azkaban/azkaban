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

import azkaban.ServiceProvider;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.sla.SlaOption;
import azkaban.sla.SlaType.ComponentType;
import azkaban.sla.SlaType;
import azkaban.sla.SlaType.StatusType;
import azkaban.trigger.ConditionChecker;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

public class SlaChecker implements ConditionChecker {

  public static final String type = "SlaChecker";
  private static final Logger logger = Logger.getLogger(SlaChecker.class);
  private final String id;
  private final SlaOption slaOption;
  private final int execId;
  private final ExecutorLoader executorLoader;
  private long checkTime = -1;

  //todo chengren311: move this class to executor module when all existing triggers in db are expired
  public SlaChecker(final String id, final SlaOption slaOption, final int execId) {
    this.id = id;
    this.slaOption = slaOption;
    this.execId = execId;
    this.executorLoader = ServiceProvider.SERVICE_PROVIDER.getInstance(ExecutorLoader.class);
  }

  public static SlaChecker createFromJson(final Object obj) throws Exception {
    return createFromJson((HashMap<String, Object>) obj);
  }

  public static SlaChecker createFromJson(final HashMap<String, Object> obj)
      throws Exception {
    if (!obj.get("type").equals(type)) {
      throw new Exception("Cannot create checker of " + type + " from "
          + obj.get("type"));
    }
    final String id = (String) obj.get("id");
    final SlaOption slaOption = SlaOption.fromObject(obj.get("slaOption"));
    final int execId = Integer.valueOf((String) obj.get("execId"));
    return new SlaChecker(id, slaOption, execId);
  }

  private Boolean isSlaMissed(final ExecutableFlow flow) {
    final SlaType type = slaOption.getType();
    logger.info("SLA type for flow " + flow.getId() + " is " + type);
    if (flow.getStartTime() < 0) {
      logger.info("Start time is less than 0 for flow " + flow.getId());
      return false;
    }

    Status status;
    if (type.getComponent() == SlaType.ComponentType.FLOW) {
      logger.info("SLA type is flow.");
      if (this.checkTime < flow.getStartTime()) {
        logger.info("checktime = " + this.checkTime);
        logger.info("SLA duration = " + slaOption.getDuration().toMillis() + " ms");
        this.checkTime = flow.getStartTime() + slaOption.getDuration().toMillis();
        logger.info("checktime updated to " + this.checkTime);
      }
      status = flow.getStatus();
      logger.info("Flow status = " + status.toString());
    } else { // JOB
      final ExecutableNode node = flow.getExecutableNode(slaOption.getJobName());
      if (node.getStartTime() < 0) {
        return false;
      }
      if (this.checkTime < node.getStartTime()) {
        this.checkTime = node.getStartTime() + slaOption.getDuration().toMillis();
      }
      status = node.getStatus();
    }
    if (this.checkTime < DateTime.now().getMillis()) {
      switch (slaOption.getType()) {
        case FLOW_FINISH:
          logger.info("isFlowFinished?");
          return !isFlowFinished(status);
         case FLOW_SUCCEED:
           logger.info("isFlowSucceeded?");
            return !isFlowSucceeded(status);
         case JOB_FINISH:
            return !isJobFinished(status);
         case JOB_SUCCEED:
            return !isJobFinished(status);
      }
    } else if (slaOption.getType().getStatus() == StatusType.SUCCEED) {
      logger.info("slaOption.status = SUCCEED and status = " + status.toString());
      return (status == Status.FAILED || status == Status.KILLED);
    }
    return false;
  }

  private Boolean isSlaGood(final ExecutableFlow flow) {
    final SlaType type = this.slaOption.getType();
    if (flow.getStartTime() < 0) {
      return false;
    }
    Status status;

    if (type.getComponent() == ComponentType.FLOW) {
      if (this.checkTime < flow.getStartTime()) {
        this.checkTime = flow.getStartTime() + this.slaOption.getDuration().toMillis();
      }
      status = flow.getStatus();
    } else { // JOB
      final String jobName = this.slaOption.getJobName();
      final ExecutableNode node = flow.getExecutableNode(jobName);
      if (node.getStartTime() < 0) {
        return false;
      }
      if (this.checkTime < node.getStartTime()) {
         this.checkTime = node.getStartTime() + slaOption.getDuration().toMillis();
      }
      status = node.getStatus();
    }
    switch(type) {
      case FLOW_FINISH:
        return isFlowFinished(status);
      case FLOW_SUCCEED:
        return isFlowSucceeded(status);
      case JOB_FINISH:
        return isJobFinished(status);
      case JOB_SUCCEED:
        return isJobSucceeded(status);
    }
    return false;
  }

  // return true to trigger sla action
  @Override
  public Object eval() {
    logger.info("Checking sla for execution " + this.execId);
    final ExecutableFlow flow;
    try {
      flow = this.executorLoader.fetchExecutableFlow(this.execId);
    } catch (final ExecutorManagerException e) {
      logger.error("Can't get executable flow.", e);
      e.printStackTrace();
      // something wrong, send out alerts
      return true;
    }
    return isSlaMissed(flow);
  }

  public Object isSlaFailed() {
    final ExecutableFlow flow;
    try {
      flow = this.executorLoader.fetchExecutableFlow(this.execId);
      logger.info("Flow for execid " + this.execId + " is " + flow.getId());
    } catch (final ExecutorManagerException e) {
      logger.error("Can't get executable flow.", e);
      // something wrong, send out alerts
      return true;
    }
    return isSlaMissed(flow);
  }

  public Object isSlaPassed() {
    final ExecutableFlow flow;
    try {
      flow = this.executorLoader.fetchExecutableFlow(this.execId);
    } catch (final ExecutorManagerException e) {
      logger.error("Can't get executable flow.", e);
      // something wrong, send out alerts
      return true;
    }
    return isSlaGood(flow);
  }

  @Override
  public Object getNum() { return null; }

  @Override
  public void reset() { }

  @Override
  public String getId() { return id; }

  @Override
  public String getType() { return type; }

  @Override
  public ConditionChecker fromJson(final Object obj) throws Exception {
    return createFromJson(obj);
  }

  @Override
  public Object toJson() {
    final Map<String, Object> jsonObj = new HashMap<>();
    jsonObj.put("type", type);
    jsonObj.put("id", id);
    // TODO edlu: is this stored in db? Can we convert to the new format?
    jsonObj.put("slaOption", this.slaOption.toObject());
    jsonObj.put("execId", String.valueOf(this.execId));

    return jsonObj;
  }

  @Override
  public void stopChecker() { }

  @Override
  public void setContext(final Map<String, Object> context) { }

  @Override
  public long getNextCheckTime() { return this.checkTime; }

  private boolean isFlowFinished(final Status status) {
    if (status.equals(Status.FAILED) || status.equals(Status.KILLED)
        || status.equals(Status.SUCCEEDED)) {
      return true;
    } else {
      return false;
    }
  }

  private boolean isFlowSucceeded(final Status status) {
    return status.equals(Status.SUCCEEDED);
  }

  private boolean isJobFinished(final Status status) {
    if (status.equals(Status.FAILED) || status.equals(Status.KILLED)
        || status.equals(Status.SUCCEEDED)) {
      return true;
    } else {
      return false;
    }
  }

  private boolean isJobSucceeded(final Status status) {
    return status.equals(Status.SUCCEEDED);
  }
}
