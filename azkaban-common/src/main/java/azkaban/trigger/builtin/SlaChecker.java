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
import azkaban.trigger.ConditionChecker;
import azkaban.utils.TimeUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;

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
    final Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    if (!jsonObj.get("type").equals(type)) {
      throw new Exception("Cannot create checker of " + type + " from "
          + jsonObj.get("type"));
    }
    final String id = (String) jsonObj.get("id");
    final SlaOption slaOption = SlaOption.fromObject(jsonObj.get("slaOption"));
    final int execId = Integer.valueOf((String) jsonObj.get("execId"));
    return new SlaChecker(id, slaOption, execId);
  }

  private Boolean isSlaMissed(final ExecutableFlow flow) {
    final String type = this.slaOption.getType();
    if (flow.getStartTime() < 0) {
      return Boolean.FALSE;
    }
    final Status status;
    if (type.equals(SlaOption.TYPE_FLOW_FINISH)) {
      if (this.checkTime < flow.getStartTime()) {
        final ReadablePeriod duration =
            TimeUtils.parsePeriodString((String) this.slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        final DateTime startTime = new DateTime(flow.getStartTime());
        final DateTime nextCheckTime = startTime.plus(duration);
        this.checkTime = nextCheckTime.getMillis();
      }
      status = flow.getStatus();
      if (this.checkTime < DateTime.now().getMillis()) {
        return !isFlowFinished(status);
      }
    } else if (type.equals(SlaOption.TYPE_FLOW_SUCCEED)) {
      if (this.checkTime < flow.getStartTime()) {
        final ReadablePeriod duration =
            TimeUtils.parsePeriodString((String) this.slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        final DateTime startTime = new DateTime(flow.getStartTime());
        final DateTime nextCheckTime = startTime.plus(duration);
        this.checkTime = nextCheckTime.getMillis();
      }
      status = flow.getStatus();
      if (this.checkTime < DateTime.now().getMillis()) {
        return !isFlowSucceeded(status);
      } else {
        return status.equals(Status.FAILED) || status.equals(Status.KILLED);
      }
    } else if (type.equals(SlaOption.TYPE_JOB_FINISH)) {
      final String jobName =
          (String) this.slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
      final ExecutableNode node = flow.getExecutableNode(jobName);
      if (node.getStartTime() < 0) {
        return Boolean.FALSE;
      }
      if (this.checkTime < node.getStartTime()) {
        final ReadablePeriod duration =
            TimeUtils.parsePeriodString((String) this.slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        final DateTime startTime = new DateTime(node.getStartTime());
        final DateTime nextCheckTime = startTime.plus(duration);
        this.checkTime = nextCheckTime.getMillis();
      }
      status = node.getStatus();
      if (this.checkTime < DateTime.now().getMillis()) {
        return !isJobFinished(status);
      }
    } else if (type.equals(SlaOption.TYPE_JOB_SUCCEED)) {
      final String jobName =
          (String) this.slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
      final ExecutableNode node = flow.getExecutableNode(jobName);
      if (node.getStartTime() < 0) {
        return Boolean.FALSE;
      }
      if (this.checkTime < node.getStartTime()) {
        final ReadablePeriod duration =
            TimeUtils.parsePeriodString((String) this.slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        final DateTime startTime = new DateTime(node.getStartTime());
        final DateTime nextCheckTime = startTime.plus(duration);
        this.checkTime = nextCheckTime.getMillis();
      }
      status = node.getStatus();
      if (this.checkTime < DateTime.now().getMillis()) {
        return !isJobFinished(status);
      } else {
        return status.equals(Status.FAILED) || status.equals(Status.KILLED);
      }
    }
    return Boolean.FALSE;
  }

  private Boolean isSlaGood(final ExecutableFlow flow) {
    final String type = this.slaOption.getType();
    if (flow.getStartTime() < 0) {
      return Boolean.FALSE;
    }
    final Status status;
    if (type.equals(SlaOption.TYPE_FLOW_FINISH)) {
      if (this.checkTime < flow.getStartTime()) {
        final ReadablePeriod duration =
            TimeUtils.parsePeriodString((String) this.slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        final DateTime startTime = new DateTime(flow.getStartTime());
        final DateTime nextCheckTime = startTime.plus(duration);
        this.checkTime = nextCheckTime.getMillis();
      }
      status = flow.getStatus();
      return isFlowFinished(status);
    } else if (type.equals(SlaOption.TYPE_FLOW_SUCCEED)) {
      if (this.checkTime < flow.getStartTime()) {
        final ReadablePeriod duration =
            TimeUtils.parsePeriodString((String) this.slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        final DateTime startTime = new DateTime(flow.getStartTime());
        final DateTime nextCheckTime = startTime.plus(duration);
        this.checkTime = nextCheckTime.getMillis();
      }
      status = flow.getStatus();
      return isFlowSucceeded(status);
    } else if (type.equals(SlaOption.TYPE_JOB_FINISH)) {
      final String jobName =
          (String) this.slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
      final ExecutableNode node = flow.getExecutableNode(jobName);
      if (node.getStartTime() < 0) {
        return Boolean.FALSE;
      }
      if (this.checkTime < node.getStartTime()) {
        final ReadablePeriod duration =
            TimeUtils.parsePeriodString((String) this.slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        final DateTime startTime = new DateTime(node.getStartTime());
        final DateTime nextCheckTime = startTime.plus(duration);
        this.checkTime = nextCheckTime.getMillis();
      }
      status = node.getStatus();
      return isJobFinished(status);
    } else if (type.equals(SlaOption.TYPE_JOB_SUCCEED)) {
      final String jobName =
          (String) this.slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
      final ExecutableNode node = flow.getExecutableNode(jobName);
      if (node.getStartTime() < 0) {
        return Boolean.FALSE;
      }
      if (this.checkTime < node.getStartTime()) {
        final ReadablePeriod duration =
            TimeUtils.parsePeriodString((String) this.slaOption.getInfo().get(
                SlaOption.INFO_DURATION));
        final DateTime startTime = new DateTime(node.getStartTime());
        final DateTime nextCheckTime = startTime.plus(duration);
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
    logger.info("Checking sla for execution " + this.execId);
    final ExecutableFlow flow;
    try {
      flow = this.executorLoader.fetchExecutableFlow(this.execId);
    } catch (final ExecutorManagerException e) {
      logger.error("Can't get executable flow.", e);
      e.printStackTrace();
      // something wrong, send out alerts
      return Boolean.TRUE;
    }
    return isSlaMissed(flow);
  }

  public Object isSlaFailed() {
    final ExecutableFlow flow;
    try {
      flow = this.executorLoader.fetchExecutableFlow(this.execId);
    } catch (final ExecutorManagerException e) {
      logger.error("Can't get executable flow.", e);
      // something wrong, send out alerts
      return Boolean.TRUE;
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
    return this.id;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public ConditionChecker fromJson(final Object obj) throws Exception {
    return createFromJson(obj);
  }

  @Override
  public Object toJson() {
    final Map<String, Object> jsonObj = new HashMap<>();
    jsonObj.put("type", type);
    jsonObj.put("id", this.id);
    jsonObj.put("slaOption", this.slaOption.toObject());
    jsonObj.put("execId", String.valueOf(this.execId));

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
    return this.checkTime;
  }

  private boolean isFlowFinished(final Status status) {
    if (status.equals(Status.FAILED) || status.equals(Status.KILLED)
        || status.equals(Status.SUCCEEDED)) {
      return Boolean.TRUE;
    } else {
      return Boolean.FALSE;
    }
  }

  private boolean isFlowSucceeded(final Status status) {
    return status.equals(Status.SUCCEEDED);
  }

  private boolean isJobFinished(final Status status) {
    if (status.equals(Status.FAILED) || status.equals(Status.KILLED)
        || status.equals(Status.SUCCEEDED)) {
      return Boolean.TRUE;
    } else {
      return Boolean.FALSE;
    }
  }

  private boolean isJobSucceeded(final Status status) {
    return status.equals(Status.SUCCEEDED);
  }
}
