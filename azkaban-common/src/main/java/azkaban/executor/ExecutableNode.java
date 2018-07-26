/*
 * Copyright 2013 LinkedIn Corp.
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

import azkaban.flow.ConditionOnJobStatus;
import azkaban.flow.Node;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.TypedMapWrapper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base Executable that nodes and flows are based.
 */
public class ExecutableNode {

  public static final String ID_PARAM = "id";
  public static final String STATUS_PARAM = "status";
  public static final String STARTTIME_PARAM = "startTime";
  public static final String ENDTIME_PARAM = "endTime";
  public static final String UPDATETIME_PARAM = "updateTime";
  public static final String INNODES_PARAM = "inNodes";
  public static final String OUTNODES_PARAM = "outNodes";
  public static final String TYPE_PARAM = "type";
  public static final String CONDITION_PARAM = "condition";
  public static final String CONDITION_ON_JOB_STATUS_PARAM = "conditionOnJobStatus";
  public static final String PROPS_SOURCE_PARAM = "propSource";
  public static final String JOB_SOURCE_PARAM = "jobSource";
  public static final String OUTPUT_PROPS_PARAM = "outputProps";
  public static final String ATTEMPT_PARAM = "attempt";
  public static final String PASTATTEMPTS_PARAM = "pastAttempts";
  private final AtomicInteger attempt = new AtomicInteger(0);
  private String id;
  private String type = null;
  private volatile Status status = Status.READY;
  private volatile long startTime = -1;
  private volatile long endTime = -1;
  private long updateTime = -1;
  private volatile boolean killedBySLA = false;
  // Path to Job File
  private String jobSource;
  // Path to top level props file
  private String propsSource;
  private Set<String> inNodes = new HashSet<>();
  private Set<String> outNodes = new HashSet<>();
  private Props inputProps;
  private Props outputProps;
  private long delayExecution = 0;
  private ArrayList<ExecutionAttempt> pastAttempts = null;
  private String condition;
  private ConditionOnJobStatus conditionOnJobStatus = ConditionOnJobStatus.ALL_SUCCESS;

  // Transient. These values aren't saved, but rediscovered.
  private ExecutableFlowBase parentFlow;

  public ExecutableNode(final Node node) {
    this.id = node.getId();
    this.jobSource = node.getJobSource();
    this.propsSource = node.getPropsSource();
  }

  public ExecutableNode(final Node node, final ExecutableFlowBase parent) {
    this(node.getId(), node.getType(), node.getCondition(), node.getConditionOnJobStatus(), node
        .getJobSource(), node
        .getPropsSource(), parent);
  }

  public ExecutableNode(final String id, final String type, final String condition,
      final ConditionOnJobStatus conditionOnJobStatus, final String jobSource,
      final String propsSource, final ExecutableFlowBase parent) {
    this.id = id;
    this.jobSource = jobSource;
    this.propsSource = propsSource;
    this.type = type;
    this.condition = condition;
    this.conditionOnJobStatus = conditionOnJobStatus;
    setParentFlow(parent);
  }

  public ExecutableNode() {
  }

  public ExecutableFlow getExecutableFlow() {
    if (this.parentFlow == null) {
      return null;
    }

    return this.parentFlow.getExecutableFlow();
  }

  public ExecutableFlowBase getParentFlow() {
    return this.parentFlow;
  }

  public void setParentFlow(final ExecutableFlowBase flow) {
    this.parentFlow = flow;
  }

  public String getId() {
    return this.id;
  }

  public void setId(final String id) {
    this.id = id;
  }

  public Status getStatus() {
    return this.status;
  }

  public void setStatus(final Status status) {
    this.status = status;
  }

  public String getType() {
    return this.type;
  }

  public void setType(final String type) {
    this.type = type;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public void setStartTime(final long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return this.endTime;
  }

  public void setEndTime(final long endTime) {
    this.endTime = endTime;
  }

  public long getUpdateTime() {
    return this.updateTime;
  }

  public void setUpdateTime(final long updateTime) {
    this.updateTime = updateTime;
  }

  public boolean isKilledBySLA() {
    return this.killedBySLA;
  }

  public void setKilledBySLA(final boolean killedBySLA) {
    this.killedBySLA = killedBySLA;
  }

  public void addOutNode(final String exNode) {
    this.outNodes.add(exNode);
  }

  public void addInNode(final String exNode) {
    this.inNodes.add(exNode);
  }

  public Set<String> getOutNodes() {
    return this.outNodes;
  }

  public Set<String> getInNodes() {
    return this.inNodes;
  }

  public boolean hasJobSource() {
    return this.jobSource != null;
  }

  public boolean hasPropsSource() {
    return this.propsSource != null;
  }

  public String getJobSource() {
    return this.jobSource;
  }

  public String getPropsSource() {
    return this.propsSource;
  }

  public Props getInputProps() {
    return this.inputProps;
  }

  public void setInputProps(final Props input) {
    this.inputProps = input;
  }

  public Props getOutputProps() {
    return this.outputProps;
  }

  public void setOutputProps(final Props output) {
    this.outputProps = output;
  }

  public long getDelayedExecution() {
    return this.delayExecution;
  }

  public void setDelayedExecution(final long delayMs) {
    this.delayExecution = delayMs;
  }

  public List<ExecutionAttempt> getPastAttemptList() {
    return this.pastAttempts;
  }

  public int getAttempt() {
    return this.attempt.get();
  }

  public void resetForRetry() {
    final ExecutionAttempt pastAttempt = new ExecutionAttempt(this.attempt.get(), this);
    this.attempt.incrementAndGet();

    synchronized (this) {
      if (this.pastAttempts == null) {
        this.pastAttempts = new ArrayList<>();
      }

      this.pastAttempts.add(pastAttempt);
    }

    this.setStartTime(-1);
    this.setEndTime(-1);
    this.setUpdateTime(System.currentTimeMillis());
    this.setStatus(Status.READY);
    this.setKilledBySLA(false);
  }

  public List<Object> getAttemptObjects() {
    final ArrayList<Object> array = new ArrayList<>();

    for (final ExecutionAttempt attempt : this.pastAttempts) {
      array.add(attempt.toObject());
    }

    return array;
  }

  public String getNestedId() {
    return getPrintableId(":");
  }

  public String getPrintableId(final String delimiter) {
    if (this.getParentFlow() == null
        || this.getParentFlow() instanceof ExecutableFlow) {
      return getId();
    }
    return getParentFlow().getPrintableId(delimiter) + delimiter + getId();
  }

  public Map<String, Object> toObject() {
    final Map<String, Object> mapObj = new HashMap<>();
    fillMapFromExecutable(mapObj);

    return mapObj;
  }

  protected void fillMapFromExecutable(final Map<String, Object> objMap) {
    objMap.put(ID_PARAM, this.id);
    objMap.put(STATUS_PARAM, this.status.toString());
    objMap.put(STARTTIME_PARAM, this.startTime);
    objMap.put(ENDTIME_PARAM, this.endTime);
    objMap.put(UPDATETIME_PARAM, this.updateTime);
    objMap.put(TYPE_PARAM, this.type);
    objMap.put(CONDITION_PARAM, this.condition);
    if (this.conditionOnJobStatus != null) {
      objMap.put(CONDITION_ON_JOB_STATUS_PARAM, this.conditionOnJobStatus.toString());
    }
    objMap.put(ATTEMPT_PARAM, this.attempt);

    if (this.inNodes != null && !this.inNodes.isEmpty()) {
      objMap.put(INNODES_PARAM, this.inNodes);
    }
    if (this.outNodes != null && !this.outNodes.isEmpty()) {
      objMap.put(OUTNODES_PARAM, this.outNodes);
    }

    if (hasPropsSource()) {
      objMap.put(PROPS_SOURCE_PARAM, this.propsSource);
    }
    if (hasJobSource()) {
      objMap.put(JOB_SOURCE_PARAM, this.jobSource);
    }

    if (this.outputProps != null && this.outputProps.size() > 0) {
      objMap.put(OUTPUT_PROPS_PARAM, PropsUtils.toStringMap(this.outputProps, true));
    }

    if (this.pastAttempts != null) {
      final ArrayList<Object> attemptsList =
          new ArrayList<>(this.pastAttempts.size());
      for (final ExecutionAttempt attempts : this.pastAttempts) {
        attemptsList.add(attempts.toObject());
      }
      objMap.put(PASTATTEMPTS_PARAM, attemptsList);
    }
  }

  public void fillExecutableFromMapObject(
      final TypedMapWrapper<String, Object> wrappedMap) {
    this.id = wrappedMap.getString(ID_PARAM);
    this.type = wrappedMap.getString(TYPE_PARAM);
    this.condition = wrappedMap.getString(CONDITION_PARAM);
    this.conditionOnJobStatus = ConditionOnJobStatus.fromString(wrappedMap.getString
        (CONDITION_ON_JOB_STATUS_PARAM));
    this.status = Status.valueOf(wrappedMap.getString(STATUS_PARAM));
    this.startTime = wrappedMap.getLong(STARTTIME_PARAM);
    this.endTime = wrappedMap.getLong(ENDTIME_PARAM);
    this.updateTime = wrappedMap.getLong(UPDATETIME_PARAM);
    this.attempt.set(wrappedMap.getInt(ATTEMPT_PARAM, 0));

    this.inNodes = new HashSet<>();
    this.inNodes.addAll(wrappedMap.getStringCollection(INNODES_PARAM,
        Collections.<String>emptySet()));

    this.outNodes = new HashSet<>();
    this.outNodes.addAll(wrappedMap.getStringCollection(OUTNODES_PARAM,
        Collections.<String>emptySet()));

    this.propsSource = wrappedMap.getString(PROPS_SOURCE_PARAM);
    this.jobSource = wrappedMap.getString(JOB_SOURCE_PARAM);

    final Map<String, String> outputProps =
        wrappedMap.<String, String>getMap(OUTPUT_PROPS_PARAM);
    if (outputProps != null) {
      this.outputProps = new Props(null, outputProps);
    }

    final Collection<Object> pastAttempts =
        wrappedMap.<Object>getCollection(PASTATTEMPTS_PARAM);
    if (pastAttempts != null) {
      final ArrayList<ExecutionAttempt> attempts = new ArrayList<>();
      for (final Object attemptObj : pastAttempts) {
        final ExecutionAttempt attempt = ExecutionAttempt.fromObject(attemptObj);
        attempts.add(attempt);
      }

      this.pastAttempts = attempts;
    }
  }

  public void fillExecutableFromMapObject(final Map<String, Object> objMap) {
    final TypedMapWrapper<String, Object> wrapper =
        new TypedMapWrapper<>(objMap);
    fillExecutableFromMapObject(wrapper);
  }

  public Map<String, Object> toUpdateObject() {
    final Map<String, Object> updatedNodeMap = new HashMap<>();
    updatedNodeMap.put(ID_PARAM, getId());
    updatedNodeMap.put(STATUS_PARAM, getStatus().getNumVal());
    updatedNodeMap.put(STARTTIME_PARAM, getStartTime());
    updatedNodeMap.put(ENDTIME_PARAM, getEndTime());
    updatedNodeMap.put(UPDATETIME_PARAM, getUpdateTime());

    updatedNodeMap.put(ATTEMPT_PARAM, getAttempt());

    if (getAttempt() > 0) {
      final ArrayList<Map<String, Object>> pastAttempts =
          new ArrayList<>();
      for (final ExecutionAttempt attempt : getPastAttemptList()) {
        pastAttempts.add(attempt.toObject());
      }
      updatedNodeMap.put(PASTATTEMPTS_PARAM, pastAttempts);
    }

    return updatedNodeMap;
  }

  public void applyUpdateObject(final TypedMapWrapper<String, Object> updateData) {
    this.status =
        Status.fromInteger(updateData.getInt(STATUS_PARAM,
            this.status.getNumVal()));
    this.startTime = updateData.getLong(STARTTIME_PARAM);
    this.updateTime = updateData.getLong(UPDATETIME_PARAM);
    this.endTime = updateData.getLong(ENDTIME_PARAM);

    if (updateData.containsKey(ATTEMPT_PARAM)) {
      this.attempt.set(updateData.getInt(ATTEMPT_PARAM));
      if (this.attempt.get() > 0) {
        updatePastAttempts(updateData.<Object>getList(PASTATTEMPTS_PARAM,
            Collections.<Object>emptyList()));
      }
    }
  }

  public void applyUpdateObject(final Map<String, Object> updateData) {
    final TypedMapWrapper<String, Object> wrapper =
        new TypedMapWrapper<>(updateData);
    applyUpdateObject(wrapper);
  }

  public void cancelNode(final long cancelTime) {
    if (this.status == Status.DISABLED) {
      skipNode(cancelTime);
    } else {
      this.setStatus(Status.CANCELLED);
      this.setStartTime(cancelTime);
      this.setEndTime(cancelTime);
      this.setUpdateTime(cancelTime);
    }
  }

  public void skipNode(final long skipTime) {
    this.setStatus(Status.SKIPPED);
    this.setStartTime(skipTime);
    this.setEndTime(skipTime);
    this.setUpdateTime(skipTime);
  }

  private void updatePastAttempts(final List<Object> pastAttemptsList) {
    if (pastAttemptsList == null) {
      return;
    }

    synchronized (this) {
      if (this.pastAttempts == null) {
        this.pastAttempts = new ArrayList<>();
      }

      // We just check size because past attempts don't change
      if (pastAttemptsList.size() <= this.pastAttempts.size()) {
        return;
      }

      final Object[] pastAttemptArray = pastAttemptsList.toArray();
      for (int i = this.pastAttempts.size(); i < pastAttemptArray.length; ++i) {
        final ExecutionAttempt attempt =
            ExecutionAttempt.fromObject(pastAttemptArray[i]);
        this.pastAttempts.add(attempt);
      }
    }
  }

  public int getRetries() {
    return this.inputProps.getInt("retries", 0);
  }

  public long getRetryBackoff() {
    return this.inputProps.getLong("retry.backoff", 0);
  }

  public String getCondition() {
    return this.condition;
  }

  public void setCondition(final String condition) {
    this.condition = condition;
  }

  public ConditionOnJobStatus getConditionOnJobStatus() {
    return this.conditionOnJobStatus == null ? ConditionOnJobStatus.ALL_SUCCESS
        : this.conditionOnJobStatus;
  }

  public void setConditionOnJobStatus(final ConditionOnJobStatus conditionOnJobStatus) {
    this.conditionOnJobStatus = conditionOnJobStatus;
  }
}
