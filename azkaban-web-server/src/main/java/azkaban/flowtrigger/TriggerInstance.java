/*
 * Copyright 2017 LinkedIn Corp.
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

package azkaban.flowtrigger;

import azkaban.project.FlowTrigger;
import azkaban.project.Project;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TriggerInstance {

  private final List<DependencyInstance> depInstances;
  private final String id;
  private final String submitUser;
  private final Project project;
  private final String flowId;
  private final int flowVersion;
  private FlowTrigger flowTrigger;
  private volatile int flowExecId; // associated flow execution id

  //todo chengren311: convert it to builder
  public TriggerInstance(final String id, final FlowTrigger flowTrigger, final String flowId,
      final int flowVersion, final String submitUser, final List<DependencyInstance>
      depInstances, final int flowExecId, final Project project) {

    this.depInstances = ImmutableList.copyOf(depInstances);
    this.id = id;
    this.flowTrigger = flowTrigger;
    this.submitUser = submitUser;
    this.flowId = flowId;
    this.flowVersion = flowVersion;
    this.flowExecId = flowExecId;
    this.project = project;
    for (final DependencyInstance depInst : this.depInstances) {
      depInst.setTriggerInstance(this);
    }
  }

  @Override
  public String toString() {
    return "TriggerInstance{" +
        "depInstances=" + this.depInstances +
        ", id='" + this.id + '\'' +
        ", submitUser='" + this.submitUser + '\'' +
        ", project=" + this.project +
        ", flowId='" + this.flowId + '\'' +
        ", flowVersion=" + this.flowVersion +
        ", flowTrigger=" + this.flowTrigger +
        ", flowExecId=" + this.flowExecId +
        '}';
  }

  public Project getProject() {
    return this.project;
  }

  public String getProjectName() {
    return this.project.getName();
  }

  public List<String> getFailureEmails() {
    return this.project.getFlow(this.getFlowId()).getFailureEmails();
  }

  public String getFlowId() {
    return this.flowId;
  }

  public int getFlowVersion() {
    return this.flowVersion;
  }

  public int getFlowExecId() {
    return this.flowExecId;
  }

  public void setFlowExecId(final int flowExecId) {
    this.flowExecId = flowExecId;
  }

  public final FlowTrigger getFlowTrigger() {
    return this.flowTrigger;
  }

  public void setFlowTrigger(final FlowTrigger flowTrigger) {
    this.flowTrigger = flowTrigger;
  }

  public String getSubmitUser() {
    return this.submitUser;
  }

  public void addDependencyInstance(final DependencyInstance depInst) {
    this.depInstances.add(depInst);
  }

  public List<DependencyInstance> getDepInstances() {
    return this.depInstances;
  }

  public String getId() {
    return this.id;
  }

  private boolean isRunning(final Set<Status> statuses) {
    if (statuses.contains(Status.RUNNING)) {
      for (final Status status : statuses) {
        if (!status.equals(Status.SUCCEEDED) && !status.equals(Status.RUNNING)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private boolean isSucceed(final Set<Status> statuses) {
    return statuses.contains(Status.SUCCEEDED) && statuses.size() == 1;
  }

  private boolean isCancelled(final Set<Status> statuses) {
    if (statuses.contains(Status.CANCELLED)) {
      for (final Status status : statuses) {
        if (!status.equals(Status.SUCCEEDED) && !status.equals(Status.CANCELLED)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  public Status getStatus() {
    // no-dependency trigger is always considered as success
    if (this.depInstances.isEmpty()) {
      return Status.SUCCEEDED;
    }
    final Set<Status> statusSet = new HashSet<>();

    for (final DependencyInstance depInst : this.depInstances) {
      statusSet.add(depInst.getStatus());
    }

    if (isRunning(statusSet)) {
      return Status.RUNNING;
    } else if (isSucceed(statusSet)) {
      return Status.SUCCEEDED;
    } else if (isCancelled(statusSet)) {
      return Status.CANCELLED;
    } else {
      return Status.CANCELLING;
    }
  }

  public long getStartTime() {
    final List<Long> startTimeList = this.depInstances.stream()
        .map(DependencyInstance::getStartTime).collect(Collectors.toList());
    return startTimeList.isEmpty() ? 0 : Collections.min(startTimeList);
  }

  public long getEndTime() {
    if (Status.isDone(this.getStatus())) {
      final List<Long> endTimeList = this.depInstances.stream()
          .map(DependencyInstance::getEndTime).filter(endTime -> endTime != 0)
          .collect(Collectors.toList());
      return endTimeList.isEmpty() ? 0 : Collections.max(endTimeList);
    } else {
      return 0;
    }
  }
}
