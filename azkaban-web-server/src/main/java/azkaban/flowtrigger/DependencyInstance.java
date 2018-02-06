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

import flowtrigger.DependencyInstanceContext;
import java.util.Date;

public class DependencyInstance {

  private final Date startTime;
  private final String depName;
  private TriggerInstance triggerInstance;
  private DependencyInstanceContext context;
  private volatile Date endTime;
  private volatile Status status;
  private volatile CancellationCause cause;

  //todo chengren311: convert it to builder
  public DependencyInstance(final String depName, final Date startTime,
      final Date endTime, final DependencyInstanceContext context, final Status status,
      final CancellationCause cause) {
    this.status = status;
    this.depName = depName;
    this.startTime = startTime;
    this.endTime = endTime;
    this.context = context;
    this.cause = cause;
  }

  @Override
  public String toString() {
    return "DependencyInstance{" +
        "startTime=" + this.startTime.getTime() +
        ", depName='" + this.depName + '\'' +
        ", context=" + this.context +
        ", endTime=" + (this.endTime == null ? null : this.endTime.getTime()) +
        ", status=" + this.status +
        ", cause=" + this.cause +
        '}';
  }

  public CancellationCause getCancellationCause() {
    return this.cause;
  }

  public void setCancellationCause(final CancellationCause cancellationCause) {
    this.cause = cancellationCause;
  }

  public TriggerInstance getTriggerInstance() {
    return this.triggerInstance;
  }

  public void setTriggerInstance(final TriggerInstance triggerInstance) {
    this.triggerInstance = triggerInstance;
  }

  public void setDependencyInstanceContext(final DependencyInstanceContext context) {
    this.context = context;
  }

  public Date getStartTime() {
    return this.startTime;
  }

  public Date getEndTime() {
    return this.endTime;
  }

  public void setEndTime(final Date endTime) {
    this.endTime = endTime;
  }

  public String getDepName() {
    return this.depName;
  }

  public DependencyInstanceContext getContext() {
    return this.context;
  }

  public Status getStatus() {
    return this.status;
  }

  public void setStatus(final Status status) {
    this.status = status;
  }

}
