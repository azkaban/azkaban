/*
 * Copyright 2019 LinkedIn Corp.
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

import com.sun.istack.NotNull;
import java.util.Optional;


/**
 * Map of Executable Ramp Exceptional Items at Flow Level, Map.key = rampId
 */
public final class ExecutableRampExceptionalFlowItemsMap
    extends BaseRefreshableMap<String, ExecutableRampExceptionalItems> {

  private ExecutableRampExceptionalFlowItemsMap() {
    super();
  }

  public static ExecutableRampExceptionalFlowItemsMap createInstance() {
    return new ExecutableRampExceptionalFlowItemsMap();
  }

  public ExecutableRampExceptionalFlowItemsMap add(@NotNull final String rampId, @NotNull final String flowId,
      @NotNull final ExecutableRampStatus treatment, final long timeStamp) {
    return add(rampId, flowId, treatment, timeStamp, false);
  }

  public ExecutableRampExceptionalFlowItemsMap add(@NotNull final String rampId, @NotNull final String flowId,
      @NotNull final ExecutableRampStatus treatment, final long timeStamp, boolean isCacheOnly) {
    if (this.containsKey(rampId)) {
      this.get(rampId).add(flowId, treatment, timeStamp, isCacheOnly);
    } else {
      this.put(rampId, ExecutableRampExceptionalItems.createInstance().add(flowId, treatment, timeStamp, isCacheOnly));
    }
    return this;
  }

  public ExecutableRampExceptionalItems.RampRecord get(@NotNull final String rampId, @NotNull final String flowId) {
    return Optional.ofNullable(this.get(rampId))
        .map(items -> items.getItems().get(flowId))
        .orElse(null);
  }

  public boolean exists(@NotNull final String rampId, @NotNull final String flowId) {
    return Optional.ofNullable(this.get(rampId))
        .map(items -> items.exists(flowId))
        .orElse(false);
  }

  public ExecutableRampStatus check(@NotNull final String rampId, @NotNull final String flowId) {
    return Optional.ofNullable(this.get(rampId))
        .map(table -> table.get(flowId))
        .map(ExecutableRampExceptionalItems.RampRecord::getStatus)
        .orElse(ExecutableRampStatus.UNDETERMINED);
  }

  @Override
  public ExecutableRampExceptionalFlowItemsMap clone() {
    return (ExecutableRampExceptionalFlowItemsMap) super.clone();
  }
}
