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

import azkaban.utils.Pair;
import com.sun.istack.NotNull;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Map of Executable Ramp Exceptional Items at Job Level, Map.key = Pair(rampId, flowId)
 */
public class ExecutableRampExceptionalJobItemsMap
    extends BaseRefreshableMap<Pair<String, String>, ExecutableRampExceptionalItems> {

  private ExecutableRampExceptionalJobItemsMap() {
    super();
  }

  public static ExecutableRampExceptionalJobItemsMap createInstance() {
    return new ExecutableRampExceptionalJobItemsMap();
  }

  public void add(@NotNull final String rampId, @NotNull final String flowId,
      @NotNull final String jobId, @NotNull final ExecutableRampStatus treatment, final long timeStamp) {
    Pair<String, String> key = new Pair<>(rampId, flowId);
    if (this.containsKey(key)) {
      this.get(key).add(jobId, treatment, timeStamp);
    } else {
      this.put(key, ExecutableRampExceptionalItems.createInstance().add(jobId, treatment, timeStamp));
    }
  }

  public ExecutableRampStatus check(@NotNull final String rampId, @NotNull final String flowId, @NotNull final String jobId) {
    Pair<String, String> key = new Pair<>(rampId, flowId);
    ExecutableRampExceptionalItems exceptionalTable = this.get(key);
    if (exceptionalTable == null) {
      return null;
    }
    return exceptionalTable.getStatus(jobId);
  }

  public Map<String, ExecutableRampExceptionalItems> getExceptionalJobItemsByFlow(@NotNull final String flowId) {
    return this.entrySet().stream()
        .filter(entitySet -> entitySet.getKey().getSecond().equalsIgnoreCase(flowId))
        .collect(Collectors.toMap(
            items -> items.getKey().getFirst(),
            items -> items.getValue()
        ));
  }

  @Override
  public ExecutableRampExceptionalJobItemsMap clone() {
    return (ExecutableRampExceptionalJobItemsMap) super.clone();
  }
}
