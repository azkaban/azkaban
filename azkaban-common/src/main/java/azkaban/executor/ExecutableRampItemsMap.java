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

import azkaban.utils.Props;
import com.sun.istack.NotNull;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;


/**
 * Map of Executable Ramp Items, Mak.key = rampId
 */
public final class ExecutableRampItemsMap extends BaseRefreshableMap<String, ExecutableRampItems> {

  private ExecutableRampItemsMap() {
    super();
  }

  public static ExecutableRampItemsMap createInstance() {
    return new ExecutableRampItemsMap();
  }

  public ExecutableRampItemsMap add(@NotNull final String rampId, @NotNull final String dependency,
      @NotNull final String rampValue) {

    ExecutableRampItems executableRampItems = this.getOrDefault(rampId, null);
    if (executableRampItems == null) {
      executableRampItems = ExecutableRampItems.createInstance();
      this.put(rampId, executableRampItems);
    }

    executableRampItems.addRampItem(dependency, rampValue);

    return this;
  }

  public Props getRampItems(@NotNull final String rampId) {
    return Optional.ofNullable(this.get(rampId))
        .map(ExecutableRampItems::getRampItems)
        .orElse(new Props());
  }

  public Set<String> getDependencies(@NotNull final String rampId) {
    return Optional.ofNullable(this.get(rampId))
        .map(ExecutableRampItems::getDependencies)
        .orElse(Collections.emptySet());
  }

  @Override
  public ExecutableRampItemsMap clone() {
    return (ExecutableRampItemsMap) super.clone();
  }
}
