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

import java.util.Collection;
import java.util.stream.Collectors;


/**
 * Map of ExecutableRamp, Map.key = RampId
 */
public class ExecutableRampMap extends BaseRefreshableMap<String, ExecutableRamp> {

  public static ExecutableRampMap createInstance() {
    return new ExecutableRampMap();
  }

  public Collection<ExecutableRamp> getActivatedAll() {
    return this.values().stream()
        .filter(ExecutableRamp::isActive)
        .collect(Collectors.toSet());
  }

  public Collection<ExecutableRamp> getAll() {
    return this.values().stream().collect(Collectors.toSet());
  }

  @Override
  public ExecutableRampMap clone() {
    return (ExecutableRampMap) super.clone();
  }
}
