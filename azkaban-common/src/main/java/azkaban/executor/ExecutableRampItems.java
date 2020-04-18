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
import java.util.Optional;
import java.util.Set;


/**
 * Object of Executable Ramp Items
 */
public final class ExecutableRampItems implements IRefreshable<ExecutableRampItems> {
  public static String RAMP_SOURCE_NAME = "ramp";

  private volatile Props rampItems;

  private ExecutableRampItems() {

  }

  public static ExecutableRampItems createInstance() {
    return new ExecutableRampItems()
        .setRampItems(new Props().setSource(RAMP_SOURCE_NAME));
  }

  private ExecutableRampItems setRampItems(Props rampItems) {
    this.rampItems = rampItems;
    return this;
  }

  public Props getRampItems() {
    return rampItems;
  }

  public Set<String> getDependencies() {
    return this.rampItems.getKeySet();
  }

  public ExecutableRampItems addRampItem(@NotNull final String dependency, @NotNull final String rampValue) {
    this.rampItems.put(dependency, rampValue);
    return this;
  }

  @Override
  public ExecutableRampItems refresh(ExecutableRampItems source) {
    rampItems = source.rampItems;
    return this;
  }

  @Override
  public ExecutableRampItems clone() {
    return ExecutableRampItems
        .createInstance()
        .setRampItems(Props.clone(this.rampItems));
  }

  @Override
  public int elementCount() {
    return Optional.ofNullable(this.rampItems)
        .map(Props::size)
        .orElse(0);
  }
}
