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

import azkaban.utils.StringUtils;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


/**
 * Object of Executable Ramp Dependency
 */
public class ExecutableRampDependency implements IRefreshable<ExecutableRampDependency> {
  private static String DELIMITED = ",";

  private volatile String defaultValue = null;
  private volatile Set<String> associatedJobTypes = null;

  private ExecutableRampDependency() {

  }

  public static ExecutableRampDependency createInstance() {
    return new ExecutableRampDependency();
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public ExecutableRampDependency setDefaultValue(final String defaultValue) {
    this.defaultValue = StringUtils.isEmpty(defaultValue) ? null : defaultValue.trim();
    return this;
  }

  public Set<String> getAssociatedJobTypes() {
    return associatedJobTypes;
  }

  public ExecutableRampDependency setAssociatedJobTypes(Set<String> associatedJobTypes) {
    this.associatedJobTypes = associatedJobTypes;
    return this;
  }

  public ExecutableRampDependency setAssociatedJobTypes(final String jobTypes) {
    this.associatedJobTypes = StringUtils.isEmpty(jobTypes)
        ? null : new HashSet<>(Arrays.asList(jobTypes.split(DELIMITED)));
    return this;
  }

  @Override
  public ExecutableRampDependency refresh(ExecutableRampDependency source) {
    this.defaultValue = source.getDefaultValue();
    this.associatedJobTypes = source.getAssociatedJobTypes();
    return this;
  }

  @Override
  public ExecutableRampDependency clone() {
    Set<String> clonedAssociatedJobTypes = new HashSet<>();
    clonedAssociatedJobTypes.addAll(this.associatedJobTypes);
    return ExecutableRampDependency
        .createInstance()
        .setDefaultValue(this.getDefaultValue())
        .setAssociatedJobTypes(clonedAssociatedJobTypes);
  }
}
