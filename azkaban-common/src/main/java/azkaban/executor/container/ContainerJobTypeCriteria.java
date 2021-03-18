/*
 * Copyright 2021 LinkedIn Corp.
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
package azkaban.executor.container;

import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.DispatchMethod;
import azkaban.executor.ExecutableFlow;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashSet;
import java.util.Set;

/**
 * Class for determining {@link DispatchMethod} based on allowList.
 */
public class ContainerJobTypeCriteria {

  public static final String ALL = "ALL";
  private Set<String> allowList;

  public ContainerJobTypeCriteria(final Props azkProps) {
    this.allowList = new HashSet<>(azkProps
        .getStringList(ContainerizedDispatchManagerProperties.CONTAINERIZED_JOBTYPE_ALLOWLIST));
  }

  @VisibleForTesting
  public void updateAllowList(Set<String> allowList) {
    this.allowList = allowList;
  }

  @VisibleForTesting
  public void appendAllowList(Set<String> allowList) {
    this.allowList.addAll(allowList);
  }

  @VisibleForTesting
  public void removeFromAllowList(Set<String> allowList) {
    this.allowList.removeAll(allowList);
  }

  @VisibleForTesting
  public Set<String> getAllowList() {
    return this.allowList;
  }

  @VisibleForTesting
  public String getAllowListAsString() {
    return String.join(",", this.allowList);
  }

  /**
   * If the set of JobTypes extracted from the flow is not allowed, the return DispatchMethod.POLL
   * other DispatchMethod.CONTAINERIZED. If the allowList contains "ALL", then
   * DispatchMethod.CONTAINERIZED is returned.
   */
  public DispatchMethod getDispatchMethod(final ExecutableFlow flow) {
    if (this.allowList.contains(ALL)) {
      return DispatchMethod.CONTAINERIZED;
    }
    Set<String> jobTypesForFlow = ContainerImplUtils.getJobTypesForFlow(flow);
    for (String jobType : jobTypesForFlow) {
      if (!this.allowList.contains(jobType)) {
        return DispatchMethod.POLL;
      }
    }
    return DispatchMethod.CONTAINERIZED;
  }
}
