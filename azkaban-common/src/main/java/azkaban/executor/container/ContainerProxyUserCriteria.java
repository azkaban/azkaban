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
import org.apache.commons.collections4.CollectionUtils;

/**
 * Class for determining {@link DispatchMethod} based on deny list
 */
public class ContainerProxyUserCriteria {

  private Set<String> denyList;

  public ContainerProxyUserCriteria(final Props azkProps) {
    this.denyList =
        new HashSet<>(azkProps.getStringList(ContainerizedDispatchManagerProperties.CONTAINERIZED_PROXY_USER_DENYLIST));
  }

  @VisibleForTesting
  public void appendDenyList(final Set<String> denyList) { this.denyList.addAll(denyList); }

  @VisibleForTesting
  public void removeFromDenyList(final Set<String> denyList) { this.denyList.removeAll(denyList); }

  @VisibleForTesting
  public Set<String> getDenyList() { return this.denyList; }

  @VisibleForTesting
  public String getDenyListAsString() {
    return String.join(",", this.denyList);
  }

  /**
   * If the set of flow proxy users contains a proxy user on the deny list,
   * then return DispatchMethod.POLL, else return DispatchMethod.CONTAINERIZED
   *
   * @param flow
   * @return DispatchMethod
   */
  public DispatchMethod getDispatchMethod(final ExecutableFlow flow) {
    if (CollectionUtils.containsAny(flow.getProxyUsers(), this.denyList)) {
      return DispatchMethod.POLL;
    } else {
      return DispatchMethod.CONTAINERIZED;
    }
  }
}
