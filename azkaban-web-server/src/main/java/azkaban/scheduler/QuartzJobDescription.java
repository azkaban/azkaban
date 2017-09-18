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

package azkaban.scheduler;

import java.io.Serializable;
import java.util.Map;

public class QuartzJobDescription {

  private final String groupName;
  private final Class<? extends AbstractQuartzJob> jobClass;
  private final Map<String, ? extends Serializable> contextMap;

  public QuartzJobDescription(final Class<? extends AbstractQuartzJob> jobClass,
      final String groupName,
      final Map<String, ? extends Serializable> contextMap) {
    this.jobClass = jobClass;
    this.groupName = groupName;
    this.contextMap = contextMap;
  }

  public Class<? extends AbstractQuartzJob> getJobClass() {
    return this.jobClass;
  }

  public Map<String, ? extends Serializable> getContextMap() {
    return this.contextMap;
  }

  @Override
  public String toString() {
    return "QuartzJobDescription{" +
        "jobClass=" + this.jobClass +
        ", groupName='" + this.groupName + '\'' +
        ", contextMap=" + this.contextMap +
        '}';
  }

  public String getGroupName() {
    return this.groupName;
  }
}
