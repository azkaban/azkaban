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
import java.util.HashMap;
import java.util.Map;

/**
 * Manage one quartz job's variables. Every AZ Quartz Job should come with a QuartzJobDescription.
 */
public class QuartzJobDescription<T extends AbstractQuartzJob> {

  private final String groupName;
  private final String jobName;
  private final Class<T> jobClass;
  private final Map<String, ? extends Serializable> contextMap;
  public QuartzJobDescription(final Class<T> jobClass,
      final String jobName, final String groupName,
      final Map<String, ? extends Serializable> contextMap) {

    /**
     * This check is necessary for raw type. Please see test
     * {@link QuartzJobDescriptionTest#testCreateQuartzJobDescription2}
     */
    if (jobClass.getSuperclass() != AbstractQuartzJob.class) {
      throw new ClassCastException("jobClass must extend AbstractQuartzJob class");
    }
    this.jobClass = jobClass;
    this.jobName = jobName;
    this.groupName = groupName;
    this.contextMap = contextMap;
  }

  public QuartzJobDescription(final Class<T> jobClass,
      final String jobName, final String groupName) {
    this(jobClass, jobName, groupName, new HashMap<String, String>());
  }

  public String getJobName() {
    return jobName;
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
