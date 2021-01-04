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
 *
 */

package azkaban.project;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Java bean loaded from YAML file to represent a flow trigger.
 */
public class FlowTriggerBean {

  private Long maxWaitMins = null;
  private Map<String, String> schedule;
  private List<TriggerDependencyBean> triggerDependencies;

  public Long getMaxWaitMins() {
    return this.maxWaitMins;
  }

  public void setMaxWaitMins(final Long maxWaitMins) {
    this.maxWaitMins = maxWaitMins;
  }

  public Map<String, String> getSchedule() {
    return this.schedule;
  }

  public void setSchedule(final Map<String, String> schedule) {
    this.schedule = schedule;
  }

  public List<TriggerDependencyBean> getTriggerDependencies() {
    return this.triggerDependencies == null ? Collections.emptyList() : this.triggerDependencies;
  }

  public void setTriggerDependencies(
      final List<TriggerDependencyBean> triggerDependencies) {
    this.triggerDependencies = triggerDependencies;
  }

  @Override
  public String toString() {
    return "FlowTriggerBean{" +
        "maxWaitMins='" + this.maxWaitMins + '\'' +
        ", schedule=" + this.schedule +
        ", triggerDependencies=" + this.triggerDependencies +
        '}';
  }
}
