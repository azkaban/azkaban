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

import azkaban.Constants;
import azkaban.utils.Props;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Used by the YAML loader to deserialize DAG nodes in the flow
 */
public class NodeBean implements Serializable {

  private String name;
  private Map<String, String> config;
  private List<String> dependsOn;
  private String type;
  private String condition;
  private List<NodeBean> nodes;
  private FlowTriggerBean trigger;

  public String getName() {
    return this.name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public Map<String, String> getConfig() {
    return this.config;
  }

  public void setConfig(final Map<String, String> config) {
    this.config = config;
  }

  public List<String> getDependsOn() {
    return this.dependsOn;
  }

  public void setDependsOn(final List<String> dependsOn) {
    this.dependsOn = dependsOn;
  }

  public String getType() {
    return this.type;
  }

  public void setType(final String type) {
    this.type = type;
  }

  public String getCondition() {
    return this.condition;
  }

  public void setCondition(final String condition) {
    this.condition = condition;
  }

  public List<NodeBean> getNodes() {
    return this.nodes;
  }

  public void setNodes(final List<NodeBean> nodes) {
    this.nodes = nodes;
  }

  public Props getProps() {
    final Props props = new Props(null, this.getConfig());
    props.put(Constants.NODE_TYPE, this.getType());
    return props;
  }

  public FlowTriggerBean getTrigger() {
    return this.trigger;
  }

  public void setTrigger(final FlowTriggerBean trigger) {
    this.trigger = trigger;
  }

  @Override
  public String toString() {
    return "NodeBean{" +
        "name='" + this.name + '\'' +
        ", config=" + this.config +
        ", dependsOn=" + this.dependsOn +
        ", type='" + this.type + '\'' +
        ", condition='" + this.condition + '\'' +
        ", nodes=" + this.nodes +
        ", trigger=" + this.trigger +
        '}';
  }
}
