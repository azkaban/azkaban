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
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
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
  private File flowFile;

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

  public File getFlowFile() {
    return this.flowFile;
  }

  public void setFlowFile(final File flowFile) {
    this.flowFile = flowFile;
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

  private boolean containsNode(String nodeName) {
    for (NodeBean node : this.nodes) {
      if (node.getName().equals(nodeName)) {
        return true;
      }
    }
    return false;
  }

  public boolean addNode(NodeBean newNode) {
    return this.nodes.add(newNode);
  }

  public boolean addNodes(List<NodeBean> newNodes) {
    return this.nodes.addAll(newNodes);
  }

  public List<NodeBean> getExternalDependencies(Map<String, NodeBean> ymlFlowList) {

    List<NodeBean> externalDependencies = new ArrayList<>();
    if (this.getNodes() == null) {
      return externalDependencies;
    }
    for (NodeBean nodeBeanSubNode : this.getNodes()) {
      // continue if there is no dependency
      if (nodeBeanSubNode.getDependsOn() == null) {
        continue;
      }
      for (final String dependsOn : nodeBeanSubNode.getDependsOn()) {
        if (!this.containsNode(dependsOn) && ymlFlowList.containsKey(dependsOn)
            && !this.getName().equals(dependsOn)) {
          // ymlFlowList is not containing dependency! and we found dependency as separate
          // yml flow! create dependency between this two flows
          externalDependencies.add(ymlFlowList.get(dependsOn));
        }
      }
    }
    return externalDependencies;
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
