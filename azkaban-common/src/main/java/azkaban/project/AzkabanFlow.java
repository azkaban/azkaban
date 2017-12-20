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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Flow level definition of the DAG.
 * Contains a list of AzkabanNodes and related flow properties.
 * Introduced in Flow 2.0 design.
 */
public class AzkabanFlow extends AzkabanNode {

  private final Map<String, AzkabanNode> nodes;
  private final FlowTrigger flowTrigger;

  private AzkabanFlow(final String name, final Props props, final Map<String, AzkabanNode> nodes,
      final List<String> dependsOn, final FlowTrigger flowTrigger) {
    super(name, Constants.FLOW_NODE_TYPE, props, dependsOn);
    this.nodes = nodes;
    this.flowTrigger = flowTrigger;
  }

  public Map<String, AzkabanNode> getNodes() {
    return this.nodes;
  }

  public AzkabanNode getNode(final String name) {
    return this.nodes.get(name);
  }

  public FlowTrigger getFlowTrigger() {
    return this.flowTrigger;
  }

  public static class AzkabanFlowBuilder {

    private String name;
    private Props props;
    private List<String> dependsOn;
    private Map<String, AzkabanNode> nodes;
    private FlowTrigger flowTrigger;

    public AzkabanFlowBuilder name(final String name) {
      this.name = name;
      return this;
    }

    public AzkabanFlowBuilder props(final Props props) {
      this.props = props;
      return this;
    }

    public AzkabanFlowBuilder dependsOn(final List<String> dependsOn) {
      this.dependsOn = dependsOn == null
          ? Collections.emptyList()
          : ImmutableList.copyOf(dependsOn);
      return this;
    }

    public AzkabanFlowBuilder nodes(final Collection<? extends AzkabanNode> azkabanNodes) {
      final Map<String, AzkabanNode> tempNodes = new HashMap<>();
      for (final AzkabanNode node : azkabanNodes) {
        tempNodes.put(node.getName(), node);
      }
      this.nodes = ImmutableMap.copyOf(tempNodes);
      return this;
    }

    public AzkabanFlowBuilder flowTrigger(final FlowTrigger flowTrigger) {
      this.flowTrigger = flowTrigger;
      return this;
    }

    public AzkabanFlow build() {
      return new AzkabanFlow(this.name, this.props, this.nodes, this.dependsOn, this.flowTrigger);
    }
  }
}
