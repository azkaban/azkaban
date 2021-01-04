/*
 * Copyright 2012 LinkedIn, Inc
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

import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.FlowProps;
import azkaban.flow.Node;
import azkaban.flow.SpecialJobTypes;
import azkaban.project.Project;
import azkaban.utils.TypedMapWrapper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutableFlowBase extends ExecutableNode {

  public static final String FLOW_ID_PARAM = "flowId";
  public static final String NODES_PARAM = "nodes";
  public static final String PROPERTIES_PARAM = "properties";
  public static final String SOURCE_PARAM = "source";
  public static final String INHERITED_PARAM = "inherited";
  private static final String FLOW_ID_FORMAT_PATTERN = "%s.%s";
  private static final Logger logger = LoggerFactory.getLogger(ExecutableFlowBase.class);

  private final HashMap<String, ExecutableNode> executableNodes =
      new HashMap<>();
  private final HashMap<String, FlowProps> flowProps =
      new HashMap<>();
  private ArrayList<String> startNodes;
  private ArrayList<String> endNodes;
  private String flowId;

  public ExecutableFlowBase(final Project project, final Node node, final Flow flow,
      final ExecutableFlowBase parent) {
    super(node, parent);

    setFlow(project, flow);
  }

  public ExecutableFlowBase() {
  }

  public int getExecutionId() {
    if (this.getParentFlow() != null) {
      return this.getParentFlow().getExecutionId();
    }

    return -1;
  }

  public int getProjectId() {
    if (this.getParentFlow() != null) {
      return this.getParentFlow().getProjectId();
    }

    return -1;
  }

  public String getProjectName() {
    if (this.getParentFlow() != null) {
      return this.getParentFlow().getProjectName();
    }

    return null;
  }

  public int getVersion() {
    if (this.getParentFlow() != null) {
      return this.getParentFlow().getVersion();
    }

    return -1;
  }

  public String getExecutionSource() {
    if (this.getParentFlow() != null) {
      return this.getParentFlow().getExecutionSource();
    }

    return null;
  }

  public String getLastModifiedByUser() {
    if (this.getParentFlow() != null) {
      return this.getParentFlow().getLastModifiedByUser();
    }

    return null;
  }

  public long getLastModifiedTimestamp() {
    if (this.getParentFlow() != null) {
      return this.getParentFlow().getLastModifiedTimestamp();
    }

    return -1;
  }

  public Collection<FlowProps> getFlowProps() {
    return this.flowProps.values();
  }

  public String getFlowId() {
    return this.flowId;
  }

  public String getFlowName() {
    return String.format(FLOW_ID_FORMAT_PATTERN, this.getProjectName(), this.getFlowId());
  }

  public int getRampPercentageId() {
    return Math.abs(getFlowName().hashCode() % 100);
  }

  protected void setFlow(final Project project, final Flow flow) {
    this.flowId = flow.getId();
    this.flowProps.putAll(flow.getAllFlowProps());

    for (final Node node : flow.getNodes()) {
      final String id = node.getId();
      if (node.getType().equals(SpecialJobTypes.EMBEDDED_FLOW_TYPE)) {
        final String embeddedFlowId = node.getEmbeddedFlowId();
        final Flow subFlow = project.getFlow(embeddedFlowId);

        final ExecutableFlowBase embeddedFlow =
            new ExecutableFlowBase(project, node, subFlow, this);
        this.executableNodes.put(id, embeddedFlow);
      } else {
        final ExecutableNode exNode = new ExecutableNode(node, this);
        this.executableNodes.put(id, exNode);
      }
    }

    for (final Edge edge : flow.getEdges()) {
      final ExecutableNode sourceNode = this.executableNodes.get(edge.getSourceId());
      final ExecutableNode targetNode = this.executableNodes.get(edge.getTargetId());

      if (sourceNode == null) {
        logger.info("Source node " + edge.getSourceId() + " doesn't exist");
      }
      sourceNode.addOutNode(edge.getTargetId());
      targetNode.addInNode(edge.getSourceId());
    }
  }

  public List<ExecutableNode> getExecutableNodes() {
    return new ArrayList<>(this.executableNodes.values());
  }

  public ExecutableNode getExecutableNode(final String id) {
    return this.executableNodes.get(id);
  }

  public ExecutableNode getExecutableNodePath(final String ids) {
    final String[] split = ids.split(":");
    return getExecutableNodePath(split);
  }

  public ExecutableNode getExecutableNodePath(final String... ids) {
    return getExecutableNodePath(this, ids, 0);
  }

  private ExecutableNode getExecutableNodePath(final ExecutableFlowBase flow,
      final String[] ids, int currentIdIdx) {
    final ExecutableNode node = flow.getExecutableNode(ids[currentIdIdx]);
    currentIdIdx++;

    if (node == null) {
      return null;
    }

    if (ids.length == currentIdIdx) {
      return node;
    } else if (node instanceof ExecutableFlowBase) {
      return getExecutableNodePath((ExecutableFlowBase) node, ids, currentIdIdx);
    } else {
      return null;
    }

  }

  public List<String> getStartNodes() {
    if (this.startNodes == null) {
      this.startNodes = new ArrayList<>();
      for (final ExecutableNode node : this.executableNodes.values()) {
        if (node.getInNodes().isEmpty()) {
          this.startNodes.add(node.getId());
        }
      }
    }

    return this.startNodes;
  }

  public List<String> getEndNodes() {
    if (this.endNodes == null) {
      this.endNodes = new ArrayList<>();
      for (final ExecutableNode node : this.executableNodes.values()) {
        if (node.getOutNodes().isEmpty()) {
          this.endNodes.add(node.getId());
        }
      }
    }

    return this.endNodes;
  }

  @Override
  public Map<String, Object> toObject() {
    final Map<String, Object> mapObj = new HashMap<>();
    fillMapFromExecutable(mapObj);

    return mapObj;
  }

  @Override
  protected void fillMapFromExecutable(final Map<String, Object> flowObjMap) {
    super.fillMapFromExecutable(flowObjMap);

    flowObjMap.put(FLOW_ID_PARAM, this.flowId);

    final ArrayList<Object> nodes = new ArrayList<>();
    for (final ExecutableNode node : this.executableNodes.values()) {
      nodes.add(node.toObject());
    }
    flowObjMap.put(NODES_PARAM, nodes);

    // Flow properties
    final ArrayList<Object> props = new ArrayList<>();
    for (final FlowProps fprop : this.flowProps.values()) {
      final HashMap<String, Object> propObj = new HashMap<>();
      final String source = fprop.getSource();
      final String inheritedSource = fprop.getInheritedSource();

      propObj.put(SOURCE_PARAM, source);
      if (inheritedSource != null) {
        propObj.put(INHERITED_PARAM, inheritedSource);
      }
      props.add(propObj);
    }
    flowObjMap.put(PROPERTIES_PARAM, props);
  }

  @Override
  public void fillExecutableFromMapObject(
      final TypedMapWrapper<String, Object> flowObjMap) {
    super.fillExecutableFromMapObject(flowObjMap);

    this.flowId = flowObjMap.getString(FLOW_ID_PARAM);
    final List<Object> nodes = flowObjMap.<Object>getList(NODES_PARAM);

    if (nodes != null) {
      for (final Object nodeObj : nodes) {
        final Map<String, Object> nodeObjMap = (Map<String, Object>) nodeObj;
        final TypedMapWrapper<String, Object> wrapper =
            new TypedMapWrapper<>(nodeObjMap);

        final String type = wrapper.getString(TYPE_PARAM);
        if (type != null && type.equals(SpecialJobTypes.EMBEDDED_FLOW_TYPE)) {
          final ExecutableFlowBase exFlow = new ExecutableFlowBase();
          exFlow.fillExecutableFromMapObject(wrapper);
          exFlow.setParentFlow(this);

          this.executableNodes.put(exFlow.getId(), exFlow);
        } else {
          final ExecutableNode exJob = new ExecutableNode();
          exJob.fillExecutableFromMapObject(nodeObjMap);
          exJob.setParentFlow(this);

          this.executableNodes.put(exJob.getId(), exJob);
        }
      }
    }

    final List<Object> properties = flowObjMap.<Object>getList(PROPERTIES_PARAM);
    for (final Object propNode : properties) {
      final HashMap<String, Object> fprop = (HashMap<String, Object>) propNode;
      final String source = (String) fprop.get("source");
      final String inheritedSource = (String) fprop.get("inherited");

      final FlowProps flowProps = new FlowProps(inheritedSource, source);
      this.flowProps.put(source, flowProps);
    }
  }

  public Map<String, Object> toUpdateObject(final long lastUpdateTime) {
    final Map<String, Object> updateData = super.toUpdateObject();

    final List<Map<String, Object>> updatedNodes =
        new ArrayList<>();
    for (final ExecutableNode node : this.executableNodes.values()) {
      if (node instanceof ExecutableFlowBase) {
        final Map<String, Object> updatedNodeMap =
            ((ExecutableFlowBase) node).toUpdateObject(lastUpdateTime);
        // We add only flows to the list which either have a good update time,
        // or has updated descendants.
        if (node.getUpdateTime() > lastUpdateTime
            || updatedNodeMap.containsKey(NODES_PARAM)) {
          updatedNodes.add(updatedNodeMap);
        }
      } else {
        if (node.getUpdateTime() > lastUpdateTime) {
          final Map<String, Object> updatedNodeMap = node.toUpdateObject();
          updatedNodes.add(updatedNodeMap);
        }
      }
    }

    // if there are no updated nodes, we just won't add it to the list. This is
    // good
    // since if this is a nested flow, the parent is given the option to include
    // or
    // discard these subflows.
    if (!updatedNodes.isEmpty()) {
      updateData.put(NODES_PARAM, updatedNodes);
    }
    return updateData;
  }

  public void applyUpdateObject(final TypedMapWrapper<String, Object> updateData,
      final List<ExecutableNode> updatedNodes) {
    super.applyUpdateObject(updateData);

    if (updatedNodes != null) {
      updatedNodes.add(this);
    }

    final List<Map<String, Object>> nodes =
        (List<Map<String, Object>>) updateData
            .<Map<String, Object>>getList(NODES_PARAM);
    if (nodes != null) {
      for (final Map<String, Object> node : nodes) {
        final TypedMapWrapper<String, Object> nodeWrapper =
            new TypedMapWrapper<>(node);
        String id = nodeWrapper.getString(ID_PARAM);
        if (id == null) {
          // Legacy case
          id = nodeWrapper.getString("jobId");
        }

        final ExecutableNode exNode = this.executableNodes.get(id);
        if (updatedNodes != null) {
          updatedNodes.add(exNode);
        }

        if (exNode instanceof ExecutableFlowBase) {
          ((ExecutableFlowBase) exNode).applyUpdateObject(nodeWrapper,
              updatedNodes);
        } else {
          exNode.applyUpdateObject(nodeWrapper);
        }
      }
    }
  }

  public void applyUpdateObject(final Map<String, Object> updateData,
      final List<ExecutableNode> updatedNodes) {
    final TypedMapWrapper<String, Object> typedMapWrapper =
        new TypedMapWrapper<>(updateData);
    applyUpdateObject(typedMapWrapper, updatedNodes);
  }

  @Override
  public void applyUpdateObject(final Map<String, Object> updateData) {
    final TypedMapWrapper<String, Object> typedMapWrapper =
        new TypedMapWrapper<>(updateData);
    applyUpdateObject(typedMapWrapper, null);
  }

  public String getFlowPath() {
    if (this.getParentFlow() == null) {
      return this.getFlowId();
    } else {
      return this.getId() + ":" + this.getFlowId();
    }
  }
}
