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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.FlowProps;
import azkaban.flow.Node;
import azkaban.flow.SpecialJobTypes;
import azkaban.project.Project;
import azkaban.utils.TypedMapWrapper;

public class ExecutableFlowBase extends ExecutableNode {
  public static final String FLOW_ID_PARAM = "flowId";
  public static final String NODES_PARAM = "nodes";
  public static final String PROPERTIES_PARAM = "properties";
  public static final String SOURCE_PARAM = "source";
  public static final String INHERITED_PARAM = "inherited";

  private HashMap<String, ExecutableNode> executableNodes =
      new HashMap<String, ExecutableNode>();
  private ArrayList<String> startNodes;
  private ArrayList<String> endNodes;

  private HashMap<String, FlowProps> flowProps =
      new HashMap<String, FlowProps>();
  private String flowId;

  public ExecutableFlowBase(Project project, Node node, Flow flow,
      ExecutableFlowBase parent) {
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
    return flowProps.values();
  }

  public String getFlowId() {
    return flowId;
  }

  protected void setFlow(Project project, Flow flow) {
    this.flowId = flow.getId();
    flowProps.putAll(flow.getAllFlowProps());

    for (Node node : flow.getNodes()) {
      String id = node.getId();
      if (node.getType().equals(SpecialJobTypes.EMBEDDED_FLOW_TYPE)) {
        String embeddedFlowId = node.getEmbeddedFlowId();
        Flow subFlow = project.getFlow(embeddedFlowId);

        ExecutableFlowBase embeddedFlow =
            new ExecutableFlowBase(project, node, subFlow, this);
        executableNodes.put(id, embeddedFlow);
      } else {
        ExecutableNode exNode = new ExecutableNode(node, this);
        executableNodes.put(id, exNode);
      }
    }

    for (Edge edge : flow.getEdges()) {
      ExecutableNode sourceNode = executableNodes.get(edge.getSourceId());
      ExecutableNode targetNode = executableNodes.get(edge.getTargetId());

      if (sourceNode == null) {
        System.out.println("Source node " + edge.getSourceId()
            + " doesn't exist");
      }
      sourceNode.addOutNode(edge.getTargetId());
      targetNode.addInNode(edge.getSourceId());
    }
  }

  public List<ExecutableNode> getExecutableNodes() {
    return new ArrayList<ExecutableNode>(executableNodes.values());
  }

  public ExecutableNode getExecutableNode(String id) {
    return executableNodes.get(id);
  }

  public ExecutableNode getExecutableNodePath(String ids) {
    String[] split = ids.split(":");
    return getExecutableNodePath(split);
  }

  public ExecutableNode getExecutableNodePath(String... ids) {
    return getExecutableNodePath(this, ids, 0);
  }

  private ExecutableNode getExecutableNodePath(ExecutableFlowBase flow,
      String[] ids, int currentIdIdx) {
    ExecutableNode node = flow.getExecutableNode(ids[currentIdIdx]);
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
    if (startNodes == null) {
      startNodes = new ArrayList<String>();
      for (ExecutableNode node : executableNodes.values()) {
        if (node.getInNodes().isEmpty()) {
          startNodes.add(node.getId());
        }
      }
    }

    return startNodes;
  }

  public List<String> getEndNodes() {
    if (endNodes == null) {
      endNodes = new ArrayList<String>();
      for (ExecutableNode node : executableNodes.values()) {
        if (node.getOutNodes().isEmpty()) {
          endNodes.add(node.getId());
        }
      }
    }

    return endNodes;
  }

  @Override
  public Map<String, Object> toObject() {
    Map<String, Object> mapObj = new HashMap<String, Object>();
    fillMapFromExecutable(mapObj);

    return mapObj;
  }

  @Override
  protected void fillMapFromExecutable(Map<String, Object> flowObjMap) {
    super.fillMapFromExecutable(flowObjMap);

    flowObjMap.put(FLOW_ID_PARAM, flowId);

    ArrayList<Object> nodes = new ArrayList<Object>();
    for (ExecutableNode node : executableNodes.values()) {
      nodes.add(node.toObject());
    }
    flowObjMap.put(NODES_PARAM, nodes);

    // Flow properties
    ArrayList<Object> props = new ArrayList<Object>();
    for (FlowProps fprop : flowProps.values()) {
      HashMap<String, Object> propObj = new HashMap<String, Object>();
      String source = fprop.getSource();
      String inheritedSource = fprop.getInheritedSource();

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
      TypedMapWrapper<String, Object> flowObjMap) {
    super.fillExecutableFromMapObject(flowObjMap);

    this.flowId = flowObjMap.getString(FLOW_ID_PARAM);
    List<Object> nodes = flowObjMap.<Object> getList(NODES_PARAM);

    if (nodes != null) {
      for (Object nodeObj : nodes) {
        @SuppressWarnings("unchecked")
        Map<String, Object> nodeObjMap = (Map<String, Object>) nodeObj;
        TypedMapWrapper<String, Object> wrapper =
            new TypedMapWrapper<String, Object>(nodeObjMap);

        String type = wrapper.getString(TYPE_PARAM);
        if (type != null && type.equals(SpecialJobTypes.EMBEDDED_FLOW_TYPE)) {
          ExecutableFlowBase exFlow = new ExecutableFlowBase();
          exFlow.fillExecutableFromMapObject(wrapper);
          exFlow.setParentFlow(this);

          executableNodes.put(exFlow.getId(), exFlow);
        } else {
          ExecutableNode exJob = new ExecutableNode();
          exJob.fillExecutableFromMapObject(nodeObjMap);
          exJob.setParentFlow(this);

          executableNodes.put(exJob.getId(), exJob);
        }
      }
    }

    List<Object> properties = flowObjMap.<Object> getList(PROPERTIES_PARAM);
    for (Object propNode : properties) {
      @SuppressWarnings("unchecked")
      HashMap<String, Object> fprop = (HashMap<String, Object>) propNode;
      String source = (String) fprop.get("source");
      String inheritedSource = (String) fprop.get("inherited");

      FlowProps flowProps = new FlowProps(inheritedSource, source);
      this.flowProps.put(source, flowProps);
    }
  }

  public Map<String, Object> toUpdateObject(long lastUpdateTime) {
    Map<String, Object> updateData = super.toUpdateObject();

    List<Map<String, Object>> updatedNodes =
        new ArrayList<Map<String, Object>>();
    for (ExecutableNode node : executableNodes.values()) {
      if (node instanceof ExecutableFlowBase) {
        Map<String, Object> updatedNodeMap =
            ((ExecutableFlowBase) node).toUpdateObject(lastUpdateTime);
        // We add only flows to the list which either have a good update time,
        // or has updated descendants.
        if (node.getUpdateTime() > lastUpdateTime
            || updatedNodeMap.containsKey(NODES_PARAM)) {
          updatedNodes.add(updatedNodeMap);
        }
      } else {
        if (node.getUpdateTime() > lastUpdateTime) {
          Map<String, Object> updatedNodeMap = node.toUpdateObject();
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

  public void applyUpdateObject(TypedMapWrapper<String, Object> updateData,
      List<ExecutableNode> updatedNodes) {
    super.applyUpdateObject(updateData);

    if (updatedNodes != null) {
      updatedNodes.add(this);
    }

    List<Map<String, Object>> nodes =
        (List<Map<String, Object>>) updateData
        .<Map<String, Object>> getList(NODES_PARAM);
    if (nodes != null) {
      for (Map<String, Object> node : nodes) {
        TypedMapWrapper<String, Object> nodeWrapper =
            new TypedMapWrapper<String, Object>(node);
        String id = nodeWrapper.getString(ID_PARAM);
        if (id == null) {
          // Legacy case
          id = nodeWrapper.getString("jobId");
        }

        ExecutableNode exNode = executableNodes.get(id);
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

  public void applyUpdateObject(Map<String, Object> updateData,
      List<ExecutableNode> updatedNodes) {
    TypedMapWrapper<String, Object> typedMapWrapper =
        new TypedMapWrapper<String, Object>(updateData);
    applyUpdateObject(typedMapWrapper, updatedNodes);
  }

  @Override
  public void applyUpdateObject(Map<String, Object> updateData) {
    TypedMapWrapper<String, Object> typedMapWrapper =
        new TypedMapWrapper<String, Object>(updateData);
    applyUpdateObject(typedMapWrapper, null);
  }

  public void reEnableDependents(ExecutableNode... nodes) {
    for (ExecutableNode node : nodes) {
      for (String dependent : node.getOutNodes()) {
        ExecutableNode dependentNode = getExecutableNode(dependent);

        if (dependentNode.getStatus() == Status.KILLED) {
          dependentNode.setStatus(Status.READY);
          dependentNode.setUpdateTime(System.currentTimeMillis());
          reEnableDependents(dependentNode);

          if (dependentNode instanceof ExecutableFlowBase) {

            ((ExecutableFlowBase) dependentNode).reEnableDependents();
          }
        } else if (dependentNode.getStatus() == Status.SKIPPED) {
          dependentNode.setStatus(Status.DISABLED);
          dependentNode.setUpdateTime(System.currentTimeMillis());
          reEnableDependents(dependentNode);
        }
      }
    }
  }

  /**
   * Only returns true if the status of all finished nodes is true.
   *
   * @return
   */
  public boolean isFlowFinished() {
    for (String end : getEndNodes()) {
      ExecutableNode node = getExecutableNode(end);
      if (!Status.isStatusFinished(node.getStatus())) {
        return false;
      }
    }

    return true;
  }

  /**
   * Finds all jobs which are ready to run. This occurs when all of its
   * dependency nodes are finished running.
   *
   * It will also return any subflow that has been completed such that the
   * FlowRunner can properly handle them.
   *
   * @param flow
   * @return
   */
  public List<ExecutableNode> findNextJobsToRun() {
    ArrayList<ExecutableNode> jobsToRun = new ArrayList<ExecutableNode>();

    if (isFlowFinished() && !Status.isStatusFinished(getStatus())) {
      jobsToRun.add(this);
    } else {
      nodeloop: for (ExecutableNode node : executableNodes.values()) {
        if (Status.isStatusFinished(node.getStatus())) {
          continue;
        }

        if ((node instanceof ExecutableFlowBase)
            && Status.isStatusRunning(node.getStatus())) {
          // If the flow is still running, we traverse into the flow
          jobsToRun.addAll(((ExecutableFlowBase) node).findNextJobsToRun());
        } else if (Status.isStatusRunning(node.getStatus())) {
          continue;
        } else {
          for (String dependency : node.getInNodes()) {
            // We find that the outer-loop is unfinished.
            if (!Status.isStatusFinished(getExecutableNode(dependency)
                .getStatus())) {
              continue nodeloop;
            }
          }

          jobsToRun.add(node);
        }
      }
    }

    return jobsToRun;
  }

  public String getFlowPath() {
    if (this.getParentFlow() == null) {
      return this.getFlowId();
    } else {
      return this.getParentFlow().getFlowPath() + "," + this.getId() + ":"
          + this.getFlowId();
    }
  }
}
