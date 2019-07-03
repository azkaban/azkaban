/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.flow;

import azkaban.Constants;
import azkaban.executor.mail.DefaultMailCreator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Flow {

  private final String id;
  private final HashMap<String, Node> nodes = new HashMap<>();
  private final HashMap<String, Edge> edges = new HashMap<>();
  private final HashMap<String, Set<Edge>> outEdges =
      new HashMap<>();
  private final HashMap<String, Set<Edge>> inEdges = new HashMap<>();
  private final HashMap<String, FlowProps> flowProps =
      new HashMap<>();
  private int projectId;
  private ArrayList<Node> startNodes = null;
  private ArrayList<Node> endNodes = null;
  private int numLevels = -1;
  private List<String> failureEmail = new ArrayList<>();
  private List<String> successEmail = new ArrayList<>();
  private String mailCreator = DefaultMailCreator.DEFAULT_MAIL_CREATOR;
  private ArrayList<String> errors;
  private int version = -1;
  private Map<String, Object> metadata = new HashMap<>();

  private boolean isLayedOut = false;
  private boolean isEmbeddedFlow = false;
  private double azkabanFlowVersion = Constants.DEFAULT_AZKABAN_FLOW_VERSION;
  private String condition = null;
  private boolean isLocked = false;
  private String displayName;

  public Flow(final String id) {
    this.id = id;
  }

  public static Flow flowFromObject(final Object object) {
    final Map<String, Object> flowObject = (Map<String, Object>) object;

    final String id = (String) flowObject.get("id");
    final String displayName = (String) flowObject.get("displayName");
    final Boolean layedout = (Boolean) flowObject.get("layedout");
    final Boolean isEmbeddedFlow = (Boolean) flowObject.get("embeddedFlow");
    final Double azkabanFlowVersion = (Double) flowObject.get("azkabanFlowVersion");
    final String condition = (String) flowObject.get("condition");
    final Boolean isLocked = (Boolean) flowObject.get("isLocked");

    final Flow flow = new Flow(id);
    flow.displayName = displayName;

    if (layedout != null) {
      flow.setLayedOut(layedout);
    }

    if (isEmbeddedFlow != null) {
      flow.setEmbeddedFlow(isEmbeddedFlow);
    }

    if (azkabanFlowVersion != null) {
      flow.setAzkabanFlowVersion(azkabanFlowVersion);
    }

    if (condition != null) {
      flow.setCondition(condition);
    }

    if (isLocked != null) {
      flow.setLocked(isLocked);
    }

    final int projId = (Integer) flowObject.get("project.id");
    flow.setProjectId(projId);

    final int version = (Integer) flowObject.get("version");
    flow.setVersion(version);

    // Loading projects
    final List<Object> propertiesList = (List<Object>) flowObject.get("props");
    final Map<String, FlowProps> properties =
        loadPropertiesFromObject(propertiesList);
    flow.addAllFlowProperties(properties.values());

    // Loading nodes
    final List<Object> nodeList = (List<Object>) flowObject.get("nodes");
    final Map<String, Node> nodes = loadNodesFromObjects(nodeList);
    flow.addAllNodes(nodes.values());

    // Loading edges
    final List<Object> edgeList = (List<Object>) flowObject.get("edges");
    final List<Edge> edges = loadEdgeFromObjects(edgeList, nodes);
    flow.addAllEdges(edges);

    final Map<String, Object> metadata =
        (Map<String, Object>) flowObject.get("metadata");

    if (metadata != null) {
      flow.setMetadata(metadata);
    }

    flow.failureEmail = (List<String>) flowObject.get("failure.email");
    flow.successEmail = (List<String>) flowObject.get("success.email");
    if (flowObject.containsKey("mailCreator")) {
      flow.mailCreator = flowObject.get("mailCreator").toString();
    }
    return flow;
  }

  private static Map<String, Node> loadNodesFromObjects(final List<Object> nodeList) {
    final Map<String, Node> nodeMap = new HashMap<>();

    for (final Object obj : nodeList) {
      final Node node = Node.fromObject(obj);
      nodeMap.put(node.getId(), node);
    }

    return nodeMap;
  }

  private static List<Edge> loadEdgeFromObjects(final List<Object> edgeList,
      final Map<String, Node> nodes) {
    final List<Edge> edgeResult = new ArrayList<>();

    for (final Object obj : edgeList) {
      final Edge edge = Edge.fromObject(obj);
      edgeResult.add(edge);
    }

    return edgeResult;
  }

  private static Map<String, FlowProps> loadPropertiesFromObject(
      final List<Object> propertyObjectList) {
    final Map<String, FlowProps> properties = new HashMap<>();

    for (final Object propObj : propertyObjectList) {
      final FlowProps prop = FlowProps.fromObject(propObj);
      properties.put(prop.getSource(), prop);
    }

    return properties;
  }

  public int getVersion() {
    return this.version;
  }

  public void setVersion(final int version) {
    this.version = version;
  }

  public void initialize() {
    if (this.startNodes == null) {
      this.startNodes = new ArrayList<>();
      this.endNodes = new ArrayList<>();
      for (final Node node : this.nodes.values()) {
        // If it doesn't have any incoming edges, its a start node
        if (!this.inEdges.containsKey(node.getId())) {
          this.startNodes.add(node);
        }

        // If it doesn't contain any outgoing edges, its an end node.
        if (!this.outEdges.containsKey(node.getId())) {
          this.endNodes.add(node);
        }
      }

      setLevelsAndEdgeNodes(new HashSet<>(this.startNodes), 0);
    }
  }

  private void setLevelsAndEdgeNodes(final Set<Node> levelNodes, final int level) {
    final Set<Node> nextLevelNodes = new HashSet<>();

    for (final Node node : levelNodes) {
      node.setLevel(level);

      final Set<Edge> edges = this.outEdges.get(node.getId());
      if (edges != null) {
        edges.forEach(edge -> {
          edge.setSource(node);
          edge.setTarget(this.nodes.get(edge.getTargetId()));

          nextLevelNodes.add(edge.getTarget());
        });
      }
    }

    this.numLevels = level;

    if (!nextLevelNodes.isEmpty()) {
      setLevelsAndEdgeNodes(nextLevelNodes, level + 1);
    }
  }

  public Node getNode(final String nodeId) {
    return this.nodes.get(nodeId);
  }

  public List<String> getSuccessEmails() {
    return this.successEmail;
  }

  public String getMailCreator() {
    return this.mailCreator;
  }

  public void setMailCreator(final String mailCreator) {
    this.mailCreator = mailCreator;
  }

  public List<String> getFailureEmails() {
    return this.failureEmail;
  }

  public void addSuccessEmails(final Collection<String> emails) {
    this.successEmail.addAll(emails);
  }

  public void addFailureEmails(final Collection<String> emails) {
    this.failureEmail.addAll(emails);
  }

  public int getNumLevels() {
    return this.numLevels;
  }

  public List<Node> getStartNodes() {
    return this.startNodes;
  }

  public List<Node> getEndNodes() {
    return this.endNodes;
  }

  public Set<Edge> getInEdges(final String id) {
    return this.inEdges.get(id);
  }

  public Set<Edge> getOutEdges(final String id) {
    return this.outEdges.get(id);
  }

  public void addAllNodes(final Collection<Node> nodes) {
    for (final Node node : nodes) {
      addNode(node);
    }
  }

  public void addNode(final Node node) {
    this.nodes.put(node.getId(), node);
  }

  public void addAllFlowProperties(final Collection<FlowProps> props) {
    for (final FlowProps prop : props) {
      this.flowProps.put(prop.getSource(), prop);
    }
  }

  public String getId() {
    return this.id;
  }


  public String getDisplayName() {
    return this.displayName == null ? getId() : this.displayName;
  }

  public void setDisplayName(final String displayName) {
    this.displayName = displayName;
  }

  public void addError(final String error) {
    if (this.errors == null) {
      this.errors = new ArrayList<>();
    }

    this.errors.add(error);
  }

  public List<String> getErrors() {
    return this.errors;
  }

  public boolean hasErrors() {
    return this.errors != null && !this.errors.isEmpty();
  }

  public Collection<Node> getNodes() {
    return this.nodes.values();
  }

  public Collection<Edge> getEdges() {
    return this.edges.values();
  }

  public void addAllEdges(final Collection<Edge> edges) {
    for (final Edge edge : edges) {
      addEdge(edge);
    }
  }

  public void addEdge(final Edge edge) {
    final String source = edge.getSourceId();
    final String target = edge.getTargetId();

    if (edge.hasError()) {
      addError("Error on " + edge.getId() + ". " + edge.getError());
    }

    final Set<Edge> sourceSet = getEdgeSet(this.outEdges, source);
    sourceSet.add(edge);

    final Set<Edge> targetSet = getEdgeSet(this.inEdges, target);
    targetSet.add(edge);

    this.edges.put(edge.getId(), edge);
  }

  private Set<Edge> getEdgeSet(final HashMap<String, Set<Edge>> map, final String id) {
    Set<Edge> edges = map.get(id);
    if (edges == null) {
      edges = new HashSet<>();
      map.put(id, edges);
    }

    return edges;
  }

  public Map<String, Object> toObject() {
    final HashMap<String, Object> flowObj = new HashMap<>();
    flowObj.put("type", "flow");
    flowObj.put("id", getId());
    if (this.displayName != null) {
      flowObj.put("displayName", this.displayName);
    }
    flowObj.put("project.id", this.projectId);
    flowObj.put("version", this.version);
    flowObj.put("props", objectizeProperties());
    flowObj.put("nodes", objectizeNodes());
    flowObj.put("edges", objectizeEdges());
    flowObj.put("failure.email", this.failureEmail);
    flowObj.put("success.email", this.successEmail);
    flowObj.put("mailCreator", this.mailCreator);
    flowObj.put("layedout", this.isLayedOut);
    flowObj.put("embeddedFlow", this.isEmbeddedFlow);
    flowObj.put("azkabanFlowVersion", this.azkabanFlowVersion);
    flowObj.put("condition", this.condition);
    flowObj.put("isLocked", this.isLocked);

    if (this.errors != null) {
      flowObj.put("errors", this.errors);
    }

    if (this.metadata != null) {
      flowObj.put("metadata", this.metadata);
    }

    return flowObj;
  }

  private List<Object> objectizeProperties() {
    final ArrayList<Object> result = new ArrayList<>();
    for (final FlowProps props : this.flowProps.values()) {
      final Object objProps = props.toObject();
      result.add(objProps);
    }

    return result;
  }

  private List<Object> objectizeNodes() {
    final ArrayList<Object> result = new ArrayList<>();
    for (final Node node : getNodes()) {
      final Object nodeObj = node.toObject();
      result.add(nodeObj);
    }

    return result;
  }

  private List<Object> objectizeEdges() {
    final ArrayList<Object> result = new ArrayList<>();
    for (final Edge edge : getEdges()) {
      final Object edgeObj = edge.toObject();
      result.add(edgeObj);
    }

    return result;
  }

  public boolean isLayedOut() {
    return this.isLayedOut;
  }

  public void setLayedOut(final boolean layedOut) {
    this.isLayedOut = layedOut;
  }

  public boolean isEmbeddedFlow() {
    return this.isEmbeddedFlow;
  }

  public void setEmbeddedFlow(final boolean embeddedFlow) {
    this.isEmbeddedFlow = embeddedFlow;
  }

  public double getAzkabanFlowVersion() {
    return this.azkabanFlowVersion;
  }

  public void setAzkabanFlowVersion(final double azkabanFlowVersion) {
    this.azkabanFlowVersion = azkabanFlowVersion;
  }

  public String getCondition() {
    return this.condition;
  }

  public void setCondition(final String condition) {
    this.condition = condition;
  }

  public Map<String, Object> getMetadata() {
    if (this.metadata == null) {
      this.metadata = new HashMap<>();
    }
    return this.metadata;
  }

  public void setMetadata(final Map<String, Object> metadata) {
    this.metadata = metadata;
  }

  public Map<String, Node> getNodeMap() {
    return this.nodes;
  }

  public Map<String, Set<Edge>> getOutEdgeMap() {
    return this.outEdges;
  }

  public Map<String, Set<Edge>> getInEdgeMap() {
    return this.inEdges;
  }

  public FlowProps getFlowProps(final String propSource) {
    return this.flowProps.get(propSource);
  }

  public Map<String, FlowProps> getAllFlowProps() {
    return this.flowProps;
  }

  public int getProjectId() {
    return this.projectId;
  }

  public void setProjectId(final int projectId) {
    this.projectId = projectId;
  }

  public boolean isLocked() {
    return this.isLocked;
  }

  public void setLocked(final boolean locked) {
    this.isLocked = locked;
  }
}
