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

import static azkaban.flow.CommonJobProperties.FAILURE_ACTION_PROPERTY;

import azkaban.Constants;
import azkaban.executor.mail.DefaultMailCreator;
import azkaban.utils.Pair;
import azkaban.utils.SecurityTag;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Flow {

  private static final String FLOW_NAME_PROPERTY = "id";
  private static final String PROJECT_ID_PROPERTY = "project.id";
  private static final String PROJECT_VERSION_PROPERTY = "version";
  private static final String AZKABAN_FLOW_VERSION_PROPERTY = "azkabanFlowVersion";
  private static final String EMBEDDED_FLOW_PROPERTY = "embeddedFlow";
  private static final String TYPE_PROPERTY = "type";
  private static final String LAYEDOUT_PROPERTY = "layedout";
  private static final String CONDITION_PROPERTY = "condition";
  private static final String PROPS_PROPERTY = "props";
  private static final String NODES_PROPERTY = "nodes";
  private static final String EDGES_PROPERTY = "edges";
  private static final String METADATA_PROPERTY = "metadata";
  private static final String FAILURE_EMAIL_PROPERTY = "failure.email";
  private static final String SUCCESS_EMAIL_PROPERTY = "success.email";
  private static final String OVERRIDE_EMAIL_PROPERTY = "override.email";
  private static final String MAIL_CREATOR_PROPERTY = "mailCreator";
  private static final String ERRORS_PROPERTY = "errors";
  private static final String IS_LOCKED_PROPERTY = "isLocked";
  private static final String FLOW_LOCK_ERROR_MESSAGE_PROPERTY = "flowLockErrorMessage";
  private static final String SECURITY_TAG_PROPERTY = "securityTag";

  private final String id;  // This is actually the node path. I.e. rootFlow:subFlow1:subFlow2
  private int projectId;
  private int version = -1; // This is actually the project version
  private final HashMap<String, Node> nodes = new HashMap<>();
  private final HashMap<String, Edge> edges = new HashMap<>();
  private final HashMap<String, Set<Edge>> outEdges = new HashMap<>();
  private final HashMap<String, Set<Edge>> inEdges = new HashMap<>();
  private final HashMap<String, ImmutableFlowProps> flowProps = new HashMap<>();
  private ArrayList<Node> startNodes = null;
  private ArrayList<Node> endNodes = null;
  private int numLevels = -1;
  private final List<String> failureEmail = new ArrayList<>();
  private final List<String> successEmail = new ArrayList<>();
  private final List<String> overrideEmail = new ArrayList<>();
  private SecurityTag securityTag;

  public String getFailureAction() {
    return failureAction;
  }

  public void setFailureAction(final String failureAction) {
    this.failureAction = failureAction;
  }

  private String failureAction;
  private String mailCreator = DefaultMailCreator.DEFAULT_MAIL_CREATOR;
  private ArrayList<String> errors;
  private Map<String, Object> metadata = new HashMap<>();

  private boolean isLayedOut = false;
  private boolean isEmbeddedFlow = false;
  private double azkabanFlowVersion = Constants.DEFAULT_AZKABAN_FLOW_VERSION;
  private String condition = null;
  private boolean isLocked = false;
  private String flowLockErrorMessage = null;

  public Flow(final String id) {
    this.id = id;
  }

  public static Flow flowFromObject(final Object object) {
    final Map<String, Object> flowObject = (Map<String, Object>) object;

    final String id = (String) flowObject.get(FLOW_NAME_PROPERTY);
    final Flow flow = new Flow(id);

    final Boolean layedout = (Boolean) flowObject
        .getOrDefault(LAYEDOUT_PROPERTY, flow.isLayedOut());
    final Boolean isEmbeddedFlow = (Boolean) flowObject
        .getOrDefault(EMBEDDED_FLOW_PROPERTY, flow.isEmbeddedFlow());
    final Double azkabanFlowVersion = (Double) flowObject
        .getOrDefault(AZKABAN_FLOW_VERSION_PROPERTY, flow.getAzkabanFlowVersion());
    final String condition = (String) flowObject
        .getOrDefault(CONDITION_PROPERTY, flow.getCondition());
    final Boolean isLocked = (Boolean) flowObject.getOrDefault(IS_LOCKED_PROPERTY, flow.isLocked());
    final String flowLockErrorMessage = (String) flowObject
        .getOrDefault(FLOW_LOCK_ERROR_MESSAGE_PROPERTY, flow.getFlowLockErrorMessage());
    final int projectId = (Integer) flowObject
        .getOrDefault(PROJECT_ID_PROPERTY, flow.getProjectId());
    final int projectVersion = (Integer) flowObject
        .getOrDefault(PROJECT_VERSION_PROPERTY, flow.getVersion());
    final List<Object> propertiesList = (List<Object>) flowObject
        .getOrDefault(PROPS_PROPERTY, new HashMap<>());
    final List<Object> nodeList = (List<Object>) flowObject
        .getOrDefault(NODES_PROPERTY, new HashMap<>());
    final List<Object> edgeList = (List<Object>) flowObject
        .getOrDefault(EDGES_PROPERTY, new HashMap<>());
    final Map<String, Object> metadata = (Map<String, Object>) flowObject
        .getOrDefault(METADATA_PROPERTY, flow.getMetadata());
    final List<String> failureEmails = (List<String>) flowObject
        .getOrDefault(FAILURE_EMAIL_PROPERTY, flow.getFailureEmails());
    final List<String> successEmails = (List<String>) flowObject
        .getOrDefault(SUCCESS_EMAIL_PROPERTY, flow.getSuccessEmails());
    final List<String> overrideEmails = (List<String>) flowObject
        .getOrDefault(OVERRIDE_EMAIL_PROPERTY, flow.getOverrideEmails());
    final String mailCreator = (String) flowObject
        .getOrDefault(MAIL_CREATOR_PROPERTY, flow.getMailCreator());
    final String failureAction = (String) flowObject.getOrDefault(
        FAILURE_ACTION_PROPERTY, flow.getFailureAction()
    );
    final String securityTag = (String) flowObject.get(SECURITY_TAG_PROPERTY);
    flow.setLayedOut(layedout);
    flow.setEmbeddedFlow(isEmbeddedFlow);
    flow.setAzkabanFlowVersion(azkabanFlowVersion);
    flow.setCondition(condition);
    flow.setLocked(isLocked);
    flow.setFlowLockErrorMessage(flowLockErrorMessage);
    flow.setProjectId(projectId);
    flow.setVersion(projectVersion);
    flow.setMetadata(metadata);
    flow.addFailureEmails(failureEmails);
    flow.addSuccessEmails(successEmails);
    flow.setMailCreator(mailCreator);
    flow.setFailureAction(failureAction);
    flow.setSecurityTag(securityTag != null ? SecurityTag.valueOf(securityTag) : null);
    // Loading projects
    final Map<String, ImmutableFlowProps> properties = loadPropertiesFromObject(propertiesList);
    flow.addAllFlowProperties(properties.values());

    // Loading nodes
    final Map<String, Node> nodes = loadNodesFromObjects(nodeList);
    flow.addAllNodes(nodes.values());

    // Loading edges
    final List<Edge> edges = loadEdgeFromObjects(edgeList);
    flow.addAllEdges(edges);

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

  private static List<Edge> loadEdgeFromObjects(final List<Object> edgeList) {
    final List<Edge> edgeResult = new ArrayList<>();

    for (final Object obj : edgeList) {
      final Edge edge = Edge.fromObject(obj);
      edgeResult.add(edge);
    }

    return edgeResult;
  }

  private static Map<String, ImmutableFlowProps> loadPropertiesFromObject(
      final List<Object> propertyObjectList) {
    final Map<String, ImmutableFlowProps> properties = new HashMap<>();

    for (final Object propObj : propertyObjectList) {
      final ImmutableFlowProps prop = ImmutableFlowProps.fromObject(propObj);
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

  public List<String> getOverrideEmails() {
    return this.overrideEmail;
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

  public void addOverrideEmails(final Collection<String> emails) {
    this.overrideEmail.addAll(emails);
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

  public void addAllFlowProperties(final Collection<ImmutableFlowProps> props) {
    for (final ImmutableFlowProps prop : props) {
      this.flowProps.put(prop.getSource(), prop);
    }
  }

  public String getId() {
    return this.id;
  }

  /**
   * Return all flow parents of this flow. From each parent its name and node path are returned.
   *
   * @return the list of parents
   */
  public List<Pair<String, String>> getParents() {
    final List<Pair<String, String>> parents = new ArrayList<>(); // Tuple (flow name, node path)
    final String[] parentNames = getId().split(Constants.PATH_DELIMITER, 0);
    parents.add(new Pair<>(parentNames[0], parentNames[0])); // root flow is first element
    for (int i = 1; i < parentNames.length; i++) {
      final String nodePath = String
          .join(Constants.PATH_DELIMITER, parents.get(parents.size() - 1).getSecond(),
              parentNames[i]);
      parents.add(new Pair<>(parentNames[i], nodePath));
    }
    return parents;
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
    flowObj.put(TYPE_PROPERTY, "flow");
    flowObj.put(FLOW_NAME_PROPERTY, getId());
    flowObj.put(PROJECT_ID_PROPERTY, this.projectId);
    flowObj.put(PROJECT_VERSION_PROPERTY, this.version);
    flowObj.put(PROPS_PROPERTY, objectizeProperties());
    flowObj.put(NODES_PROPERTY, objectizeNodes());
    flowObj.put(EDGES_PROPERTY, objectizeEdges());
    flowObj.put(FAILURE_EMAIL_PROPERTY, this.failureEmail);
    flowObj.put(SUCCESS_EMAIL_PROPERTY, this.successEmail);
    flowObj.put(OVERRIDE_EMAIL_PROPERTY, this.overrideEmail);
    flowObj.put(MAIL_CREATOR_PROPERTY, this.mailCreator);
    flowObj.put(LAYEDOUT_PROPERTY, this.isLayedOut);
    flowObj.put(EMBEDDED_FLOW_PROPERTY, this.isEmbeddedFlow);
    flowObj.put(AZKABAN_FLOW_VERSION_PROPERTY, this.azkabanFlowVersion);
    flowObj.put(CONDITION_PROPERTY, this.condition);
    flowObj.put(IS_LOCKED_PROPERTY, this.isLocked);
    flowObj.put(FLOW_LOCK_ERROR_MESSAGE_PROPERTY, this.flowLockErrorMessage);
    if (this.securityTag != null) {
      flowObj.put(SECURITY_TAG_PROPERTY, this.securityTag.name());
    }

    if (this.failureAction != null) {
      flowObj.put(FAILURE_ACTION_PROPERTY, this.failureAction);
    }
    if (this.errors != null) {
      flowObj.put(ERRORS_PROPERTY, this.errors);
    }

    if (this.metadata != null) {
      flowObj.put(METADATA_PROPERTY, this.metadata);
    }

    return flowObj;
  }

  private List<Object> objectizeProperties() {
    final ArrayList<Object> result = new ArrayList<>();
    for (final ImmutableFlowProps props : this.flowProps.values()) {
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

  public ImmutableFlowProps getFlowProps(final String propSource) {
    return this.flowProps.get(propSource);
  }

  public Map<String, ImmutableFlowProps> getAllFlowProps() {
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

  public String getFlowLockErrorMessage() {
    return this.flowLockErrorMessage;
  }

  public void setFlowLockErrorMessage(final String flowLockErrorMessage) {
    this.flowLockErrorMessage = flowLockErrorMessage;
  }

  public SecurityTag getSecurityTag() {
    return this.securityTag;
  }

  public void setSecurityTag(final SecurityTag securityTag) {
    this.securityTag = securityTag;
  }

  public void unsetSecurityTag() {
    this.securityTag = null;
  }
}
