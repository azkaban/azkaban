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

package azkaban.project;

import azkaban.flow.CommonJobProperties;
import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.FlowProps;
import azkaban.flow.Node;
import azkaban.flow.SpecialJobTypes;
import azkaban.project.FlowLoaderUtils.DirFilter;
import azkaban.project.FlowLoaderUtils.SuffixFilter;
import azkaban.project.validator.ValidationReport;
import azkaban.utils.Props;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads job and properties files to flows from project directory.
 */
public class DirectoryFlowLoader implements FlowLoader {

  private static final String PROPERTY_SUFFIX = ".properties";
  private static final String JOB_SUFFIX = ".job";

  private static final Logger logger = LoggerFactory.getLogger(DirectoryFlowLoader.class);
  private final Props props;
  private final Set<String> errors = new HashSet<>();
  private final Map<String, Flow> flowMap = new HashMap<>();
  private HashSet<String> rootNodes;
  private HashMap<String, Node> nodeMap;
  private HashMap<String, Map<String, Edge>> nodeDependencies;
  private HashMap<String, Props> jobPropsMap;

  // Flow dependencies for embedded flows.
  private HashMap<String, Set<String>> flowDependencies;

  private ArrayList<FlowProps> flowPropsList;
  private ArrayList<Props> propsList;
  private Set<String> duplicateJobs;

  /**
   * Creates a new DirectoryFlowLoader.
   *
   * @param props Properties to add.
   */
  public DirectoryFlowLoader(final Props props) {
    this.props = props;
  }

  /**
   * Returns the flow map constructed from the loaded flows.
   *
   * @return Map of flow name to Flow.
   */
  @Override
  public Map<String, Flow> getFlowMap() {
    return this.flowMap;
  }

  /**
   * Returns errors caught when loading flows.
   *
   * @return Set of error strings.
   */
  @Override
  public Set<String> getErrors() {
    return this.errors;
  }

  /**
   * Returns job properties.
   *
   * @return Map of job name to properties.
   */
  public HashMap<String, Props> getJobPropsMap() {
    return this.jobPropsMap;
  }

  /**
   * Returns list of properties.
   *
   * @return List of Props.
   */
  public ArrayList<Props> getPropsList() {
    return this.propsList;
  }

  /**
   * Loads all project flows from the directory.
   *
   * @param project The project.
   * @param projectDir The directory to load flows from.
   * @return the validation report.
   */
  @Override
  public ValidationReport loadProjectFlow(final Project project, final File projectDir) {
    this.propsList = new ArrayList<>();
    this.flowPropsList = new ArrayList<>();
    this.jobPropsMap = new HashMap<>();
    this.nodeMap = new HashMap<>();
    this.duplicateJobs = new HashSet<>();
    this.nodeDependencies = new HashMap<>();
    this.rootNodes = new HashSet<>();
    this.flowDependencies = new HashMap<>();

    // Load all the props files and create the Node objects
    loadProjectFromDir(projectDir.getPath(), projectDir, null);

    // Create edges and find missing dependencies
    resolveDependencies();

    // Create the flows.
    buildFlowsFromDependencies();

    // Resolve embedded flows
    resolveEmbeddedFlows();

    FlowLoaderUtils.checkJobProperties(project.getId(), this.props, this.jobPropsMap, this.errors);

    return FlowLoaderUtils.generateFlowLoaderReport(this.errors);

  }

  private void loadProjectFromDir(final String base, final File dir, Props parent) {
    final File[] propertyFiles = dir.listFiles(new SuffixFilter(PROPERTY_SUFFIX));
    Arrays.sort(propertyFiles);

    for (final File file : propertyFiles) {
      final String relative = getRelativeFilePath(base, file.getPath());
      try {
        parent = new Props(parent, file);
        parent.setSource(relative);

        final FlowProps flowProps = new FlowProps(parent);
        this.flowPropsList.add(flowProps);
      } catch (final IOException e) {
        this.errors.add("Error loading properties " + file.getName() + ":"
            + e.getMessage());
      }

      this.logger.info("Adding " + relative);
      this.propsList.add(parent);
    }

    // Load all Job files. If there's a duplicate name, then we don't load
    final File[] jobFiles = dir.listFiles(new SuffixFilter(JOB_SUFFIX));
    for (final File file : jobFiles) {
      final String jobName = getNameWithoutExtension(file);
      try {
        if (!this.duplicateJobs.contains(jobName)) {
          if (this.jobPropsMap.containsKey(jobName)) {
            this.errors.add("Duplicate job names found '" + jobName + "'.");
            this.duplicateJobs.add(jobName);
            this.jobPropsMap.remove(jobName);
            this.nodeMap.remove(jobName);
          } else {
            final Props prop = new Props(parent, file);
            final String relative = getRelativeFilePath(base, file.getPath());
            prop.setSource(relative);

            final Node node = new Node(jobName);
            final String type = prop.getString("type", null);
            if (type == null) {
              this.errors.add("Job doesn't have type set '" + jobName + "'.");
            }

            node.setType(type);

            node.setJobSource(relative);
            if (parent != null) {
              node.setPropsSource(parent.getSource());
            }

            // Force root node
            if (prop.getBoolean(CommonJobProperties.ROOT_NODE, false)) {
              this.rootNodes.add(jobName);
            }

            this.jobPropsMap.put(jobName, prop);
            this.nodeMap.put(jobName, node);
          }
        }
      } catch (final IOException e) {
        this.errors.add("Error loading job file " + file.getName() + ":"
            + e.getMessage());
      }
    }

    for (final File file : dir.listFiles(new DirFilter())) {
      loadProjectFromDir(base, file, parent);
    }
  }

  private void resolveEmbeddedFlows() {
    for (final String flowId : this.flowDependencies.keySet()) {
      final HashSet<String> visited = new HashSet<>();
      resolveEmbeddedFlow(flowId, visited);
    }
  }

  private void resolveEmbeddedFlow(final String flowId, final Set<String> visited) {
    final Set<String> embeddedFlow = this.flowDependencies.get(flowId);
    if (embeddedFlow == null) {
      return;
    }

    visited.add(flowId);
    for (final String embeddedFlowId : embeddedFlow) {
      if (visited.contains(embeddedFlowId)) {
        this.errors.add("Embedded flow cycle found in " + flowId + "->"
            + embeddedFlowId);
        return;
      } else if (!this.flowMap.containsKey(embeddedFlowId)) {
        this.errors.add("Flow " + flowId + " depends on " + embeddedFlowId
            + " but can't be found.");
        return;
      } else {
        resolveEmbeddedFlow(embeddedFlowId, visited);
      }
    }

    visited.remove(flowId);
  }

  private void resolveDependencies() {
    // Add all the in edges and out edges. Catch bad dependencies and self
    // referrals. Also collect list of nodes who are parents.
    for (final Node node : this.nodeMap.values()) {
      final Props props = this.jobPropsMap.get(node.getId());

      if (props == null) {
        this.logger.error("Job props not found!! For some reason.");
        continue;
      }

      final List<String> dependencyList =
          props.getStringList(CommonJobProperties.DEPENDENCIES,
              (List<String>) null);

      if (dependencyList != null) {
        Map<String, Edge> dependencies = this.nodeDependencies.get(node.getId());
        if (dependencies == null) {
          dependencies = new HashMap<>();

          for (String dependencyName : dependencyList) {
            dependencyName =
                dependencyName == null ? null : dependencyName.trim();
            if (dependencyName == null || dependencyName.isEmpty()) {
              continue;
            }

            final Edge edge = new Edge(dependencyName, node.getId());
            final Node dependencyNode = this.nodeMap.get(dependencyName);
            if (dependencyNode == null) {
              if (this.duplicateJobs.contains(dependencyName)) {
                edge.setError("Ambiguous Dependency. Duplicates found.");
                dependencies.put(dependencyName, edge);
                this.errors.add(node.getId() + " has ambiguous dependency "
                    + dependencyName);
              } else {
                edge.setError("Dependency not found.");
                dependencies.put(dependencyName, edge);
                this.errors.add(node.getId() + " cannot find dependency "
                    + dependencyName);
              }
            } else if (dependencyNode == node) {
              // We have a self cycle
              edge.setError("Self cycle found.");
              dependencies.put(dependencyName, edge);
              this.errors.add(node.getId() + " has a self cycle");
            } else {
              dependencies.put(dependencyName, edge);
            }
          }

          if (!dependencies.isEmpty()) {
            this.nodeDependencies.put(node.getId(), dependencies);
          }
        }
      }
    }
  }

  private void buildFlowsFromDependencies() {
    // Find all root nodes by finding ones without dependents.
    final HashSet<String> nonRootNodes = new HashSet<>();
    for (final Map<String, Edge> edges : this.nodeDependencies.values()) {
      for (final String sourceId : edges.keySet()) {
        nonRootNodes.add(sourceId);
      }
    }

    // Now create flows. Bad flows are marked invalid
    for (final Node base : this.nodeMap.values()) {
      // Root nodes can be discovered when parsing jobs
      if (this.rootNodes.contains(base.getId())
          || !nonRootNodes.contains(base.getId())) {
        this.rootNodes.add(base.getId());
        final Flow flow = new Flow(base.getId());
        final Props jobProp = this.jobPropsMap.get(base.getId());

        FlowLoaderUtils.addEmailPropsToFlow(flow, jobProp);

        flow.addAllFlowProperties(this.flowPropsList);
        final Set<String> visitedNodesOnPath = new HashSet<>();
        final Set<String> visitedNodesEver = new HashSet<>();
        constructFlow(flow, base, visitedNodesOnPath, visitedNodesEver);

        flow.initialize();
        this.flowMap.put(base.getId(), flow);
      }
    }
  }

  private void constructFlow(final Flow flow, final Node node, final Set<String> visitedOnPath,
      final Set<String> visitedEver) {
    visitedOnPath.add(node.getId());
    visitedEver.add(node.getId());

    flow.addNode(node);
    if (SpecialJobTypes.EMBEDDED_FLOW_TYPE.equals(node.getType())) {
      final Props props = this.jobPropsMap.get(node.getId());
      final String embeddedFlow = props.get(SpecialJobTypes.FLOW_NAME);

      Set<String> embeddedFlows = this.flowDependencies.get(flow.getId());
      if (embeddedFlows == null) {
        embeddedFlows = new HashSet<>();
        this.flowDependencies.put(flow.getId(), embeddedFlows);
      }

      node.setEmbeddedFlowId(embeddedFlow);
      embeddedFlows.add(embeddedFlow);
    }
    final Map<String, Edge> dependencies = this.nodeDependencies.get(node.getId());

    if (dependencies != null) {
      for (Edge edge : dependencies.values()) {
        if (edge.hasError()) {
          flow.addEdge(edge);
        } else if (visitedOnPath.contains(edge.getSourceId())) {
          // We have a cycle. We set it as an error edge
          edge = new Edge(edge.getSourceId(), node.getId());
          edge.setError("Cyclical dependencies found.");
          this.errors.add("Cyclical dependency found at " + edge.getId());
          flow.addEdge(edge);
        } else if (visitedEver.contains(edge.getSourceId())) {
          // this node was already checked, don't need to check further
          flow.addEdge(edge);
        } else {
          // This should not be null
          flow.addEdge(edge);
          final Node sourceNode = this.nodeMap.get(edge.getSourceId());
          constructFlow(flow, sourceNode, visitedOnPath, visitedEver);
        }
      }
    }

    visitedOnPath.remove(node.getId());
  }

  private String getNameWithoutExtension(final File file) {
    final String filename = file.getName();
    final int index = filename.lastIndexOf('.');

    return index < 0 ? filename : filename.substring(0, index);
  }

  private String getRelativeFilePath(final String basePath, final String filePath) {
    return filePath.substring(basePath.length() + 1);
  }
}
