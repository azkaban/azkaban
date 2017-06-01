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
import azkaban.jobcallback.JobCallbackValidator;
import azkaban.project.validator.ProjectValidator;
import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.XmlValidatorManager;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

public class DirectoryFlowLoader implements ProjectValidator {

  public static final String JOB_MAX_XMS = "job.max.Xms";
  public static final String MAX_XMS_DEFAULT = "1G";
  public static final String JOB_MAX_XMX = "job.max.Xmx";
  public static final String MAX_XMX_DEFAULT = "2G";
  private static final DirFilter DIR_FILTER = new DirFilter();
  private static final String PROPERTY_SUFFIX = ".properties";
  private static final String JOB_SUFFIX = ".job";
  private static final String XMS = "Xms";
  private static final String XMX = "Xmx";

  private final Logger logger;
  private final Props props;
  private HashSet<String> rootNodes;
  private HashMap<String, Flow> flowMap;
  private HashMap<String, Node> nodeMap;
  private HashMap<String, Map<String, Edge>> nodeDependencies;
  private HashMap<String, Props> jobPropsMap;

  // Flow dependencies for embedded flows.
  private HashMap<String, Set<String>> flowDependencies;

  private ArrayList<FlowProps> flowPropsList;
  private ArrayList<Props> propsList;
  private Set<String> errors;
  private Set<String> duplicateJobs;

  /**
   * Creates a new DirectoryFlowLoader.
   *
   * @param props Properties to add.
   * @param logger The Logger to use.
   */
  public DirectoryFlowLoader(final Props props, final Logger logger) {
    this.logger = logger;
    this.props = props;
  }

  /**
   * Returns the flow map constructed from the loaded flows.
   *
   * @return Map of flow name to Flow.
   */
  public Map<String, Flow> getFlowMap() {
    return this.flowMap;
  }

  /**
   * Returns errors caught when loading flows.
   *
   * @return Set of error strings.
   */
  public Set<String> getErrors() {
    return this.errors;
  }

  /**
   * Returns job properties.
   *
   * @return Map of job name to properties.
   */
  public Map<String, Props> getJobProps() {
    return this.jobPropsMap;
  }

  /**
   * Returns list of properties.
   *
   * @return List of Props.
   */
  public List<Props> getProps() {
    return this.propsList;
  }

  /**
   * Loads all flows from the directory into the project.
   *
   * @param project The project to load flows to.
   * @param baseDirectory The directory to load flows from.
   */
  public void loadProjectFlow(final Project project, final File baseDirectory) {
    this.propsList = new ArrayList<>();
    this.flowPropsList = new ArrayList<>();
    this.jobPropsMap = new HashMap<>();
    this.nodeMap = new HashMap<>();
    this.flowMap = new HashMap<>();
    this.errors = new HashSet<>();
    this.duplicateJobs = new HashSet<>();
    this.nodeDependencies = new HashMap<>();
    this.rootNodes = new HashSet<>();
    this.flowDependencies = new HashMap<>();

    // Load all the props files and create the Node objects
    loadProjectFromDir(baseDirectory.getPath(), baseDirectory, null);

    jobPropertiesCheck(project);

    // Create edges and find missing dependencies
    resolveDependencies();

    // Create the flows.
    buildFlowsFromDependencies();

    // Resolve embedded flows
    resolveEmbeddedFlows();

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

    final File[] subDirs = dir.listFiles(DIR_FILTER);
    for (final File file : subDirs) {
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
    final Set<String> visitedNodes = new HashSet<>();
    for (final Node base : this.nodeMap.values()) {
      // Root nodes can be discovered when parsing jobs
      if (this.rootNodes.contains(base.getId())
          || !nonRootNodes.contains(base.getId())) {
        this.rootNodes.add(base.getId());
        final Flow flow = new Flow(base.getId());
        final Props jobProp = this.jobPropsMap.get(base.getId());

        // Dedup with sets
        final List<String> successEmailList =
            jobProp.getStringList(CommonJobProperties.SUCCESS_EMAILS,
                Collections.EMPTY_LIST);
        final Set<String> successEmail = new HashSet<>();
        for (final String email : successEmailList) {
          successEmail.add(email.toLowerCase());
        }

        final List<String> failureEmailList =
            jobProp.getStringList(CommonJobProperties.FAILURE_EMAILS,
                Collections.EMPTY_LIST);
        final Set<String> failureEmail = new HashSet<>();
        for (final String email : failureEmailList) {
          failureEmail.add(email.toLowerCase());
        }

        final List<String> notifyEmailList =
            jobProp.getStringList(CommonJobProperties.NOTIFY_EMAILS,
                Collections.EMPTY_LIST);
        for (String email : notifyEmailList) {
          email = email.toLowerCase();
          successEmail.add(email);
          failureEmail.add(email);
        }

        flow.addFailureEmails(failureEmail);
        flow.addSuccessEmails(successEmail);

        flow.addAllFlowProperties(this.flowPropsList);
        constructFlow(flow, base, visitedNodes);
        flow.initialize();
        this.flowMap.put(base.getId(), flow);
      }
    }
  }

  private void constructFlow(final Flow flow, final Node node, final Set<String> visited) {
    visited.add(node.getId());

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
        } else if (visited.contains(edge.getSourceId())) {
          // We have a cycle. We set it as an error edge
          edge = new Edge(edge.getSourceId(), node.getId());
          edge.setError("Cyclical dependencies found.");
          this.errors.add("Cyclical dependency found at " + edge.getId());
          flow.addEdge(edge);
        } else {
          // This should not be null
          flow.addEdge(edge);
          final Node sourceNode = this.nodeMap.get(edge.getSourceId());
          constructFlow(flow, sourceNode, visited);
        }
      }
    }

    visited.remove(node.getId());
  }

  private void jobPropertiesCheck(final Project project) {
    // if project is in the memory check whitelist, then we don't need to check
    // its memory settings
    if (ProjectWhitelist.isProjectWhitelisted(project.getId(),
        ProjectWhitelist.WhitelistType.MemoryCheck)) {
      return;
    }

    final String maxXms = this.props.getString(JOB_MAX_XMS, MAX_XMS_DEFAULT);
    final String maxXmx = this.props.getString(JOB_MAX_XMX, MAX_XMX_DEFAULT);
    final long sizeMaxXms = Utils.parseMemString(maxXms);
    final long sizeMaxXmx = Utils.parseMemString(maxXmx);

    for (final String jobName : this.jobPropsMap.keySet()) {

      final Props jobProps = this.jobPropsMap.get(jobName);
      final String xms = jobProps.getString(XMS, null);
      if (xms != null && !PropsUtils.isVarialbeReplacementPattern(xms)
          && Utils.parseMemString(xms) > sizeMaxXms) {
        this.errors.add(String.format(
            "%s: Xms value has exceeded the allowed limit (max Xms = %s)",
            jobName, maxXms));
      }
      final String xmx = jobProps.getString(XMX, null);
      if (xmx != null && !PropsUtils.isVarialbeReplacementPattern(xmx)
          && Utils.parseMemString(xmx) > sizeMaxXmx) {
        this.errors.add(String.format(
            "%s: Xmx value has exceeded the allowed limit (max Xmx = %s)",
            jobName, maxXmx));
      }

      // job callback properties check
      JobCallbackValidator.validate(jobName, this.props, jobProps, this.errors);
    }
  }

  private String getNameWithoutExtension(final File file) {
    final String filename = file.getName();
    final int index = filename.lastIndexOf('.');

    return index < 0 ? filename : filename.substring(0, index);
  }

  private String getRelativeFilePath(final String basePath, final String filePath) {
    return filePath.substring(basePath.length() + 1);
  }

  @Override
  public boolean initialize(final Props configuration) {
    return true;
  }

  @Override
  public String getValidatorName() {
    return XmlValidatorManager.DEFAULT_VALIDATOR_KEY;
  }

  @Override
  public ValidationReport validateProject(final Project project, final File projectDir) {
    loadProjectFlow(project, projectDir);
    final ValidationReport report = new ValidationReport();
    report.addErrorMsgs(this.errors);
    return report;
  }

  private static class DirFilter implements FileFilter {

    @Override
    public boolean accept(final File pathname) {
      return pathname.isDirectory();
    }
  }

  private static class SuffixFilter implements FileFilter {

    private final String suffix;

    public SuffixFilter(final String suffix) {
      this.suffix = suffix;
    }

    @Override
    public boolean accept(final File pathname) {
      final String name = pathname.getName();

      return pathname.isFile() && !pathname.isHidden()
          && name.length() > this.suffix.length() && name.endsWith(this.suffix);
    }
  }
}
