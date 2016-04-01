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

public class DirectoryFlowLoader implements ProjectValidator {
  private static final DirFilter DIR_FILTER = new DirFilter();
  private static final String PROPERTY_SUFFIX = ".properties";
  private static final String JOB_SUFFIX = ".job";
  public static final String JOB_MAX_XMS = "job.max.Xms";
  public static final String MAX_XMS_DEFAULT = "1G";
  public static final String JOB_MAX_XMX = "job.max.Xmx";
  public static final String MAX_XMX_DEFAULT = "2G";
  private static final String XMS = "Xms";
  private static final String XMX = "Xmx";

  private final Logger logger;
  private Props props;
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
  public DirectoryFlowLoader(Props props, Logger logger) {
    this.logger = logger;
    this.props = props;
  }

  /**
   * Returns the flow map constructed from the loaded flows.
   *
   * @return Map of flow name to Flow.
   */
  public Map<String, Flow> getFlowMap() {
    return flowMap;
  }

  /**
   * Returns errors caught when loading flows.
   *
   * @return Set of error strings.
   */
  public Set<String> getErrors() {
    return errors;
  }

  /**
   * Returns job properties.
   *
   * @return Map of job name to properties.
   */
  public Map<String, Props> getJobProps() {
    return jobPropsMap;
  }

  /**
   * Returns list of properties.
   *
   * @return List of Props.
   */
  public List<Props> getProps() {
    return propsList;
  }

  /**
   * Loads all flows from the directory into the project.
   *
   * @param project The project to load flows to.
   * @param baseDirectory The directory to load flows from.
   */
  public void loadProjectFlow(Project project, File baseDirectory) {
    propsList = new ArrayList<Props>();
    flowPropsList = new ArrayList<FlowProps>();
    jobPropsMap = new HashMap<String, Props>();
    nodeMap = new HashMap<String, Node>();
    flowMap = new HashMap<String, Flow>();
    errors = new HashSet<String>();
    duplicateJobs = new HashSet<String>();
    nodeDependencies = new HashMap<String, Map<String, Edge>>();
    rootNodes = new HashSet<String>();
    flowDependencies = new HashMap<String, Set<String>>();

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

  private void loadProjectFromDir(String base, File dir, Props parent) {
    File[] propertyFiles = dir.listFiles(new SuffixFilter(PROPERTY_SUFFIX));
    Arrays.sort(propertyFiles);

    for (File file : propertyFiles) {
      String relative = getRelativeFilePath(base, file.getPath());
      try {
        parent = new Props(parent, file);
        parent.setSource(relative);

        FlowProps flowProps = new FlowProps(parent);
        flowPropsList.add(flowProps);
      } catch (IOException e) {
        errors.add("Error loading properties " + file.getName() + ":"
            + e.getMessage());
      }

      logger.info("Adding " + relative);
      propsList.add(parent);
    }

    // Load all Job files. If there's a duplicate name, then we don't load
    File[] jobFiles = dir.listFiles(new SuffixFilter(JOB_SUFFIX));
    for (File file : jobFiles) {
      String jobName = getNameWithoutExtension(file);
      try {
        if (!duplicateJobs.contains(jobName)) {
          if (jobPropsMap.containsKey(jobName)) {
            errors.add("Duplicate job names found '" + jobName + "'.");
            duplicateJobs.add(jobName);
            jobPropsMap.remove(jobName);
            nodeMap.remove(jobName);
          } else {
            Props prop = new Props(parent, file);
            String relative = getRelativeFilePath(base, file.getPath());
            prop.setSource(relative);

            Node node = new Node(jobName);
            String type = prop.getString("type", null);
            if (type == null) {
              errors.add("Job doesn't have type set '" + jobName + "'.");
            }

            node.setType(type);

            node.setJobSource(relative);
            if (parent != null) {
              node.setPropsSource(parent.getSource());
            }

            // Force root node
            if (prop.getBoolean(CommonJobProperties.ROOT_NODE, false)) {
              rootNodes.add(jobName);
            }

            jobPropsMap.put(jobName, prop);
            nodeMap.put(jobName, node);
          }
        }
      } catch (IOException e) {
        errors.add("Error loading job file " + file.getName() + ":"
            + e.getMessage());
      }
    }

    File[] subDirs = dir.listFiles(DIR_FILTER);
    for (File file : subDirs) {
      loadProjectFromDir(base, file, parent);
    }
  }

  private void resolveEmbeddedFlows() {
    for (String flowId : flowDependencies.keySet()) {
      HashSet<String> visited = new HashSet<String>();
      resolveEmbeddedFlow(flowId, visited);
    }
  }

  private void resolveEmbeddedFlow(String flowId, Set<String> visited) {
    Set<String> embeddedFlow = flowDependencies.get(flowId);
    if (embeddedFlow == null) {
      return;
    }

    visited.add(flowId);
    for (String embeddedFlowId : embeddedFlow) {
      if (visited.contains(embeddedFlowId)) {
        errors.add("Embedded flow cycle found in " + flowId + "->"
            + embeddedFlowId);
        return;
      } else if (!flowMap.containsKey(embeddedFlowId)) {
        errors.add("Flow " + flowId + " depends on " + embeddedFlowId
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
    for (Node node : nodeMap.values()) {
      Props props = jobPropsMap.get(node.getId());

      if (props == null) {
        logger.error("Job props not found!! For some reason.");
        continue;
      }

      List<String> dependencyList =
          props.getStringList(CommonJobProperties.DEPENDENCIES,
              (List<String>) null);

      if (dependencyList != null) {
        Map<String, Edge> dependencies = nodeDependencies.get(node.getId());
        if (dependencies == null) {
          dependencies = new HashMap<String, Edge>();

          for (String dependencyName : dependencyList) {
            dependencyName =
                dependencyName == null ? null : dependencyName.trim();
            if (dependencyName == null || dependencyName.isEmpty()) {
              continue;
            }

            Edge edge = new Edge(dependencyName, node.getId());
            Node dependencyNode = nodeMap.get(dependencyName);
            if (dependencyNode == null) {
              if (duplicateJobs.contains(dependencyName)) {
                edge.setError("Ambiguous Dependency. Duplicates found.");
                dependencies.put(dependencyName, edge);
                errors.add(node.getId() + " has ambiguous dependency "
                    + dependencyName);
              } else {
                edge.setError("Dependency not found.");
                dependencies.put(dependencyName, edge);
                errors.add(node.getId() + " cannot find dependency "
                    + dependencyName);
              }
            } else if (dependencyNode == node) {
              // We have a self cycle
              edge.setError("Self cycle found.");
              dependencies.put(dependencyName, edge);
              errors.add(node.getId() + " has a self cycle");
            } else {
              dependencies.put(dependencyName, edge);
            }
          }

          if (!dependencies.isEmpty()) {
            nodeDependencies.put(node.getId(), dependencies);
          }
        }
      }
    }
  }

  private void buildFlowsFromDependencies() {
    // Find all root nodes by finding ones without dependents.
    HashSet<String> nonRootNodes = new HashSet<String>();
    for (Map<String, Edge> edges : nodeDependencies.values()) {
      for (String sourceId : edges.keySet()) {
        nonRootNodes.add(sourceId);
      }
    }

    // Now create flows. Bad flows are marked invalid
    Set<String> visitedNodes = new HashSet<String>();
    for (Node base : nodeMap.values()) {
      // Root nodes can be discovered when parsing jobs
      if (rootNodes.contains(base.getId())
          || !nonRootNodes.contains(base.getId())) {
        rootNodes.add(base.getId());
        Flow flow = new Flow(base.getId());
        Props jobProp = jobPropsMap.get(base.getId());

        // Dedup with sets
        @SuppressWarnings("unchecked")
        List<String> successEmailList =
            jobProp.getStringList(CommonJobProperties.SUCCESS_EMAILS,
                Collections.EMPTY_LIST);
        Set<String> successEmail = new HashSet<String>();
        for (String email : successEmailList) {
          successEmail.add(email.toLowerCase());
        }

        @SuppressWarnings("unchecked")
        List<String> failureEmailList =
            jobProp.getStringList(CommonJobProperties.FAILURE_EMAILS,
                Collections.EMPTY_LIST);
        Set<String> failureEmail = new HashSet<String>();
        for (String email : failureEmailList) {
          failureEmail.add(email.toLowerCase());
        }

        @SuppressWarnings("unchecked")
        List<String> notifyEmailList =
            jobProp.getStringList(CommonJobProperties.NOTIFY_EMAILS,
                Collections.EMPTY_LIST);
        for (String email : notifyEmailList) {
          email = email.toLowerCase();
          successEmail.add(email);
          failureEmail.add(email);
        }

        flow.addFailureEmails(failureEmail);
        flow.addSuccessEmails(successEmail);

        flow.addAllFlowProperties(flowPropsList);
        constructFlow(flow, base, visitedNodes);
        flow.initialize();
        flowMap.put(base.getId(), flow);
      }
    }
  }

  private void constructFlow(Flow flow, Node node, Set<String> visited) {
    visited.add(node.getId());

    flow.addNode(node);
    if (SpecialJobTypes.EMBEDDED_FLOW_TYPE.equals(node.getType())) {
      Props props = jobPropsMap.get(node.getId());
      String embeddedFlow = props.get(SpecialJobTypes.FLOW_NAME);

      Set<String> embeddedFlows = flowDependencies.get(flow.getId());
      if (embeddedFlows == null) {
        embeddedFlows = new HashSet<String>();
        flowDependencies.put(flow.getId(), embeddedFlows);
      }

      node.setEmbeddedFlowId(embeddedFlow);
      embeddedFlows.add(embeddedFlow);
    }
    Map<String, Edge> dependencies = nodeDependencies.get(node.getId());

    if (dependencies != null) {
      for (Edge edge : dependencies.values()) {
        if (edge.hasError()) {
          flow.addEdge(edge);
        } else if (visited.contains(edge.getSourceId())) {
          // We have a cycle. We set it as an error edge
          edge = new Edge(edge.getSourceId(), node.getId());
          edge.setError("Cyclical dependencies found.");
          errors.add("Cyclical dependency found at " + edge.getId());
          flow.addEdge(edge);
        } else {
          // This should not be null
          flow.addEdge(edge);
          Node sourceNode = nodeMap.get(edge.getSourceId());
          constructFlow(flow, sourceNode, visited);
        }
      }
    }

    visited.remove(node.getId());
  }

  private void jobPropertiesCheck(Project project) {
    // if project is in the memory check whitelist, then we don't need to check
    // its memory settings
    if (ProjectWhitelist.isProjectWhitelisted(project.getId(),
        ProjectWhitelist.WhitelistType.MemoryCheck)) {
      return;
    }

    String maxXms = props.getString(JOB_MAX_XMS, MAX_XMS_DEFAULT);
    String maxXmx = props.getString(JOB_MAX_XMX, MAX_XMX_DEFAULT);
    long sizeMaxXms = Utils.parseMemString(maxXms);
    long sizeMaxXmx = Utils.parseMemString(maxXmx);

    for (String jobName : jobPropsMap.keySet()) {

      Props jobProps = jobPropsMap.get(jobName);
      String xms = jobProps.getString(XMS, null);
      if (xms != null && !PropsUtils.isVarialbeReplacementPattern(xms)
          && Utils.parseMemString(xms) > sizeMaxXms) {
        errors.add(String.format(
            "%s: Xms value has exceeded the allowed limit (max Xms = %s)",
            jobName, maxXms));
      }
      String xmx = jobProps.getString(XMX, null);
      if (xmx != null && !PropsUtils.isVarialbeReplacementPattern(xmx)
          && Utils.parseMemString(xmx) > sizeMaxXmx) {
        errors.add(String.format(
            "%s: Xmx value has exceeded the allowed limit (max Xmx = %s)",
            jobName, maxXmx));
      }

      // job callback properties check
      JobCallbackValidator.validate(jobName, props, jobProps, errors);
    }
  }

  private String getNameWithoutExtension(File file) {
    String filename = file.getName();
    int index = filename.lastIndexOf('.');

    return index < 0 ? filename : filename.substring(0, index);
  }

  private String getRelativeFilePath(String basePath, String filePath) {
    return filePath.substring(basePath.length() + 1);
  }

  private static class DirFilter implements FileFilter {
    @Override
    public boolean accept(File pathname) {
      return pathname.isDirectory();
    }
  }

  private static class SuffixFilter implements FileFilter {
    private String suffix;

    public SuffixFilter(String suffix) {
      this.suffix = suffix;
    }

    @Override
    public boolean accept(File pathname) {
      String name = pathname.getName();

      return pathname.isFile() && !pathname.isHidden()
          && name.length() > suffix.length() && name.endsWith(suffix);
    }
  }

  @Override
  public boolean initialize(Props configuration) {
    return true;
  }

  @Override
  public String getValidatorName() {
    return XmlValidatorManager.DEFAULT_VALIDATOR_KEY;
  }

  @Override
  public ValidationReport validateProject(Project project, File projectDir) {
    loadProjectFlow(project, projectDir);
    ValidationReport report = new ValidationReport();
    report.addErrorMsgs(errors);
    return report;
  }
}
