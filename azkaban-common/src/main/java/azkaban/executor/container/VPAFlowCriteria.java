package azkaban.executor.container;

import azkaban.Constants;
import azkaban.executor.ExecutableFlow;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;


/**
 * Class for denying VPA feature based on project or flow name filter.
 * A sample file can look like this:
 * proj1:flow1
 * proj1:flow2
 * proj2
 * proj3:flow1
 */
public class VPAFlowCriteria {
  // Projects are stored in a set: any flow under those projects will be
  private Set<String> projects = new HashSet<>();
  // Flows are stored in a set: will be filtered no matter which project it belongs to
  private Set<String> flows = new HashSet<>();
  // Flows are stored in a map where key is project name and value is set of flows: any (project, flow) pair will be filtered
  private Map<String, Set<String>> projectFlows = new HashMap<>();
  private final String fileLocation;
  private final Logger logger;

  /**
   *
   * @param azkProps : Azkaban properties
   * @param logger : Logger from the caller
   */
  public VPAFlowCriteria(final Props azkProps, final Logger logger) {
    this.fileLocation = azkProps.getString(Constants.
        ContainerizedDispatchManagerProperties.KUBERNETES_VPA_FLOW_FILTER_FILE, null);
    this.logger = logger;
    loadFlowFilter();
  }

  /**
   * Reloads the in-memory flows map with the flow filter file.
   */
  public void reloadFlowFilter() {
    // Reset the filter map
    loadFlowFilter();
  }

  @VisibleForTesting
  public void reloadFlowFilter(final String fileLocation) {
    // Reset the filter map
    loadFlowFilter(fileLocation);
  }

  private void loadFlowFilter() {
    loadFlowFilter(this.fileLocation);
  }

  /**
   * Read the flow filter file and create a map of flow names. The read happens
   * on best effort basis. i.e, if a flow name is not correctly formatted, it
   * is ignored.
   */
  synchronized private void loadFlowFilter(final String fileLocation) {
    // Basic checks
    if (fileLocation == null) {
      return;
    }
    final Path filePath = Paths.get(fileLocation);
    if (Files.notExists(filePath) || !Files.isReadable(filePath)) {
      logger.info("Flow filter file at location " + fileLocation + " does not exist");
      return;
    }

    final Set<String> projectSet = new HashSet<>();
    final Set<String> flowSet = new HashSet<>();
    final Map<String, Set<String>> projectFlowMap = new HashMap<>();
    // Read the file line by line and populate the map
    try (BufferedReader reader = Files.newBufferedReader(filePath, Charset.defaultCharset())) {
      for (String line; (line = reader.readLine()) != null;) {
        line = line.trim();
        List<String> flowFQN = Arrays.asList(line.split(":"));
        validateAndAdd(flowFQN, projectSet, flowSet, projectFlowMap);
      }
    } catch (final IOException e) {
      // Log and ignore
      logger.info("Caught exception while reading the file." + e);
    }
    this.projects = projectSet;
    this.flows = flowSet;
    this.projectFlows = projectFlowMap;
  }

  /** Validate flowFQN and add it to flows map.
   * @param flowFQN stored in the file is of format:
   * <project name> when entire project needs to be in the filter and
   * <project name>:<flow name>.
   */
  private void validateAndAdd(final List<String> flowFQN, final Set<String> projects, final Set<String> flows, final Map<String, Set<String>> projectFlowMap) {
    if (!(flowFQN.size() == 1 || flowFQN.size() == 2)) {
      return;
    }

    final String project = flowFQN.get(0);
    if (flowFQN.size() == 1) {
      projects.add(project);
      return;
    }

    final String flow = flowFQN.get(1);
    if (project.isEmpty()) {
      flows.add(flow);
      return;
    }

    // Handle the case when the FQN also contains flow name
    if (!projectFlowMap.containsKey(project)) {
      // Make an entry for the project
      projectFlowMap.put(project, new HashSet<>(1));
    }

    projectFlowMap.get(project).add(flowFQN.get(1));
  }

  /**
   *  If the flow exists in the flow filter, then return false,
   *  else return true.
   * @param flow executable flow object.
   * @return VPA Enable flag.
   */
  public boolean IsVPAEnabledForFlow(final ExecutableFlow flow) {
    return !flowExists(flow.getProjectName(), flow.getFlowId());
  }

  @VisibleForTesting
  public boolean flowExists(final String projectName, final String flowName) {
    return this.projects.contains(projectName) || this.flows.contains(flowName)
        || (this.projectFlows.containsKey(projectName) && this.projectFlows.get(projectName).contains(flowName));
  }
}
