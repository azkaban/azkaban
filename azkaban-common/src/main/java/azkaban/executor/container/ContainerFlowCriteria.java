package azkaban.executor.container;

import azkaban.Constants;
import azkaban.DispatchMethod;
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
 * Class for determining {@link azkaban.DispatchMethod} based on flow name filter.
 */
public class ContainerFlowCriteria {
  // Flows are stored in a map where key is project name and value is set of flows.
  private Map<String, Set<String>> flows = new HashMap<>(1);
  private final String fileLocation;
  private final org.slf4j.Logger logger;

  /**
   *
   * @param azkProps : Azkaban properties
   * @param logger : Logger from the caller
   */
  public ContainerFlowCriteria(final Props azkProps, final Logger logger) {
    this.fileLocation = azkProps.getString(Constants.
        ContainerizedDispatchManagerProperties.CONTAINERIZED_FLOW_FILTER_FILE, null);
    this.logger = logger;
    loadFlowFilter();
  }

  /**
   * Reloads the in-memory flows map with the flow filter file.
   */
  public void reloadFlowFilter() {
    loadFlowFilter();
  }

  @VisibleForTesting
  public void reloadFlowFilter(final String fileLocation) {
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
  private void loadFlowFilter(final String fileLocation) {
    // Basic checks
    if (fileLocation == null) {
      return;
    }
    final Path filePath = Paths.get(fileLocation);
    if (Files.notExists(filePath) || !Files.isReadable(filePath)) {
      logger.info("Flow filter file at location " + fileLocation + " does not exist");
      return;
    }

    // Read the file line by line and populate the map
    try (BufferedReader reader = Files.newBufferedReader(filePath, Charset.defaultCharset())) {
      for (String line; (line = reader.readLine()) != null;) {
        line = line.trim();
        List<String> flowFQN = Arrays.asList(line.split(":"));
        validateAndAddFlow(flowFQN);
      }
    } catch (final IOException e) {
      // Log and ignore
    }
  }

  /** Validate flowFQN and add it to flows map.
   * @param flowFQN stored in the file is of format,
   * <project name>:<flow name>. Each flowFQN must be of size two.
   */
  private void validateAndAddFlow(final List<String> flowFQN) {
    if (flowFQN.size() != 2) {
      return;
    }
    final String project = flowFQN.get(0);
    if (!this.flows.containsKey(project)) {
      // Make an entry for the project
      this.flows.put(project, new HashSet<>(1));
    }
    this.flows.get(project).add(flowFQN.get(1));
  }

  /**
   *  If the flow exists in the flow filter, then return POLL,
   *  else return CONTAINERIZED.
   * @param flow executable flow object.
   * @return DispatchMethod.
   */
  public DispatchMethod getDispatchMethod(final ExecutableFlow flow) {
    return flowExists(flow.getProjectName(), flow.getFlowId()) ?
        DispatchMethod.POLL : DispatchMethod.CONTAINERIZED;
  }

  @VisibleForTesting
  public boolean flowExists(final String projectName, final String flowName) {
    final Set<String> flowNames = this.flows.get(projectName);
    return flowNames != null && flowNames.contains(flowName);
  }
}
