package azkaban.executor.container;

import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.DispatchMethod;
import azkaban.executor.ExecutableFlow;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManager;
import azkaban.utils.Props;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for determining {@link DispatchMethod} based on project2version deny list
 */
public class ContainerProjectVersionCriteria {
  private static final Logger logger = LoggerFactory.getLogger(ContainerProjectVersionCriteria.class);

  // If current version of the project is less than or equal to the version in denyList,
  // its flows will be dispatched to executor.
  private final Map<Integer, Integer> project2versionDenyList;
  private final ProjectManager projectManager;
  private final ProjectLoader projectLoader;

  public ContainerProjectVersionCriteria(final Props azkProps,
      final ProjectManager projectManager, final ProjectLoader projectLoader) {
    this.project2versionDenyList =
        azkProps.getStringStringMap(ContainerizedDispatchManagerProperties.CONTAINERIZED_PROJECT2VERSION_DENYLIST)
            .entrySet().stream().collect(
            Collectors.toMap(entry -> Integer.parseInt(entry.getKey()),
                entry -> Integer.parseInt(entry.getValue())));

    this.projectManager = projectManager;
    this.projectLoader = projectLoader;
  }

  /**
   * If current version of the project is less than or equal to the version in denyList,
   * then return DispatchMethod.POLL, else return DispatchMethod.CONTAINERIZED.
   *
   * @param flow
   * @return DispatchMethod
   */
  public DispatchMethod getDispatchMethod(final ExecutableFlow flow) {
    if (this.projectLoader.getLatestProjectVersion(projectManager.getProject(flow.getProjectId())) <=
        this.project2versionDenyList.getOrDefault(flow.getProjectId(), -1)) {
      return DispatchMethod.POLL;
    } else {
      return DispatchMethod.CONTAINERIZED;
    }
  }
}
