package azkaban.project;

import azkaban.flow.Flow;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract Class implementing common methods for Project Cache Implementation
 */

public abstract class AbstractProjectCache implements ProjectCache {

  private final ProjectLoader loader;

  public AbstractProjectCache(final ProjectLoader loader) {
    this.loader = loader;
  }

  /**
   * loadAllFlowsForAllProjects  : To load all flows corresponding to projects from the database
   *
   * @param projects list of Projects to fetch flows for.
   */
  public void loadAllFlowsForAllProjects(final List<Project> projects) {
    try {
      final Map<Project, List<Flow>> projectToFlows = this.loader
          .fetchAllFlowsForProjects(projects);

      // Load the flows into the project objects
      for (final Map.Entry<Project, List<Flow>> entry : projectToFlows.entrySet()) {
        final Project project = entry.getKey();
        final List<Flow> flows = entry.getValue();

        final Map<String, Flow> flowMap = new HashMap<>();
        for (final Flow flow : flows) {
          flowMap.put(flow.getId(), flow);
        }

        project.setFlows(flowMap);
      }
    } catch (final ProjectManagerException e) {
      throw new RuntimeException("Could not load projects flows from store.", e);
    }
  }

  /**
   * get all active projects from database.
   *
   * @return List of projects;
   */
  @Override
  public Collection<Project> getAllProjects() {
    final List<Project> result;
    try {
      result = this.loader.fetchAllActiveProjects();
    } catch (final ProjectManagerException e) {
      throw new RuntimeException("Could not load projects from store.", e);
    }
    return result;
  }

}
