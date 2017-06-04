package azkaban.execapp;

import azkaban.flow.Flow;
import azkaban.project.DirectoryFlowLoader;
import azkaban.project.Project;
import azkaban.project.ProjectManagerException;
import azkaban.utils.Props;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

class FlowRunnerTestUtil {

  /**
   * Initialize the project with the flow definitions stored in the given source directory.
   * Also copy the source directory to the working directory.
   *
   * @param project project to initialize
   * @param sourceDir the source dir
   * @param logger the logger
   * @param workingDir the working dir
   * @return the flow name to flow map
   * @throws ProjectManagerException the project manager exception
   * @throws IOException the io exception
   */
  static Map<String, Flow> prepareProject(final Project project, final File sourceDir,
      final Logger logger, final File workingDir)
      throws ProjectManagerException, IOException {
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props(), logger);
    loader.loadProjectFlow(project, sourceDir);
    if (!loader.getErrors().isEmpty()) {
      for (final String error : loader.getErrors()) {
        System.out.println(error);
      }
      throw new RuntimeException(String.format(
          "Errors found in loading flows into a project ( %s ). From the directory: ( %s ).",
          project.getName(), sourceDir));
    }

    final Map<String, Flow> flowMap = loader.getFlowMap();
    project.setFlows(flowMap);
    FileUtils.copyDirectory(sourceDir, workingDir);

    return flowMap;
  }

}
