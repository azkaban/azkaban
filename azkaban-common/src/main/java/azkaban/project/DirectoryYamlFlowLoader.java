/*
* Copyright 2017 LinkedIn Corp.
*
* Licensed under the Apache License, Version 2.0 (the “License”); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/

package azkaban.project;

import azkaban.flow.Flow;
import azkaban.project.FlowLoaderUtils.SuffixFilter;
import azkaban.utils.Props;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads yaml files to flows from project directory.
 */
public class DirectoryYamlFlowLoader implements FlowLoader {

  private static final Logger logger = LoggerFactory.getLogger(DirectoryYamlFlowLoader.class);
  private static final String PROJECT_FILE_SUFFIX = ".project";
  private static final String FLOW_FILE_SUFFIX = ".flow";

  private final Props props;
  private final Set<String> errors = new HashSet<>();
  private final Map<String, Flow> flowMap = new HashMap<>();

  /**
   * Creates a new DirectoryYamlFlowLoader.
   *
   * @param props Properties to add.
   */
  public DirectoryYamlFlowLoader(final Props props) {
    this.props = props;
  }

  /**
   * Loads all flows from the directory into the project.
   *
   * @param project The project to load flows to.
   * @param projectDir The directory to load flows from.
   */
  @Override
  public void loadProjectFlow(final Project project, final File projectDir) {
    convertYamlFiles(projectDir);
    checkJobProperties(project);
    fillProjectInfo(project);
  }

  private void convertYamlFiles(final File projectDir) {
    // Todo jamiesjc: convert project yaml file. It will contain properties for all flows.

    //covert flow yaml files
    final File[] flowFiles = projectDir.listFiles(new SuffixFilter(FLOW_FILE_SUFFIX));
    for (final File file : flowFiles) {
      final FlowBeanLoader loader = new FlowBeanLoader();
      try {
        final FlowBean flowBean = loader.load(file);
        final AzkabanFlow azkabanFlow = loader.toAzkabanFlow(loader.getFlowName(file), flowBean);
        final Flow flow = new Flow(azkabanFlow.getName());
        flow.setAzkabanFlow(azkabanFlow);
        this.flowMap.put(azkabanFlow.getName(), flow);
        final Props flowProps = azkabanFlow.getProps();
        FlowLoaderUtils.addEmailPropsToFlow(flow, flowProps);
      } catch (final FileNotFoundException e) {
        logger.error("Error loading flow yaml files", e);
      }
    }
  }

  public void checkJobProperties(final Project project) {
    // Todo jamiesjc: implement the check later
  }

  private void fillProjectInfo(final Project project) {
    // Todo jamiesjc: fill out other project info later
    project.setFlows(this.flowMap);
    project.setErrors(this.errors);
  }

}
