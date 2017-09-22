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
import azkaban.utils.Props;
import java.io.File;
import java.io.FileNotFoundException;
import org.apache.log4j.Logger;

/**
 * Loads yaml files to flows from project directory.
 */
public class DirectoryYamlFlowLoader extends FlowLoader {

  private static final Logger logger = Logger.getLogger(DirectoryYamlFlowLoader.class);
  private static final String PROJECT_YAML_SUFFIX = ".project";
  private static final String FLOW_YAML_SUFFIX = ".flow";

  private final Props props;

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
   * @param baseDirectory The directory to load flows from.
   */
  @Override
  public void loadProjectFlow(final Project project, final File baseDirectory) {
    convertYamlFiles(baseDirectory);
    project.setFlows(this.flowMap);
  }

  private void convertYamlFiles(final File projectDir) {
    // Todo jamiesjc: convert project yaml file. It will contain properties for all flows.

    //covert flow yaml files
    final File[] flowFiles = projectDir.listFiles(new SuffixFilter(FLOW_YAML_SUFFIX));
    for (final File file : flowFiles) {
      final FlowBeanLoader loader = new FlowBeanLoader();
      try {
        final FlowBean flowBean = loader.load(file);
        final AzkabanFlow azkabanFlow = loader.toAzkabanFlow(loader.getFlowName(file), flowBean);
        final Flow flow = new Flow(azkabanFlow.getName());
        flow.setAzkabanFlow(azkabanFlow);
        this.flowMap.put(azkabanFlow.getName(), flow);
        final Props flowProps = azkabanFlow.getProps();
        addEmailPropsToFlow(flow, flowProps);
      } catch (final FileNotFoundException e) {
        logger.error("Error loading flow yaml files", e);
      }
    }
  }

  @Override
  public void checkJobProperties(final Project project) {
    // Todo jamiesjc: implement the check later
  }

}
