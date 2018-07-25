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

import static java.util.Objects.requireNonNull;

import azkaban.Constants;
import azkaban.project.FlowLoaderUtils.DirFilter;
import azkaban.project.FlowLoaderUtils.SuffixFilter;
import azkaban.utils.Props;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import javax.inject.Inject;
import org.apache.commons.lang.ArrayUtils;
import org.yaml.snakeyaml.Yaml;

/**
 * Factory class to generate flow loaders.
 */
public class FlowLoaderFactory {

  private final Props props;

  /**
   * Instantiates a new Flow loader factory.
   *
   * @param props the props
   */
  @Inject
  public FlowLoaderFactory(final Props props) {
    this.props = requireNonNull(props, "Props is null");
  }

  /**
   * Creates flow loader based on project YAML file inside project directory.
   *
   * @param projectDir the project directory
   * @return the flow loader
   */
  public FlowLoader createFlowLoader(final File projectDir) throws ProjectManagerException {
    if (checkForValidProjectYamlFile(projectDir)) {
      return new DirectoryYamlFlowLoader(this.props);
    } else {
      return new DirectoryFlowLoader(this.props);
    }
  }

  private boolean checkForValidProjectYamlFile(final File projectDir) throws
      ProjectManagerException {
    final File[] projectFileList = projectDir.listFiles(new SuffixFilter(Constants
        .PROJECT_FILE_SUFFIX));

    if (projectFileList == null) {
      throw new ProjectManagerException("Error reading project directory. Input is not a "
          + "directory or IO error happens.");
    }

    if (ArrayUtils.isNotEmpty(projectFileList)) {
      if (projectFileList.length > 1) {
        throw new ProjectManagerException("Duplicate project YAML files found in the project "
            + "directory. Only one is allowed.");
      }

      final Map<String, Object> azkabanProject;
      try (FileInputStream fis = new FileInputStream(projectFileList[0])) {
        azkabanProject = (Map<String, Object>) new Yaml().load(fis);
      } catch (final IOException e) {
        throw new ProjectManagerException("Error reading project YAML file.", e);
      }

      if (azkabanProject == null || !azkabanProject
          .containsKey(Constants.ConfigurationKeys.AZKABAN_FLOW_VERSION)) {
        throw new ProjectManagerException("azkaban-flow-version is not specified in the project "
            + "YAML file.");
      }

      if (azkabanProject.get(Constants.ConfigurationKeys.AZKABAN_FLOW_VERSION).equals
          (Constants.AZKABAN_FLOW_VERSION_2_0)) {
        return true;
      } else {
        throw new ProjectManagerException("Invalid azkaban-flow-version in the project YAML file.");
      }
    } else {
      for (final File file : projectDir.listFiles(new DirFilter())) {
        if (checkForValidProjectYamlFile(file)) {
          return true;
        }
      }
      return false;
    }
  }
}
