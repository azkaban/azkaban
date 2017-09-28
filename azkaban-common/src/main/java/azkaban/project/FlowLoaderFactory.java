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
import azkaban.utils.Props;
import java.io.File;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Factory class to generate flow loaders.
 */
@Singleton
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
   * Creates flow loader based on manifest file inside project directory.
   *
   * @param projectDir the project directory
   * @return the flow loader
   */
  public FlowLoader createFlowLoader(final File projectDir) throws ProjectManagerException {
    // Todo jamiesjc: need to check if manifest file exists in project directory,
    // and create FlowLoader based on different flow versions specified in the manifest file.
    final String flowVersion = null;
    if (flowVersion == null) {
      return new DirectoryFlowLoader(this.props);
    } else if (flowVersion.equals(Constants.AZKABAN_FLOW_VERSION_2_0)) {
      return new DirectoryYamlFlowLoader(this.props);
    } else {
      throw new ProjectManagerException("Flow version " + flowVersion + "is invalid.");
    }
  }
}
