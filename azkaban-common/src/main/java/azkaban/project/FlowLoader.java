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
import azkaban.project.validator.ValidationReport;
import java.io.File;
import java.util.Map;
import java.util.Set;

/**
 * Interface to load project flows.
 */
public interface FlowLoader {

  /**
   * Loads all project flows from the directory.
   *
   * @param project The project.
   * @param projectDir The directory to load flows from.
   * @return the validation report.
   */
  ValidationReport loadProjectFlow(final Project project, final File projectDir);

  /**
   * Returns the flow map constructed from the loaded flows.
   *
   * @return Map of flow name to Flow.
   */
  Map<String, Flow> getFlowMap();

  /**
   * Returns errors caught when loading flows.
   *
   * @return Set of error strings.
   */
  Set<String> getErrors();
}
