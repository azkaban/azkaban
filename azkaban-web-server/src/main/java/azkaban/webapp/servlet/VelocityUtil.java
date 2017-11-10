/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.webapp.servlet;

import azkaban.project.Project;
import azkaban.project.ProjectManager;

public class VelocityUtil {

  ProjectManager projectManager;

  public VelocityUtil(final ProjectManager projectManager) {
    this.projectManager = projectManager;
  }

  public String getProjectName(final int id) {
    final Project project = this.projectManager.getProject(id);
    if (project == null) {
      return String.valueOf(id);
    }
    return project.getName();
  }
}
