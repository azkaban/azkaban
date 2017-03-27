/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.execapp;

import com.google.common.base.Preconditions;
import java.io.File;
import org.apache.log4j.Logger;

import static com.google.common.base.Preconditions.*;


public class ProjectVersion implements Comparable<ProjectVersion> {
  private final int projectId;
  private final int version;

  private File installedDir;

  public ProjectVersion(int projectId, int version) {
    checkArgument(projectId > 0);
    checkArgument(version > 0);

    this.projectId = projectId;
    this.version = version;
  }

  public ProjectVersion(int projectId, int version, File installedDir) {
    this(projectId, version);
    this.installedDir = installedDir;
  }

  public int getProjectId() {
    return projectId;
  }

  public int getVersion() {
    return version;
  }

  public File getInstalledDir() {
    return installedDir;
  }

  public void setInstalledDir(File installedDir) {
    this.installedDir = installedDir;
  }

  @Override
  public int compareTo(ProjectVersion o) {
    if (projectId == o.projectId) {
      return version - o.version;
    }

    return projectId - o.projectId;
  }

  @Override
  public String toString() {
    return "ProjectVersion{" + "projectId=" + projectId + ", version=" + version + ", installedDir=" + installedDir
        + '}';
  }
}
