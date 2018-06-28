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

import static com.google.common.base.Preconditions.checkArgument;

import azkaban.Constants;
import azkaban.utils.FileIOUtils;
import java.io.File;
import java.nio.file.Paths;


public class ProjectVersion implements Comparable<ProjectVersion> {

  private final int projectId;
  private final int version;

  private File installedDir;
  private Long dirSize;

  public ProjectVersion(final int projectId, final int version) {
    checkArgument(projectId > 0);
    checkArgument(version > 0);

    this.projectId = projectId;
    this.version = version;
  }

  public ProjectVersion(final int projectId, final int version, final File installedDir) {
    this(projectId, version);
    this.installedDir = installedDir;
    this.dirSize = readFileSize();
  }

  private Long readFileSize() {
    Long fileSize = null;
    try {
      fileSize = FileIOUtils.readNumberFromFile(
          Paths.get(this.installedDir.getPath(), Constants.PROJECT_DIR_SIZE_FILE_NAME).toString());
    } catch (final Exception ex) {
    }
    return fileSize;
  }

  public Long getDirSize() {
    return this.dirSize;
  }

  public int getProjectId() {
    return this.projectId;
  }

  public int getVersion() {
    return this.version;
  }

  public File getInstalledDir() {
    return this.installedDir;
  }

  public void setInstalledDir(final File installedDir) {
    this.installedDir = installedDir;
    this.dirSize = readFileSize();
  }

  @Override
  public int compareTo(final ProjectVersion o) {
    if (this.projectId == o.projectId) {
      return this.version - o.version;
    }

    return this.projectId - o.projectId;
  }

  @Override
  public String toString() {
    return "ProjectVersion{" + "projectId=" + this.projectId + ", version=" + this.version
        + ", installedDir="
        + this.installedDir
        + '}';
  }
}
