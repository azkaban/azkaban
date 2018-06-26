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

import java.io.File;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;


public class ProjectVersion implements Comparable<ProjectVersion> {

  private final int projectId;
  private final int version;

  private File installedDir;

  public ProjectVersion(final int projectId, final int version) {
    checkArgument(projectId > 0);
    checkArgument(version > 0);

    this.projectId = projectId;
    this.version = version;
  }

  public ProjectVersion(final int projectId, final int version, final File installedDir) {
    this(projectId, version);
    this.installedDir = installedDir;
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
  }

  @Override
  public int compareTo(final ProjectVersion o) {
    if (this.projectId == o.projectId) {
      return this.version - o.version;
    }

    return this.projectId - o.projectId;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ProjectVersion that = (ProjectVersion) o;

    return new EqualsBuilder()
        .append(this.projectId, that.projectId)
        .append(this.version, that.version)
        .append(this.installedDir, that.installedDir)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(this.projectId)
        .append(this.version)
        .append(this.installedDir)
        .toHashCode();
  }

  @Override
  public String toString() {
    return "ProjectVersion{" + "projectId=" + this.projectId + ", version=" + this.version
        + ", installedDir="
        + this.installedDir
        + '}';
  }

  /**
   * Returns the ID for the projectVersion. Two different instances sharing the same {@code
   * projectId} and {@code version} return the same ID.
   */
  public String getID() {
    return "ProjectVersion{" + String.valueOf(this.projectId) + "." + String.valueOf(this.version)
        + "}";
  }
}
