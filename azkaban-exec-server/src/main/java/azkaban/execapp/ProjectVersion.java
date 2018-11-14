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
import java.nio.file.attribute.FileTime;


public class ProjectVersion implements Comparable<ProjectVersion> {

  private final int projectId;
  private final int version;

  private File installedDir;
  private Long dirSize;
  private Integer fileCount;
  private FileTime lastAccessTime;

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

  public Long getDirSizeInBytes() {
    return this.dirSize;
  }

  public void setDirSizeInBytes(final Long dirSize) {
    this.dirSize = dirSize;
  }

  public Integer getFileCount() {
    return this.fileCount;
  }

  public void setFileCount(final Integer fileCount) {
    this.fileCount = fileCount;
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
  public String toString() {
    return "ProjectVersion{" +
        "projectId=" + this.projectId +
        ", version=" + this.version +
        ", installedDir=" + this.installedDir +
        ", dirSize=" + this.dirSize +
        ", fileCount=" + this.fileCount +
        ", lastAccessTime=" + this.lastAccessTime +
        '}';
  }

  public FileTime getLastAccessTime() {
    return this.lastAccessTime;
  }

  public void setLastAccessTime(final FileTime lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
  }
}
