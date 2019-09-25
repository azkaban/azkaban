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
 *
 */

package azkaban.spi;

import static java.util.Objects.requireNonNull;

import java.util.Objects;


public class ProjectStorageMetadata {

  private final int projectId;
  private final int version;
  private final String uploader;
  private final byte[] hash;

  public ProjectStorageMetadata(final int projectId, final int version, final String uploader,
      final byte[] hash) {
    this.projectId = projectId;
    this.version = version;
    this.uploader = requireNonNull(uploader);
    this.hash = hash;
  }

  @Override
  public String toString() {
    return "StorageMetadata{" + "projectId='" + this.projectId + '\'' + ", version='" + this.version
        + '\''
        + '}';
  }

  public int getProjectId() {
    return this.projectId;
  }

  public int getVersion() {
    return this.version;
  }

  public String getUploader() {
    return this.uploader;
  }

  public byte[] getHash() {
    return this.hash;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ProjectStorageMetadata that = (ProjectStorageMetadata) o;
    return Objects.equals(this.projectId, that.projectId) &&
        Objects.equals(this.version, that.version) &&
        Objects.equals(this.uploader, that.uploader);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.projectId, this.version, this.uploader);
  }
}
