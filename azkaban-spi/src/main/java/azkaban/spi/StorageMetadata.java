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

import java.util.Objects;

import static java.util.Objects.*;


public class StorageMetadata {
  private final int projectId;
  private final int version;
  private final String uploader;
  private byte[] hash;

  public StorageMetadata(int projectId, int version, String uploader, byte[] hash) {
    this.projectId = projectId;
    this.version = version;
    this.uploader = requireNonNull(uploader);
    this.hash = hash;
  }

  @Override
  public String toString() {
    return "StorageMetadata{" + "projectId='" + projectId + '\'' + ", version='" + version + '\'' + '}';
  }

  public int getProjectId() {
    return projectId;
  }

  public int getVersion() {
    return version;
  }

  public String getUploader() {
    return uploader;
  }

  public byte[] getHash() {
    return hash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StorageMetadata that = (StorageMetadata) o;
    return Objects.equals(projectId, that.projectId) &&
        Objects.equals(version, that.version) &&
        Objects.equals(uploader, that.uploader);
  }

  @Override
  public int hashCode() {
    return Objects.hash(projectId, version, uploader);
  }
}
