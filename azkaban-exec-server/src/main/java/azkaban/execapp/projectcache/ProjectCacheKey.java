/*
 * Copyright 2019 LinkedIn Corp.
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

package azkaban.execapp.projectcache;

import java.util.Objects;

/** Key for the project cache is project ID, version */
public class ProjectCacheKey {
  private final int projectId;
  private final int version;

  public ProjectCacheKey(int projectId, int version) {
    this.projectId = projectId;
    this.version = version;
  }

  public int getProjectId() {
    return projectId;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProjectCacheKey that = (ProjectCacheKey) o;
    return projectId == that.projectId &&
        version == that.version;
  }

  @Override
  public int hashCode() { return Objects.hash(projectId, version); }

  @Override
  public String toString() { return projectId + "." + version; }
}
