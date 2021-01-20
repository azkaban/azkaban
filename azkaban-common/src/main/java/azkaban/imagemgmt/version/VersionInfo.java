/*
 * Copyright 2021 LinkedIn Corp.
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
package azkaban.imagemgmt.version;

import azkaban.imagemgmt.models.ImageVersion.State;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * This class represents image version specific info such as version, path and current state.
 * This POJO class is stored as a version information for the given image type in VersionSet.
 */
public class VersionInfo {

  private final String version;
  // This refers to the path of the image types. This field is applicable for azkaban-base image
  // and job type images. As by default azkaban-configs are baked in as part of azkaban image, so
  // the path is readily available and will be fetched based on specified version. Hence, for
  // azkaban-config path value can be null or empty.
  private final String path;
  private final State state;

  @JsonCreator
  public VersionInfo(@JsonProperty("version") final String version,
      @JsonProperty("path") final String path,
      @JsonProperty("state") final State state) {
    this.version = version;
    this.path = path;
    this.state = state;
  }

  public String getVersion() {
    return this.version;
  }

  public String getPath() {
    return this.path;
  }

  public State getState() {
    return state;
  }

  public String pathWithVersion() {
    Preconditions.checkNotNull(path, "path should not be null");
    Preconditions.checkNotNull(version, "version should not be null");
    return String.join(":", path, version);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VersionInfo that = (VersionInfo) o;
    return Objects.equals(version, that.version) &&
        Objects.equals(path, that.path) &&
        state == that.state;
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, path, state);
  }
}
