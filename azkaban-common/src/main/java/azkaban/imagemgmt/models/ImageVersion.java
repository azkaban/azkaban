/*
 * Copyright 2020 LinkedIn Corp.
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
package azkaban.imagemgmt.models;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.codehaus.jackson.annotate.JsonProperty;
import javax.validation.constraints.NotBlank;

/**
 * This class represents image version metadata.
 */
public class ImageVersion extends BaseModel {
  // Path of the image version
  private String path;
  // Represents image version. Version is in major.minor.patch format
  private String version;
  // Name of the image type
  private String name;
  // Description of the image version
  private String description;
  // State of the image version
  private State state;
  /**
   * Associated release of the image version. For example, in case of azkaban images the
   * corresponding azkaban release number. In case of job type release, the corresponding release
   * number
    */
  private String releaseTag;

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public State getState() {
    return state;
  }

  public void setState(State state) {
    this.state = state;
  }

  public String getReleaseTag() {
    return releaseTag;
  }

  public void setReleaseTag(String releaseTag) {
    this.releaseTag = releaseTag;
  }

  @Override
  public String toString() {
    return "ImageVersion{" +
        "path='" + path + '\'' +
        ", version='" + version + '\'' +
        ", name='" + name + '\'' +
        ", description='" + description + '\'' +
        ", state=" + state +
        ", releaseTag='" + releaseTag + '\'' +
        ", id=" + id +
        ", createdBy='" + createdBy + '\'' +
        ", createdOn='" + createdOn + '\'' +
        ", modifiedBy='" + modifiedBy + '\'' +
        ", modifiedOn='" + modifiedOn + '\'' +
        '}';
  }

  /**
   * Enum to represent state of the image version. Below are the significance of the enums
   * NEW - An image type version is marked with state as NEW when it is first created/registered
   * using image management API
   * ACTIVE - An image type version goes through the ramp up process and once the version is
   * fully ramped upto 100% the version is marked as ACTIVE in the image_versions table.
   * UNSTABLE - An image type version goes through the ramp up process and once the version is
   * identified as faulty or unstable, the version is marked as UNSTABLE in the image_versions
   * table.
   * DEPRECATED - An image type version which is no longer in use is marked as DEPRECATED
   */
  public enum State {
    NEW("new"),
    ACTIVE("active"),
    UNSTABLE("unstable"),
    DEPRECATED("deprecated");
    private final String stateValue;
    private State(String stateValue) {
      this.stateValue = stateValue;
    }

    // State value and state enum map
    private static final ImmutableMap<String, State> stateMap = Arrays.stream(State.values())
        .collect(ImmutableMap.toImmutableMap(state -> state.getStateValue(), state -> state));

    public String getStateValue() {
      return stateValue;
    }

    /**
     * Create state enum from state value
     * @param stateValue
     * @return
     */
    public static State fromStateValue(final String stateValue) {
      return stateMap.getOrDefault(stateValue, NEW);
    }
  }
}
