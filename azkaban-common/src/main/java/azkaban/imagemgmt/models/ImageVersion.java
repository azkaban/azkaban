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
import java.util.Set;
import java.util.stream.Collectors;

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
    return this.path;
  }

  public void setPath(final String path) {
    this.path = path;
  }

  public String getDescription() {
    return this.description;
  }

  public void setDescription(final String description) {
    this.description = description;
  }

  public String getVersion() {
    return this.version;
  }

  public void setVersion(final String version) {
    this.version = version;
  }

  public String getName() {
    return this.name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public State getState() {
    return this.state;
  }

  public void setState(final State state) {
    this.state = state;
  }

  public String getReleaseTag() {
    return this.releaseTag;
  }

  public void setReleaseTag(final String releaseTag) {
    this.releaseTag = releaseTag;
  }

  @Override
  public String toString() {
    return "ImageVersion{" +
        "path='" + this.path + '\'' +
        ", version='" + this.version + '\'' +
        ", name='" + this.name + '\'' +
        ", description='" + this.description + '\'' +
        ", state=" + this.state +
        ", releaseTag='" + this.releaseTag + '\'' +
        ", id=" + this.id +
        ", createdBy='" + this.createdBy + '\'' +
        ", createdOn='" + this.createdOn + '\'' +
        ", modifiedBy='" + this.modifiedBy + '\'' +
        ", modifiedOn='" + this.modifiedOn + '\'' +
        '}';
  }

  /**
   * Enum to represent state of the image version. Below are the significance of the enums NEW - An
   * image type version is marked with state as NEW when it is first created/registered using image
   * management API ACTIVE - An image type version goes through the ramp up process and once the
   * version is fully ramped upto 100% the version is marked as ACTIVE in the image_versions table.
   * UNSTABLE - An image type version goes through the ramp up process and once the version is
   * identified as faulty or unstable, the version is marked as UNSTABLE in the image_versions
   * table. DEPRECATED - An image type version which is no longer in use is marked as DEPRECATED
   * TEST - This is to represent a TEST version of the image and once the version is tested it can
   * be marked as NEW.
   */
  public enum State {
    NEW("new"),
    ACTIVE("active"),
    UNSTABLE("unstable"),
    DEPRECATED("deprecated"),
    TEST("test");
    private final String stateValue;

    private State(final String stateValue) {
      this.stateValue = stateValue;
    }

    // State value and state enum map
    private static final ImmutableMap<String, State> stateMap = Arrays.stream(State.values())
        .collect(ImmutableMap.toImmutableMap(state -> state.getStateValue(), state -> state));

    public String getStateValue() {
      return this.stateValue;
    }

    /**
     * Create state enum from state value
     *
     * @param stateValue
     * @return
     */
    public static State fromStateValue(final String stateValue) {
      return stateMap.getOrDefault(stateValue, NEW);
    }

    /**
     * Create set of non active state values
     */
    private static final Set<String> nonActiveStateValueSet =
        Arrays.stream(State.values()).filter(state -> !state.equals(State.ACTIVE))
            .map(nonActiveState -> nonActiveState.getStateValue()).collect(
            Collectors.toSet());

    public static Set<String> getNonActiveStateValues() {
      return nonActiveStateValueSet;
    }

    /**
     * Create set of new and active state
     */
    private static final Set<State> newAndActiveState =
        Arrays.stream(State.values())
            .filter(state -> (state.equals(State.NEW) || state.equals(State.ACTIVE)))
            .collect(Collectors.toSet());

    /**
     * Create set of new, active and test state
     */
    private static final Set<State> newActiveAndTestState =
        Arrays.stream(State.values())
            .filter(state -> (state.equals(State.NEW) || state.equals(State.ACTIVE)
                || state.equals(State.TEST)))
            .collect(Collectors.toSet());

    /**
     * Gets a set with NEW and ACTIVE state
     *
     * @return Set<State>
     */
    public static Set<State> getNewAndActiveStateFilter() {
      return newAndActiveState;
    }

    /**
     * Gets a set with NEW, ACTIVE and TEST state
     *
     * @return Set<State>
     */
    public static Set<State> getNewActiveAndTestStateFilter() {
      return newActiveAndTestState;
    }
  }
}
